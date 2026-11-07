//! Handles taken before a supervised restart keep naming the same logical actor
//! afterwards: they upgrade, kill and wait on the incarnation that is running now,
//! not on the one that already stopped (#392).

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use kameo::{error::Infallible, prelude::*, supervision::RestartPolicy};
use tokio::sync::mpsc;

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Started(u32),
    Stopped(u32),
}

struct Supervisor;

impl Actor for Supervisor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

#[derive(Clone)]
struct WorkerArgs {
    starts: Arc<AtomicU32>,
    events: mpsc::UnboundedSender<Event>,
}

struct Worker {
    incarnation: u32,
    events: mpsc::UnboundedSender<Event>,
    /// Taken in `on_start`, the way an actor hands itself to the tasks it spawns.
    me: WeakActorRef<Self>,
}

impl Actor for Worker {
    type Args = WorkerArgs;
    type Error = Infallible;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let incarnation = args.starts.fetch_add(1, Ordering::SeqCst) + 1;
        args.events.send(Event::Started(incarnation)).unwrap();
        Ok(Worker {
            incarnation,
            events: args.events,
            me: actor_ref.downgrade(),
        })
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.events.send(Event::Stopped(self.incarnation)).unwrap();
        Ok(())
    }
}

struct Crash;

impl Message<Crash> for Worker {
    type Reply = ();

    async fn handle(&mut self, _: Crash, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("worker crashed on request");
    }
}

/// Which incarnation handled the request.
struct Which;

impl Message<Which> for Worker {
    type Reply = u32;

    async fn handle(&mut self, _: Which, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.incarnation
    }
}

/// Whether the worker's own `on_start` handle still upgrades.
struct SelfUpgrades;

impl Message<SelfUpgrades> for Worker {
    type Reply = bool;

    async fn handle(&mut self, _: SelfUpgrades, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.me.upgrade().is_some()
    }
}

struct Fixture {
    _supervisor: ActorRef<Supervisor>,
    worker: ActorRef<Worker>,
    events: mpsc::UnboundedReceiver<Event>,
}

async fn spawn_worker() -> Fixture {
    let supervisor = Supervisor::spawn(Supervisor);
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let args = WorkerArgs {
        starts: Arc::new(AtomicU32::new(0)),
        events: events_tx,
    };
    let worker = Worker::supervise_with(&supervisor, move || args.clone())
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(10, Duration::from_secs(60))
        .spawn()
        .await;
    assert_eq!(next(&mut events).await, Event::Started(1));
    Fixture {
        _supervisor: supervisor,
        worker,
        events,
    }
}

async fn next(events: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    match tokio::time::timeout(TIMEOUT, events.recv()).await {
        Ok(Some(event)) => event,
        Ok(None) => panic!("event channel closed"),
        Err(_) => panic!("timed out waiting for a lifecycle event"),
    }
}

/// Crash the first incarnation and wait until the second has started.
async fn restart_once(f: &mut Fixture) {
    f.worker.tell(Crash).await.unwrap();
    assert_eq!(next(&mut f.events).await, Event::Stopped(1));
    assert_eq!(next(&mut f.events).await, Event::Started(2));
    f.worker.wait_for_startup().await;
}

#[tokio::test(start_paused = true)]
async fn old_actor_ref_ask_targets_current_incarnation() {
    let mut f = spawn_worker().await;
    restart_once(&mut f).await;
    assert_eq!(f.worker.ask(Which).await.unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_weak_ref_taken_in_on_start_upgrades_after_a_restart() {
    let mut f = spawn_worker().await;
    assert!(f.worker.ask(SelfUpgrades).await.unwrap());
    restart_once(&mut f).await;
    assert!(
        f.worker.ask(SelfUpgrades).await.unwrap(),
        "the restarted worker's own handle must upgrade while the original ref is alive"
    );
}

#[tokio::test(start_paused = true)]
async fn old_weak_ref_upgrades_to_the_logical_actor() {
    let mut f = spawn_worker().await;
    let weak = f.worker.downgrade();
    restart_once(&mut f).await;
    let upgraded = weak.upgrade().expect("the original ref is alive");
    assert_eq!(upgraded.ask(Which).await.unwrap(), 2);
    assert_eq!(upgraded.strong_count(), f.worker.strong_count());
}

#[tokio::test(start_paused = true)]
async fn old_actor_ref_kill_targets_current_incarnation() {
    let mut f = spawn_worker().await;
    restart_once(&mut f).await;
    f.worker.kill();
    assert_eq!(
        next(&mut f.events).await,
        Event::Stopped(2),
        "kill through the original ref stops the incarnation that is running"
    );
    assert_eq!(next(&mut f.events).await, Event::Started(3));
}

#[tokio::test(start_paused = true)]
async fn old_weak_ref_kill_targets_current_incarnation() {
    let mut f = spawn_worker().await;
    let weak = f.worker.downgrade();
    restart_once(&mut f).await;
    weak.kill();
    assert_eq!(next(&mut f.events).await, Event::Stopped(2));
}

#[tokio::test(start_paused = true)]
async fn old_refs_see_the_restarted_actor_alive() {
    let mut f = spawn_worker().await;
    let weak = f.worker.downgrade();
    restart_once(&mut f).await;
    assert!(f.worker.is_alive());
    assert!(weak.is_alive());
    assert!(f.worker.get_shutdown_result().is_none());
}

#[tokio::test(start_paused = true)]
async fn old_ref_waits_for_the_current_incarnation_to_stop() {
    let mut f = spawn_worker().await;
    restart_once(&mut f).await;
    let wait = tokio::time::timeout(Duration::from_millis(100), f.worker.wait_for_shutdown());
    assert!(
        wait.await.is_err(),
        "the actor is running again, so waiting for its shutdown must not return"
    );
}
