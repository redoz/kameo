//! A supervisor that stops for good frees its children's restart specs, and with them
//! whatever the children's args factories captured.
//!
//! The natural way for a child to reach its supervisor is a `WeakActorRef` captured in its
//! args factory. That ref holds the supervisor's links, and the supervisor's links hold the
//! child's spec, which owns the factory: a cycle, broken only if the spec goes away.

use std::{
    any::Any,
    sync::{Arc, Weak},
    time::Duration,
};

use kameo::{error::Infallible, prelude::*};

struct Supervisor;

impl Actor for Supervisor {
    type Args = ();
    type Error = Infallible;

    async fn on_start(_: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Supervisor)
    }
}

/// A supervisor whose `on_start` spawns a child and then fails.
struct FailingSupervisor;

impl Actor for FailingSupervisor {
    type Args = Arc<()>;
    type Error = &'static str;

    async fn on_start(marker: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let parent = actor_ref.downgrade();
        Child::supervise_with(&actor_ref, move || {
            Holds::new((marker.clone(), parent.clone()))
        })
        .spawn()
        .await;
        Err("startup failed after spawning a child")
    }
}

/// What a child's args factory captured: here, a drop marker and its parent's weak ref.
#[derive(Clone)]
struct Holds(#[allow(dead_code)] Arc<dyn Any + Send + Sync>);

impl Holds {
    fn new(held: impl Any + Send + Sync) -> Self {
        Holds(Arc::new(held))
    }
}

struct Child;

impl Actor for Child {
    type Args = Holds;
    type Error = Infallible;

    async fn on_start(_: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Child)
    }
}

/// Lets the stopped actors' tasks finish dropping their state.
async fn settle(marker: &Weak<()>) -> usize {
    for _ in 0..100 {
        if marker.strong_count() == 0 {
            return 0;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    marker.strong_count()
}

#[tokio::test(start_paused = true)]
async fn a_stopped_supervisor_frees_what_its_childrens_args_factories_hold() {
    let marker = Arc::new(());
    let held = Arc::downgrade(&marker);
    let supervisor = Supervisor::spawn(());
    let parent = supervisor.downgrade();
    let child = Child::supervise_with(&supervisor, move || {
        Holds::new((marker.clone(), parent.clone()))
    })
    .spawn()
    .await;

    supervisor.kill();
    supervisor.wait_for_shutdown().await;
    child.wait_for_shutdown().await;
    drop((supervisor, child));

    assert_eq!(
        settle(&held).await,
        0,
        "the child's args factory outlived its stopped supervisor"
    );
}

#[tokio::test(start_paused = true)]
async fn a_supervisor_whose_startup_fails_frees_its_childrens_args_factories() {
    let marker = Arc::new(());
    let held = Arc::downgrade(&marker);
    let supervisor = FailingSupervisor::spawn(marker);
    assert!(supervisor.wait_for_startup_result().await.is_err());
    supervisor.wait_for_shutdown().await;
    drop(supervisor);

    assert_eq!(
        settle(&held).await,
        0,
        "the child's args factory outlived a supervisor whose startup failed"
    );
}
