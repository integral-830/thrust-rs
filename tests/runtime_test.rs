use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use thrust_rs::futures::yield_once::YieldOnce;
use thrust_rs::runtime::spawn::spawn;
use thrust_rs::runtime::Runtime;

#[test]
fn runtime_yield_once() {
    let runtime = Runtime::new().unwrap();

    runtime.spawn(async {
        YieldOnce::new().await;
    });

    runtime.run();
}

#[test]
fn runtime_spawn_compiles() {
    let rt = Runtime::new().unwrap();

    rt.spawn(async {});
}

#[test]
#[should_panic(expected = "No runtime found")]
fn with_reactor_panics_outside_runtime() {
    thrust_rs::runtime::with_reactor::with_reactor(|_| {});
}

#[test]
#[should_panic(expected = "No runtime task counter installed")]
fn spawn_panics_outside_runtime() {
    thrust_rs::runtime::spawn::spawn(async {});
}

#[test]
fn block_on_returns_value() {
    let rt = Runtime::new().unwrap();

    let value = rt.block_on(async { 42 });

    assert_eq!(value, 42);
}

#[test]
fn nested_spawn_runs() {
    let rt = Runtime::new().unwrap();

    let value = Arc::new(AtomicUsize::new(0));

    let value2 = value.clone();

    rt.block_on(async move {
        spawn(async move {
            value2.store(42, Ordering::SeqCst);
        });
    });

    assert_eq!(value.load(Ordering::SeqCst), 42);
}
