use thrust_rs::futures::yield_once::YieldOnce;
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
#[should_panic(expected = "No runtime found")]
fn spawn_panics_outside_runtime() {
    thrust_rs::runtime::spawn::spawn(async {});
}
