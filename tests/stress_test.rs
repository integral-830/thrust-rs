use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Instant;

use thrust_rs::executor::Executor;
use thrust_rs::futures::countdown::Countdown;
use thrust_rs::futures::yield_once::YieldOnce;
use thrust_rs::reactor::Reactor;

#[test]
fn stress_10k_each_5_yields() {
    let reactor = Arc::new(Reactor::new().unwrap());
    let executor = Executor::new(reactor);
    let count = Arc::new(AtomicU32::new(0));
    let start = Instant::now();

    for _ in 0..10000 {
        let count = count.clone();
        executor.spawn(async move {
            Countdown::new(5).await;
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    executor.run();
    let elapsed = start.elapsed();
    println!("completed 10,000 tasks in {elapsed:?}");
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 10000);
}

#[test]
fn yield_once_100k() {
    let reactor = Arc::new(Reactor::new().unwrap());
    let executor = Executor::new(reactor);
    let count = Arc::new(AtomicU32::new(0));
    let start = Instant::now();

    for _ in 0..100_000 {
        let count = count.clone();
        executor.spawn(async move {
            YieldOnce::default().await;
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    executor.run();
    let elapsed = start.elapsed();
    println!("completed 10,000 tasks in {elapsed:?}");
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 100_000);
}
#[test]
fn yield_once_1m() {
    let reactor = Arc::new(Reactor::new().unwrap());
    let executor = Executor::new(reactor);
    let count = Arc::new(AtomicU32::new(0));
    let start = Instant::now();

    for _ in 0..1_000_000 {
        let count = count.clone();
        executor.spawn(async move {
            YieldOnce::default().await;
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    executor.run();
    let elapsed = start.elapsed();
    println!("completed 10,000 tasks in {elapsed:?}");
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1_000_000);
}
