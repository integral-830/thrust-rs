use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use thrust_rs::executor::executor::Executor;
use thrust_rs::futures::countdown::Countdown;
use thrust_rs::futures::wake_latency::WakeLatencyFuture;
use thrust_rs::futures::yield_once::YieldOnce;

fn bench_spawn_10k(c: &mut Criterion) {
    c.bench_function("spawn_10k", |b| {
        b.iter(|| {
            let executor = Executor::new();
            for _ in 1..10_000 {
                executor.spawn(YieldOnce::default());
            }
            executor
        });
    });
}

fn bench_spawn_100k(c: &mut Criterion) {
    c.bench_function("spawn_100k", |b| {
        b.iter(|| {
            let executor = Executor::new();
            for _ in 1..100_000 {
                executor.spawn(YieldOnce::default());
            }
            executor
        });
    });
}

fn bench_yield_once_10k(c: &mut Criterion) {
    c.bench_function("yield_once_10k", |b| {
        b.iter_batched(
            || {
                let executor = Executor::new();
                for _ in 1..10_000 {
                    executor.spawn(YieldOnce::default());
                }
                executor
            },
            |executor| {
                executor.run();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_yield_once_100k(c: &mut Criterion) {
    c.bench_function("yield_once_100k", |b| {
        b.iter_batched(
            || {
                let executor = Executor::new();
                for _ in 1..100_000 {
                    executor.spawn(YieldOnce::default());
                }
                executor
            },
            |executor| {
                executor.run();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_countdown_5_10k(c: &mut Criterion) {
    c.bench_function("countdown_5_10k", |b| {
        b.iter_batched(
            || {
                let executor = Executor::new();
                for _ in 1..10_000 {
                    executor.spawn(Countdown::new(5));
                }
                executor
            },
            |executor| {
                executor.run();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_countdown_5_100k(c: &mut Criterion) {
    c.bench_function("countdown_5_100k", |b| {
        b.iter_batched(
            || {
                let executor = Executor::new();
                for _ in 1..100_000 {
                    executor.spawn(Countdown::new(5));
                }
                executor
            },
            |executor| {
                executor.run();
            },
            BatchSize::SmallInput,
        );
    });
}
fn bench_countdown_50_10k(c: &mut Criterion) {
    c.bench_function("countdown_50_10k", |b| {
        b.iter_batched(
            || {
                let executor = Executor::new();
                for _ in 1..10_000 {
                    executor.spawn(Countdown::new(50));
                }
                executor
            },
            |executor| {
                executor.run();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_countdown_50_100k(c: &mut Criterion) {
    c.bench_function("countdown_50_100k", |b| {
        b.iter_batched(
            || {
                let executor = Executor::new();
                for _ in 1..100_000 {
                    executor.spawn(Countdown::new(50));
                }
                executor
            },
            |executor| {
                executor.run();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_wake_latency(c: &mut Criterion) {
    c.bench_function("wake_latency", |b| {
        b.iter(|| {
            let latency = Arc::new(Mutex::new(Duration::ZERO));
            let wake_time = Arc::new(Mutex::new(None));
            let latency_clone = latency.clone();
            let wake_time_clone = wake_time.clone();
            let executor = Executor::new();
            executor.spawn(async move {
                let wake_future = WakeLatencyFuture::new(wake_time_clone).await;
                *latency_clone.lock().unwrap() = wake_future;
            });

            executor.run();
            *latency.lock().unwrap()
        });
    });
}

criterion_group!(
    benches,
    bench_spawn_10k,
    bench_spawn_100k,
    bench_yield_once_10k,
    bench_yield_once_100k,
    bench_countdown_5_10k,
    bench_countdown_5_100k,
    bench_countdown_50_10k,
    bench_countdown_50_100k,
    bench_wake_latency
);
criterion_main!(benches);
