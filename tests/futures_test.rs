use std::sync::atomic::AtomicU32;
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll};

use thrust_rs::executor::executor::Executor;
use thrust_rs::executor::raw_waker::waker_for_task;
use thrust_rs::executor::tasks::Task;
use thrust_rs::futures::yield_once::YieldOnce;
use thrust_rs::reactor::Reactor;

#[test]
fn yield_once_test() {
    let (tx, rx) = mpsc::channel::<Arc<Task>>();

    let yield_one_fut = Box::pin(YieldOnce::default());

    let task = Arc::new(Task {
        future: Mutex::new(yield_one_fut),
        sender: tx,
    });

    assert_eq!(Arc::strong_count(&task), 1);

    let waker = waker_for_task(task.clone());
    assert_eq!(Arc::strong_count(&task), 2);
    let mut cx = Context::from_waker(&waker);

    {
        let mut future = task.future.lock().unwrap();
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
    }
    assert!(rx.try_recv().is_ok());
    {
        let mut future = task.future.lock().unwrap();
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(()));
    }
    drop(waker);
    assert_eq!(Arc::strong_count(&task), 1);
}

#[test]
fn yield_once_thousand() {
    let reactor = Arc::new(Reactor::new().unwrap());
    let executor = Executor::new(reactor);
    let count = Arc::new(AtomicU32::new(0));

    for _ in 0..1000 {
        let count = count.clone();
        executor.spawn(async move {
            YieldOnce::default().await;
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    executor.run();
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1000);
}
