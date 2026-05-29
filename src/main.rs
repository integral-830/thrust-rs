#![allow(unused)]

use std::future::{self, Future};
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use fut::futs::countdown_fut::Countdown;
use fut::futs::immediate_fut::ImmediateReady;
use fut::futs::yield_once::YieldOnce;
use fut::runtime::executor::{self, Executor};
use fut::runtime::raw_waker::waker_for_task;
use fut::runtime::tasks::Task;

struct DummyWaker;

impl Wake for DummyWaker {
    fn wake(self: Arc<Self>) {}
}

fn main() {
    // let (tx, rx) = mpsc::channel::<Arc<Task>>();
    // let mut _fut = Box::pin(ImmediateReady::new(42));

    // let mut yield_one_fut = Box::pin(YieldOnce::default());
    // let mut _countdown_fut = Box::pin(Countdown::new(4));

    // let task = Arc::new(Task {
    //     future: Mutex::new(yield_one_fut),
    //     sender: tx,
    // });

    // let waker = waker_for_task(task.clone());
    // let mut cx = Context::from_waker(&waker);

    // loop {
    //     let poll_result = {
    //         let mut future = task.future.lock().unwrap();
    //         future.as_mut().poll(&mut cx)
    //     };
    //     match poll_result {
    //         Poll::Ready(v) => {
    //             println!("Fut completed.");
    //             break;
    //         }
    //         Poll::Pending => {
    //             println!("Fut is pending...");
    //         }
    //     }
    // }
    let executor = Executor::new();
    executor.spawn(YieldOnce::default());
    executor.run();
}

#[test]
fn yield_once_test() {
    let (tx, rx) = mpsc::channel::<Arc<Task>>();

    let mut yield_one_fut = Box::pin(YieldOnce::default());

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
    let executor = Executor::new();
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
