#![allow(unused)]

use std::future::{self, Future};
use std::pin::Pin;
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use fut::futs::countdown_fut::Countdown;
use fut::futs::immediate_fut::ImmediateReady;
use fut::futs::yield_once::YieldOnce;
use fut::runtime::raw_waker::waker_for_task;
use fut::runtime::tasks::Task;

struct DummyWaker;

impl Wake for DummyWaker {
    fn wake(self: Arc<Self>) {}
}

fn main() {
    let (tx, rx) = mpsc::channel::<Arc<Task>>();
    let mut _fut = Box::pin(ImmediateReady::new(42));

    let mut yield_one_fut = Box::pin(YieldOnce::default());
    let mut _countdown_fut = Box::pin(Countdown::new(4));

    let task = Arc::new(Task {
        future: Mutex::new(yield_one_fut),
        sender: tx,
    });

    let waker = waker_for_task(task.clone());
    let mut cx = Context::from_waker(&waker);

    loop {
        let poll_result = {
            let mut future = task.future.lock().unwrap();
            future.as_mut().poll(&mut cx)
        };
        match poll_result {
            Poll::Ready(v) => {
                println!("Fut completed.");
                break;
            }
            Poll::Pending => {
                println!("Fut is pending...");
            }
        }
    }
}
