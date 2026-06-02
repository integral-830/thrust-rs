use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::reactor::Reactor;

use super::raw_waker::waker_for_task;
use super::tasks::Task;

pub struct Executor {
    pub sender: Sender<Arc<Task>>,
    pub receiver: Receiver<Arc<Task>>,
    pub reactor: Arc<Reactor>,
    pub task_count: AtomicUsize,
}

impl Executor {
    pub fn new(reactor: Arc<Reactor>) -> Self {
        let (tx, rx) = mpsc::channel::<Arc<Task>>();
        Self {
            sender: tx,
            receiver: rx,
            reactor,
            task_count: AtomicUsize::new(0),
        }
    }

    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
            sender: self.sender.clone(),
        });
        self.sender.send(task).unwrap();
        self.task_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    pub fn run(&self) {
        loop {
            let mut work_done = false;
            while let Ok(task) = self.receiver.try_recv() {
                work_done = true;
                let waker = waker_for_task(task.clone());
                let mut cx = Context::from_waker(&waker);
                let poll_result = {
                    let mut future = task.future.lock().unwrap();
                    future.as_mut().poll(&mut cx)
                };
                match poll_result {
                    Poll::Ready(()) => {
                        self.task_count
                            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                    }
                    Poll::Pending => {}
                }
            }
            if self.task_count.load(std::sync::atomic::Ordering::Acquire) == 0 {
                break;
            }
            if !work_done {
                self.reactor.run_once(Some(10));
            }
        }
    }
}
