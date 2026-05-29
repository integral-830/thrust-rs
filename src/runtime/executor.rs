use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::runtime::tasks::Task;

use super::raw_waker::waker_for_task;

pub struct Executor {
    pub sender: Sender<Arc<Task>>,
    pub reciever: Receiver<Arc<Task>>,
}

impl Executor {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<Arc<Task>>();
        Self {
            sender: tx,
            reciever: rx,
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
    }

    pub fn run(&self) {
        loop {
            match self.reciever.try_recv() {
                Ok(task) => {
                    let waker = waker_for_task(task.clone());
                    let mut cx = Context::from_waker(&waker);
                    let poll_result = {
                        let mut future = task.future.lock().unwrap();
                        future.as_mut().poll(&mut cx)
                    };
                    match poll_result {
                        Poll::Ready(()) | Poll::Pending => {}
                    }
                }
                Err(_) => break,
            }
        }
    }
}
