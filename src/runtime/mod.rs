pub mod spawn;
pub mod with_reactor;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Waker;
use std::{future::Future, task::Context};

use crate::executor::Executor;
use crate::reactor::Reactor;
use crate::runtime::spawn::clear_task_count;

use self::spawn::{clear_spawn_tx, set_spawn_tx, set_task_count};
use self::with_reactor::{clear_reactor, set_reactor};

pub struct Runtime {
    pub executor: Executor,
    pub reactor: Arc<Reactor>,
}

struct RuntimeContextGuard;

impl Drop for RuntimeContextGuard {
    fn drop(&mut self) {
        clear_reactor();
        clear_spawn_tx();
        clear_task_count();
    }
}

impl Runtime {
    pub fn new() -> io::Result<Self> {
        let reactor = Arc::new(Reactor::new().unwrap());
        let executor = Executor::new(reactor.clone());
        Ok(Self { executor, reactor })
    }

    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.executor.spawn(future);
    }

    pub fn run(&self) {
        set_reactor(self.reactor.clone());
        set_spawn_tx(self.executor.sender.clone());
        set_task_count(self.executor.task_count.clone());
        let _guard = RuntimeContextGuard;
        self.executor.run();
    }

    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        set_reactor(self.reactor.clone());
        set_spawn_tx(self.executor.sender.clone());
        set_task_count(self.executor.task_count.clone());
        let _guard = RuntimeContextGuard;
        let mut future = Box::pin(future);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        loop {
            match future.as_mut().poll(&mut cx) {
                std::task::Poll::Ready(value) => {
                    while self.executor.task_count.load(Ordering::Acquire) > 0 {
                        let work_done = self.executor.run_until_empty();

                        if self.executor.task_count.load(Ordering::Acquire) > 0 && !work_done {
                            self.reactor.run_once(None);
                        }
                    }
                    return value;
                }
                std::task::Poll::Pending => {
                    self.executor.run_until_empty();
                    self.reactor.run_once(None);
                }
            }
        }
    }
}
