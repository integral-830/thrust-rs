pub mod spawn;
pub mod with_reactor;
use std::future::Future;
use std::io;
use std::sync::Arc;

use crate::{executor::executor::Executor, reactor::Reactor, runtime::spawn::set_spawn_tx};

use self::with_reactor::set_reactor;

pub struct Runtime {
    pub executor: Executor,
    pub reactor: Arc<Reactor>,
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
        self.executor.run();
    }
}
