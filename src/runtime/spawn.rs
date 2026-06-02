use std::sync::{Arc, Mutex};
use std::{cell::RefCell, sync::mpsc::Sender};

use crate::executor::tasks::Task;

thread_local! {
    static SPAWN_TX: RefCell<Option<Sender<Arc<Task>>>> = const{RefCell::new(None)};
}

pub fn set_spawn_tx(sender: Sender<Arc<Task>>) {
    SPAWN_TX.with(|tx| {
        *tx.borrow_mut() = Some(sender);
    });
}

pub fn with_spawn_tx<T>(f: impl FnOnce(&Sender<Arc<Task>>) -> T) -> T {
    SPAWN_TX.with(|tx| {
        let tx = tx.borrow();
        let tx = tx.as_ref().expect(
            "No runtime found\
            Try calling spawn from Runtime::run()",
        );
        f(tx)
    })
}

pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    with_spawn_tx(|tx| {
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
            sender: tx.clone(),
        });
        tx.send(task).unwrap();
    })
}
