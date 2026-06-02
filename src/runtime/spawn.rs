use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::{cell::RefCell, sync::mpsc::Sender};

use crate::executor::tasks::Task;

thread_local! {
    static SPAWN_TX: RefCell<Option<Sender<Arc<Task>>>> = const{RefCell::new(None)};
}

thread_local! {
    static TASK_COUNT:
        RefCell<Option<Arc<AtomicUsize>>> =
        const { RefCell::new(None) };
}

pub fn set_spawn_tx(sender: Sender<Arc<Task>>) {
    SPAWN_TX.with(|tx| {
        *tx.borrow_mut() = Some(sender);
    });
}

pub fn set_task_count(task_count: Arc<AtomicUsize>) {
    TASK_COUNT.with(|c| {
        *c.borrow_mut() = Some(task_count);
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

fn with_task_count<T>(f: impl FnOnce(&Arc<AtomicUsize>) -> T) -> T {
    TASK_COUNT.with(|c| {
        let c = c.borrow();

        let c = c.as_ref().expect("No runtime task counter installed");

        f(c)
    })
}

pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    with_task_count(|count| {
        count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    });
    with_spawn_tx(|tx| {
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
            sender: tx.clone(),
        });
        tx.send(task).unwrap();
    })
}

pub fn clear_spawn_tx() {
    SPAWN_TX.with(|tx| *tx.borrow_mut() = None);
}

pub fn clear_task_count() {
    TASK_COUNT.with(|c| {
        *c.borrow_mut() = None;
    });
}
