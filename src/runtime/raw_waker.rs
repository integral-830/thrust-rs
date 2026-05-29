use std::mem;
use std::task::{RawWaker, Waker};
use std::{sync::Arc, task::RawWakerVTable};

use super::tasks::Task;

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

unsafe fn clone(ptr: *const ()) -> RawWaker {
    let task = unsafe { Arc::<Task>::from_raw(ptr as *const Task) };

    let cloned = Arc::clone(&task);
    let _ = Arc::into_raw(task);

    RawWaker::new(Arc::into_raw(cloned) as *const (), &VTABLE)
}

unsafe fn wake(ptr: *const ()) {
    let task = unsafe { Arc::<Task>::from_raw(ptr as *const Task) };
    task.sender.send(Arc::clone(&task)).unwrap();
}

unsafe fn wake_by_ref(ptr: *const ()) {
    let task = unsafe { Arc::<Task>::from_raw(ptr as *const Task) };
    task.sender.send(Arc::clone(&task)).unwrap();

    mem::forget(task);
}

unsafe fn drop(ptr: *const ()) {
    unsafe { mem::drop(Arc::<Task>::from_raw(ptr as *const Task)) };
}

pub fn waker_for_task(task: Arc<Task>) -> Waker {
    let raw_waker = RawWaker::new(Arc::into_raw(task) as *const (), &VTABLE);
    unsafe { Waker::from_raw(raw_waker) }
}
