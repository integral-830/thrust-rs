use std::mem;
use std::task::{RawWaker, Waker};
use std::{sync::Arc, task::RawWakerVTable};

use super::tasks::Task;

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

unsafe fn clone(ptr: *const ()) -> RawWaker {
    let task = unsafe { Arc::<Task>::from_raw(ptr.cast::<Task>()) };

    let cloned = Arc::clone(&task);
    let _ = Arc::into_raw(task);

    RawWaker::new(Arc::into_raw(cloned).cast::<()>(), &VTABLE)
}

unsafe fn wake(ptr: *const ()) {
    let task = unsafe { Arc::<Task>::from_raw(ptr.cast::<Task>()) };
    task.sender.send(Arc::clone(&task)).ok();
}

unsafe fn wake_by_ref(ptr: *const ()) {
    let task = unsafe { Arc::<Task>::from_raw(ptr.cast::<Task>()) };
    task.sender.send(Arc::clone(&task)).ok();

    mem::forget(task);
}

unsafe fn drop(ptr: *const ()) {
    unsafe { mem::drop(Arc::<Task>::from_raw(ptr.cast::<Task>())) };
}

pub fn waker_for_task(task: Arc<Task>) -> Waker {
    let raw_waker = RawWaker::new(Arc::into_raw(task).cast::<()>(), &VTABLE);
    unsafe { Waker::from_raw(raw_waker) }
}
