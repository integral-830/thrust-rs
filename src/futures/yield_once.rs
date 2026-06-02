use std::pin::Pin;
use std::task::{Context, Poll};

pub struct YieldOnce {
    yielded: bool,
}

impl YieldOnce {
    pub fn new() -> Self {
        Self { yielded: false }
    }
}

impl Default for YieldOnce {
    fn default() -> Self {
        Self::new()
    }
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.yielded {
            self.yielded = true;
            let waker = cx.waker().clone();
            waker.wake();
            return Poll::Pending;
        }
        Poll::Ready(())
    }
}
