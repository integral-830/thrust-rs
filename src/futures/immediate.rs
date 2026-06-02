use std::pin::Pin;
use std::task::{Context, Poll};

pub struct ImmediateReady<T: Unpin> {
    value: Option<T>,
}

impl<T: Unpin> ImmediateReady<T> {
    pub fn new(val: T) -> Self {
        Self { value: Some(val) }
    }
}

impl<T: Unpin> Future for ImmediateReady<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.value.take().unwrap())
    }
}
