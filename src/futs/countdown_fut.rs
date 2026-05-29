use std::pin::Pin;
use std::task::{Context, Poll};

pub struct Countdown {
    value: Option<u32>,
}

impl Countdown {
    pub fn new(val: u32) -> Self {
        Self { value: Some(val) }
    }
}

impl Future for Countdown {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let value = self.value.as_mut().unwrap();
        if *value != 0 {
            println!("{}", *value);
            *value -= 1;
            let waker = cx.waker().clone();
            waker.wake();
            return Poll::Pending;
        }
        self.value.take();
        Poll::Ready(())
    }
}
