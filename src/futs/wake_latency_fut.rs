use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct WakeLatencyFuture {
    wake_time: Arc<Mutex<Option<Instant>>>,
    yielded: bool,
}

impl WakeLatencyFuture {
    pub fn new(wake_time: Arc<Mutex<Option<Instant>>>) -> Self {
        Self {
            wake_time,
            yielded: false,
        }
    }
}

impl Future for WakeLatencyFuture {
    type Output = Duration;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if !self.yielded {
            self.yielded = true;
            *self.wake_time.lock().unwrap() = Some(Instant::now());
            cx.waker().wake_by_ref();
            return std::task::Poll::Pending;
        }
        let wake_time = self.wake_time.lock().unwrap().take().unwrap();
        std::task::Poll::Ready(wake_time.elapsed())
    }
}
