use std::io;
use std::task::Poll;

use crate::net::tcp_stream::AsyncTcpStream;

pub struct WriteFuture<'a> {
    pub stream: &'a mut AsyncTcpStream,
    pub buffer: &'a [u8],
    pub written: usize,
}

impl<'a> Future for WriteFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        while self.written < self.buffer.len() {
            match self.stream.poll_write(cx, &self.buffer[self.written..]) {
                Poll::Ready(Ok(n)) => {
                    self.written += n;
                    if self.written < self.buffer.len() {
                        return Poll::Ready(Ok(self.written));
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        std::task::Poll::Ready(Ok(self.written))
    }
}
