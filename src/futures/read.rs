use std::io;

use crate::net::tcp_stream::AsyncTcpStream;

pub struct ReadFuture<'a> {
    pub stream: &'a mut AsyncTcpStream,
    pub buffer: &'a mut [u8],
}

impl<'a> Future for ReadFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        this.stream.poll_read(cx, this.buffer)
    }
}
