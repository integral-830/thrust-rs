use std::io;
use std::net::SocketAddr;

use crate::net::tcp_listener::AsyncTcpListener;
use crate::net::tcp_stream::AsyncTcpStream;

pub struct AcceptFuture<'a> {
    pub listener: &'a mut AsyncTcpListener,
}

impl<'a> Future for AcceptFuture<'a> {
    type Output = io::Result<(AsyncTcpStream, SocketAddr)>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().listener.poll_accept(cx)
    }
}
