use std::io;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::futures::accept::AcceptFuture;
use crate::net::tcp_stream::AsyncTcpStream;
use crate::reactor;
use crate::reactor::registration::RegistrationState;
use crate::runtime::with_reactor::{try_with_reactor, with_reactor};

pub struct AsyncTcpListener {
    pub inner: TcpListener,
    pub state: Arc<RegistrationState>,
}

impl AsyncTcpListener {
    pub fn bind(addr: &str) -> io::Result<Self> {
        let tcp_listner = TcpListener::bind(addr)?;
        tcp_listner.set_nonblocking(true)?;
        Ok(Self {
            inner: tcp_listner,
            state: RegistrationState::new(),
        })
    }

    pub fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(AsyncTcpStream, SocketAddr)>> {
        match self.inner.accept() {
            Ok((stream, addr)) => {
                let stream = AsyncTcpStream::new(stream).unwrap();
                Poll::Ready(Ok((stream, addr)))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let fd = self.inner.as_raw_fd();
                if !self.state.is_registered() {
                    with_reactor(|reactor| {
                        reactor.register(
                            fd,
                            reactor::Interest::READ,
                            cx.waker().clone(),
                            self.state.clone(),
                        )
                    })?;
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    pub fn accept(&mut self) -> AcceptFuture<'_> {
        AcceptFuture { listener: self }
    }
}

impl Drop for AsyncTcpListener {
    fn drop(&mut self) {
        let fd = self.inner.as_raw_fd();
        if let Some(token) = self.state.get_token() {
            let _ = try_with_reactor(|reactor| {
                reactor.deregister_fd(fd, token);
            });
        }
    }
}
