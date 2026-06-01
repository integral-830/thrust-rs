use std::io;
use std::net::TcpListener;
use std::net::{SocketAddr, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::reactor::registration::RegistrationState;
use crate::reactor::{self, Reactor};

pub struct AsyncTcpListener {
    pub inner: TcpListener,
    pub reactor: Arc<Reactor>,
    pub state: Arc<RegistrationState>,
}

impl AsyncTcpListener {
    pub fn bind(addr: &str, reactor: Arc<Reactor>) -> io::Result<Self> {
        let tcp_listner = TcpListener::bind(addr)?;
        tcp_listner.set_nonblocking(true)?;
        Ok(Self {
            inner: tcp_listner,
            reactor,
            state: RegistrationState::new(),
        })
    }

    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        match self.inner.accept() {
            Ok((stream, addr)) => {
                stream.set_nonblocking(true)?;
                Poll::Ready(Ok((stream, addr)))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let fd = self.inner.as_raw_fd();
                if !self.state.is_registered() {
                    self.reactor.register(
                        fd,
                        reactor::Interest::READ,
                        cx.waker().clone(),
                        self.state.clone(),
                    )?;
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl Drop for AsyncTcpListener {
    fn drop(&mut self) {
        let fd = self.inner.as_raw_fd();
        if let Some(token) = self.state.get_token() {
            self.reactor.deregister_fd(fd, token);
        }
    }
}
