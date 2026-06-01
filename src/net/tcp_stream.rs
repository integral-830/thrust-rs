use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::usize;

use crate::reactor::registration::RegistrationState;
use crate::reactor::{self, Reactor};

pub struct AsyncTcpStream {
    pub inner: TcpStream,
    pub reactor: Arc<Reactor>,
    pub read_state: Arc<RegistrationState>,
    pub write_state: Arc<RegistrationState>,
}

impl AsyncTcpStream {
    pub fn connect(addr: &str, reactor: Arc<Reactor>) -> io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr)?;
        tcp_stream.set_nonblocking(true)?;
        Ok(Self {
            inner: tcp_stream,
            reactor,
            read_state: RegistrationState::new(),
            write_state: RegistrationState::new(),
        })
    }

    pub fn poll_read(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match (&self.inner).read(buf) {
            Ok(0) => Poll::Ready(Ok(0)),
            Ok(n) => Poll::Ready(Ok(n)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let fd = self.inner.as_raw_fd();
                if !self.read_state.is_registered() {
                    self.reactor.register(
                        fd,
                        reactor::Interest::READ,
                        cx.waker().clone(),
                        self.read_state.clone(),
                    )?;
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
    pub fn poll_write(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match (&self.inner).write(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let fd = self.inner.as_raw_fd();
                if !self.write_state.is_registered() {
                    self.reactor.register(
                        fd,
                        reactor::Interest::WRITE,
                        cx.waker().clone(),
                        self.write_state.clone(),
                    )?;
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl Drop for AsyncTcpStream {
    fn drop(&mut self) {
        let fd = self.inner.as_raw_fd();

        if let Some(token) = self.read_state.get_token() {
            self.reactor.deregister_fd(fd, token);
        }
        if let Some(token) = self.write_state.get_token() {
            self.reactor.deregister_fd(fd, token);
        }
    }
}
