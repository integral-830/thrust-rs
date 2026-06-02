use std::ffi::c_int;
use std::net::{SocketAddr, TcpStream};
use std::os::fd::FromRawFd;
use std::{io, mem};
use std::{os::fd::RawFd, sync::Arc};

use libc::{
    connect, fcntl, getsockopt, socklen_t, AF_INET, EINPROGRESS, F_GETFL, F_SETFL, O_NONBLOCK,
    SOCK_STREAM, SOL_SOCKET, SO_ERROR,
};

use crate::net::tcp_stream::AsyncTcpStream;
use crate::reactor;
use crate::reactor::{registration::RegistrationState, Reactor};

pub enum ConnectState {
    Connecting,
    Waiting(Arc<RegistrationState>),
    Done,
}

pub struct ConnectFuture {
    fd: RawFd,
    reactor: Arc<Reactor>,
    state: ConnectState,
}

impl ConnectFuture {
    pub fn new(reactor: Arc<Reactor>, addr: SocketAddr) -> io::Result<Self> {
        let sock_fd = unsafe { libc::socket(AF_INET, SOCK_STREAM, 0) };

        if sock_fd == -1 {
            return Err(io::Error::last_os_error());
        }

        let flags = unsafe { fcntl(sock_fd, F_GETFL) };

        if flags == -1 {
            unsafe { libc::close(sock_fd) };
            return Err(io::Error::last_os_error());
        }

        if unsafe { fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK) } == -1 {
            unsafe { libc::close(sock_fd) };
            return Err(io::Error::last_os_error());
        }

        let sockaddr = match addr {
            SocketAddr::V4(socket_addr_v4) => libc::sockaddr_in {
                sin_len: mem::size_of::<libc::sockaddr_in>() as u8,
                sin_family: AF_INET as u8,
                sin_port: addr.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from_be_bytes(socket_addr_v4.ip().octets()).to_be(),
                },
                sin_zero: [0; 8],
            },
            SocketAddr::V6(_) => {
                unsafe { libc::close(sock_fd) };
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "IPv6 is not implemented yet...",
                ));
            }
        };

        let conn_resp = unsafe {
            connect(
                sock_fd,
                &sockaddr as *const _ as *const libc::sockaddr,
                mem::size_of::<libc::sockaddr_in>() as socklen_t,
            )
        };

        if conn_resp == 0 {
            return Ok(Self {
                fd: sock_fd,
                reactor,
                state: ConnectState::Done,
            });
        }

        let err = io::Error::last_os_error();

        if err.raw_os_error() == Some(EINPROGRESS) {
            return Ok(Self {
                fd: sock_fd,
                reactor,
                state: ConnectState::Connecting,
            });
        }

        unsafe { libc::close(sock_fd) };
        Err(err)
    }
}

impl Future for ConnectFuture {
    type Output = io::Result<AsyncTcpStream>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match &mut self.state {
            ConnectState::Connecting => {
                let state = RegistrationState::new();
                if let Err(e) = self.reactor.register(
                    self.fd,
                    reactor::Interest::WRITE,
                    cx.waker().clone(),
                    state.clone(),
                ) {
                    unsafe {
                        libc::close(self.fd);
                    }
                    return std::task::Poll::Ready(Err(e));
                }

                self.state = ConnectState::Waiting(state);
                std::task::Poll::Pending
            }
            ConnectState::Waiting(registration_state) => {
                if registration_state.is_registered() {
                    return std::task::Poll::Pending;
                }
                let mut so_error: c_int = 0;
                println!(
                    "registered={}, so_error={}",
                    registration_state.is_registered(),
                    so_error
                );
                let mut len = mem::size_of::<c_int>() as socklen_t;

                let sock_op_r = unsafe {
                    getsockopt(
                        self.fd,
                        SOL_SOCKET,
                        SO_ERROR,
                        &mut so_error as *mut _ as *mut _,
                        &mut len,
                    )
                };

                self.state = ConnectState::Done;

                if sock_op_r == -1 {
                    let err = io::Error::last_os_error();
                    unsafe {
                        libc::close(self.fd);
                    }
                    self.state = ConnectState::Done;
                    return std::task::Poll::Ready(Err(err));
                }
                if so_error != 0 {
                    unsafe {
                        libc::close(self.fd);
                    }
                    self.state = ConnectState::Done;
                    return std::task::Poll::Ready(Err(io::Error::from_raw_os_error(so_error)));
                }

                let stream = unsafe { TcpStream::from_raw_fd(self.fd) };

                let async_stream = AsyncTcpStream::new(stream, self.reactor.clone());
                self.state = ConnectState::Done;
                std::task::Poll::Ready(async_stream)
            }
            ConnectState::Done => {
                panic!("Connect can't be polled after completion...")
            }
        }
    }
}

impl Drop for ConnectFuture {
    fn drop(&mut self) {
        match &self.state {
            ConnectState::Connecting => unsafe {
                libc::close(self.fd);
            },
            ConnectState::Waiting(registration_state) => {
                if let Some(token) = registration_state.get_token() {
                    self.reactor.deregister_fd(self.fd, token);
                }
                unsafe {
                    libc::close(self.fd);
                }
            }
            ConnectState::Done => {}
        }
    }
}
