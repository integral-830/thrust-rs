use std::io;
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use thrust_rs::futures::connect::{ConnectFuture, ConnectState};

use self::common::install_test_runtime;
mod common;

#[test]
fn test_connect_future() {
    let reactor = install_test_runtime();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();

    listener.set_nonblocking(true).unwrap();

    let addr = listener.local_addr().unwrap();

    let mut connect_future = ConnectFuture::new(addr).unwrap();

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    assert!(matches!(
        Pin::new(&mut connect_future).poll(&mut cx),
        Poll::Pending
    ));

    let start = Instant::now();

    let client_stream = loop {
        reactor.run_once(Some(100));

        match Pin::new(&mut connect_future).poll(&mut cx) {
            Poll::Ready(Ok(stream)) => {
                break stream;
            }

            Poll::Ready(Err(e)) => {
                panic!("connect failed: {e}");
            }

            Poll::Pending => {
                assert!(start.elapsed() < Duration::from_secs(1), "connect timeout");
            }
        }
    };

    let (server_stream, _) = loop {
        match listener.accept() {
            Ok(v) => break v,

            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }

            Err(e) => panic!("{e}"),
        }
    };

    assert_eq!(
        server_stream.peer_addr().unwrap(),
        client_stream.inner.local_addr().unwrap()
    );
}
#[test]
fn test_connect_refused() {
    let reactor = install_test_runtime();

    let addr = "127.0.0.1:1".parse().unwrap();

    let mut future = ConnectFuture::new(addr).unwrap();

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    assert!(matches!(Pin::new(&mut future).poll(&mut cx), Poll::Pending));

    let start = Instant::now();

    loop {
        reactor.run_once(Some(100));

        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Pending => {
                assert!(start.elapsed() < Duration::from_secs(1), "connect hung");
            }

            Poll::Ready(Ok(_)) => {
                panic!("expected connection refused");
            }

            Poll::Ready(Err(err)) => {
                println!("connect failed as expected: {err}");
                break;
            }
        }
    }
}
fn fd_count() -> usize {
    std::fs::read_dir("/dev/fd").unwrap().count()
}
#[test]
fn test_connect_cancel_no_fd_leak() {
    let reactor = install_test_runtime();
    let addr: SocketAddr = "127.0.0.1:65000".parse().unwrap();

    let before_fds = fd_count();

    let mut future = ConnectFuture::new(addr).unwrap();

    let fd = future.fd;

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    assert!(matches!(Pin::new(&mut future).poll(&mut cx), Poll::Pending));

    assert_eq!(
        reactor.registry.lock().unwrap().len(),
        1,
        "registration not inserted"
    );

    match &future.state {
        ConnectState::Waiting(state) => {
            assert!(
                state.is_registered(),
                "token not stored in RegistrationState"
            );

            assert!(state.get_token().is_some(), "token missing");
        }
        _ => panic!("expected Waiting state"),
    }

    drop(future);

    assert_eq!(
        reactor.registry.lock().unwrap().len(),
        0,
        "registry entry leaked"
    );

    let after_fds = fd_count();

    assert!(
        after_fds <= before_fds + 1,
        "fd leak suspected: before={}, after={}",
        before_fds,
        after_fds
    );

    let close_result = unsafe { libc::fcntl(fd, libc::F_GETFD) };

    assert_eq!(close_result, -1, "fd still appears open");

    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EBADF),
        "fd should be invalid after drop"
    );
}
