use std::io;
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use thrust_rs::futures::connect::ConnectFuture;
use thrust_rs::reactor::Reactor;

#[test]
fn test_connect_future() {
    let reactor = Arc::new(Reactor::new().unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();

    listener.set_nonblocking(true).unwrap();

    let addr = listener.local_addr().unwrap();

    let mut connect_future = ConnectFuture::new(reactor.clone(), addr).unwrap();

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    match Pin::new(&mut connect_future).poll(&mut cx) {
        Poll::Pending => {}
        Poll::Ready(_) => {
            panic!("expected Pending");
        }
    }

    reactor.run_once(Some(100));

    let client_stream = match Pin::new(&mut connect_future).poll(&mut cx) {
        Poll::Ready(Ok(stream)) => stream,
        Poll::Ready(Err(e)) => {
            panic!("connect failed: {e}");
        }
        Poll::Pending => {
            panic!("still pending");
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
    let reactor = Arc::new(Reactor::new().unwrap());

    let addr = "127.0.0.1:1".parse().unwrap();

    let mut future = ConnectFuture::new(reactor.clone(), addr).unwrap();

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
    let reactor = Arc::new(Reactor::new().unwrap());

    let addr: SocketAddr = "127.0.0.1:65000".parse().unwrap();

    let before = fd_count();

    let mut future = ConnectFuture::new(reactor.clone(), addr).unwrap();

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Pending => {}

        Poll::Ready(_) => {
            panic!("expected pending connect");
        }
    }

    drop(future);

    let after = fd_count();

    assert_eq!(before, after, "fd leak detected");
}
