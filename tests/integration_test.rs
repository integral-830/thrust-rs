use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use thrust_rs::net::tcp_listener::AsyncTcpListener;
use thrust_rs::net::tcp_stream::AsyncTcpStream;
use thrust_rs::runtime::Runtime;

fn count_fds() -> usize {
    std::fs::read_dir("/dev/fd").unwrap().count()
}

#[test]
fn single_echo() {
    let before = count_fds();

    let received = Arc::new(Mutex::new(Vec::<u8>::new()));
    let received_server = received.clone();

    {
        let rt = Runtime::new().unwrap();

        rt.block_on(async move {
            let mut listener = AsyncTcpListener::bind("127.0.0.1:0").unwrap();

            let addr = listener.inner.local_addr().unwrap();

            thrust_rs::runtime::spawn::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();

                let mut buf = [0u8; 5];

                stream.read(&mut buf).await.unwrap();

                received_server.lock().unwrap().extend_from_slice(&buf);

                stream.write(b"hello").await.unwrap();
            });

            let mut client = AsyncTcpStream::connect(addr).unwrap().await.unwrap();

            client.write(b"world").await.unwrap();

            let mut buf = [0u8; 5];

            client.read(&mut buf).await.unwrap();

            assert_eq!(&buf, b"hello");
        });
    }

    thread::sleep(Duration::from_millis(50));

    let after = count_fds();

    assert_eq!(received.lock().unwrap().as_slice(), b"world");

    assert_eq!(
        before, after,
        "fd leak detected: before={}, after={}",
        before, after
    );
}
