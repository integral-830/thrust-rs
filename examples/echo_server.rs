use thrust_rs::net::tcp_listener::AsyncTcpListener;
use thrust_rs::net::tcp_stream::AsyncTcpStream;
use thrust_rs::runtime::spawn::spawn;
use thrust_rs::runtime::Runtime;

async fn handle_connection(mut stream: AsyncTcpStream) {
    let mut buf = vec![0u8; 4096];

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => break,

            Ok(n) => {
                if stream.write(&buf[..n]).await.is_err() {
                    break;
                }
            }

            Err(_) => break,
        }
    }
}

async fn server() {
    let mut listener = AsyncTcpListener::bind("127.0.0.1:8080").unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                spawn(handle_connection(stream));
            }

            Err(e) => {
                eprintln!("accept error: {e}");
            }
        }
    }
}

fn main() {
    let rt = Runtime::new().unwrap();

    rt.block_on(server());
}
