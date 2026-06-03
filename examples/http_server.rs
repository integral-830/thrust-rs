use thrust_rs::net::tcp_listener::AsyncTcpListener;
use thrust_rs::net::tcp_stream::AsyncTcpStream;
use thrust_rs::runtime::spawn::spawn;
use thrust_rs::runtime::Runtime;

async fn handle_connection(mut stream: AsyncTcpStream) {
    let response = b"HTTP/1.1 200 OK\r\n\
Content-Length: 13\r\n\
Connection: keep-alive\r\n\
\r\n\
Hello, World!";

    let mut buf = [0u8; 4096];

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => break,

            Ok(_) => {
                if stream.write(response).await.is_err() {
                    break;
                }
            }

            Err(_) => break,
        }
    }
}
async fn run() {
    let mut listener = AsyncTcpListener::bind("0.0.0.0:8080").unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
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

    rt.spawn(run());

    rt.run();
}
