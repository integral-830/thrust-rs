use thrust_rs::net::tcp_stream::AsyncTcpStream;
use thrust_rs::runtime::Runtime;

async fn client() {
    let addr = "127.0.0.1:8080".parse().unwrap();

    let mut stream = AsyncTcpStream::connect(addr).unwrap().await.unwrap();

    let message = b"hello from thrust-rs";

    stream.write(message).await.unwrap();

    let mut buf = [0u8; 1024];

    let n = stream.read(&mut buf).await.unwrap();

    println!("echoed: {}", String::from_utf8_lossy(&buf[..n]));
}

fn main() {
    let rt = Runtime::new().unwrap();

    rt.block_on(client());
}
