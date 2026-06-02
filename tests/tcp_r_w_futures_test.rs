use std::io;

use thrust_rs::net::tcp_listener::AsyncTcpListener;
use thrust_rs::net::tcp_stream::AsyncTcpStream;

#[allow(dead_code)]
async fn r_w_future_compile_check(
    listener: &mut AsyncTcpListener,
    stream: &mut AsyncTcpStream,
) -> io::Result<()> {
    let _accepted = listener.accept().await?;

    let mut buf = [0u8; 4];

    let n = stream.read(&mut buf).await?;

    let written = stream.write(b"hi").await?;

    let _: usize = n;
    let _: usize = written;

    Ok(())
}
