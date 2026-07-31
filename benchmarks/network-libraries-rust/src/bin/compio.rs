use std::{
    env,
    io,
    net::{SocketAddr, TcpListener as StdTcpListener},
    os::fd::{FromRawFd, IntoRawFd},
    thread,
};

use compio::{
    io::{AsyncRead, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const REQUEST_BUFFER_SIZE: usize = 16 * 1024;
const RESPONSE_HEADER: &[u8] = b"HTTP/1.1 200 OK\r\nServer: unified-http-bench\r\nContent-Type: text/plain\r\nContent-Length: 512\r\nConnection: keep-alive\r\n\r\n";
const RESPONSE_BODY: [u8; 512] = [b'x'; 512];

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn response() -> Vec<u8> {
    let mut response = Vec::with_capacity(RESPONSE_HEADER.len() + RESPONSE_BODY.len());
    response.extend_from_slice(RESPONSE_HEADER);
    response.extend_from_slice(&RESPONSE_BODY);
    response
}

fn has_header_terminator(buffer: &[u8]) -> bool {
    buffer.windows(4).any(|window| window == b"\r\n\r\n")
}

fn bind_listener(port: u16) -> io::Result<StdTcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&SocketAddr::from(([127, 0, 0, 1], port)).into())?;
    socket.listen(4096)?;
    socket.set_nonblocking(true)?;
    let fd = socket.into_raw_fd();
    Ok(unsafe { StdTcpListener::from_raw_fd(fd) })
}

async fn session(mut stream: TcpStream) -> io::Result<()> {
    let mut read_buffer = Vec::with_capacity(REQUEST_BUFFER_SIZE);
    let mut request = Vec::with_capacity(REQUEST_BUFFER_SIZE);

    loop {
        let result = stream.read(read_buffer).await;
        let read = result.0?;
        read_buffer = result.1;
        if read == 0 {
            return Ok(());
        }

        request.extend_from_slice(&read_buffer[..read]);
        read_buffer.clear();
        if request.len() > REQUEST_BUFFER_SIZE {
            return Ok(());
        }

        if has_header_terminator(&request) {
            let result = stream.write_all(response()).await;
            result.0?;
            request.clear();
        }
    }
}

fn worker(port: u16) -> io::Result<()> {
    compio::runtime::Runtime::new()?.block_on(async move {
        let listener = TcpListener::from_std(bind_listener(port)?)?;
        loop {
            let (stream, _) = listener.accept().await?;
            compio::runtime::spawn(async move {
                let _ = session(stream).await;
            })
            .detach();
        }
    })
}

fn main() {
    let port = env_usize("PORT", 19090) as u16;
    let workers = env_usize("COMPIO_WORKERS", 4);
    let mut threads = Vec::with_capacity(workers);

    for _ in 0..workers {
        threads.push(thread::spawn(move || {
            if let Err(error) = worker(port) {
                eprintln!("compio worker stopped: {error}");
            }
        }));
    }

    for thread in threads {
        let _ = thread.join();
    }
}
