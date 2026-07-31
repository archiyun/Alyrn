use std::{env, io, thread};

use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::{ListenerOpts, TcpListener, TcpStream},
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

async fn session(mut stream: TcpStream) -> io::Result<()> {
    let mut read_buffer = Vec::with_capacity(REQUEST_BUFFER_SIZE);
    let mut request = Vec::with_capacity(REQUEST_BUFFER_SIZE);

    loop {
        let (result, buffer) = stream.read(read_buffer).await;
        read_buffer = buffer;
        let read = result?;
        if read == 0 {
            return Ok(());
        }

        request.extend_from_slice(&read_buffer[..read]);
        read_buffer.clear();
        if request.len() > REQUEST_BUFFER_SIZE {
            return Ok(());
        }

        if has_header_terminator(&request) {
            let (result, buffer) = stream.write_all(response()).await;
            result?;
            let _ = buffer;
            request.clear();
        }
    }
}

fn worker(port: u16) -> io::Result<()> {
    monoio::start::<monoio::IoUringDriver, _>(async move {
        let options = ListenerOpts::new()
            .reuse_addr(true)
            .reuse_port(true)
            .backlog(4096);
        let listener = TcpListener::bind_with_config(("127.0.0.1", port), &options)?;

        loop {
            let (stream, _) = listener.accept().await?;
            monoio::spawn(async move {
                let _ = session(stream).await;
            });
        }
    })
}

fn main() {
    let port = env_usize("PORT", 19090) as u16;
    let workers = env_usize("MONOIO_WORKERS", 4);
    let mut threads = Vec::with_capacity(workers);

    for _ in 0..workers {
        threads.push(thread::spawn(move || {
            if let Err(error) = worker(port) {
                eprintln!("monoio worker stopped: {error}");
            }
        }));
    }

    for thread in threads {
        let _ = thread.join();
    }
}
