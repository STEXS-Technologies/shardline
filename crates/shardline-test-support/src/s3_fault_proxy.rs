use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

/// One-shot provider response fault injected by [`S3FaultProxy`].
#[derive(Clone)]
pub enum S3ProxyFault {
    /// Forward a successful PUT, then close the client connection before its response.
    DropAcceptedPut,
    /// Return one retryable `503 Service Unavailable` without contacting the provider.
    ServiceUnavailableOnce,
    /// Forward successful GET headers, pause, then forward the original body.
    DelaySuccessfulGetBody(Duration),
    /// Replace one successful GET body with same-length stale provider bytes.
    ReplaceSuccessfulGetBody(Arc<[u8]>),
}

/// Provider-backed, one-shot HTTP fault injector in front of an HTTP S3 endpoint.
pub struct S3FaultProxy {
    endpoint: String,
    armed: Arc<AtomicBool>,
    injected: Arc<AtomicBool>,
    stop: Option<mpsc::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl S3FaultProxy {
    /// Starts a fault proxy in front of `upstream_endpoint`.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream is not HTTP or the local listener cannot bind.
    pub fn start(upstream_endpoint: &str, fault: S3ProxyFault) -> std::io::Result<Self> {
        Self::start_with_armed_state(upstream_endpoint, fault, true)
    }

    /// Starts a fault proxy that forwards normally until [`Self::arm`] is called.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream is not HTTP or the local listener cannot bind.
    pub fn start_disarmed(upstream_endpoint: &str, fault: S3ProxyFault) -> std::io::Result<Self> {
        Self::start_with_armed_state(upstream_endpoint, fault, false)
    }

    fn start_with_armed_state(
        upstream_endpoint: &str,
        fault: S3ProxyFault,
        initially_armed: bool,
    ) -> std::io::Result<Self> {
        let upstream = upstream_endpoint
            .strip_prefix("http://")
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "S3 test endpoint must use HTTP",
                )
            })?
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let armed = Arc::new(AtomicBool::new(initially_armed));
        let injected = Arc::new(AtomicBool::new(false));
        let worker_armed = Arc::clone(&armed);
        let worker_injected = Arc::clone(&injected);
        let (stop, stopped) = mpsc::channel();
        let thread = thread::spawn(move || {
            loop {
                if stopped.try_recv().is_ok() {
                    break;
                }
                match listener.accept() {
                    Ok((client, _address)) => {
                        proxy_one_s3_request(
                            client,
                            &upstream,
                            fault.clone(),
                            &worker_armed,
                            &worker_injected,
                        )
                        .ok();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_error) => break,
                }
            }
        });
        Ok(Self {
            endpoint,
            armed,
            injected,
            stop: Some(stop),
            thread: Some(thread),
        })
    }

    /// Returns the proxy endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Arms the configured fault for the next matching request.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    /// Returns whether the configured one-shot fault was injected.
    #[must_use]
    pub fn fault_was_injected(&self) -> bool {
        self.injected.load(Ordering::Acquire)
    }
}

impl Drop for S3FaultProxy {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.send(()).ok();
        }
        if let Some(thread) = self.thread.take() {
            thread.join().ok();
        }
    }
}

fn proxy_one_s3_request(
    mut client: TcpStream,
    upstream: &str,
    fault: S3ProxyFault,
    armed: &AtomicBool,
    injected: &AtomicBool,
) -> std::io::Result<()> {
    client.set_read_timeout(Some(Duration::from_secs(10)))?;
    let request = read_http_request(&mut client)?;
    let is_put = request.starts_with(b"PUT ");
    let is_get = request.starts_with(b"GET ");
    if is_put
        && matches!(fault, S3ProxyFault::ServiceUnavailableOnce)
        && mark_injected(armed, injected)
    {
        client.write_all(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        return Ok(());
    }
    let request = force_connection_close(&request)?;

    let mut provider = TcpStream::connect(upstream)?;
    provider.set_read_timeout(Some(Duration::from_secs(10)))?;
    provider.write_all(&request)?;
    let mut response = Vec::new();
    provider.read_to_end(&mut response)?;
    let accepted = response.starts_with(b"HTTP/1.1 200 ")
        || response.starts_with(b"HTTP/1.1 201 ")
        || response.starts_with(b"HTTP/1.1 206 ")
        || response.starts_with(b"HTTP/1.1 204 ");

    if is_put
        && accepted
        && matches!(fault, S3ProxyFault::DropAcceptedPut)
        && mark_injected(armed, injected)
    {
        client.shutdown(Shutdown::Both)?;
        return Ok(());
    }
    if is_get
        && accepted
        && let S3ProxyFault::DelaySuccessfulGetBody(delay) = fault
        && mark_injected(armed, injected)
    {
        let header_end = response_header_end(&response)?;
        client.write_all(
            response
                .get(..header_end)
                .ok_or_else(invalid_response_offset)?,
        )?;
        thread::sleep(delay);
        return client.write_all(
            response
                .get(header_end..)
                .ok_or_else(invalid_response_offset)?,
        );
    }
    if is_get
        && accepted
        && let S3ProxyFault::ReplaceSuccessfulGetBody(stale_body) = fault
        && mark_injected(armed, injected)
    {
        let header_end = response_header_end(&response)?;
        let provider_body = response
            .get(header_end..)
            .ok_or_else(invalid_response_offset)?;
        if provider_body.len() != stale_body.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stale S3 response body length differs from provider body",
            ));
        }
        client.write_all(
            response
                .get(..header_end)
                .ok_or_else(invalid_response_offset)?,
        )?;
        return client.write_all(&stale_body);
    }
    client.write_all(&response)
}

fn mark_injected(armed: &AtomicBool, injected: &AtomicBool) -> bool {
    armed.load(Ordering::Acquire)
        && injected
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
}

fn response_header_end(response: &[u8]) -> std::io::Result<usize> {
    response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .and_then(|offset| offset.checked_add(4))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing provider response header end",
            )
        })
}

fn invalid_response_offset() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "invalid provider response offset",
    )
}

fn read_http_request(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    const HEADER_LIMIT: usize = 64 * 1024;
    let mut request = Vec::new();
    let mut buffer = [0_u8; 8 * 1024];
    let header_end = loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before HTTP headers completed",
            ));
        }
        request.extend_from_slice(buffer.get(..read).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HTTP read length")
        })?);
        if let Some(offset) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break offset.checked_add(4).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "HTTP header offset overflow",
                )
            })?;
        }
        if request.len() > HEADER_LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP request headers exceed test proxy limit",
            ));
        }
    };

    let headers = std::str::from_utf8(request.get(..header_end).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid HTTP header length",
        )
    })?)
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let content_length = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    });
    let transfer_chunked = headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding")
                && value
                    .split(',')
                    .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        })
    });
    if transfer_chunked {
        while !request
            .get(header_end..)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HTTP body offset")
            })?
            .windows(5)
            .any(|window| window == b"0\r\n\r\n")
        {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "client closed before chunked HTTP body completed",
                ));
            }
            request.extend_from_slice(buffer.get(..read).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HTTP read length")
            })?);
        }
    } else {
        let expected = header_end.saturating_add(content_length.unwrap_or(0));
        while request.len() < expected {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "client closed before HTTP body completed",
                ));
            }
            request.extend_from_slice(buffer.get(..read).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HTTP read length")
            })?);
        }
    }
    Ok(request)
}

fn force_connection_close(request: &[u8]) -> std::io::Result<Vec<u8>> {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .and_then(|offset| offset.checked_add(4))
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing HTTP header end")
        })?;
    let headers = std::str::from_utf8(request.get(..header_end).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid HTTP header length",
        )
    })?)
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut rewritten = String::new();
    for line in headers.trim_end_matches("\r\n").split("\r\n") {
        if line
            .split_once(':')
            .is_some_and(|(name, _value)| name.eq_ignore_ascii_case("connection"))
        {
            continue;
        }
        rewritten.push_str(line);
        rewritten.push_str("\r\n");
    }
    rewritten.push_str("Connection: close\r\n\r\n");
    let mut result = rewritten.into_bytes();
    result.extend_from_slice(
        request
            .get(header_end..)
            .ok_or_else(invalid_response_offset)?,
    );
    Ok(result)
}
