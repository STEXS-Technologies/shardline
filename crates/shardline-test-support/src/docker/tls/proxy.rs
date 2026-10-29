//! Loopback streaming HTTPS front door for unmodified native clients.
use super::*;

pub struct HttpsProxyFixture {
    material: TlsMaterial,
    child: std::process::Child,
    origin: String,
}

impl HttpsProxyFixture {
    /// Starts a disposable HTTPS listener. Configure its loopback upstream
    /// before sending requests; dropping the fixture stops all proxy threads.
    ///
    /// # Errors
    /// Returns an error if certificates, Python or the listener are unavailable.
    pub fn start() -> Result<Self, IoError> {
        Self::start_for_hostname("localhost", "127.0.0.1")
    }

    /// Starts a trusted-CA fixture whose certificate does not match its URL.
    /// No DNS override or change to the client's certificate checks is needed.
    ///
    /// # Errors
    /// Returns an error if certificates, Python or the listener are unavailable.
    pub fn start_with_mismatched_hostname() -> Result<Self, IoError> {
        Self::start_for_hostname("shardline-mismatch.invalid", "localhost")
    }

    fn start_for_hostname(server_name: &str, url_host: &str) -> Result<Self, IoError> {
        let material = TlsMaterial::generate_for_server(false, server_name)?;
        let child = Command::new("python3")
            .args(["-c", include_str!("proxy.py")])
            .arg(material.directory.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut fixture = Self {
            material,
            child,
            origin: String::new(),
        };
        let started = Instant::now();
        loop {
            if fixture.child.try_wait()?.is_some() {
                return Err(IoError::other("HTTPS proxy fixture exited"));
            }
            if let Ok(port) = fs::read_to_string(fixture.material.directory.path().join("port")) {
                let port: u16 = port.parse().map_err(IoError::other)?;
                fixture.origin = format!("https://{url_host}:{port}");
                return Ok(fixture);
            }
            if started.elapsed() >= Duration::from_secs(5) {
                return Err(IoError::other("HTTPS proxy fixture startup timed out"));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    #[must_use]
    pub fn ca_cert_path(&self) -> PathBuf {
        self.material.directory.path().join("ca-cert.pem")
    }

    /// # Errors
    /// Rejects non-loopback, IPv6 or zero-port targets and configuration I/O errors.
    pub fn set_upstream(&self, address: std::net::SocketAddr) -> Result<(), IoError> {
        if !address.ip().is_loopback() || !address.is_ipv4() || address.port() == 0 {
            return Err(IoError::other(
                "HTTPS proxy requires a loopback IPv4 upstream",
            ));
        }
        fs::write(
            self.material.directory.path().join("upstream"),
            address.port().to_string(),
        )
    }

    /// # Errors
    /// Returns an error when the successful native-transfer request counter cannot be read.
    pub fn xorb_download_requests(&self) -> Result<usize, IoError> {
        fs::read_to_string(self.material.directory.path().join("xorb-downloads"))?
            .parse()
            .map_err(IoError::other)
    }
}

impl Drop for HttpsProxyFixture {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}
