//! Loopback-only HTTPS POST receiver with disposable certificates.
use super::*;

pub struct HttpsWebhookFixture {
    material: TlsMaterial,
    child: std::process::Child,
    address: std::net::SocketAddr,
}

impl HttpsWebhookFixture {
    /// Starts an isolated receiver. Callers must explicitly pin its test hostname
    /// to `address()`; no public server receives fixture requests.
    ///
    /// # Errors
    /// Returns an error if certificate generation or receiver startup fails.
    pub fn start() -> Result<Self, IoError> {
        let material = TlsMaterial::generate_for_server(false, "example.com")?;
        let child = Command::new("python3")
            .args([
                "-c",
                r#"
import http.server, json, pathlib, ssl, sys
root = pathlib.Path(sys.argv[1])
class Receiver(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        size = int(self.headers.get('Content-Length', '0'))
        if size > 65536:
            self.send_error(413)
            return
        body = self.rfile.read(size)
        (root / 'received.json').write_text(json.dumps({
            'body': body.decode(), 'signature': self.headers.get('X-Hub-Signature-256'),
            'content_type': self.headers.get('Content-Type')}))
        self.send_response(204)
        self.end_headers()
server = http.server.HTTPServer(('127.0.0.1', 0), Receiver)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(root / 'server-cert.pem', root / 'server-key.pem')
server.socket = context.wrap_socket(server.socket, server_side=True)
(root / 'port').write_text(str(server.server_port))
server.serve_forever()
"#,
            ])
            .arg(material.directory.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut fixture = Self {
            material,
            child,
            address: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        };
        let started = Instant::now();
        loop {
            if fixture.child.try_wait()?.is_some() {
                return Err(IoError::other("HTTPS webhook fixture exited"));
            }
            if let Ok(port) = fs::read_to_string(fixture.material.directory.path().join("port")) {
                fixture
                    .address
                    .set_port(port.parse().map_err(IoError::other)?);
                return Ok(fixture);
            }
            if started.elapsed() >= Duration::from_secs(5) {
                return Err(IoError::other("HTTPS webhook fixture startup timed out"));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[must_use]
    pub const fn address(&self) -> std::net::SocketAddr {
        self.address
    }

    /// Reads the isolated receiver's trust anchor.
    ///
    /// # Errors
    /// Returns an error if the fixture certificate cannot be read.
    pub fn ca_certificate(&self) -> Result<Vec<u8>, IoError> {
        fs::read(self.material.directory.path().join("ca-cert.pem"))
    }

    /// Reads the last accepted request; absent before any successful TLS POST.
    ///
    /// # Errors
    /// Returns an error if no request exists or its record cannot be read.
    pub fn received(&self) -> Result<String, IoError> {
        fs::read_to_string(self.material.directory.path().join("received.json"))
    }
}

impl Drop for HttpsWebhookFixture {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}
