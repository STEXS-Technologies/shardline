//! Disposable certificates shared by TLS service fixtures, never production keys.
use super::*;
use std::{process::Stdio, time::Instant};

/// Owned HTTPS document server for authentication integration tests.
/// Certificates and listeners are disposable; dropping the fixture stops its server.
pub struct HttpsDocumentFixture {
    material: TlsMaterial,
    child: std::process::Child,
    origin: String,
}

impl HttpsDocumentFixture {
    /// Serves bounded static test documents. `{{ORIGIN}}` expands to this server.
    ///
    /// # Errors
    /// Returns an error if certificate generation or listener startup fails.
    pub fn start(documents: &[(&str, &str)]) -> Result<Self, IoError> {
        let material = TlsMaterial::generate(false)?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let origin = format!("https://{address}");
        let document_root = material.directory.path().join("documents");
        fs::create_dir(&document_root)?;
        for (path, body) in documents {
            let relative = Path::new(path.trim_start_matches('/'));
            if relative
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
            {
                return Err(IoError::other("invalid HTTPS fixture document path"));
            }
            let target = document_root.join(relative);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(target, body.replace("{{ORIGIN}}", &origin))?;
        }
        drop(listener);
        let child = Command::new("openssl")
            .args([
                "s_server",
                "-quiet",
                "-WWW",
                "-accept",
                &address.to_string(),
            ])
            .arg("-cert")
            .arg(material.directory.path().join("server-cert.pem"))
            .arg("-key")
            .arg(material.directory.path().join("server-key.pem"))
            .current_dir(document_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut fixture = Self {
            material,
            child,
            origin,
        };
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(5) {
            if fixture.child.try_wait()?.is_some() {
                return Err(IoError::other(
                    "HTTPS document fixture exited before readiness",
                ));
            }
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
                return Ok(fixture);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(IoError::other(
            "HTTPS document fixture startup deadline exceeded",
        ))
    }

    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    #[must_use]
    pub fn ca_cert_path(&self) -> PathBuf {
        self.material.directory.path().join("ca-cert.pem")
    }
}

impl Drop for HttpsDocumentFixture {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

#[derive(Debug)]
pub(super) struct TlsMaterial {
    pub(super) directory: tempfile::TempDir,
}

impl TlsMaterial {
    pub(super) fn generate(with_client: bool) -> Result<Self, IoError> {
        let directory = tempfile::tempdir()?;
        let ca_key = directory.path().join("ca-key.pem");
        let ca_cert = directory.path().join("ca-cert.pem");
        run_command_checked(
            Command::new("openssl")
                .args([
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-sha256",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=shardline-test-ca",
                    "-addext",
                    "basicConstraints=critical,CA:TRUE",
                    "-addext",
                    "keyUsage=critical,keyCertSign,cRLSign",
                    "-keyout",
                ])
                .arg(&ca_key)
                .arg("-out")
                .arg(&ca_cert),
            "generate fixture TLS certificate authority",
        )?;
        identity(directory.path(), "server", &ca_key, &ca_cert)?;
        if with_client {
            identity(directory.path(), "client", &ca_key, &ca_cert)?;
        }
        #[cfg(unix)]
        {
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755))?;
            // Container service users need these ephemeral test identities. The
            // CA private key and requests remain inaccessible to those users.
            for name in [
                "ca-cert.pem",
                "server-cert.pem",
                "server-key.pem",
                "client-cert.pem",
                "client-key.pem",
            ] {
                let path = directory.path().join(name);
                if path.exists() {
                    fs::set_permissions(path, fs::Permissions::from_mode(0o644))?;
                }
            }
        }
        Ok(Self { directory })
    }
}

fn identity(directory: &Path, name: &str, ca_key: &Path, ca_cert: &Path) -> Result<(), IoError> {
    let key = directory.join(format!("{name}-key.pem"));
    let request = directory.join(format!("{name}.csr"));
    let certificate = directory.join(format!("{name}-cert.pem"));
    let extensions = directory.join(format!("{name}.ext"));
    let usage = if name == "server" {
        "serverAuth"
    } else {
        "clientAuth"
    };
    fs::write(
        &extensions,
        format!(
            "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage={usage}\n"
        ),
    )?;
    run_command_checked(
        Command::new("openssl")
            .args(["req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj"])
            .arg(format!("/CN=shardline-test-{name}"))
            .arg("-keyout")
            .arg(&key)
            .arg("-out")
            .arg(&request),
        "generate fixture TLS certificate request",
    )?;
    run_command_checked(
        Command::new("openssl")
            .args(["x509", "-req", "-in"])
            .arg(request)
            .arg("-CA")
            .arg(ca_cert)
            .arg("-CAkey")
            .arg(ca_key)
            .args(["-CAcreateserial", "-days", "1", "-sha256", "-extfile"])
            .arg(extensions)
            .arg("-out")
            .arg(certificate),
        "sign fixture TLS certificate",
    )?;
    Ok(())
}
