//! Disposable certificates shared by TLS service fixtures, never production keys.
use super::*;

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
