mod support;

use std::{env::var, error::Error, path::PathBuf, process::Command};

use shardline_auth::{AuthProvider, Ed25519AuthProvider};
use shardline_protocol::{RepositoryProvider, TokenScope};
use support::CliE2eInvariantError;

#[test]
fn admin_cli_mints_ed25519_token_that_provider_verifies() {
    let result = exercise_admin_cli_mints_ed25519_token_that_provider_verifies();
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "Ed25519 admin-token e2e failed: {error:?}");
}

fn exercise_admin_cli_mints_ed25519_token_that_provider_verifies() -> Result<(), Box<dyn Error>> {
    let seed = [11_u8; 32];
    let key = tempfile::NamedTempFile::new()?;
    std::fs::write(key.path(), seed)?;

    let output = Command::new(shardline_binary()?)
        .args([
            "admin",
            "token",
            "--auth-provider",
            "ed25519",
            "--issuer",
            "local-ed25519",
            "--subject",
            "operator-1",
            "--scope",
            "write",
            "--provider",
            "generic",
            "--owner",
            "team",
            "--repo",
            "assets",
            "--revision",
            "main",
            "--key-file",
            key.path().to_str().ok_or("key path was not valid UTF-8")?,
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "admin token failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let token = String::from_utf8(output.stdout)?.trim().to_owned();
    let provider = Ed25519AuthProvider::new(&seed)?;
    let claims = provider.verify_token(&token)?;
    if claims.issuer() != "local-ed25519"
        || claims.subject() != "operator-1"
        || claims.scope() != TokenScope::Write
        || claims.repository().provider() != RepositoryProvider::Generic
        || claims.repository().owner() != "team"
        || claims.repository().name() != "assets"
        || claims.repository().revision() != Some("main")
    {
        return Err(
            CliE2eInvariantError::new("verified Ed25519 claims did not match CLI input").into(),
        );
    }

    Ok(())
}

fn shardline_binary() -> Result<PathBuf, Box<dyn Error>> {
    if let Ok(path) = var("CARGO_BIN_EXE_shardline") {
        return Ok(PathBuf::from(path));
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let Some(workspace_root) = manifest_dir.ancestors().nth(2) else {
        return Err(CliE2eInvariantError::new("workspace root could not be resolved").into());
    };
    let binary = workspace_root
        .join("target")
        .join("debug")
        .join("shardline");
    if !binary.is_file() {
        return Err(CliE2eInvariantError::new(format!(
            "shardline binary was not built at {}",
            binary.display()
        ))
        .into());
    }
    Ok(binary)
}
