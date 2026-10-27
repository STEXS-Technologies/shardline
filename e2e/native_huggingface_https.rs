//! Stock Hub and native Xet through a verified HTTPS Shardline front door.
use super::*;
use shardline_test_support::HttpsProxyFixture;

const CLIENT: &str = r#"
import hashlib,json,sys
from huggingface_hub import HfApi,hf_hub_download
r=json.load(sys.stdin)
try:
    api=HfApi(endpoint=r['endpoint'],token=r['token'])
    if r['action'] in ('untrusted', 'hostname_mismatch'):
        api.whoami()
    elif r['action']=='upload':
        api.create_repo('team/cli-model',private=True,exist_ok=True)
        api.upload_file(path_or_fileobj=r['path'],path_in_repo='tls.bin',repo_id='team/cli-model')
    elif r['action']=='download':
        path=hf_hub_download('team/cli-model','tls.bin',endpoint=r['endpoint'],token=r['token'])
        with open(path,'rb') as source:
            assert hashlib.file_digest(source,'sha256').hexdigest()==r['sha256']
    else:
        raise ValueError('unknown fixture operation')
    print(json.dumps({'success':True}))
except BaseException as error:
    diagnostic = str(error).lower()
    print(json.dumps({'success':False,
        'certificate_rejected':'certificate_verify_failed' in diagnostic,
        'hostname_rejected':'hostname mismatch' in diagnostic or 'ip address mismatch' in diagnostic}))
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires stock hf/hf_xet, Python/OpenSSL and owned PostgreSQL/MinIO"]
async fn native_huggingface_https_frontdoor_preserves_native_transfer() -> Result<(), TestError> {
    assert!(command_available("hf"), "HTTPS qualification requires hf");
    assert!(
        python_package_available("hf_xet"),
        "HTTPS qualification requires hf_xet"
    );
    let proxy = HttpsProxyFixture::start()?;
    let mismatched = HttpsProxyFixture::start_with_mismatched_hostname()?;
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("HTTPS qualification requires Docker")?;
    let runtime = start_runtime_with_public_base(
        &[ServerFrontend::Hub],
        Some(services),
        Some(proxy.origin()),
    )
    .await?;
    proxy.set_upstream(
        runtime
            .base_url
            .strip_prefix("http://")
            .ok_or("expected HTTP upstream")?
            .parse()?,
    )?;
    let source = tempfile::tempdir()?;
    let path = source.path().join("tls.bin");
    let mut state = 0x5f51_375a_7321_9193_u64;
    let mut bytes = Vec::with_capacity(9 * 1024 * 1024);
    for _ in 0..(9 * 1024 * 1024 / 8) {
        state ^= state.wrapping_shl(13);
        state ^= state.wrapping_shr(7);
        state ^= state.wrapping_shl(17);
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    write(&path, &bytes)?;
    let digest = hex::encode(Sha256::digest(&bytes));
    for action in ["hostname_mismatch", "untrusted", "upload", "download"] {
        // A separate process/cache also prevents upload-side state or a cached
        // HTTP client's earlier trust configuration from masking the download.
        let home = tempfile::tempdir()?;
        let frontdoor = if action == "hostname_mismatch" {
            &mismatched
        } else {
            &proxy
        };
        let request = serde_json::to_vec(&serde_json::json!({
            "endpoint": frontdoor.origin(), "token": runtime.model_token,
            "action": action, "path": path, "sha256": digest,
        }))?;
        let mut client = Command::new(hf_python());
        client
            .args(["-c", CLIENT])
            .env("HF_HOME", home.path())
            .env("HF_XET_CACHE", home.path().join("xet"))
            .env("HF_HUB_DISABLE_XET", "0")
            .env("HF_HUB_DISABLE_PROGRESS_BARS", "1")
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost");
        for name in [
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "REQUESTS_CA_BUNDLE",
            "CURL_CA_BUNDLE",
            "HF_TOKEN",
            "HF_ENDPOINT",
        ] {
            client.env_remove(name);
        }
        if action != "untrusted" {
            for name in ["SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"] {
                client.env(name, frontdoor.ca_cert_path());
            }
        }
        let output = shardline_test_support::native_client_output_with_input(
            &mut client,
            std::time::Duration::from_secs(120),
            &request,
        )?;
        assert!(
            output.status.success(),
            "HTTPS client exited unsuccessfully; raw diagnostics withheld"
        );
        let result: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        if action == "untrusted" || action == "hostname_mismatch" {
            assert_eq!(result["success"], false);
            assert_eq!(result["certificate_rejected"], true);
            if action == "hostname_mismatch" {
                assert_eq!(result["hostname_rejected"], true);
            }
        } else {
            assert_eq!(
                result["success"], true,
                "verified HTTPS operation failed; raw diagnostics withheld"
            );
        }
    }
    assert!(read_metric_counter(&runtime, "shardline_xet_xorb_upload_bytes_total").await? > 0);
    assert!(
        proxy.xorb_download_requests()? > 0,
        "stock download must use the HTTPS Xorb path, not a plain Hub fallback"
    );
    Ok(())
}
