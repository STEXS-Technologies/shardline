//! Stock Hub and native Xet through a verified HTTPS Shardline front door.
use super::*;
use shardline_test_support::HttpsProxyFixture;
use std::io::Read;

const CLIENT: &str = r#"
import hashlib,json,sys,time
from huggingface_hub import HfApi,hf_hub_download
r=json.load(sys.stdin)
try:
    api=HfApi(endpoint=r['endpoint'],token=r['token'])
    if r['action'] in ('native_untrusted', 'native_hostname_mismatch'):
        import hf_xet
        hf_xet.upload_files([r['path']],r['endpoint'],
            (r['token'],int(time.time())+120),None,None,None)
    elif r['action'] in ('untrusted', 'hostname_mismatch'):
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
        'certificate_rejected':any(value in diagnostic for value in
            ('certificate_verify_failed','invalid peer certificate','unknownissuer','notvalidforname')),
        'hostname_rejected':any(value in diagnostic for value in
            ('hostname mismatch','ip address mismatch','notvalidforname','not valid for name'))}))
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
    for action in [
        "hostname_mismatch",
        "untrusted",
        "native_hostname_mismatch",
        "native_untrusted",
        "upload",
        "download",
    ] {
        // A separate process/cache also prevents upload-side state or a cached
        // HTTP client's earlier trust configuration from masking the download.
        let home = tempfile::tempdir()?;
        let frontdoor = if action.ends_with("hostname_mismatch") {
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
            // Certificate failures are deterministic; one native attempt is
            // enough and keeps negative qualification independent of backoff.
            .env("HF_XET_CLIENT_RETRY_MAX_ATTEMPTS", "1")
            .env("HF_XET_LOG_DEST", home.path().join("native-tls.log"))
            .env("RUST_LOG", "debug")
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
        if !action.ends_with("untrusted") {
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
        if action.ends_with("untrusted") || action.ends_with("hostname_mismatch") {
            assert_eq!(result["success"], false);
            // Native Xet can put the TLS cause in its Rust diagnostics while
            // exposing only a generic request failure through Python. Inspect
            // known certificate markers without printing URLs or credentials.
            let mut diagnostics = String::from_utf8_lossy(&output.stderr).into_owned();
            let log = home.path().join("native-tls.log");
            if log.exists() {
                std::fs::File::open(log)?
                    .take(1024 * 1024)
                    .read_to_string(&mut diagnostics)?;
            }
            let diagnostics = diagnostics.to_lowercase();
            let certificate_rejected = result["certificate_rejected"] == true
                || [
                    "certificate_verify_failed",
                    "invalid peer certificate",
                    "unknownissuer",
                    "notvalidforname",
                ]
                .iter()
                .any(|marker| diagnostics.contains(marker));
            assert!(
                certificate_rejected,
                "{action}: no certificate rejection evidence"
            );
            if action.ends_with("hostname_mismatch") {
                assert!(
                    result["hostname_rejected"] == true
                        || diagnostics.contains("notvalidforname")
                        || diagnostics.contains("not valid for name"),
                    "{action}: no hostname rejection evidence"
                );
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
