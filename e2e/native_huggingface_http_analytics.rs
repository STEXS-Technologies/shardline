//! Real analytical HTTP readers over both regular and native-Xet Hub files.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_queries_authenticated_regular_and_xet_hub_ranges() -> Result<(), TestError> {
    exercise_analytical_ranges(None, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_queries_authenticated_hub_ranges_split_roles() -> Result<(), TestError> {
    exercise_analytical_ranges(None, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and disk-backed MinIO plus native analytical clients"]
async fn duckdb_queries_authenticated_hub_ranges_with_postgres_and_minio() -> Result<(), TestError>
{
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("remote analytical qualification requires Docker")?;
    assert!(
        services.s3_raw_config(None).is_some(),
        "remote analytical qualification requires S3 objects"
    );
    exercise_analytical_ranges(Some(services), false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and disk-backed MinIO plus native analytical clients"]
async fn duckdb_queries_authenticated_hub_ranges_remote_split_roles() -> Result<(), TestError> {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("remote split-role analytical qualification requires Docker")?;
    assert!(services.s3_raw_config(None).is_some());
    exercise_analytical_ranges(Some(services), true).await
}

async fn exercise_analytical_ranges(
    services: Option<shardline_test_support::DockerLocalStack>,
    split: bool,
) -> Result<(), TestError> {
    let required = services.is_some();
    if !command_available("hf") {
        assert!(!required, "remote analytical qualification requires hf");
        return Ok(());
    }
    if !native_client_policy::require_available("hf_xet", python_package_available("hf_xet")) {
        assert!(!required, "remote analytical qualification requires hf_xet");
        return Ok(());
    }
    let analytics_available = shardline_test_support::native_client_output(
        Command::new("python3").args(["-c", "import duckdb, pyarrow"]),
        std::time::Duration::from_secs(10),
    )
    .is_ok_and(|output| output.status.success());
    if !native_client_policy::require_available("python3 + duckdb + pyarrow", analytics_available) {
        assert!(
            !required,
            "remote analytical qualification requires Python DuckDB and PyArrow"
        );
        return Ok(());
    }
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let front_door = format!("http://{}", listener.local_addr()?);
    let runtime =
        start_runtime_with_roles(&[ServerFrontend::Hub], services, Some(&front_door), split)
            .await?;
    let transfer = runtime
        .transfer
        .as_ref()
        .map_or(runtime.base_url.as_str(), |(url, _)| url.as_str());
    let (_proxy, observations) =
        native_huggingface_proxy::start_observing_ranges(listener, &runtime.base_url, transfer);
    let home = tempfile::tempdir()?;
    let script = r#"
import hashlib, os, pathlib, subprocess, sys, tempfile, time, urllib.request, urllib.error
stage = 'setup'
def safe_failure(kind, error, traceback):
    # Native progress output can leave a carriage-return line unfinished.
    # Start diagnostics on a fresh line without exposing URLs or credentials.
    print('\nHUB_HTTP_FAILED_STAGE=' + stage, flush=True)
    print('HUB_HTTP_FAILED_TYPE=' + kind.__name__, flush=True)
    # Classify known transport failures without printing exception text, which
    # can contain authenticated URLs or SQL credentials.
    message = str(error).lower()
    for category, needles in [
        ('TIMEOUT', ('timeout', 'timed out')),
        ('CONNECTION', ('connection', 'socket')),
        ('BODY', ('body', 'content-length', 'content length', 'unexpected end')),
        ('RANGE', ('range', 'partial')),
        ('PARQUET', ('parquet',)),
        ('HTTP', ('http',)),
    ]:
        if any(needle in message for needle in needles):
            print('HUB_HTTP_FAILED_CATEGORY=' + category, flush=True)
sys.excepthook = safe_failure
import duckdb, pyarrow as pa, pyarrow.parquet as pq
endpoint, read_endpoint, token = os.environ['HF_ENDPOINT'], os.environ['READ_ENDPOINT'], os.environ['HF_TOKEN']
repo = 'team/cli-model'
# Use DuckDB's documented HTTP bearer secret, without replacing its transport.
# https://duckdb.org/docs/stable/core_extensions/httpfs/https.html
connection = duckdb.connect()
# This is wire/integrity qualification with unoptimized Rust verification on
# small CI hosts, not a latency benchmark. Keep a finite client transfer budget
# above the server's bounded admission wait; retain range-byte and query-result
# assertions below.
http_timeout = int(os.environ.get('SHARDLINE_ANALYTICAL_HTTP_TIMEOUT_SECONDS', '120'))
assert 1 <= http_timeout <= 120
connection.execute('SET http_timeout = ' + str(http_timeout))
stage = 'secret'
connection.execute('CREATE SECRET hub_http (TYPE HTTP, BEARER_TOKEN ?, SCOPE ?)', [token, read_endpoint])
with tempfile.TemporaryDirectory() as work:
    for name, rows, native in [('regular.parquet', 4096, False), ('native.parquet', 300000, True)]:
        source = pathlib.Path(work, name)
        table = pa.table({'id': range(rows), 'payload': [hashlib.sha256(str(i).encode()).digest() for i in range(rows)]})
        pq.write_table(table, source, compression='NONE', use_dictionary=False, row_group_size=2048)
        assert (source.stat().st_size > 8 * 1024 * 1024) == native
        if native:
            print('HUB_HTTP_NATIVE_OID=' + hashlib.sha256(source.read_bytes()).hexdigest(), flush=True)
        # Keep the actual hf environment independent of the analytical client's
        # Python environment; neither client needs the other's dependencies.
        stage = 'native_upload' if native else 'regular_upload'
        upload = subprocess.run([os.environ['HF_PYTHON'], '-c', '''
import os, pathlib, sys
from huggingface_hub import HfApi
phase = 'create'
def safe_upload_failure(kind, error, traceback):
    print('HUB_HTTP_FAILED_UPLOAD=' + phase, flush=True)
    print('HUB_HTTP_FAILED_TYPE=' + kind.__name__, flush=True)
    print('HUB_HTTP_FAILED_STATUS=' + str(getattr(getattr(error, 'response', None), 'status_code', 0)), flush=True)
sys.excepthook = safe_upload_failure
api = HfApi(endpoint=os.environ['HF_ENDPOINT'], token=os.environ['HF_TOKEN'])
api.create_repo('team/cli-model', private=True, exist_ok=True)
phase = 'upload'
source = pathlib.Path(sys.argv[1])
api.upload_file(path_or_fileobj=source, path_in_repo=source.name, repo_id='team/cli-model')
''', str(source)], timeout=120, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if upload.returncode:
            for line in upload.stdout.decode(errors='replace').splitlines():
                if line.startswith('HUB_HTTP_FAILED_'):
                    print(line, flush=True)
            raise RuntimeError('native upload did not complete')
        stage = 'native_read' if native else 'regular_read'
        url = read_endpoint + '/' + repo + '/resolve/main/' + name
        stage = 'native_head' if native else 'regular_head'
        head = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token}, method='HEAD')
        with urllib.request.urlopen(head, timeout=30) as response:
            assert bool(response.headers.get('X-Xet-Hash')) == native
            assert int(response.headers['Content-Length']) == source.stat().st_size
            etag = response.headers['ETag']
        stage = 'native_conditions' if native else 'regular_conditions'
        for condition, value, expected in [('If-None-Match', etag, 304), ('If-Match', '"different"', 412)]:
            request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token, condition: value})
            try:
                urllib.request.urlopen(request, timeout=30)
            except urllib.error.HTTPError as error:
                assert error.code == expected
            else:
                raise AssertionError('conditional request unexpectedly returned content')
        request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token, 'Range': 'bytes=1-4096', 'If-Range': etag})
        with urllib.request.urlopen(request, timeout=30) as response:
            assert response.status == 206
            assert response.headers['Content-Range'] == 'bytes 1-4096/' + str(source.stat().st_size)
            assert response.headers['ETag'] == etag
            with source.open('rb') as expected:
                expected.seek(1)
                assert response.read() == expected.read(4096)
        stage = 'native_range_query' if native else 'regular_range_query'
        result = connection.execute('SELECT id, payload FROM read_parquet(?) WHERE id IN (3, 2050) ORDER BY id', [url]).fetchall()
        assert result == [(i, hashlib.sha256(str(i).encode()).digest()) for i in [3, 2050]]
        stage = 'native_aggregate' if native else 'regular_aggregate'
        aggregate_started = time.perf_counter()
        result = connection.execute('SELECT count(*), sum(id) FROM read_parquet(?)', [url]).fetchone()
        assert result == (rows, rows * (rows - 1) // 2)
        print('HUB_HTTP_' + ('NATIVE' if native else 'REGULAR') + '_AGGREGATE_MS=' + str(round((time.perf_counter() - aggregate_started) * 1000)), flush=True)
        try:
            urllib.request.urlopen(url, timeout=30)
        except urllib.error.HTTPError as error:
            assert error.code in (401, 403, 404)
        else:
            raise AssertionError('private analytical source allowed anonymous access')
"#;
    let output = native_output(
        Command::new("python3")
            .args(["-c", script])
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_PYTHON", hf_python())
            .env("READ_ENDPOINT", &front_door)
            .env("HF_TOKEN", &runtime.model_token)
            .env("HF_HOME", home.path())
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("HF_HUB_DISABLE_XET", "0")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    let diagnostic: Vec<_> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| {
            line.starts_with("HUB_HTTP_FAILED_")
                && line.len() < 128
                && line
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'='))
        })
        .map(str::to_owned)
        .collect();
    let mut range_statuses = std::collections::BTreeMap::new();
    for (path, status) in observations
        .lock()
        .map_err(|_| "range observations poisoned")?
        .iter()
    {
        let route = if path.starts_with("/lfs/objects/") {
            "lfs"
        } else {
            "resolve"
        };
        *range_statuses
            .entry((route, status.as_u16()))
            .or_insert(0_u64) += 1;
    }
    assert!(
        output.status.success(),
        "native Hub analytical reader failed with status {} ({diagnostic:?}; range statuses {range_statuses:?})",
        output.status,
    );
    let requests = observations
        .lock()
        .map_err(|_| "range observations poisoned")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        for prefix in [
            "HUB_HTTP_REGULAR_AGGREGATE_MS=",
            "HUB_HTTP_NATIVE_AGGREGATE_MS=",
        ] {
            if let Some(milliseconds) = line.strip_prefix(prefix)
                && !milliseconds.is_empty()
                && milliseconds.bytes().all(|byte| byte.is_ascii_digit())
            {
                eprintln!("{prefix}{milliseconds}");
            }
        }
    }
    let oid = stdout
        .lines()
        .find_map(|line| line.strip_prefix("HUB_HTTP_NATIVE_OID="))
        .filter(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or("native analytical fixture identity was not reported")?;
    for path in [
        "/team/cli-model/resolve/main/regular.parquet".to_owned(),
        format!("/lfs/objects/{oid}"),
    ] {
        assert!(
            requests.iter().any(|(observed, status)| observed == &path
                && *status == reqwest::StatusCode::PARTIAL_CONTENT),
            "analytical reader must receive an actual partial response for {path}"
        );
    }
    Ok(())
}
