#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::dbg_macro,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::print_stdout,
    clippy::print_stderr
)]

use std::{
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use reqwest::Client;
use tokio::{sync::Semaphore, task, time::sleep};

struct BenchmarkConfig {
    base_url: String,
    token: Option<String>,
    concurrency: usize,
    duration: Duration,
    warmup: Duration,
    upload_size: usize,
}

struct BenchmarkResult {
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    latencies_ms: Vec<f64>,
    duration: Duration,
    peak_memory_bytes: u64,
}

impl BenchmarkResult {
    fn rps(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        f64::from(u32::try_from(self.total_requests).unwrap_or(u32::MAX)) / secs
    }

    fn error_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 0.0;
        }
        (f64::from(u32::try_from(self.failed_requests).unwrap_or(u32::MAX))
            / f64::from(u32::try_from(self.total_requests).unwrap_or(u32::MAX)))
            * 100.0
    }

    fn percentile(&self, p: f64) -> f64 {
        if self.latencies_ms.is_empty() {
            return 0.0;
        }
        let idx = ((p / 100.0) * f64::from(u32::try_from(self.latencies_ms.len()).unwrap_or(u32::MAX)))
            .ceil() as usize;
        let idx = idx.saturating_sub(1).min(self.latencies_ms.len() - 1);
        self.latencies_ms[idx]
    }

    fn p50(&self) -> f64 {
        self.percentile(50.0)
    }

    fn p95(&self) -> f64 {
        self.percentile(95.0)
    }

    fn p99(&self) -> f64 {
        self.percentile(99.0)
    }

    fn min_latency(&self) -> f64 {
        self.latencies_ms.first().copied().unwrap_or(0.0)
    }

    fn max_latency(&self) -> f64 {
        self.latencies_ms.last().copied().unwrap_or(0.0)
    }
}

struct SharedMetrics {
    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    latencies: Mutex<Vec<f64>>,
}

impl SharedMetrics {
    fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            latencies: Mutex::new(Vec::new()),
        }
    }

    fn record_success(&self, latency_ms: f64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.successful_requests.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut guard) = self.latencies.lock() {
            guard.push(latency_ms);
        }
    }

    fn record_failure(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.failed_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, duration: Duration) -> BenchmarkResult {
        let mut latencies = self
            .latencies
            .lock()
            .map(|mut guard| std::mem::take(&mut *guard))
            .unwrap_or_default();
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let peak_memory = get_peak_memory_bytes();

        BenchmarkResult {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            successful_requests: self.successful_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            latencies_ms: latencies,
            duration,
            peak_memory_bytes: peak_memory,
        }
    }
}

fn get_peak_memory_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmRSS:"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            })
            .unwrap_or(0)
    }
    #[cfg(target_os = "macos")]
    {
        unsafe {
            let mut info: libc::mach_task_basic_info = std::mem::zeroed();
            let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
            let ret = libc::task_info(
                libc::mach_task_self(),
                libc::MACH_TASK_BASIC_INFO,
                &mut info as *mut _ as *mut libc::integer_t,
                &mut count,
            );
            if ret == libc::KERN_SUCCESS {
                info.resident_size
            } else {
                0
            }
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

async fn run_load_loop(
    client: Client,
    url: String,
    token: Option<String>,
    upload_data: Vec<u8>,
    metrics: Arc<SharedMetrics>,
    deadline: Instant,
) {
    loop {
        if Instant::now() >= deadline {
            break;
        }

        let start = Instant::now();
        let result = {
            let mut req = client.post(&url);
            if let Some(ref t) = token {
                req = req.bearer_auth(t);
            }
            req.body(upload_data.clone()).send().await
        };
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(resp) if resp.status().is_success() => {
                metrics.record_success(elapsed);
            }
            Ok(resp) => {
                let _status = resp.status();
                metrics.record_failure();
            }
            Err(_) => {
                metrics.record_failure();
            }
        }
    }
}

async fn run_download_loop(
    client: Client,
    url: String,
    token: Option<String>,
    metrics: Arc<SharedMetrics>,
    deadline: Instant,
) {
    loop {
        if Instant::now() >= deadline {
            break;
        }

        let start = Instant::now();
        let result = {
            let mut req = client.get(&url);
            if let Some(ref t) = token {
                req = req.bearer_auth(t);
            }
            req.send().await
        };
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(resp) if resp.status().is_success() => {
                let _ = resp.bytes().await;
                metrics.record_success(elapsed);
            }
            Ok(_) => {
                metrics.record_failure();
            }
            Err(_) => {
                metrics.record_failure();
            }
        }
    }
}

fn parse_config() -> BenchmarkConfig {
    let base_url = std::env::var("BENCH_URL").unwrap_or_else(|_| "http://127.0.0.1:18080".to_owned());
    let token = std::env::var("BENCH_TOKEN").ok();
    let concurrency: usize = std::env::var("BENCH_CONCURRENCY")
        .unwrap_or_else(|_| "10".to_owned())
        .parse()
        .expect("BENCH_CONCURRENCY must be a positive integer");
    let duration_secs: u64 = std::env::var("BENCH_DURATION")
        .unwrap_or_else(|_| "30".to_owned())
        .parse()
        .expect("BENCH_DURATION must be a positive integer");
    let warmup_secs: u64 = std::env::var("BENCH_WARMUP")
        .unwrap_or_else(|_| "5".to_owned())
        .parse()
        .expect("BENCH_WARMUP must be a positive integer");
    let upload_size: usize = std::env::var("BENCH_UPLOAD_SIZE")
        .unwrap_or_else(|_| "4096".to_owned())
        .parse()
        .expect("BENCH_UPLOAD_SIZE must be a positive integer");

    BenchmarkConfig {
        base_url,
        token,
        concurrency,
        duration: Duration::from_secs(duration_secs),
        warmup: Duration::from_secs(warmup_secs),
        upload_size,
    }
}

fn print_result(label: &str, result: &BenchmarkResult) {
    eprintln!("--- {label} ---");
    eprintln!("  Duration:        {:.2}s", result.duration.as_secs_f64());
    eprintln!("  Total requests:  {}", result.total_requests);
    eprintln!("  Successful:      {}", result.successful_requests);
    eprintln!("  Failed:          {}", result.failed_requests);
    eprintln!("  RPS:             {:.2}", result.rps());
    eprintln!("  Error rate:      {:.2}%", result.error_rate());
    eprintln!(
        "  Latency min:     {:.2}ms",
        result.min_latency()
    );
    eprintln!("  Latency P50:     {:.2}ms", result.p50());
    eprintln!("  Latency P95:     {:.2}ms", result.p95());
    eprintln!("  Latency P99:     {:.2}ms", result.p99());
    eprintln!(
        "  Latency max:     {:.2}ms",
        result.max_latency()
    );
    if result.peak_memory_bytes > 0 {
        eprintln!(
            "  Peak RSS:        {:.2} MB",
            f64::from(u32::try_from(result.peak_memory_bytes / 1024).unwrap_or(u32::MAX)) / 1024.0
        );
    }
    eprintln!();
}

fn print_json_result(label: &str, result: &BenchmarkResult) {
    let obj = serde_json::json!({
        "label": label,
        "duration_secs": result.duration.as_secs_f64(),
        "total_requests": result.total_requests,
        "successful_requests": result.successful_requests,
        "failed_requests": result.failed_requests,
        "rps": result.rps(),
        "error_rate_pct": result.error_rate(),
        "latency_min_ms": result.min_latency(),
        "latency_p50_ms": result.p50(),
        "latency_p95_ms": result.p95(),
        "latency_p99_ms": result.p99(),
        "latency_max_ms": result.max_latency(),
        "peak_memory_bytes": result.peak_memory_bytes,
    });
    eprintln!("{obj}");
}

#[tokio::main]
async fn main() {
    let config = parse_config();
    let json_output = std::env::var("BENCH_JSON").is_ok();

    let client = Client::builder()
        .pool_max_idle_per_host(config.concurrency)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client should build");

    let upload_url = format!("{}/healthz", config.base_url);
    let download_url = format!("{}/healthz", config.base_url);

    eprintln!("=== Shardline Load Benchmark ===");
    eprintln!("  Base URL:     {}", config.base_url);
    eprintln!("  Concurrency:  {}", config.concurrency);
    eprintln!("  Duration:     {}s", config.duration.as_secs());
    eprintln!("  Warmup:       {}s", config.warmup.as_secs());
    eprintln!("  Upload size:  {} bytes", config.upload_size);
    eprintln!();

    // Wait for server readiness
    eprintln!("Waiting for server...");
    let mut ready = false;
    for attempt in 0..30 {
        let health_url = format!("{}/healthz", config.base_url);
        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                ready = true;
                break;
            }
            _ => {
                if attempt < 29 {
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
    if !ready {
        eprintln!("ERROR: Server not ready after 30s");
        std::process::exit(1);
    }
    eprintln!("Server is ready.\n");

    // --- Warmup ---
    eprintln!("Warming up for {}s...", config.warmup.as_secs());
    {
        let metrics = Arc::new(SharedMetrics::new());
        let semaphore = Arc::new(Semaphore::new(config.concurrency));
        let deadline = Instant::now() + config.warmup;
        let upload_data = vec![0xAB_u8; config.upload_size];

        let mut handles = Vec::with_capacity(config.concurrency);
        for _ in 0..config.concurrency {
            let permit = semaphore.clone().acquire_owned().await.expect("semaphore open");
            let client = client.clone();
            let url = upload_url.clone();
            let token = config.token.clone();
            let data = upload_data.clone();
            let metrics = Arc::clone(&metrics);
            handles.push(task::spawn(async move {
                run_load_loop(client, url, token, data, metrics, deadline).await;
                drop(permit);
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }
    eprintln!("Warmup complete.\n");

    // --- Upload benchmark ---
    eprintln!("Running upload benchmark...");
    let upload_metrics = Arc::new(SharedMetrics::new());
    {
        let semaphore = Arc::new(Semaphore::new(config.concurrency));
        let deadline = Instant::now() + config.duration;
        let upload_data = vec![0xAB_u8; config.upload_size];

        let mut handles = Vec::with_capacity(config.concurrency);
        for _ in 0..config.concurrency {
            let permit = semaphore.clone().acquire_owned().await.expect("semaphore open");
            let client = client.clone();
            let url = upload_url.clone();
            let token = config.token.clone();
            let data = upload_data.clone();
            let metrics = Arc::clone(&upload_metrics);
            handles.push(task::spawn(async move {
                run_load_loop(client, url, token, data, metrics, deadline).await;
                drop(permit);
            }));
        }
        let start = Instant::now();
        for h in handles {
            let _ = h.await;
        }
        let elapsed = start.elapsed();
        let result = upload_metrics.snapshot(elapsed);

        if json_output {
            print_json_result("uploads", &result);
        } else {
            print_result("Uploads", &result);
        }
    }

    // --- Download benchmark ---
    eprintln!("Running download benchmark...");
    let download_metrics = Arc::new(SharedMetrics::new());
    {
        let semaphore = Arc::new(Semaphore::new(config.concurrency));
        let deadline = Instant::now() + config.duration;

        let mut handles = Vec::with_capacity(config.concurrency);
        for _ in 0..config.concurrency {
            let permit = semaphore.clone().acquire_owned().await.expect("semaphore open");
            let client = client.clone();
            let url = download_url.clone();
            let token = config.token.clone();
            let metrics = Arc::clone(&download_metrics);
            handles.push(task::spawn(async move {
                run_download_loop(client, url, token, metrics, deadline).await;
                drop(permit);
            }));
        }
        let start = Instant::now();
        for h in handles {
            let _ = h.await;
        }
        let elapsed = start.elapsed();
        let result = download_metrics.snapshot(elapsed);

        if json_output {
            print_json_result("downloads", &result);
        } else {
            print_result("Downloads", &result);
        }
    }

    // --- Mixed workload (80% reads, 20% writes) ---
    eprintln!("Running mixed workload (80/20 read/write)...");
    let mixed_metrics = Arc::new(SharedMetrics::new());
    {
        let semaphore = Arc::new(Semaphore::new(config.concurrency));
        let deadline = Instant::now() + config.duration;
        let upload_data = vec![0xAB_u8; config.upload_size];

        let mut handles = Vec::with_capacity(config.concurrency);
        for i in 0..config.concurrency {
            let permit = semaphore.clone().acquire_owned().await.expect("semaphore open");
            let client = client.clone();
            let upload_url = upload_url.clone();
            let download_url = download_url.clone();
            let token = config.token.clone();
            let data = upload_data.clone();
            let metrics = Arc::clone(&mixed_metrics);
            let is_reader = (i % 5) != 0; // 80% readers, 20% writers
            handles.push(task::spawn(async move {
                if is_reader {
                    run_download_loop(client, download_url, token, metrics, deadline).await;
                } else {
                    run_load_loop(client, upload_url, token, data, metrics, deadline).await;
                }
                drop(permit);
            }));
        }
        let start = Instant::now();
        for h in handles {
            let _ = h.await;
        }
        let elapsed = start.elapsed();
        let result = mixed_metrics.snapshot(elapsed);

        if json_output {
            print_json_result("mixed_80r_20w", &result);
        } else {
            print_result("Mixed (80% read / 20% write)", &result);
        }
    }

    eprintln!("Benchmark complete.");
}
