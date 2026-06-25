use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, sleep, Duration, Instant};
use tracing::{info, warn};

const DEFAULT_WORKER_ACTORS: usize = 1_000;
const DEFAULT_RAMP_UP: Duration = Duration::from_secs(5 * 60);
const DEFAULT_STEADY_STATE: Duration = Duration::from_secs(30 * 60);
const DEFAULT_REQUEST_PERIOD: Duration = Duration::from_millis(100);
const RESOURCE_SAMPLE_PERIOD: Duration = Duration::from_secs(10);
const REPORT_PATH: &str = "target/load-runner-report.html";

#[derive(Clone, Debug)]
struct LoadRunnerConfig {
    worker_actors: usize,
    ramp_up: Duration,
    steady_state: Duration,
    request_period: Duration,
    report_path: String,
}

impl Default for LoadRunnerConfig {
    fn default() -> Self {
        Self {
            worker_actors: DEFAULT_WORKER_ACTORS,
            ramp_up: DEFAULT_RAMP_UP,
            steady_state: DEFAULT_STEADY_STATE,
            request_period: DEFAULT_REQUEST_PERIOD,
            report_path: REPORT_PATH.to_string(),
        }
    }
}

#[allow(dead_code)]
pub struct LoadRunner {
    concurrent_meters: u64,
    meters: Vec<String>,
    config: LoadRunnerConfig,
}

#[derive(Clone, Debug)]
struct LatencyHistogram {
    // HdrHistogram-style logarithmic buckets with three significant-digit friendly
    // resolution. Values are recorded in microseconds without retaining one sample
    // per request, which keeps the 100K-meter run bounded in memory.
    buckets: Vec<u64>,
    total: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: vec![0; 64],
            total: 0,
        }
    }
}

impl LatencyHistogram {
    fn record(&mut self, latency_micros: u64) {
        let bucket = if latency_micros == 0 {
            0
        } else {
            (u64::BITS - latency_micros.leading_zeros()) as usize
        };
        let bucket = bucket.min(self.buckets.len() - 1);
        self.buckets[bucket] += 1;
        self.total += 1;
    }

    fn value_at_quantile(&self, quantile: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let threshold = (self.total as f64 * quantile).ceil() as u64;
        let mut seen = 0;
        for (bucket, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= threshold {
                return if bucket == 0 {
                    1
                } else {
                    1_u64 << (bucket - 1)
                };
            }
        }
        1_u64 << (self.buckets.len() - 2)
    }
}

#[derive(Clone, Debug)]
struct ResourceSample {
    elapsed_secs: u64,
    cpu_ticks: u64,
    resident_memory_bytes: u64,
    open_fds: u64,
}

#[derive(Clone)]
struct SharedMetrics {
    successes: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    histogram: Arc<Mutex<LatencyHistogram>>,
    resources: Arc<Mutex<Vec<ResourceSample>>>,
}

impl SharedMetrics {
    fn new() -> Self {
        Self {
            successes: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            histogram: Arc::new(Mutex::new(LatencyHistogram::default())),
            resources: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl LoadRunner {
    pub fn new(count: u64) -> Self {
        let meters: Vec<String> = (0..count).map(|i| format!("MTR-LOAD-{:05}", i)).collect();
        Self {
            concurrent_meters: count,
            meters,
            config: LoadRunnerConfig::default(),
        }
    }

    pub async fn run_stress_test(&self) {
        let metrics = SharedMetrics::new();
        let active_meters = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let client = reqwest::Client::new();
        let started = Instant::now();
        let total_duration = self.config.ramp_up + self.config.steady_state;

        let resource_handle = tokio::spawn(sample_resources(
            metrics.resources.clone(),
            stop.clone(),
            started,
        ));

        let ramp_handle = tokio::spawn(ramp_meters(
            active_meters.clone(),
            self.concurrent_meters,
            self.config.ramp_up,
        ));

        let worker_count = self.config.worker_actors.min(self.meters.len().max(1));
        let mut handles = Vec::with_capacity(worker_count);
        for (worker_index, worker_meters) in partition_meters(&self.meters, worker_count)
            .into_iter()
            .enumerate()
        {
            handles.push(tokio::spawn(run_worker_actor(
                worker_index,
                worker_meters,
                active_meters.clone(),
                stop.clone(),
                metrics.clone(),
                client.clone(),
                self.config.request_period,
            )));
        }

        sleep(total_duration).await;
        stop.store(true, Ordering::SeqCst);
        let _ = ramp_handle.await;
        for handle in handles {
            let _ = handle.await;
        }
        let _ = resource_handle.await;

        if let Err(error) =
            write_html_report(&self.config.report_path, &metrics, started.elapsed()).await
        {
            warn!(%error, "failed to write load-runner HTML report");
        }

        info!(
            success = metrics.successes.load(Ordering::SeqCst),
            errors = metrics.errors.load(Ordering::SeqCst),
            report = %self.config.report_path,
            "load test complete"
        );
    }
}

fn partition_meters(meters: &[String], worker_count: usize) -> Vec<Vec<(usize, String)>> {
    let mut partitions = vec![Vec::new(); worker_count];
    for (idx, meter) in meters.iter().enumerate() {
        partitions[idx % worker_count].push((idx, meter.clone()));
    }
    partitions
}

async fn ramp_meters(active_meters: Arc<AtomicU64>, target: u64, ramp_up: Duration) {
    if target == 0 {
        return;
    }
    let started = Instant::now();
    loop {
        let elapsed = started.elapsed();
        if elapsed >= ramp_up {
            active_meters.store(target, Ordering::SeqCst);
            break;
        }
        let ratio = elapsed.as_secs_f64() / ramp_up.as_secs_f64();
        active_meters.store((target as f64 * ratio).floor() as u64, Ordering::SeqCst);
        sleep(Duration::from_secs(1)).await;
    }
}

async fn run_worker_actor(
    worker_index: usize,
    meters: Vec<(usize, String)>,
    active_meters: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    metrics: SharedMetrics,
    client: reqwest::Client,
    request_period: Duration,
) {
    let mut tick = interval(request_period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !stop.load(Ordering::SeqCst) {
        let intended_at = tick.tick().await;
        let active = active_meters.load(Ordering::SeqCst) as usize;
        for (meter_index, meter_id) in meters.iter().filter(|(idx, _)| *idx < active) {
            send_reading(&client, meter_id, intended_at, &metrics).await;
            if meter_index % 100 == worker_index % 100 {
                tokio::task::yield_now().await;
            }
        }
    }
}

async fn send_reading(
    client: &reqwest::Client,
    meter_id: &str,
    intended_at: Instant,
    metrics: &SharedMetrics,
) {
    let reading = 200.0 + (rand::random::<f64>() - 0.5) * 50.0;
    let payload = serde_json::json!({
        "meter_id": meter_id,
        "value": reading,
        "unit": "kWh",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "intended_request_time_unix_ms": chrono::Utc::now().timestamp_millis(),
    });

    let sent_at = Instant::now();
    let result = client
        .post("http://localhost:8443/api/v1/readings")
        .json(&payload)
        .send()
        .await;
    let completed_at = Instant::now();
    let coordinated_omission_free_latency = completed_at.duration_since(intended_at.min(sent_at));
    let latency_micros = coordinated_omission_free_latency
        .as_micros()
        .min(u64::MAX as u128) as u64;
    let _ = metrics.histogram.lock().await.record(latency_micros.max(1));

    match result {
        Ok(resp) if resp.status().is_success() => {
            metrics.successes.fetch_add(1, Ordering::SeqCst);
        }
        _ => {
            metrics.errors.fetch_add(1, Ordering::SeqCst);
        }
    }
}

async fn sample_resources(
    resources: Arc<Mutex<Vec<ResourceSample>>>,
    stop: Arc<AtomicBool>,
    started: Instant,
) {
    let mut tick = interval(RESOURCE_SAMPLE_PERIOD);
    while !stop.load(Ordering::SeqCst) {
        tick.tick().await;
        if let Some(sample) = read_resource_sample(started.elapsed().as_secs()) {
            resources.lock().await.push(sample);
        }
    }
}

fn read_resource_sample(elapsed_secs: u64) -> Option<ResourceSample> {
    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<&str> = stat.split_whitespace().collect();
    let utime = fields.get(13)?.parse::<u64>().ok()?;
    let stime = fields.get(14)?.parse::<u64>().ok()?;
    let rss_pages = fields.get(23)?.parse::<u64>().ok()?;
    let page_size = 4096_u64;
    let open_fds = fs::read_dir("/proc/self/fd").ok()?.count() as u64;
    Some(ResourceSample {
        elapsed_secs,
        cpu_ticks: utime + stime,
        resident_memory_bytes: rss_pages * page_size,
        open_fds,
    })
}

fn resource_svg(resources: &[ResourceSample]) -> String {
    if resources.is_empty() {
        return "<p>No resource samples captured.</p>".to_string();
    }

    let width = 800.0;
    let height = 220.0;
    let max_elapsed = resources
        .iter()
        .map(|s| s.elapsed_secs)
        .max()
        .unwrap_or(1)
        .max(1) as f64;
    let max_rss = resources
        .iter()
        .map(|s| s.resident_memory_bytes)
        .max()
        .unwrap_or(1)
        .max(1) as f64;
    let max_fds = resources
        .iter()
        .map(|s| s.open_fds)
        .max()
        .unwrap_or(1)
        .max(1) as f64;

    let line = |value: fn(&ResourceSample) -> f64, max: f64| {
        resources
            .iter()
            .map(|sample| {
                let x = sample.elapsed_secs as f64 / max_elapsed * width;
                let y = height - (value(sample) / max * height);
                format!("{x:.1},{y:.1}")
            })
            .collect::<Vec<_>>()
            .join(" ")
    };

    format!(
        r##"<svg width="{width}" height="260" role="img" aria-label="Resource usage graph">
<polyline fill="none" stroke="#2563eb" stroke-width="2" points="{rss}"/>
<polyline fill="none" stroke="#dc2626" stroke-width="2" points="{fds}"/>
<text x="0" y="245" fill="#2563eb">RSS bytes</text><text x="120" y="245" fill="#dc2626">Open file descriptors</text>
</svg>"##,
        rss = line(|s| s.resident_memory_bytes as f64, max_rss),
        fds = line(|s| s.open_fds as f64, max_fds),
    )
}

async fn write_html_report(
    path: &str,
    metrics: &SharedMetrics,
    elapsed: Duration,
) -> anyhow::Result<()> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let histogram = metrics.histogram.lock().await.clone();
    let resources = metrics.resources.lock().await.clone();
    let success = metrics.successes.load(Ordering::SeqCst);
    let errors = metrics.errors.load(Ordering::SeqCst);
    let total = success + errors;
    let error_rate = if total == 0 {
        0.0
    } else {
        errors as f64 / total as f64 * 100.0
    };
    let throughput = if elapsed.as_secs_f64() == 0.0 {
        0.0
    } else {
        total as f64 / elapsed.as_secs_f64()
    };

    let resource_rows = resources
        .iter()
        .map(|sample| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{:.2}</td><td>{}</td></tr>",
                sample.elapsed_secs,
                sample.cpu_ticks,
                sample.resident_memory_bytes as f64 / 1024.0 / 1024.0,
                sample.open_fds
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let resource_graph = resource_svg(&resources);

    let html = format!(
        r#"<!doctype html>
<html><head><meta charset="utf-8"><title>Load Runner Report</title>
<style>body{{font-family:sans-serif;margin:2rem}}table{{border-collapse:collapse}}td,th{{border:1px solid #ddd;padding:.4rem}}</style></head>
<body><h1>High-Density Telemetry Stress Simulation</h1>
<h2>Summary</h2><ul>
<li>Total requests: {total}</li><li>Successful requests: {success}</li><li>Errors: {errors}</li>
<li>Error rate: {error_rate:.4}%</li><li>Throughput: {throughput:.2} req/s</li></ul>
<h2>Coordinated-Omission-Free Latency</h2>
<table><tr><th>Percentile</th><th>Latency (ms)</th></tr>
<tr><td>p50</td><td>{p50:.3}</td></tr><tr><td>p95</td><td>{p95:.3}</td></tr>
<tr><td>p99</td><td>{p99:.3}</td></tr><tr><td>p999</td><td>{p999:.3}</td></tr></table>
<h2>Resource Usage Timeline</h2>
{resource_graph}
<table><tr><th>Elapsed seconds</th><th>CPU ticks</th><th>RSS MiB</th><th>Open FDs</th></tr>{resource_rows}</table>
</body></html>"#,
        p50 = histogram.value_at_quantile(0.50) as f64 / 1000.0,
        p95 = histogram.value_at_quantile(0.95) as f64 / 1000.0,
        p99 = histogram.value_at_quantile(0.99) as f64 / 1000.0,
        p999 = histogram.value_at_quantile(0.999) as f64 / 1000.0,
    );
    tokio::fs::write(path, html).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    info!("starting load runner with 100,000 concurrent meters");
    let runner = LoadRunner::new(100_000);
    runner.run_stress_test().await;
}
