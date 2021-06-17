use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, RecvTimeoutError, Sender};
use histogram::Histogram;

#[derive(Clone)]
pub struct Monitor {
    op_count: Arc<AtomicU64>,
    duration_tx: Sender<(Duration, u64)>,
}

impl Monitor {
    pub fn start_monitoring(report_interval_ms: u64) -> Self {
        let (tx, rx) = unbounded::<(Duration, u64)>();
        let count = Arc::new(AtomicU64::new(0));
        let count0 = count.clone();
        std::thread::spawn(move || {
            let mut now = Instant::now();
            let mut next_report_time = now + Duration::from_millis(report_interval_ms);
            let mut histogram = Histogram::new();
            let mut last_count = 0;

            loop {
                now = Instant::now();
                if now >= next_report_time {
                    let count = count0.load(SeqCst);
                    let qps = (count - last_count) as f64 / (report_interval_ms as f64 / 1_000.0);
                    let p50 =
                        histogram.percentile(50.0).unwrap_or(9999999999999) as f64 / 1_000_000.0;
                    let p90 =
                        histogram.percentile(90.0).unwrap_or(9999999999999) as f64 / 1_000_000.0;
                    let p99 =
                        histogram.percentile(99.0).unwrap_or(9999999999999) as f64 / 1_000_000.0;
                    let p999 =
                        histogram.percentile(99.9).unwrap_or(9999999999999) as f64 / 1_000_000.0;
                    let minimum = histogram.minimum().unwrap_or(9999999999999) as f64 / 1_000_000.0;
                    let mean = histogram.mean().unwrap_or(9999999999999) as f64 / 1_000_000.0;
                    let maximum = histogram.maximum().unwrap_or(9999999999999) as f64 / 1_000_000.0;
                    let stddev = histogram.stddev().unwrap_or(9999999999999) as f64 / 1_000_000.0;

                    println!("QPS: {:.2} | Percentiles (ms): p50: {:.2} p90: {:.2} p99: {:.2} p999: {:.2} | Latency (ms): Min: {:.2} Avg: {:.2} Max: {:.2} StdDev: {:.2}",
                             qps, p50, p90, p99, p999, minimum, mean, maximum, stddev,
                    );

                    last_count = count;
                    next_report_time = now + Duration::from_millis(report_interval_ms);
                    histogram = Histogram::new();
                }

                match rx.recv_timeout(next_report_time - now) {
                    Ok((value, count)) => histogram
                        .increment_by(value.as_nanos() as _, count)
                        .unwrap(),
                    Err(RecvTimeoutError::Disconnected) => break,
                    _ => {}
                }
            }
        });

        Self {
            op_count: count,
            duration_tx: tx,
        }
    }

    pub fn incr_count(&self, count: u64) {
        self.op_count.fetch_add(count, Relaxed);
    }

    pub fn add_response_time(&self, duration: Duration) {
        let _ = self.duration_tx.send((duration, 1));
    }
}
