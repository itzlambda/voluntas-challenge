//! WebSocket-opening pacing and a separate request budget for Gamma.

use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{Context, ensure};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep_until};

pub struct RateLimiter {
    interval: Duration,
    next: Mutex<Option<Instant>>,
}

impl RateLimiter {
    /// At most `per_second` starts per second.
    pub fn per_second(per_second: f64) -> Self {
        assert!(per_second > 0.0);
        Self {
            interval: Duration::from_secs_f64(1.0 / per_second),
            next: Mutex::new(None),
        }
    }

    /// Waits for the next free slot.
    pub async fn wait(&self) {
        let at = {
            let mut next = self.next.lock().await;
            let now = Instant::now();
            let at = next.map_or(now, |next| next.max(now));
            *next = Some(at + self.interval);
            at
        };
        sleep_until(at).await;
    }
}

pub(crate) struct RequestBudget {
    capacity: usize,
    window: Duration,
    starts: Mutex<VecDeque<Instant>>,
}

impl RequestBudget {
    /// Integer rates grant that many immediate requests per rolling second.
    /// Fractional rates use a whole quota over a proportionally longer window.
    pub(crate) fn per_second(per_second: f64) -> anyhow::Result<Self> {
        ensure!(
            per_second.is_finite() && per_second > 0.0,
            "request rate must be finite and positive"
        );
        let capacity = per_second.ceil() as usize;
        let window = Duration::try_from_secs_f64(capacity as f64 / per_second)
            .context("request budget window is out of range")?;
        Ok(Self {
            capacity,
            window,
            starts: Mutex::new(VecDeque::new()),
        })
    }

    pub(crate) async fn wait(&self) {
        loop {
            let deadline = {
                let mut starts = self.starts.lock().await;
                let now = Instant::now();
                while starts
                    .front()
                    .is_some_and(|start| now.duration_since(*start) >= self.window)
                {
                    starts.pop_front();
                }
                if starts.len() < self.capacity {
                    starts.push_back(now);
                    return;
                }
                starts
                    .front()
                    .and_then(|start| start.checked_add(self.window))
            };
            match deadline {
                Some(deadline) => sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn request_budget_does_not_space_requests_within_quota() {
        let limiter = RequestBudget::per_second(3.0).unwrap();
        let start = Instant::now();
        for _ in 0..3 {
            limiter.wait().await;
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn request_budget_expires_each_start_after_a_rolling_window() {
        let limiter = RequestBudget::per_second(2.0).unwrap();
        let start = Instant::now();
        limiter.wait().await;
        tokio::time::advance(Duration::from_millis(400)).await;
        limiter.wait().await;
        limiter.wait().await;
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        limiter.wait().await;
        assert_eq!(start.elapsed(), Duration::from_millis(1400));
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_requests_share_one_budget() {
        let limiter = std::sync::Arc::new(RequestBudget::per_second(3.0).unwrap());
        let start = Instant::now();
        let tasks: Vec<_> = (0..6)
            .map(|_| {
                let limiter = limiter.clone();
                tokio::spawn(async move {
                    limiter.wait().await;
                    Instant::now() - start
                })
            })
            .collect();
        let mut starts = Vec::new();
        for task in tasks {
            starts.push(task.await.unwrap());
        }
        starts.sort();
        assert_eq!(
            starts,
            [
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1)
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fractional_request_rates_keep_their_budget() {
        let limiter = RequestBudget::per_second(0.5).unwrap();
        let start = Instant::now();
        limiter.wait().await;
        limiter.wait().await;
        assert_eq!(start.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn spaces_starts() {
        let limiter = RateLimiter::per_second(4.0);
        let start = Instant::now();
        for _ in 0..5 {
            limiter.wait().await;
        }
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }
}
