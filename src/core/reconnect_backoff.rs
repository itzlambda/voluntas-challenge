//! Per-connection reconnect delays, independent of the connection-opening limiter.

use std::time::Duration;

/// Retry immediately, then after 1, 5, 10, and at most 30 seconds.
#[derive(Default)]
pub(crate) struct ReconnectBackoff {
    retries: u8,
}

impl ReconnectBackoff {
    pub(crate) fn next_delay(&mut self, lasted: Duration) -> Duration {
        if lasted > Duration::from_secs(60) {
            self.reset();
        }
        let seconds = match self.retries {
            0 => 0,
            1 => 1,
            2 => 5,
            3 => 10,
            _ => 30,
        };
        self.retries = self.retries.saturating_add(1);
        Duration::from_secs(seconds)
    }

    pub(crate) fn reset(&mut self) {
        self.retries = 0;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::core::reconnect_backoff::ReconnectBackoff;

    #[test]
    fn fixed_schedule_caps_and_saturates() {
        let mut backoff = ReconnectBackoff::default();
        for seconds in [0, 1, 5, 10, 30, 30] {
            assert_eq!(
                backoff.next_delay(Duration::ZERO),
                Duration::from_secs(seconds)
            );
        }
        for _ in 0..1000 {
            assert_eq!(backoff.next_delay(Duration::ZERO), Duration::from_secs(30));
        }
    }

    #[test]
    fn resets_only_after_strictly_more_than_sixty_seconds() {
        let mut backoff = ReconnectBackoff::default();
        for _ in 0..5 {
            backoff.next_delay(Duration::ZERO);
        }
        assert_eq!(
            backoff.next_delay(Duration::from_secs(60)),
            Duration::from_secs(30)
        );
        assert_eq!(
            backoff.next_delay(Duration::from_secs(60) + Duration::from_nanos(1)),
            Duration::ZERO
        );
        assert_eq!(backoff.next_delay(Duration::ZERO), Duration::from_secs(1));
    }

    #[test]
    fn explicit_reset_restarts_the_schedule() {
        let mut backoff = ReconnectBackoff::default();
        for _ in 0..5 {
            backoff.next_delay(Duration::ZERO);
        }
        backoff.reset();
        assert_eq!(backoff.next_delay(Duration::ZERO), Duration::ZERO);
        assert_eq!(backoff.next_delay(Duration::ZERO), Duration::from_secs(1));
    }
}
