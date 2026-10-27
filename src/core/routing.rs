//! Worker dispatch and the routing function (SPEC section 4).

use anyhow::anyhow;
use tokio::sync::mpsc;

use crate::model::{MarketId, WorkerMessage};

/// Worker inputs. Clones hold their own sender for each worker.
#[derive(Clone)]
pub struct Workers {
    senders: Vec<mpsc::Sender<WorkerMessage>>,
}

impl From<Vec<mpsc::Sender<WorkerMessage>>> for Workers {
    fn from(senders: Vec<mpsc::Sender<WorkerMessage>>) -> Self {
        Self { senders }
    }
}

impl Workers {
    /// Routes market messages and broadcasts connection resets in worker order.
    pub async fn send(&self, message: WorkerMessage) -> anyhow::Result<()> {
        let market = match &message {
            WorkerMessage::MarketData(data) => &data.market,
            WorkerMessage::AddMarket { market, .. }
            | WorkerMessage::RemoveMarket(market)
            | WorkerMessage::SourceAttached { market, .. } => market,
            WorkerMessage::MarkUnsynced { .. } => {
                for sender in &self.senders {
                    Self::send_one(sender, message.clone()).await?;
                }
                return Ok(());
            }
        };
        let index = worker_index(market, self.senders.len());
        Self::send_one(&self.senders[index], message).await
    }

    async fn send_one(
        sender: &mpsc::Sender<WorkerMessage>,
        message: WorkerMessage,
    ) -> anyhow::Result<()> {
        sender
            .send(message)
            .await
            .map_err(|_| anyhow!("worker input closed"))
    }
}

/// The low 64 bits of the condition ID, modulo the positive worker count.
///
/// Condition IDs are uniformly distributed hex hashes. Powers of two need only
/// their low bits, so counts up to 16 route by the final hex digit.
/// A non-hex final character (never expected) routes to worker 0.
pub fn worker_index(market: &MarketId, workers: usize) -> usize {
    debug_assert!(workers > 0);
    let power_of_two = workers.is_power_of_two();
    let bits = if power_of_two {
        workers.trailing_zeros()
    } else {
        u64::BITS
    };
    let mut suffix = 0u64;
    for (offset, &byte) in market
        .0
        .as_bytes()
        .iter()
        .rev()
        .take(bits.div_ceil(4) as usize)
        .enumerate()
    {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => break,
        };
        suffix |= u64::from(digit) << (offset * 4);
    }
    if power_of_two {
        suffix as usize & (workers - 1)
    } else {
        (suffix % workers as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use crate::core::routing::{Workers, worker_index};
    use crate::model::{ConnectionId, MarketId, UnsyncReason, WorkerMessage};

    #[test]
    fn uses_last_hex_digit() {
        let market =
            MarketId::from("0x26f92ac5a446296cf4b454e14694c3aaeb313cb84d8e258eff0d0dc491c9111b");
        assert_eq!(worker_index(&market, 1), 0);
        assert_eq!(worker_index(&market, 2), 1);
        assert_eq!(worker_index(&market, 4), 3);
        assert_eq!(worker_index(&market, 16), 11);
        assert_eq!(worker_index(&MarketId::from("0xAF"), 16), 15);
    }

    #[test]
    fn routes_markets_across_every_worker_at_arbitrary_counts() {
        for workers in [32, 3, 6, 24, 64] {
            let mut counts = vec![0; workers];
            for value in 0..workers * 4 {
                let market = MarketId::from(format!("0x{value:064x}").as_str());
                let index = worker_index(&market, workers);
                assert!(index < workers);
                assert_eq!(worker_index(&market, workers), index);
                counts[index] += 1;
            }
            assert!(
                counts.iter().all(|&count| count == 4),
                "N={workers}: {counts:?}"
            );
        }
    }

    #[tokio::test]
    async fn cloned_producer_keeps_input_open_until_queued_messages_drain() {
        let (sender, mut input) = mpsc::channel(1);
        let workers = Workers::from(vec![sender]);
        let producer = workers.clone();
        drop(workers);
        let message = WorkerMessage::RemoveMarket(MarketId::from("0xa1"));
        timeout(Duration::from_secs(1), producer.send(message.clone()))
            .await
            .unwrap()
            .unwrap();
        drop(producer);
        assert_eq!(input.recv().await, Some(message));
        assert_eq!(
            timeout(Duration::from_secs(1), input.recv()).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn closed_input_fails_market_sends_and_reset_broadcasts() {
        let (sender, input) = mpsc::channel(1);
        let workers = Workers::from(vec![sender]);
        drop(input);
        for message in [
            WorkerMessage::RemoveMarket(MarketId::from("0xa1")),
            WorkerMessage::MarkUnsynced {
                connection: ConnectionId(1),
                reason: UnsyncReason::Disconnected,
            },
        ] {
            assert!(
                timeout(Duration::from_secs(1), workers.send(message))
                    .await
                    .unwrap()
                    .is_err()
            );
        }
    }
}
