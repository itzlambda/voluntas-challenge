//! Bounded, coalesced source recovery reports. HTTP decisions never gate retries.
use crate::model::{ConnectionId, MarketId, OrderBookId};

#[derive(Debug, Clone)]
pub struct RecoveryState {
    pub market: MarketId,
    pub generation: u64,
    pub connection: ConnectionId,
    pub session: u64,
    pub revision: u64,
    pub trigger: &'static str,
    pub missing_s: f64,
    pub missing: Vec<OrderBookId>,
}
