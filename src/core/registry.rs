//! The desired market set, reconciled against complete scans and recent lifecycle events.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use serde::Serialize;
use tokio::time::Instant;

use crate::model::{
    Listing, ListingId, ListingStatus, MarketBooks, MarketId, NewMarket, OrderBookId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AddedBy {
    Hint,
    Scan,
    Config,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveReason {
    Resolved,
    AbsentFromScan,
    EligibilityChanged,
    IdentityChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Add {
        listing_id: ListingId,
        books: MarketBooks,
        added_by: AddedBy,
        created_at: Option<String>,
    },
    Remove {
        listing_id: ListingId,
        market: MarketId,
        reason: RemoveReason,
    },
}

#[derive(Debug)]
enum RecentEvent {
    Added {
        listing_id: ListingId,
        books: MarketBooks,
    },
    Resolved,
}

const PENDING_RECHECK: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct PendingAdmission {
    replacement: bool,
    market: MarketId,
    generation: u64,
    /// None while this incarnation has an outstanding lookup.
    due: Option<Instant>,
}

#[derive(Debug)]
struct PendingTicket {
    listing_id: ListingId,
    generation: u64,
}

#[derive(Debug)]
pub(crate) struct PendingBatch {
    recovery: bool,
    configured: bool,
    scan_epoch: u64,
    tickets: Vec<PendingTicket>,
}

impl PendingBatch {
    pub(crate) fn configured(ids: Vec<ListingId>) -> Self {
        Self {
            recovery: false,
            configured: true,
            scan_epoch: 0,
            tickets: ids
                .into_iter()
                .map(|listing_id| PendingTicket {
                    listing_id,
                    generation: 0,
                })
                .collect(),
        }
    }

    pub(crate) fn ids(&self) -> Vec<ListingId> {
        self.tickets
            .iter()
            .map(|ticket| ticket.listing_id.clone())
            .collect()
    }
}

#[derive(Debug)]
struct RecoveryEntry {
    generation: u64,
    sources: HashMap<crate::model::ConnectionId, crate::core::recovery::RecoveryState>,
    due: Option<Instant>,
    in_flight: bool,
}

#[derive(Debug, Default)]
pub struct Registry {
    recovery: HashMap<ListingId, RecoveryEntry>,
    prefer_recovery: bool,
    next_generation: u64,

    entries: BTreeMap<ListingId, MarketBooks>,
    by_market: HashMap<MarketId, ListingId>,
    /// Latest event per market since the last successful reconciliation.
    recent: HashMap<MarketId, RecentEvent>,
    pending: HashMap<ListingId, PendingAdmission>,
    next_admission: u64,
    scan_epoch: u64,
}

impl Registry {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Hints schedule metadata confirmation; they never create subscriptions.
    pub(crate) fn queue_hint(&mut self, hint: &NewMarket, now: Instant) -> bool {
        if self.entries.contains_key(&hint.listing_id)
            || self.by_market.contains_key(&hint.market)
            || self.pending.contains_key(&hint.listing_id)
            || self
                .pending
                .values()
                .any(|entry| entry.market == hint.market)
        {
            return false;
        }
        self.next_admission += 1;
        self.pending.insert(
            hint.listing_id.clone(),
            PendingAdmission {
                replacement: false,
                market: hint.market.clone(),
                generation: self.next_admission,
                due: Some(now),
            },
        );
        true
    }

    /// Oldest due entries go first, so repeatedly ineligible entries cannot starve others.
    pub(crate) fn poll_due(&mut self, now: Instant, limit: usize) -> Option<PendingBatch> {
        self.prefer_recovery = !self.prefer_recovery;
        let admission_due = self
            .pending
            .values()
            .any(|entry| entry.due.is_some_and(|at| at <= now));
        if (self.prefer_recovery || !admission_due)
            && let Some(batch) = self.poll_recovery(now, limit)
        {
            return Some(batch);
        }

        let mut due: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(id, entry)| {
                entry
                    .due
                    .filter(|deadline| *deadline <= now)
                    .map(|deadline| (deadline, entry.generation, id.clone()))
            })
            .collect();
        due.sort_unstable();
        let tickets: Vec<_> = due
            .into_iter()
            .take(limit)
            .filter_map(|(_, generation, id)| {
                self.pending.get_mut(&id)?.due = None;
                Some(PendingTicket {
                    listing_id: id,
                    generation,
                })
            })
            .collect();
        if tickets.is_empty() {
            None
        } else {
            Some(PendingBatch {
                recovery: false,
                configured: false,
                scan_epoch: self.scan_epoch,
                tickets,
            })
        }
    }

    /// Apply only the still-current incarnation and observations newer than the last scan.
    pub(crate) fn on_lookup(
        &mut self,
        batch: PendingBatch,
        rows: Option<Vec<Listing>>,
        now: Instant,
    ) -> Vec<Action> {
        if batch.recovery {
            return self.on_recovery_lookup(batch, rows, now);
        }
        if batch.configured {
            return rows
                .unwrap_or_default()
                .iter()
                .filter(|row| row.recovery == crate::model::RecoveryEligibility::Eligible)
                .filter_map(|row| self.add_configured(row))
                .collect();
        }
        let rows: HashMap<_, _> = rows
            .unwrap_or_default()
            .into_iter()
            .map(|row| (row.listing_id.clone(), row))
            .collect();
        let mut actions = Vec::new();
        for ticket in batch.tickets {
            let Some(entry) = self.pending.get_mut(&ticket.listing_id) else {
                continue;
            };
            if entry.generation != ticket.generation {
                continue;
            }
            entry.due = Some(
                now + if entry.replacement {
                    Duration::from_secs(60)
                } else {
                    PENDING_RECHECK
                },
            );
            if batch.scan_epoch != self.scan_epoch {
                continue;
            }
            let expected_market = entry.market.clone();
            let Some(row) = rows.get(&ticket.listing_id) else {
                continue;
            };
            if row
                .market
                .as_ref()
                .is_some_and(|market| market != &expected_market)
            {
                continue;
            }
            if entry.replacement && row.recovery != crate::model::RecoveryEligibility::Eligible {
                continue;
            }
            match row.status {
                ListingStatus::Closed | ListingStatus::Excluded => {
                    self.pending.remove(&ticket.listing_id);
                    self.recent.insert(expected_market, RecentEvent::Resolved);
                }
                ListingStatus::NotDesired => {}
                ListingStatus::Desired => {
                    if row.market.as_ref() != Some(&expected_market) {
                        continue;
                    }
                    let Some(books) = market_books(&expected_market, &row.order_books) else {
                        continue;
                    };
                    self.pending.remove(&ticket.listing_id);
                    self.recent.insert(
                        expected_market,
                        RecentEvent::Added {
                            listing_id: ticket.listing_id.clone(),
                            books: books.clone(),
                        },
                    );
                    actions.extend(self.add(
                        ticket.listing_id,
                        books,
                        AddedBy::Hint,
                        row.created_at.clone(),
                    ));
                }
            }
        }
        actions
    }

    /// Record unknown resolutions too: an in-flight scan may still contain them.
    pub fn on_resolved(&mut self, market: &MarketId) -> Option<Action> {
        self.pending.retain(|_, entry| &entry.market != market);
        self.recent.insert(market.clone(), RecentEvent::Resolved);
        let id = self.by_market.get(market)?.clone();
        self.remove(id, RemoveReason::Resolved)
    }

    /// Fixed-market mode uses one initial lookup and no reconciliation.
    pub fn add_configured(&mut self, listing: &Listing) -> Option<Action> {
        if listing.status != ListingStatus::Desired {
            return None;
        }
        let books = market_books(listing.market.as_ref()?, &listing.order_books)?;
        self.add(
            listing.listing_id.clone(),
            books,
            AddedBy::Config,
            listing.created_at.clone(),
        )
    }

    /// Apply only complete scans. Lifecycle events override scan observations,
    /// then expire so the next successful scan can correct missed events.
    pub fn reconcile(&mut self, rows: Vec<Listing>) -> Vec<Action> {
        self.scan_epoch += 1;
        let mut desired = BTreeMap::new();
        for row in rows {
            if self.pending.get(&row.listing_id).is_some_and(|entry| {
                row.market
                    .as_ref()
                    .is_some_and(|market| market != &entry.market)
            }) {
                continue;
            }
            if row.status != ListingStatus::Desired {
                continue;
            }
            let Some(books) = row
                .market
                .as_ref()
                .and_then(|market| market_books(market, &row.order_books))
            else {
                continue;
            };
            if !self.recent.contains_key(&books.market) {
                desired.insert(row.listing_id, (books, row.created_at));
            }
        }
        for (_, event) in self.recent.drain() {
            if let RecentEvent::Added { listing_id, books } = event {
                desired.insert(listing_id, (books, None));
            }
        }
        let missing: Vec<ListingId> = self
            .entries
            .keys()
            .filter(|id| !desired.contains_key(id))
            .cloned()
            .collect();
        let mut actions = Vec::new();
        for id in missing {
            actions.extend(self.remove(id, RemoveReason::AbsentFromScan));
        }
        for (id, (books, created_at)) in desired {
            actions.extend(self.add(id, books, AddedBy::Scan, created_at));
        }
        actions
    }

    fn add(
        &mut self,
        id: ListingId,
        mut books: MarketBooks,
        added_by: AddedBy,
        created_at: Option<String>,
    ) -> Option<Action> {
        if self.entries.contains_key(&id) || self.by_market.contains_key(&books.market) {
            return None;
        }
        self.pending
            .retain(|pending_id, entry| pending_id != &id && entry.market != books.market);
        self.next_generation += 1;
        books.generation = self.next_generation;
        self.by_market.insert(books.market.clone(), id.clone());
        self.entries.insert(id.clone(), books.clone());
        Some(Action::Add {
            listing_id: id,
            books,
            added_by,
            created_at,
        })
    }

    pub(crate) fn contains_listing(&self, id: &ListingId) -> bool {
        self.entries.contains_key(id)
    }

    pub(crate) fn recovery_state(
        &mut self,
        state: crate::core::recovery::RecoveryState,
        now: Instant,
    ) {
        let Some(id) = self.by_market.get(&state.market).cloned() else {
            return;
        };
        let Some(books) = self.entries.get(&id) else {
            return;
        };
        if books.generation != state.generation {
            return;
        }
        let entry = self
            .recovery
            .entry(id.clone())
            .or_insert_with(|| RecoveryEntry {
                generation: state.generation,
                sources: HashMap::new(),
                due: Some(now),
                in_flight: false,
            });
        if entry
            .sources
            .get(&state.connection)
            .is_some_and(|previous| previous.revision >= state.revision)
        {
            return;
        }
        crate::core::telemetry::emit(
            "market_recovery_state",
            serde_json::json!({
                "listing_id":id,"market":state.market,"generation":state.generation,"connection":state.connection.0,
                "session":state.session,"revision":state.revision,"missing":state.missing,"trigger":state.trigger,"missing_s":state.missing_s,
            }),
        );
        entry.sources.insert(state.connection, state);
        if entry
            .sources
            .values()
            .all(|source| source.missing.is_empty())
            && !entry.in_flight
        {
            self.recovery.remove(&id);
        }
    }

    fn poll_recovery(&mut self, now: Instant, limit: usize) -> Option<PendingBatch> {
        let mut due: Vec<_> = self
            .recovery
            .iter()
            .filter_map(|(id, entry)| entry.due.filter(|at| *at <= now).map(|at| (at, id.clone())))
            .collect();
        due.sort_unstable();
        let tickets: Vec<_> = due
            .into_iter()
            .take(limit)
            .filter_map(|(_, id)| {
                let entry = self.recovery.get_mut(&id)?;
                entry.due = None;
                entry.in_flight = true;
                Some(PendingTicket {
                    listing_id: id,
                    generation: entry.generation,
                })
            })
            .collect();
        (!tickets.is_empty()).then_some(PendingBatch {
            recovery: true,
            configured: false,
            scan_epoch: self.scan_epoch,
            tickets,
        })
    }

    fn on_recovery_lookup(
        &mut self,
        batch: PendingBatch,
        rows: Option<Vec<Listing>>,
        now: Instant,
    ) -> Vec<Action> {
        let rows: HashMap<_, _> = rows
            .unwrap_or_default()
            .into_iter()
            .map(|row| (row.listing_id.clone(), row))
            .collect();
        let mut actions = Vec::new();
        for ticket in batch.tickets {
            let Some(books) = self.entries.get(&ticket.listing_id).cloned() else {
                continue;
            };
            let Some(entry) = self.recovery.get_mut(&ticket.listing_id) else {
                continue;
            };
            if books.generation != ticket.generation || entry.generation != ticket.generation {
                continue;
            }
            entry.in_flight = false;
            let unresolved = entry
                .sources
                .values()
                .any(|source| !source.missing.is_empty());
            let fenced = batch.scan_epoch != self.scan_epoch;
            entry.due =
                unresolved.then_some(now + Duration::from_secs(if fenced { 5 } else { 60 }));
            if fenced {
                continue;
            }
            let row = rows
                .get(&ticket.listing_id)
                .filter(|row| row.market.as_ref() == Some(&books.market));
            let mut result = "unknown";
            if let Some(row) = row {
                match row.recovery {
                    crate::model::RecoveryEligibility::Ineligible => {
                        result = "ineligible";
                        self.recent
                            .insert(books.market.clone(), RecentEvent::Resolved);
                        actions.extend(
                            self.remove(
                                ticket.listing_id.clone(),
                                RemoveReason::EligibilityChanged,
                            ),
                        );
                    }
                    crate::model::RecoveryEligibility::Eligible => {
                        result = "eligible";
                        if row.order_books.as_slice() != books.order_books && unresolved {
                            result = "identity_changed";
                            self.recent
                                .insert(books.market.clone(), RecentEvent::Resolved);
                            actions.extend(
                                self.remove(
                                    ticket.listing_id.clone(),
                                    RemoveReason::IdentityChanged,
                                ),
                            );
                            self.next_admission += 1;
                            self.pending.insert(
                                ticket.listing_id.clone(),
                                PendingAdmission {
                                    replacement: true,
                                    market: books.market.clone(),
                                    generation: self.next_admission,
                                    due: Some(now + Duration::from_secs(60)),
                                },
                            );
                        } else {
                            self.recent.insert(
                                books.market.clone(),
                                RecentEvent::Added {
                                    listing_id: ticket.listing_id.clone(),
                                    books: books.clone(),
                                },
                            );
                        }
                    }
                    crate::model::RecoveryEligibility::Unknown => {}
                }
            }
            crate::core::telemetry::emit(
                "market_eligibility_checked",
                serde_json::json!({
                    "listing_id":ticket.listing_id,"market":books.market,"generation":books.generation,
                    "result":result,"sources_unresolved":unresolved,
                }),
            );
            if !unresolved {
                self.recovery.remove(&ticket.listing_id);
            }
        }
        actions
    }

    fn remove(&mut self, listing_id: ListingId, reason: RemoveReason) -> Option<Action> {
        self.recovery.remove(&listing_id);
        let books = self.entries.remove(&listing_id)?;
        self.by_market.remove(&books.market);
        Some(Action::Remove {
            listing_id,
            market: books.market,
            reason,
        })
    }
}

/// Refine authoritative listing assets before subscription actions exist.
fn market_books(market: &MarketId, order_books: &[OrderBookId]) -> Option<MarketBooks> {
    let [first, second] = order_books else {
        return None;
    };
    if first == second || first.0.is_empty() || second.0.is_empty() {
        return None;
    }
    Some(MarketBooks {
        generation: 0,
        market: market.clone(),
        order_books: [first.clone(), second.clone()],
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::core::registry::{Action, Registry, RemoveReason};
    use crate::model::{Listing, ListingId, ListingStatus, MarketId, NewMarket, OrderBookId};

    fn hint(id: u64) -> NewMarket {
        NewMarket {
            listing_id: ListingId(id.to_string()),
            market: MarketId::from(format!("0x{id:064x}").as_str()),
            order_books: vec![
                OrderBookId::from(format!("{id}1").as_str()),
                OrderBookId::from(format!("{id}2").as_str()),
            ],
        }
    }

    fn row(id: u64) -> Listing {
        let event = hint(id);
        Listing {
            recovery: crate::model::RecoveryEligibility::Eligible,
            listing_id: ListingId(id.to_string()),
            market: Some(event.market),
            order_books: event.order_books,
            status: ListingStatus::Desired,
            created_at: None,
        }
    }

    fn added(actions: &[Action]) -> Vec<u64> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Add { listing_id, .. } => Some(listing_id.0.parse::<u64>().unwrap()),
                _ => None,
            })
            .collect()
    }

    fn removed(actions: &[Action]) -> Vec<u64> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Remove { listing_id, .. } => Some(listing_id.0.parse::<u64>().unwrap()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn complete_scan_adds_missing_and_removes_absent_or_ineligible_markets() {
        let mut registry = Registry::default();
        assert_eq!(
            added(&registry.reconcile(vec![row(1), row(2), row(3)])),
            [1, 2, 3]
        );
        let mut ineligible = row(2);
        ineligible.status = ListingStatus::NotDesired;
        let actions = registry.reconcile(vec![row(1), ineligible, row(4)]);
        assert_eq!(added(&actions), [4]);
        assert_eq!(removed(&actions), [2, 3]);
        assert_eq!(registry.len(), 2);
        assert!(registry.reconcile(vec![row(1), row(4)]).is_empty());
    }

    #[test]
    fn hints_wait_for_confirmation_are_deduplicated_and_protect_confirmed_admission() {
        let mut registry = Registry::default();
        let now = Instant::now();
        assert!(registry.queue_hint(&hint(1), now));
        for _ in 0..20 {
            assert!(!registry.queue_hint(&hint(1), now));
        }
        assert!(registry.is_empty());
        let first = registry.poll_due(now, 100).unwrap();
        assert_eq!(first.ids(), [ListingId::from("1")]);
        let mut waiting = row(1);
        waiting.status = ListingStatus::NotDesired;
        assert!(
            registry
                .on_lookup(first, Some(vec![waiting]), now)
                .is_empty()
        );
        assert!(
            registry
                .poll_due(now + Duration::from_secs(4), 100)
                .is_none()
        );
        let next = registry
            .poll_due(now + Duration::from_secs(5), 100)
            .unwrap();
        let mut confirmed = row(1);
        confirmed.order_books = vec![OrderBookId::from("100"), OrderBookId::from("200")];
        let actions = registry.on_lookup(next, Some(vec![confirmed]), now);
        assert!(
            matches!(&actions[..], [Action::Add { books, .. }] if books.order_books == [OrderBookId::from("100"), OrderBookId::from("200")])
        );
        assert_eq!(registry.len(), 1);
        assert!(!registry.queue_hint(&hint(1), now));
        assert!(
            registry.reconcile(vec![]).is_empty(),
            "confirmed admission wins over an older scan"
        );
        assert_eq!(
            removed(&registry.reconcile(vec![])),
            [1],
            "next scan is authoritative"
        );
    }

    #[test]
    fn pending_resolution_and_identical_rehint_reject_old_lookup_results() {
        for consume_tombstone in [false, true] {
            let mut registry = Registry::default();
            let now = Instant::now();
            registry.queue_hint(&hint(1), now);
            let stale = registry.poll_due(now, 100).unwrap();
            assert!(registry.on_resolved(&hint(1).market).is_none());
            if consume_tombstone {
                registry.reconcile(vec![]);
            }
            registry.queue_hint(&hint(1), now);
            assert!(
                registry
                    .on_lookup(stale, Some(vec![row(1)]), now)
                    .is_empty()
            );
            assert!(registry.is_empty());
            let current = registry.poll_due(now, 100).unwrap();
            assert_eq!(
                added(&registry.on_lookup(current, Some(vec![row(1)]), now)),
                [1]
            );
        }
    }

    #[test]
    fn completed_scan_fences_older_lookups_for_still_pending_markets() {
        let mut registry = Registry::default();
        let now = Instant::now();
        registry.queue_hint(&hint(1), now);
        let stale = registry.poll_due(now, 100).unwrap();
        let mut waiting = row(1);
        waiting.status = ListingStatus::NotDesired;
        assert!(registry.reconcile(vec![waiting]).is_empty());
        assert!(
            registry
                .on_lookup(stale, Some(vec![row(1)]), now)
                .is_empty()
        );
        assert!(registry.is_empty());
        let current = registry
            .poll_due(now + Duration::from_secs(5), 100)
            .unwrap();
        assert_eq!(
            added(&registry.on_lookup(current, Some(vec![row(1)]), now)),
            [1]
        );
    }

    #[test]
    fn scan_terminal_rows_do_not_discard_newer_hints_waiting_for_confirmation() {
        let mut registry = Registry::default();
        let now = Instant::now();
        registry.queue_hint(&hint(1), now);
        let mut older_observation = row(1);
        older_observation.status = ListingStatus::Closed;
        assert!(registry.reconcile(vec![older_observation]).is_empty());
        let fresh = registry.poll_due(now, 100).unwrap();
        assert_eq!(
            added(&registry.on_lookup(fresh, Some(vec![row(1)]), now)),
            [1]
        );
    }

    #[test]
    fn scan_admission_clears_pending_and_ignores_late_lookup() {
        let mut registry = Registry::default();
        let now = Instant::now();
        registry.queue_hint(&hint(1), now);
        let stale = registry.poll_due(now, 100).unwrap();
        assert_eq!(added(&registry.reconcile(vec![row(1)])), [1]);
        assert!(registry.on_lookup(stale, None, now).is_empty());
        assert!(
            registry
                .poll_due(now + Duration::from_secs(5), 100)
                .is_none()
        );
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn missing_ineligible_or_invalid_metadata_keeps_waiting_but_terminal_rows_stop() {
        let now = Instant::now();
        let mut not_ready = row(1);
        not_ready.status = ListingStatus::NotDesired;
        let mut mismatched = row(1);
        mismatched.market = row(2).market;
        let mut malformed = row(1);
        malformed.order_books.pop();
        for response in [
            None,
            Some(vec![]),
            Some(vec![not_ready]),
            Some(vec![mismatched]),
            Some(vec![malformed]),
        ] {
            let mut registry = Registry::default();
            registry.queue_hint(&hint(1), now);
            let batch = registry.poll_due(now, 100).unwrap();
            assert!(registry.on_lookup(batch, response, now).is_empty());
            assert!(registry.is_empty());
            assert!(
                registry
                    .poll_due(now + Duration::from_secs(5), 100)
                    .is_some()
            );
        }
        for status in [ListingStatus::Closed, ListingStatus::Excluded] {
            let mut registry = Registry::default();
            registry.queue_hint(&hint(1), now);
            let batch = registry.poll_due(now, 100).unwrap();
            let mut terminal = row(1);
            terminal.status = status;
            assert!(
                registry
                    .on_lookup(batch, Some(vec![terminal]), now)
                    .is_empty()
            );
            assert!(
                registry
                    .poll_due(now + Duration::from_secs(5), 100)
                    .is_none()
            );
            assert!(
                registry.reconcile(vec![row(1)]).is_empty(),
                "terminal result overrides stale scan"
            );
        }
    }

    #[test]
    fn batches_are_bounded_and_unready_prefix_does_not_starve_later_markets() {
        let mut registry = Registry::default();
        let now = Instant::now();
        for id in 1..=205 {
            registry.queue_hint(&hint(id), now);
        }
        for expected in [1..=100, 101..=200] {
            let batch = registry.poll_due(now, 100).unwrap();
            assert_eq!(
                batch.ids(),
                expected
                    .map(|id| ListingId(id.to_string()))
                    .collect::<Vec<_>>()
            );
            assert!(registry.on_lookup(batch, None, now).is_empty());
            for _ in 0..20 {
                assert!(!registry.queue_hint(&hint(1), now));
            }
        }
        registry.queue_hint(&hint(206), now);
        let batch = registry.poll_due(now, 100).unwrap();
        assert_eq!(
            batch.ids(),
            (201..=206)
                .map(|id| ListingId(id.to_string()))
                .collect::<Vec<_>>()
        );
        assert!(registry.on_lookup(batch, None, now).is_empty());
        assert!(
            registry
                .poll_due(now + Duration::from_secs(4), 100)
                .is_none()
        );
    }

    #[test]
    fn resolutions_remove_immediately_and_override_stale_scan_rows() {
        let mut registry = Registry::default();
        registry.reconcile(vec![row(1)]);
        let action = registry.on_resolved(&hint(1).market).unwrap();
        assert!(matches!(
            action,
            Action::Remove {
                reason: RemoveReason::Resolved,
                ..
            }
        ));
        assert!(registry.is_empty());
        // A resolution for an unknown market must also suppress a stale scan row.
        assert!(registry.on_resolved(&hint(2).market).is_none());
        assert!(registry.reconcile(vec![row(1), row(2)]).is_empty());
        assert!(registry.is_empty());
        assert_eq!(added(&registry.reconcile(vec![row(1)])), [1]);
    }

    #[test]
    fn latest_lifecycle_event_wins_in_both_orders() {
        let mut registry = Registry::default();
        let now = Instant::now();
        registry.queue_hint(&hint(1), now);
        registry.on_resolved(&hint(1).market);
        registry.on_resolved(&hint(2).market);
        registry.queue_hint(&hint(2), now);
        let confirmed = registry.poll_due(now, 100).unwrap();
        registry.on_lookup(confirmed, Some(vec![row(2)]), now);
        assert!(registry.reconcile(vec![row(1)]).is_empty());
        assert_eq!(registry.len(), 1);
        assert!(registry.on_resolved(&hint(2).market).is_some());
    }

    #[test]
    fn hints_without_outcome_ids_can_wait_for_authoritative_metadata() {
        let mut registry = Registry::default();
        let now = Instant::now();
        let mut incomplete = hint(1);
        incomplete.order_books.clear();
        assert!(registry.queue_hint(&incomplete, now));
        assert!(registry.is_empty());
        let batch = registry.poll_due(now, 100).unwrap();
        assert_eq!(
            added(&registry.on_lookup(batch, Some(vec![row(1)]), now)),
            [1]
        );
    }

    #[test]
    fn malformed_metadata_book_pairs_are_never_admitted() {
        for books in [
            vec![],
            vec![OrderBookId::from("1")],
            vec![OrderBookId::from("1"), OrderBookId::from("1")],
            vec![OrderBookId::from(""), OrderBookId::from("2")],
            vec![
                OrderBookId::from("1"),
                OrderBookId::from("2"),
                OrderBookId::from("3"),
            ],
        ] {
            let mut registry = Registry::default();
            let mut invalid = hint(1);
            invalid.order_books = books.clone();
            let now = Instant::now();
            assert!(registry.queue_hint(&invalid, now));
            assert!(registry.is_empty());
            let batch = registry.poll_due(now, 100).unwrap();
            let mut invalid = row(1);
            invalid.order_books = books;
            assert!(
                registry
                    .on_lookup(batch, Some(vec![invalid.clone()]), now)
                    .is_empty()
            );
            assert!(registry.add_configured(&invalid).is_none());
            assert!(registry.reconcile(vec![invalid]).is_empty());
            assert!(registry.is_empty());
        }
    }
    fn report(
        registry: &Registry,
        id: u64,
        source: u16,
        revision: u64,
        missing: bool,
    ) -> crate::core::recovery::RecoveryState {
        let books = &registry.entries[&ListingId(id.to_string())];
        crate::core::recovery::RecoveryState {
            market: books.market.clone(),
            generation: books.generation,
            connection: crate::model::ConnectionId(source),
            session: 1,
            revision,
            trigger: "test",
            missing_s: 180.0,
            missing: if missing {
                books.order_books.to_vec()
            } else {
                vec![]
            },
        }
    }

    #[test]
    fn exhausted_sources_deduplicate_and_remove_only_confirmed_ineligible_market() {
        let mut registry = Registry::default();
        let now = Instant::now();
        registry.reconcile(vec![row(1), row(2)]);
        for source in [0, 3] {
            let state = report(&registry, 1, source, 1, true);
            registry.recovery_state(state, now);
        }
        let batch = registry.poll_due(now, 100).unwrap();
        assert_eq!(batch.ids(), [ListingId("1".into())]);
        let mut closed = row(1);
        closed.recovery = crate::model::RecoveryEligibility::Ineligible;
        closed.status = ListingStatus::Closed;
        assert_eq!(
            removed(&registry.on_lookup(batch, Some(vec![closed]), now)),
            [1]
        );
        assert_eq!(registry.len(), 1);
        assert!(
            registry.reconcile(vec![row(1), row(2)]).is_empty(),
            "older scan must not resurrect removed market"
        );
    }

    #[test]
    fn unknown_eligibility_retains_membership_and_paces_followup() {
        let mut registry = Registry::default();
        let now = Instant::now();
        registry.reconcile(vec![row(1)]);
        let state = report(&registry, 1, 0, 1, true);
        registry.recovery_state(state, now);
        let batch = registry.poll_due(now, 100).unwrap();
        assert!(registry.on_lookup(batch, None, now).is_empty());
        assert_eq!(registry.len(), 1);
        assert!(
            registry
                .poll_due(now + Duration::from_secs(59), 100)
                .is_none()
        );
        assert!(
            registry
                .poll_due(now + Duration::from_secs(60), 100)
                .is_some()
        );
    }

    #[test]
    fn old_lookup_cannot_remove_readmitted_generation() {
        let mut registry = Registry::default();
        let now = Instant::now();
        registry.reconcile(vec![row(1)]);
        let state = report(&registry, 1, 0, 1, true);
        registry.recovery_state(state, now);
        let batch = registry.poll_due(now, 100).unwrap();
        registry.on_resolved(&hint(1).market);
        registry.add_configured(&row(1));
        let mut closed = row(1);
        closed.recovery = crate::model::RecoveryEligibility::Ineligible;
        assert!(
            registry
                .on_lookup(batch, Some(vec![closed]), now)
                .is_empty()
        );
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn recovered_source_cancels_followups_but_inflight_closed_result_still_removes() {
        let mut registry = Registry::default();
        let now = Instant::now();
        registry.reconcile(vec![row(1)]);
        let state = report(&registry, 1, 0, 1, true);
        registry.recovery_state(state, now);
        let batch = registry.poll_due(now, 100).unwrap();
        let recovered = report(&registry, 1, 0, 2, false);
        registry.recovery_state(recovered, now);
        let mut closed = row(1);
        closed.recovery = crate::model::RecoveryEligibility::Ineligible;
        assert_eq!(
            removed(&registry.on_lookup(batch, Some(vec![closed]), now)),
            [1]
        );
    }

    #[test]
    fn changed_outcomes_require_new_generation_and_fresh_confirmation() {
        let mut registry = Registry::default();
        let now = Instant::now();
        registry.reconcile(vec![row(1)]);
        let old_generation = registry.entries[&ListingId("1".into())].generation;
        let state = report(&registry, 1, 0, 1, true);
        registry.recovery_state(state, now);
        let batch = registry.poll_due(now, 100).unwrap();
        let mut changed = row(1);
        changed.order_books = vec!["91".into(), "92".into()];
        assert_eq!(
            removed(&registry.on_lookup(batch, Some(vec![changed.clone()]), now)),
            [1]
        );
        assert!(
            registry
                .poll_due(now + Duration::from_secs(59), 100)
                .is_none()
        );
        let batch = registry
            .poll_due(now + Duration::from_secs(60), 100)
            .unwrap();
        assert_eq!(
            added(&registry.on_lookup(batch, Some(vec![changed]), now + Duration::from_secs(60))),
            [1]
        );
        assert!(registry.entries[&ListingId("1".into())].generation > old_generation);
    }
}
