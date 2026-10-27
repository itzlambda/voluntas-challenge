//! Order book state held by workers.

use std::collections::BTreeMap;

use crate::model::{Price, Qty};

/// One side of an order book. Holds non-zero quantities only.
///
/// TODO (benchmark before changing, SPEC section 9): compare a sorted `Vec`,
/// a `Vec`/`BTreeMap` hybrid and a tick-indexed array on realistic depth. All
/// access goes through this type, so only it changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBookSide {
    levels: BTreeMap<Price, Qty>,
}

impl OrderBookSide {
    /// Loads received levels, in any order. A repeated price keeps the last
    /// quantity; zero quantities are left out.
    pub fn from_levels(levels: &[(Price, Qty)]) -> Self {
        let mut side = Self::default();
        for &(price, qty) in levels {
            side.set(price, qty);
        }
        side
    }

    /// Sets an absolute quantity; zero removes the level.
    pub fn set(&mut self, price: Price, qty: Qty) {
        if qty.0 == 0 {
            self.levels.remove(&price);
        } else {
            self.levels.insert(price, qty);
        }
    }

    pub fn quantity(&self, price: Price) -> Qty {
        self.levels.get(&price).copied().unwrap_or(Qty(0))
    }

    pub fn len(&self) -> usize {
        self.levels.len()
    }

    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Levels by ascending price.
    pub fn ascending(&self) -> impl DoubleEndedIterator<Item = (Price, Qty)> + '_ {
        self.levels.iter().map(|(&price, &qty)| (price, qty))
    }

    /// Changes that turn `self` into `target`, by ascending price: new or
    /// changed levels with their quantity, removed levels with quantity 0.
    pub fn differences(&self, target: &Self) -> Vec<(Price, Qty)> {
        let mut out = Vec::new();
        let mut ours = self.levels.iter().peekable();
        let mut theirs = target.levels.iter().peekable();
        loop {
            match (ours.peek(), theirs.peek()) {
                (None, None) => break,
                (Some(&(&price, _)), None) => {
                    out.push((price, Qty(0)));
                    ours.next();
                }
                (None, Some(&(&price, &qty))) => {
                    out.push((price, qty));
                    theirs.next();
                }
                (Some(&(&our_price, &our_qty)), Some(&(&their_price, &their_qty))) => {
                    if our_price < their_price {
                        out.push((our_price, Qty(0)));
                        ours.next();
                    } else if their_price < our_price {
                        out.push((their_price, their_qty));
                        theirs.next();
                    } else {
                        if our_qty != their_qty {
                            out.push((their_price, their_qty));
                        }
                        ours.next();
                        theirs.next();
                    }
                }
            }
        }
        out
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    pub bids: OrderBookSide,
    pub asks: OrderBookSide,
}

impl OrderBook {
    /// Bids best first (descending price).
    pub fn bids_best_first(&self) -> Vec<(Price, Qty)> {
        self.bids.ascending().rev().collect()
    }

    /// Asks best first (ascending price).
    pub fn asks_best_first(&self) -> Vec<(Price, Qty)> {
        self.asks.ascending().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn side(levels: &[(i64, i64)]) -> OrderBookSide {
        let levels: Vec<_> = levels.iter().map(|&(p, q)| (Price(p), Qty(q))).collect();
        OrderBookSide::from_levels(&levels)
    }

    #[test]
    fn set_and_remove() {
        let mut s = side(&[(1, 5), (2, 0), (3, 7), (1, 6)]);
        assert_eq!(
            s.ascending().collect::<Vec<_>>(),
            [(Price(1), Qty(6)), (Price(3), Qty(7))]
        );
        s.set(Price(3), Qty(0));
        s.set(Price(9), Qty(0));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn differences_cover_new_changed_and_removed() {
        let held = side(&[(1, 10), (2, 20), (4, 40)]);
        let received = side(&[(2, 21), (3, 30), (4, 40), (5, 50)]);
        let diff = held.differences(&received);
        let expected = [(1, 0), (2, 21), (3, 30), (5, 50)].map(|(p, q)| (Price(p), Qty(q)));
        assert_eq!(diff, expected);
        assert!(received.differences(&received).is_empty());

        let mut applied = held.clone();
        for (price, qty) in diff {
            applied.set(price, qty);
        }
        assert_eq!(applied, received);
    }
}
