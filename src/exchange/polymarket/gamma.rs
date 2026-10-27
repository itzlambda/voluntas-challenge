//! Gamma REST discovery calls and the eligibility rule (SPEC section 7).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::core::discovery::{ListingPage, Listings};
use crate::core::rate_limit::RequestBudget;
use crate::model::{Listing, ListingId, ListingStatus, MarketId, OrderBookId};

pub const GAMMA_URL: &str = "https://gamma-api.polymarket.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    V1,
    V2,
    Other,
}

/// One Gamma market row, reduced to what discovery needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GammaMarket {
    pub recovery: crate::model::RecoveryEligibility,
    pub gamma_id: u64,
    /// None if `conditionId` is missing or not a 32-byte hex hash.
    pub market: Option<MarketId>,
    /// None unless there are exactly two valid, distinct outcome asset IDs.
    pub order_books: Option<[OrderBookId; 2]>,
    pub version: Version,
    pub active: bool,
    pub closed: bool,
    pub archived: bool,
    pub accepting_orders: bool,
    pub created_at: Option<String>,
}

impl GammaMarket {
    /// Trading v1 markets with two valid, distinct outcome asset IDs.
    /// Trading readiness follows Polymarket's market-status definition:
    /// https://docs.polymarket.com/market-data/market-details#market-status
    pub fn desired(&self) -> bool {
        self.version == Version::V1
            && self.active
            && !self.closed
            && !self.archived
            && self.accepting_orders
            && self.market.is_some()
            && self.order_books.is_some()
    }

    pub fn listing(&self) -> Listing {
        let status = if self.version != Version::V1 {
            ListingStatus::Excluded
        } else if self.closed || self.archived {
            ListingStatus::Closed
        } else if self.desired() {
            ListingStatus::Desired
        } else {
            ListingStatus::NotDesired
        };
        Listing {
            recovery: self.recovery,
            listing_id: ListingId(self.gamma_id.to_string()),
            status,
            market: self.market.clone(),
            order_books: self.order_books.iter().flatten().cloned().collect(),
            created_at: self.created_at.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMarket {
    id: String,
    #[serde(default)]
    condition_id: Option<String>,
    #[serde(default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    active: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    archived: Option<bool>,
    #[serde(default)]
    accepting_orders: Option<bool>,
    #[serde(default)]
    created_at: Option<String>,
}

#[derive(Deserialize)]
struct RawPage {
    markets: Vec<RawMarket>,
    #[serde(default)]
    next_cursor: Option<String>,
}

/// One page of a keyset listing.
#[derive(Debug, Default)]
pub struct Page {
    pub markets: Vec<GammaMarket>,
    /// Rows whose `id` isn't a number; skipped.
    pub invalid: usize,
    pub next_cursor: Option<String>,
}

pub fn parse_page(json: &[u8]) -> anyhow::Result<Page> {
    let raw: RawPage = serde_json::from_slice(json).context("parse Gamma page")?;
    let mut page = Page {
        next_cursor: raw.next_cursor.filter(|cursor| !cursor.is_empty()),
        ..Page::default()
    };
    for market in raw.markets {
        match convert(market) {
            Some(market) => page.markets.push(market),
            None => page.invalid += 1,
        }
    }
    Ok(page)
}

fn convert(raw: RawMarket) -> Option<GammaMarket> {
    let gamma_id = raw.id.parse().ok()?;
    let market = raw
        .condition_id
        .filter(|id| is_condition_id(id))
        .map(|id| MarketId::from(id.as_str()));
    let order_books = raw.clob_token_ids.as_deref().and_then(parse_token_ids);
    let version = match raw.version.as_deref() {
        // Omitting `version` in a query returns v1 only, so v1 is the default.
        None | Some("v1") => Version::V1,
        Some("v2") => Version::V2,
        Some(_) => Version::Other,
    };
    let recovery = if raw.closed == Some(true)
        || raw.archived == Some(true)
        || raw.active == Some(false)
        || raw.accepting_orders == Some(false)
        || raw.version.as_deref() == Some("v2")
    {
        crate::model::RecoveryEligibility::Ineligible
    } else if raw.version.as_deref() == Some("v1")
        && raw.active == Some(true)
        && raw.closed == Some(false)
        && raw.archived == Some(false)
        && raw.accepting_orders == Some(true)
        && market.is_some()
        && order_books.is_some()
    {
        crate::model::RecoveryEligibility::Eligible
    } else {
        crate::model::RecoveryEligibility::Unknown
    };
    Some(GammaMarket {
        recovery,
        gamma_id,
        market,
        order_books,
        version,
        active: raw.active.unwrap_or(false),
        closed: raw.closed.unwrap_or(false),
        archived: raw.archived.unwrap_or(false),
        accepting_orders: raw.accepting_orders.unwrap_or(false),
        created_at: raw.created_at,
    })
}

fn is_condition_id(id: &str) -> bool {
    id.len() == 66 && id.starts_with("0x") && id[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

/// `clobTokenIds` is a JSON-encoded array string of two decimal asset IDs.
fn parse_token_ids(text: &str) -> Option<[OrderBookId; 2]> {
    let ids: Vec<String> = serde_json::from_str(text).ok()?;
    let [yes, no] = <[String; 2]>::try_from(ids).ok()?;
    let valid = |id: &str| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit());
    (valid(&yes) && valid(&no) && yes != no).then(|| {
        [
            OrderBookId::from(yes.as_str()),
            OrderBookId::from(no.as_str()),
        ]
    })
}

/// Gamma HTTP client. All requests share one pacing budget.
#[derive(Clone)]
pub struct GammaClient {
    http: reqwest::Client,
    base: String,
    limiter: Arc<RequestBudget>,
}

const ATTEMPTS: u32 = 5;

impl GammaClient {
    pub fn new(base: &str, requests_per_second: f64) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .user_agent("orderbook-collector/0.1")
            .build()?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_owned(),
            limiter: Arc::new(RequestBudget::per_second(requests_per_second)?),
        })
    }

    /// One page of open v1 markets, oldest IDs first. `nonce` is one
    /// cache-busting value per pass (listings are CDN-cached for up to 300 s).
    pub async fn scan_page(&self, cursor: Option<&str>, nonce: &str) -> anyhow::Result<Page> {
        let mut query = vec![
            ("version", "v1"),
            ("closed", "false"),
            ("order", "id"),
            ("ascending", "true"),
            ("limit", "100"),
            ("_nonce", nonce),
        ];
        if let Some(cursor) = cursor {
            query.push(("after_cursor", cursor));
        }
        let body = self.get("/markets/keyset", &query).await?;
        parse_page(&body)
    }

    /// Looks up to 100 markets by Gamma ID, v1 and v2, open or closed.
    /// IDs Gamma doesn't know are simply absent from the result. A full page
    /// still carries a `next_cursor`, so pages are followed to the end.
    /// A fresh nonce per lookup prevents cached eligibility from delaying admission.
    pub async fn lookup(&self, ids: &[u64]) -> anyhow::Result<Vec<GammaMarket>> {
        assert!(ids.len() <= 100, "lookup batches hold at most 100 IDs");
        let ids: Vec<String> = ids.iter().map(u64::to_string).collect();
        let nonce = format!("{:016x}", fastrand::u64(..));
        let mut markets = Vec::new();
        let mut cursor: Option<String> = None;
        // At most 100 rows can match, so a second page is normally empty.
        for _ in 0..3 {
            let mut query: Vec<(&str, &str)> = ids.iter().map(|id| ("id", id.as_str())).collect();
            query.extend([
                ("version", "v1"),
                ("version", "v2"),
                ("closed", "any"),
                ("limit", "100"),
                ("_nonce", nonce.as_str()),
            ]);
            if let Some(cursor) = &cursor {
                query.push(("after_cursor", cursor));
            }
            let page = parse_page(&self.get("/markets/keyset", &query).await?)?;
            let empty = page.markets.is_empty();
            markets.extend(page.markets);
            match page.next_cursor {
                Some(next) if !empty => cursor = Some(next),
                _ => return Ok(markets),
            }
        }
        bail!("ID lookup kept returning pages")
    }

    async fn get(&self, path: &str, query: &[(&str, &str)]) -> anyhow::Result<Vec<u8>> {
        let url = reqwest::Url::parse_with_params(&format!("{}{path}", self.base), query)?;
        let mut last_error = None;
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(1 << (attempt - 1))).await;
            }
            self.limiter.wait().await;
            match self.http.get(url.clone()).send().await {
                Ok(response) if response.status().is_success() => {
                    return Ok(response.bytes().await?.to_vec());
                }
                Ok(response) => {
                    let status = response.status();
                    if !(status.is_server_error() || status.as_u16() == 429) {
                        bail!("GET {path}: HTTP {status}");
                    }
                    last_error = Some(anyhow::anyhow!("GET {path}: HTTP {status}"));
                }
                Err(error) => last_error = Some(error.into()),
            }
        }
        Err(last_error
            .expect("at least one attempt")
            .context(format!("GET {path} failed after {ATTEMPTS} attempts")))
    }
}

impl Listings for GammaClient {
    async fn scan_page(
        &self,
        cursor: Option<String>,
        nonce: String,
    ) -> anyhow::Result<ListingPage> {
        let page = GammaClient::scan_page(self, cursor.as_deref(), &nonce).await?;
        Ok(ListingPage {
            rows: page.markets.iter().map(GammaMarket::listing).collect(),
            next_cursor: page.next_cursor,
        })
    }

    async fn lookup(&self, ids: Vec<ListingId>) -> anyhow::Result<Vec<Listing>> {
        let ids = ids
            .iter()
            .map(|id| {
                id.0.parse::<u64>()
                    .context("Gamma listing ID must be numeric")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let markets = GammaClient::lookup(self, &ids).await?;
        Ok(markets.iter().map(GammaMarket::listing).collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::exchange::polymarket::gamma::{GammaMarket, Version, parse_page};
    use crate::model::{ListingStatus, OrderBookId};

    const PAGE: &str = r#"{"markets":[
        {"id":"6216898","conditionId":"0x3f94538f33f3cb4c9354a3f79fa11cb86b797ba430549376edc572f2254042e4",
         "clobTokenIds":"[\"30817104840990144788138919630069689376658139439422189921067540741758064525128\", \"89439539499380048463248616563650612400000000000000000000000000000000000000001\"]",
         "active":true,"closed":false,"archived":false,"enableOrderBook":false,"acceptingOrders":true,
         "createdAt":"2026-10-25T18:36:46.071657Z","version":"v1","question":"ignored"},
        {"id":"7","conditionId":"0x3f94538f33f3cb4c9354a3f79fa11cb86b797ba430549376edc572f2254042e5",
         "clobTokenIds":"[\"1\",\"1\"]","active":true,"closed":false,"archived":false,"enableOrderBook":true,"acceptingOrders":true},
        {"id":"8","conditionId":"bad","clobTokenIds":"[\"1\",\"2\"]","active":true,"enableOrderBook":true,"acceptingOrders":true},
        {"id":"9","conditionId":"0x3f94538f33f3cb4c9354a3f79fa11cb86b797ba430549376edc572f2254042e6",
         "clobTokenIds":"[\"1\",\"2\"]","active":true,"closed":false,"enableOrderBook":true,"acceptingOrders":true,"version":"v2"},
        {"id":"not-a-number"}
    ],"next_cursor":"abc"}"#;

    #[test]
    fn parses_rows_and_applies_the_rule() {
        let page = parse_page(PAGE.as_bytes()).unwrap();
        assert_eq!(page.invalid, 1);
        assert_eq!(page.next_cursor.as_deref(), Some("abc"));
        let desired: Vec<u64> = page
            .markets
            .iter()
            .filter(|m| m.desired())
            .map(|m| m.gamma_id)
            .collect();
        // Accepting orders is desired regardless of enableOrderBook;
        // duplicate asset IDs, a bad condition ID and v2 are not.
        assert_eq!(desired, [6_216_898]);
        assert_eq!(page.markets[3].version, Version::V2);
        assert!(page.markets[1].order_books.is_none());
        assert!(page.markets[2].market.is_none());
    }

    #[test]
    fn accepting_orders_controls_eligibility_instead_of_order_book_flag() {
        for enabled in [false, true] {
            for accepting in [
                None,
                Some(serde_json::Value::Null),
                Some(false.into()),
                Some(true.into()),
            ] {
                let mut row = serde_json::json!({
                    "id": "1", "conditionId": format!("0x{:064x}", 1),
                    "clobTokenIds": r#"["1","2"]"#,
                    "active": true, "closed": false, "archived": false,
                    "enableOrderBook": enabled,
                });
                if let Some(value) = &accepting {
                    row["acceptingOrders"] = value.clone();
                }
                let bytes = serde_json::to_vec(&serde_json::json!({"markets": [row]})).unwrap();
                let market = parse_page(&bytes).unwrap().markets.remove(0);
                let expected = if accepting == Some(true.into()) {
                    ListingStatus::Desired
                } else {
                    ListingStatus::NotDesired
                };
                assert_eq!(
                    market.listing().status,
                    expected,
                    "enabled={enabled}, accepting={accepting:?}"
                );
            }
        }
    }

    #[test]
    fn unusable_token_lists_keep_rows_and_listing_status() {
        let condition = format!("0x{:064x}", 1);
        let token_lists = [
            None,
            Some("[]"),
            Some(r#"["1"]"#),
            Some(r#"["1","2","3"]"#),
            Some(r#"["1","1"]"#),
            Some(r#"["","2"]"#),
            Some(r#"["one","2"]"#),
        ];
        let mut rows = Vec::new();
        for tokens in token_lists {
            for (closed, accepting) in [(false, true), (true, true), (false, false)] {
                let mut row = serde_json::json!({
                    "id": (rows.len() + 1).to_string(),
                    "conditionId": condition,
                    "active": true,
                    "closed": closed,
                    "acceptingOrders": accepting,
                });
                if let Some(tokens) = tokens {
                    row["clobTokenIds"] = tokens.into();
                }
                rows.push(row);
            }
        }
        rows.push(serde_json::json!({
            "id": "100", "conditionId": condition,
            "active": true, "acceptingOrders": true,
            "clobTokenIds": r#"["20","10"]"#,
        }));
        let json = serde_json::to_vec(&serde_json::json!({"markets": rows})).unwrap();
        let page = parse_page(&json).unwrap();
        assert_eq!(page.invalid, 0);
        assert_eq!(page.markets.len(), token_lists.len() * 3 + 1);
        for market in &page.markets[..page.markets.len() - 1] {
            assert!(!market.desired());
            assert!(market.order_books.is_none());
            let listing = market.listing();
            assert_eq!(listing.listing_id.0, market.gamma_id.to_string());
            assert_eq!(listing.market, market.market);
            assert!(listing.order_books.is_empty());
            assert_eq!(
                listing.status,
                if market.closed {
                    ListingStatus::Closed
                } else {
                    ListingStatus::NotDesired
                }
            );
        }
        let valid = page.markets.last().unwrap().listing();
        assert_eq!(valid.status, ListingStatus::Desired);
        assert_eq!(
            valid.order_books,
            [OrderBookId::from("20"), OrderBookId::from("10")]
        );
    }

    #[test]
    fn closed_archived_inactive_or_not_accepting_are_not_desired() {
        let page = parse_page(PAGE.as_bytes()).unwrap();
        let base = page.markets[0].clone();
        assert!(base.desired());
        for change in [
            |m: &mut GammaMarket| m.closed = true,
            |m: &mut GammaMarket| m.archived = true,
            |m: &mut GammaMarket| m.active = false,
            |m: &mut GammaMarket| m.accepting_orders = false,
        ] {
            let mut market = base.clone();
            change(&mut market);
            assert!(!market.desired());
        }
    }
    #[test]
    fn recovery_requires_explicit_facts_and_preserves_missing_version_as_unknown() {
        use crate::model::RecoveryEligibility;
        let ready = serde_json::json!({"id":"1", "conditionId":format!("0x{:064x}",1),
            "clobTokenIds":"[\"1\",\"2\"]", "version":"v1", "active":true,
            "closed":false,"archived":false,"acceptingOrders":true});
        let parse = |row: serde_json::Value| {
            parse_page(&serde_json::to_vec(&serde_json::json!({"markets":[row]})).unwrap())
                .unwrap()
                .markets
                .remove(0)
                .recovery
        };
        assert_eq!(parse(ready.clone()), RecoveryEligibility::Eligible);
        let mut unknown = ready.clone();
        unknown.as_object_mut().unwrap().remove("acceptingOrders");
        assert_eq!(parse(unknown), RecoveryEligibility::Unknown);
        let mut unknown = ready.clone();
        unknown.as_object_mut().unwrap().remove("version");
        assert_eq!(parse(unknown), RecoveryEligibility::Unknown);
        let mut paused = ready.clone();
        paused["acceptingOrders"] = false.into();
        assert_eq!(parse(paused), RecoveryEligibility::Ineligible);
        let mut closed = ready;
        closed["closed"] = true.into();
        closed.as_object_mut().unwrap().remove("clobTokenIds");
        assert_eq!(parse(closed), RecoveryEligibility::Ineligible);
    }
}
