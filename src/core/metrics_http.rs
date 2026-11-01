//! Bounded private HTTP endpoint. Slow scrapes never enter the collection path.
use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{ALLOW, CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::core::metrics::{Metrics, STALE_AFTER, View};
use crate::core::subscriptions::SocketMarkets;

pub struct MetricsServer {
    pub address: SocketAddr,
    publisher_stop: CancellationToken,
    http_stop: CancellationToken,
    draining: CancellationToken,
    publisher: JoinHandle<anyhow::Result<()>>,
    http: JoinHandle<anyhow::Result<()>>,
}
impl MetricsServer {
    pub async fn start(
        address: SocketAddr,
        metrics: Metrics,
        fatal: CancellationToken,
        directory: watch::Receiver<SocketMarkets>,
    ) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(address)
            .await
            .context("bind metrics listener")?;
        let address = listener.local_addr()?;
        let publisher_stop = CancellationToken::new();
        let http_stop = CancellationToken::new();
        let draining = CancellationToken::new();
        let (views_tx, views_rx) = watch::channel(View::default());
        let stop = publisher_stop.clone();
        let fail = fatal.clone();
        let publisher = tokio::spawn(async move {
            let guard = fail.drop_guard();
            let result = metrics.run(views_tx, stop.clone()).await;
            if stop.is_cancelled() {
                guard.disarm();
            }
            result
        });
        let stop = http_stop.clone();
        let drain = draining.clone();
        let http = tokio::spawn(async move {
            let guard = fatal.drop_guard();
            let result = serve(listener, views_rx, directory, drain, stop.clone()).await;
            if stop.is_cancelled() {
                guard.disarm();
            }
            result
        });
        Ok(Self {
            address,
            publisher_stop,
            http_stop,
            draining,
            publisher,
            http,
        })
    }
    pub fn begin_shutdown(&self) {
        self.draining.cancel();
    }
    pub async fn finish(&mut self) -> anyhow::Result<()> {
        self.publisher_stop.cancel();
        (&mut self.publisher)
            .await
            .context("metrics publisher task")??;
        self.http_stop.cancel();
        (&mut self.http).await.context("metrics HTTP task")??;
        Ok(())
    }
}
impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.publisher_stop.cancel();
        self.http_stop.cancel();
        self.publisher.abort();
        self.http.abort();
    }
}
async fn serve(
    listener: TcpListener,
    views: watch::Receiver<View>,
    directory: watch::Receiver<SocketMarkets>,
    draining: CancellationToken,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let permits = std::sync::Arc::new(Semaphore::new(64));
    let mut clients = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => { clients.abort_all(); while clients.join_next().await.is_some() {} return Ok(()); }
            Some(_) = clients.join_next() => {}
            accepted = listener.accept() => {
                let (socket, _) = accepted.context("accept metrics client")?;
                let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                let views = views.clone(); let directory = directory.clone(); let drain = draining.clone();
                clients.spawn(async move {
                    let _permit = permit;
                    let service = service_fn(move |request| {
                        let view = views.borrow().clone(); let shutting_down = drain.is_cancelled();
                        let markets = request.uri().path().strip_prefix("/sockets/")
                            .and_then(|rest| rest.strip_suffix("/markets"))
                            .and_then(|id| id.parse::<u16>().ok())
                            .and_then(|id| directory.borrow().get(usize::from(id)).cloned());
                        async move { Ok::<_,Infallible>(respond(request, &view, shutting_down, markets.as_deref())) }
                    });
                    let mut builder = http1::Builder::new();
                    builder.keep_alive(false).max_buf_size(8192);
                    let _ = tokio::time::timeout(Duration::from_secs(5), builder.serve_connection(TokioIo::new(socket), service)).await;
                });
            }
        }
    }
}
fn respond(
    request: Request<Incoming>,
    view: &View,
    draining: bool,
    markets: Option<&[crate::model::MarketId]>,
) -> Response<Full<Bytes>> {
    let mut encoded = String::new();
    let mut response = Response::new(Full::new(Bytes::new()));
    let (status, body, content_type) = if request.method() != Method::GET {
        response
            .headers_mut()
            .insert(ALLOW, hyper::header::HeaderValue::from_static("GET"));
        (
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed\n",
            "text/plain",
        )
    } else if request.uri().to_string().len() > 2048
        || request.headers().contains_key(TRANSFER_ENCODING)
        || request
            .headers()
            .get(CONTENT_LENGTH)
            .is_some_and(|value| value != "0")
    {
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body not accepted\n",
            "text/plain",
        )
    } else {
        match request.uri().path() {
            "/metrics" => (StatusCode::OK, view.text.as_ref(), prometheus::TEXT_FORMAT),
            "/metrics/sockets" => (
                StatusCode::OK,
                view.sockets_text.as_ref(),
                prometheus::TEXT_FORMAT,
            ),
            "/healthz" => (StatusCode::OK, "ok\n", "text/plain"),
            "/readyz" => {
                let ready = !draining
                    && view.fresh
                    && view.initialized
                    && view.created.is_some_and(|at| at.elapsed() <= STALE_AFTER);
                (
                    if ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    if ready { "ready\n" } else { "not ready\n" },
                    "text/plain",
                )
            }
            _ if markets.is_some() => {
                let ids: Vec<&str> = markets
                    .into_iter()
                    .flatten()
                    .map(|market| market.0.as_ref())
                    .collect();
                match serde_json::to_string(&serde_json::json!({"markets": ids})) {
                    Ok(body) => {
                        encoded.push_str(&body);
                        (StatusCode::OK, encoded.as_str(), "application/json")
                    }
                    Err(_) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "encoding failed\n",
                        "text/plain",
                    ),
                }
            }
            _ => (StatusCode::NOT_FOUND, "not found\n", "text/plain"),
        }
    };
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        hyper::header::HeaderValue::from_static(content_type),
    );
    *response.body_mut() = Full::new(Bytes::copy_from_slice(body.as_bytes()));
    response
}

#[cfg(test)]
mod tests {
    use crate::core::metrics::Metrics;
    use crate::core::metrics_http::MetricsServer;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn socket_directory_routes_and_updates() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        use crate::core::subscriptions::SocketMarkets;
        use crate::model::MarketId;
        use std::sync::Arc;
        let (tx, rx) = tokio::sync::watch::channel(SocketMarkets::default());
        let mut server = MetricsServer::start(
            "127.0.0.1:0".parse().unwrap(),
            Metrics::default(),
            CancellationToken::new(),
            rx,
        )
        .await
        .unwrap();
        let client = reqwest::Client::new();
        let base = format!("http://{}", server.address);
        assert_eq!(
            client
                .get(format!("{base}/sockets/0/markets"))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        tx.send_replace(Arc::from([
            Arc::from([MarketId::from("0xabc")]),
            Arc::from([]),
        ]));
        for (path, status, expected) in [
            (
                "/sockets/0/markets",
                200,
                Some(serde_json::json!({"markets":["0xabc"]})),
            ),
            (
                "/sockets/1/markets",
                200,
                Some(serde_json::json!({"markets":[]})),
            ),
            ("/sockets/2/markets", 404, None),
            ("/sockets/no/markets", 404, None),
            ("/sockets/65536/markets", 404, None),
            ("/sockets/0/markets/extra", 404, None),
        ] {
            let response = client.get(format!("{base}{path}")).send().await.unwrap();
            assert_eq!(response.status(), status);
            if let Some(expected) = expected {
                assert_eq!(response.headers()["content-type"], "application/json");
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&response.text().await.unwrap())
                        .unwrap(),
                    expected
                );
            }
        }
        assert_eq!(
            client
                .post(format!("{base}/sockets/0/markets"))
                .send()
                .await
                .unwrap()
                .status(),
            405
        );
        tx.send_replace(Arc::from([Arc::from([])]));
        let response = client
            .get(format!("{base}/sockets/0/markets"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&response.text().await.unwrap()).unwrap(),
            serde_json::json!({"markets":[]})
        );
        server.finish().await.unwrap();
    }

    #[tokio::test]
    async fn unexpected_http_task_loss_signals_service_failure() {
        let failure = CancellationToken::new();
        let mut server = MetricsServer::start(
            "127.0.0.1:0".parse().unwrap(),
            Metrics::default(),
            failure.clone(),
            tokio::sync::watch::channel(Default::default()).1,
        )
        .await
        .unwrap();
        tokio::task::yield_now().await;
        server.http.abort();
        tokio::time::timeout(Duration::from_secs(1), failure.cancelled())
            .await
            .unwrap();
        assert!(server.finish().await.is_err());
    }
}
