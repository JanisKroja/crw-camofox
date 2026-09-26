#![cfg(feature = "camofox")]
//! Behavioural tests for the Camofox (camofox-browser REST) renderer tier.
//! A small axum app emulates the camofox-browser `:9377` REST surface so we can
//! assert the navigate→wait→evaluate→close round-trip and `FetchResult` mapping
//! without a live Firefox.

use std::collections::HashMap;
use std::time::Duration;

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use crw_core::Deadline;
use crw_renderer::camofox::CamofoxRenderer;
use crw_renderer::traits::PageFetcher;
use serde_json::{Value, json};
use tokio::net::TcpListener;

const RENDERED_HTML: &str = "<html><body><h1>camofox rendered</h1></body></html>";

async fn create_tab(Json(body): Json<Value>) -> impl IntoResponse {
    // The real camofox-browser requires both userId and sessionKey.
    if body.get("sessionKey").and_then(|v| v.as_str()).is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "userId and sessionKey required" })),
        );
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "tabId": "tab-1", "sessionKey": "s-1" })),
    )
}

async fn navigate(Path(_id): Path<String>, Json(body): Json<Value>) -> impl IntoResponse {
    if body.get("url").and_then(|v| v.as_str()).is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "url required" })),
        );
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "url": body["url"] })),
    )
}

/// camofox's navigate on a huge page: the navigation itself succeeded, but the
/// route's post-navigation ARIA snapshot timed out and it answers 500 with a
/// sanitized body.
async fn navigate_snapshot_timeout(
    Path(_id): Path<String>,
    Json(_body): Json<Value>,
) -> impl IntoResponse {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "Internal server error" })),
    )
}

/// Evaluate for a tab whose navigation committed: `location.href` answers the
/// target, anything else the rendered document.
async fn evaluate_committed(Path(_id): Path<String>, Json(body): Json<Value>) -> Json<Value> {
    if body["expression"]
        .as_str()
        .is_some_and(|e| e.contains("location.href"))
    {
        return Json(json!({
            "ok": true, "result": "https://93.184.215.14/huge", "resultType": "string", "truncated": false
        }));
    }
    Json(json!({ "ok": true, "result": RENDERED_HTML, "resultType": "string", "truncated": false }))
}

/// Evaluate for a tab whose navigation never committed (still about:blank).
async fn evaluate_blank(Path(_id): Path<String>, Json(body): Json<Value>) -> Json<Value> {
    if body["expression"]
        .as_str()
        .is_some_and(|e| e.contains("location.href"))
    {
        return Json(
            json!({ "ok": true, "result": "about:blank", "resultType": "string", "truncated": false }),
        );
    }
    Json(json!({
        "ok": true, "result": "<html><head></head><body></body></html>", "resultType": "string", "truncated": false
    }))
}

async fn wait(Path(_id): Path<String>, Json(_body): Json<Value>) -> Json<Value> {
    Json(json!({ "ok": true }))
}

/// A `/tabs` handler that hangs far longer than any test deadline — models a
/// stalled camofox navigate (Google `/sorry` interstitial, dead upstream).
async fn create_tab_stalls(Json(_body): Json<Value>) -> impl IntoResponse {
    tokio::time::sleep(Duration::from_secs(30)).await;
    (StatusCode::OK, Json(json!({ "tabId": "tab-slow" })))
}

/// A `/tabs` handler that fails the way camofox does when a persistent profile
/// is pinned to an older Camoufox build: HTTP 500 with the reason in `error`.
async fn create_tab_profile_mismatch(Json(_body): Json<Value>) -> impl IntoResponse {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "error": "Profile for user \"crw\" was created with Camoufox 135.0.1-beta.24, but the current version is 152.0.4-beta.28"
        })),
    )
}

/// A `/tabs` handler failing through a proxy: non-JSON body that must not be
/// echoed into the renderer error.
async fn create_tab_html_error(Json(_body): Json<Value>) -> impl IntoResponse {
    (
        StatusCode::BAD_GATEWAY,
        "<html><body>Bad Gateway at /internal/x</body></html>",
    )
}

/// `/tabs` that fails the first two creates the way camofox does right after a
/// context teardown (`window is null`), then succeeds — the transient the
/// renderer must ride out.
static FLAKY_CREATES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
async fn create_tab_flaky(Json(body): Json<Value>) -> axum::response::Response {
    let n = FLAKY_CREATES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    if n < 2 {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "browserContext.newPage: Protocol error (Browser.newPage): can't access property \"delayedStartupPromise\", window is null"
            })),
        )
            .into_response();
    }
    create_tab(Json(body)).await.into_response()
}

/// Final document URL the default mocks report: a public literal address, so the
/// outbound check needs no DNS.
const PUBLIC_FINAL_URL: &str = "https://93.184.215.14/";

async fn evaluate(Path(_id): Path<String>, Json(body): Json<Value>) -> Json<Value> {
    if body["expression"]
        .as_str()
        .is_some_and(|e| e.contains("location.href"))
    {
        return Json(
            json!({ "ok": true, "result": PUBLIC_FINAL_URL, "resultType": "string", "truncated": false }),
        );
    }
    Json(json!({
        "ok": true,
        "result": RENDERED_HTML,
        "resultType": "string",
        "truncated": false,
    }))
}

/// Shared per-mock state for the challenge tests: how many challenge probes
/// have been answered, how many should say "still challenged" before
/// clearing, which probe (if any) fails once with a 500, and the title a
/// challenged probe reports.
#[derive(Clone)]
struct ChallengeState {
    probes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    challenged_for: usize,
    fail_probe: Option<usize>,
    title: &'static str,
}

const CHALLENGE_HTML: &str = "<html><head><title>Just a moment...</title></head><body><script src=\"/cdn-cgi/challenge-platform/h/b/orchestrate/chl_page/v1\"></script></body></html>";

/// Evaluate that answers the challenge probe from `ChallengeState`, the
/// location and status probes like the plain mock, and the outerHTML evaluate
/// with the challenge page until it has cleared.
async fn evaluate_challenge(
    axum::extract::State(st): axum::extract::State<ChallengeState>,
    Path(_id): Path<String>,
    Json(body): Json<Value>,
) -> axum::response::Response {
    use std::sync::atomic::Ordering;
    let expr = body["expression"].as_str().unwrap_or_default();
    let ok = |result: String| {
        Json(json!({ "ok": true, "result": result, "resultType": "string", "truncated": false }))
            .into_response()
    };
    if expr.contains("location.href") {
        return ok(PUBLIC_FINAL_URL.to_string());
    }
    if expr.contains("document.title") {
        let n = st.probes.fetch_add(1, Ordering::SeqCst);
        if st.fail_probe == Some(n) {
            // The challenge clears by reloading the tab; an evaluate that lands
            // in that window fails.
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Execution context was destroyed" })),
            )
                .into_response();
        }
        let challenged = n < st.challenged_for;
        let probe = if challenged {
            json!({ "t": st.title, "m": true })
        } else {
            json!({ "t": "Real page", "m": false })
        };
        return ok(probe.to_string());
    }
    let cleared = st.probes.load(Ordering::SeqCst) > st.challenged_for;
    ok(if cleared {
        RENDERED_HTML
    } else {
        CHALLENGE_HTML
    }
    .to_string())
}

async fn spawn_challenge_mock(
    challenged_for: usize,
    fail_probe: Option<usize>,
    title: &'static str,
) -> (String, ChallengeState) {
    let st = ChallengeState {
        probes: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        challenged_for,
        fail_probe,
        title,
    };
    let app = Router::new()
        .route("/tabs", post(create_tab))
        .route("/tabs/{id}/navigate", post(navigate))
        .route("/tabs/{id}/wait", post(wait))
        .route("/tabs/{id}/evaluate", post(evaluate_challenge))
        .route("/tabs/{id}", delete(close_tab))
        .route("/health", get(health))
        .with_state(st.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), st)
}

/// A document larger than camofox's 1 MiB single-result cap: the plain
/// outerHTML evaluate answers with the truncation placeholder, and the
/// renderer must fall back to slicing. ASCII only, so byte, char and UTF-16
/// offsets coincide in the mock.
fn big_html() -> &'static String {
    static BIG: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BIG.get_or_init(|| {
        let mut s = String::from("<html><body>");
        while s.len() < 700_000 {
            s.push_str("<p>chunked-render-payload-0123456789</p>");
        }
        s.push_str("<h1>the end</h1></body></html>");
        s
    })
}

async fn evaluate_big(Path(_id): Path<String>, Json(body): Json<Value>) -> Json<Value> {
    let expr = body["expression"].as_str().unwrap_or_default();
    if expr.contains("location.href") {
        return Json(
            json!({ "ok": true, "result": PUBLIC_FINAL_URL, "resultType": "string", "truncated": false }),
        );
    }
    let doc = big_html();
    if expr.contains("document.title") {
        return Json(
            json!({ "ok": true, "result": r#"{"t":"big","m":false}"#, "resultType": "string", "truncated": false }),
        );
    }
    if expr == "document.documentElement.outerHTML" {
        return Json(json!({
            "ok": true,
            "result": format!("[Truncated: result was {} bytes, max 1048576]", doc.len() + 2),
            "resultType": "string",
            "truncated": true,
        }));
    }
    if expr.contains("outerHTML.length") {
        return Json(
            json!({ "ok": true, "result": doc.len().to_string(), "resultType": "string", "truncated": false }),
        );
    }
    // `(function(s,a,b){...})(document.documentElement.outerHTML,A,B)`
    let args = expr
        .rsplit_once("outerHTML,")
        .map(|(_, tail)| tail.trim_end_matches(')'))
        .unwrap();
    let (a, b) = args.split_once(',').unwrap();
    let (a, b): (usize, usize) = (a.parse().unwrap(), b.parse().unwrap());
    Json(
        json!({ "ok": true, "result": &doc[a..b.min(doc.len())], "resultType": "string", "truncated": false }),
    )
}

async fn close_tab(Path(_id): Path<String>, Json(_body): Json<Value>) -> Json<Value> {
    Json(json!({ "ok": true }))
}

async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "engine": "camoufox", "browserConnected": true }))
}

async fn spawn_camofox_mock() -> String {
    let app = Router::new()
        .route("/tabs", post(create_tab))
        .route("/tabs/{id}/navigate", post(navigate))
        .route("/tabs/{id}/wait", post(wait))
        .route("/tabs/{id}/evaluate", post(evaluate))
        .route("/tabs/{id}", delete(close_tab))
        .route("/health", get(health));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn deadline() -> Deadline {
    Deadline::now_plus(Duration::from_secs(30))
}

#[tokio::test]
async fn fetch_returns_evaluated_html() {
    let base = spawn_camofox_mock().await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    let result = renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("camofox fetch should succeed against the mock");

    assert_eq!(result.status_code, 200);
    assert!(
        result.html.contains("camofox rendered"),
        "expected evaluated outerHTML, got: {}",
        result.html
    );
    assert_eq!(result.rendered_with.as_deref(), Some("camofox"));
}

#[tokio::test]
async fn name_and_js_support() {
    let renderer = CamofoxRenderer::new(
        "camofox",
        "http://127.0.0.1:1",
        None,
        Duration::from_secs(5),
    );
    assert_eq!(renderer.name(), "camofox");
    assert!(renderer.supports_js());
}

#[tokio::test]
async fn is_available_reads_health() {
    let base = spawn_camofox_mock().await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(5));
    assert!(renderer.is_available().await);
}

#[tokio::test]
async fn fetch_bounded_by_deadline_not_client_timeout() {
    // The client timeout (10s) is far longer than the caller deadline (600ms).
    // A stalled navigate must surface as a deadline-bounded failure quickly,
    // NOT run for the full client timeout — the PageFetcher contract the
    // failover ladder relies on to move to the next tier / return 504.
    let app = Router::new().route("/tabs", post(create_tab_stalls));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    let started = std::time::Instant::now();
    let res = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(600)),
        )
        .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(res, Err(crw_core::error::CrwError::Timeout(_))),
        "a stalled navigate must surface as Timeout (→504), got {res:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "must be bounded by the ~600ms deadline, not the 10s client timeout; took {elapsed:?}"
    );
}

#[tokio::test]
async fn fetch_fails_when_deadline_expired() {
    let renderer = CamofoxRenderer::new(
        "camofox",
        "http://127.0.0.1:1",
        None,
        Duration::from_secs(5),
    );
    let expired = Deadline::now_plus(Duration::from_millis(0));
    let res = renderer
        .fetch("https://example.com", &HashMap::new(), None, expired)
        .await;
    assert!(
        res.is_err(),
        "expired deadline should short-circuit before any HTTP call"
    );
}

#[tokio::test]
async fn fetch_error_carries_camofox_message() {
    // A failed camofox call must surface the server's own `error` text, not
    // just the status — it is what tells a profile-version pin apart from a
    // crashed browser.
    let app = Router::new().route("/tabs", post(create_tab_profile_mismatch));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(5));

    let err = renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect_err("500 from /tabs must fail the fetch");
    let msg = err.to_string();
    assert!(msg.contains("camofox /tabs returned 500"), "{msg}");
    assert!(
        msg.contains("was created with Camoufox 135.0.1-beta.24"),
        "{msg}"
    );
}

#[tokio::test]
async fn fetch_error_omits_non_json_body() {
    let app = Router::new().route("/tabs", post(create_tab_html_error));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(5));

    let err = renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect_err("502 from /tabs must fail the fetch");
    let msg = err.to_string();
    assert!(
        msg.ends_with("camofox /tabs returned 502 Bad Gateway"),
        "{msg}"
    );
    assert!(!msg.contains("<html"), "{msg}");
}

#[tokio::test]
async fn fetch_retries_transient_tab_create_failure() {
    FLAKY_CREATES.store(0, std::sync::atomic::Ordering::SeqCst);
    let app = Router::new()
        .route("/tabs", post(create_tab_flaky))
        .route("/tabs/{id}/navigate", post(navigate))
        .route("/tabs/{id}/wait", post(wait))
        .route("/tabs/{id}/evaluate", post(evaluate))
        .route("/tabs/{id}", delete(close_tab));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(5));

    let result = renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("two transient 500s on /tabs must be retried through");
    assert!(result.html.contains("camofox rendered"));
    assert_eq!(FLAKY_CREATES.load(std::sync::atomic::Ordering::SeqCst), 3);
}

#[tokio::test]
async fn fetch_gives_up_on_persistent_tab_create_failure() {
    // Always 500: after the retry budget the error surfaces (bounded, not a hang).
    let app = Router::new().route("/tabs", post(create_tab_profile_mismatch));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(5));

    let started = std::time::Instant::now();
    let err = renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect_err("persistent 500 must fail");
    assert!(
        err.to_string().contains("camofox /tabs returned 500"),
        "{err}"
    );
    // 3 retries with 0.5 s / 1 s / 2 s pauses ≈ 3.5 s; anything near the 30 s
    // deadline would mean the retry loop is not bounded by the attempt count.
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "retries must stay bounded"
    );
}

/// A pinned JS renderer implies `renderJs=true`. When the HTTP tier fails on
/// that path (here: an origin slower than the HTTP timeout) the request must
/// escalate to the renderer, not surface the HTTP tier's error.
#[tokio::test]
async fn render_js_true_escalates_when_http_tier_fails() {
    use crw_core::config::{CamofoxEndpoint, RendererConfig, RendererMode, StealthConfig};
    use crw_renderer::FallbackRenderer;
    use wiremock::matchers::{method, path as wpath};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // SAFETY: this test binary owns its process env.
    unsafe { std::env::set_var("CRW_ALLOW_LOOPBACK_FOR_TESTS", "1") };
    let camofox = spawn_camofox_mock().await;
    let origin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wpath("/slow"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("<html>too late</html>")
                .set_delay(Duration::from_secs(3)),
        )
        .mount(&origin)
        .await;

    let cfg = RendererConfig {
        mode: RendererMode::Camofox,
        camofox: Some(CamofoxEndpoint {
            base_url: camofox,
            api_key: None,
            challenge_wait_ms: 20_000,
            clearance_reuse: true,
            reap_orphan_tabs: true,
            manage: false,
        }),
        http_timeout_ms: Some(300),
        ..Default::default()
    };
    let renderer = FallbackRenderer::new(&cfg, "crw-test", None, &StealthConfig::default())
        .expect("camofox-mode renderer builds");

    let result = renderer
        .fetch(
            &format!("{}/slow", origin.uri()),
            &HashMap::new(),
            Some(true),
            None,
            Some("camofox"),
            Deadline::now_plus(Duration::from_secs(30)),
        )
        .await
        .expect("HTTP-tier timeout must escalate to the pinned renderer");
    assert_eq!(result.rendered_with.as_deref(), Some("camofox"));
    assert!(result.html.contains("camofox rendered"));
}

#[tokio::test]
async fn fetch_reassembles_document_over_camofox_result_cap() {
    let app = Router::new()
        .route("/tabs", post(create_tab))
        .route("/tabs/{id}/navigate", post(navigate))
        .route("/tabs/{id}/wait", post(wait))
        .route("/tabs/{id}/evaluate", post(evaluate_big))
        .route("/tabs/{id}", delete(close_tab));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    let result = renderer
        .fetch("https://example.com/big", &HashMap::new(), None, deadline())
        .await
        .expect("a document over the evaluate cap must be fetched in slices");
    assert_eq!(
        result.html.len(),
        big_html().len(),
        "reassembled document must be complete"
    );
    assert_eq!(&result.html, big_html());
    assert!(result.html.ends_with("<h1>the end</h1></body></html>"));
}

#[tokio::test]
async fn fetch_continues_when_only_the_post_navigation_snapshot_failed() {
    let app = Router::new()
        .route("/tabs", post(create_tab))
        .route("/tabs/{id}/navigate", post(navigate_snapshot_timeout))
        .route("/tabs/{id}/wait", post(wait))
        .route("/tabs/{id}/evaluate", post(evaluate_committed))
        .route("/tabs/{id}", delete(close_tab));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(5));

    let result = renderer
        .fetch(
            "https://example.com/huge",
            &HashMap::new(),
            None,
            deadline(),
        )
        .await
        .expect("a navigate failure after the page committed must not fail the render");
    assert!(result.html.contains("camofox rendered"));
}

#[tokio::test]
async fn fetch_fails_when_navigate_failed_and_tab_stayed_blank() {
    let app = Router::new()
        .route("/tabs", post(create_tab))
        .route("/tabs/{id}/navigate", post(navigate_snapshot_timeout))
        .route("/tabs/{id}/wait", post(wait))
        .route("/tabs/{id}/evaluate", post(evaluate_blank))
        .route("/tabs/{id}", delete(close_tab));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(5));

    let err = renderer
        .fetch(
            "https://example.com/dead",
            &HashMap::new(),
            None,
            deadline(),
        )
        .await
        .expect_err("a navigate failure with the tab still blank is a real failure");
    assert!(err.to_string().contains("navigate returned 500"), "{err}");
    // camofox-browser sanitizes the Firefox error (NS_ERROR_UNKNOWN_HOST etc.)
    // to "Internal server error", so the blank tab is the only evidence the
    // page never loaded. The error must say so, or the ladder cannot attribute
    // a dead origin to the caller (422) and books it as our 500.
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("navigation failed"),
        "a navigate that never left about:blank must read as a navigation failure: {err}"
    );
}

/// A `/wait` that outlives the request deadline, so the evaluate after it finds
/// no budget left.
async fn wait_stalls(Path(_id): Path<String>, Json(_body): Json<Value>) -> Json<Value> {
    tokio::time::sleep(Duration::from_secs(5)).await;
    Json(json!({ "ok": true }))
}

#[tokio::test]
async fn spent_budget_reports_the_requested_deadline_not_zero() {
    // The wait eats the whole deadline, so the evaluate sees a zero budget. It
    // must report the budget the caller gave (1200ms), not `Timeout(0)`, which
    // reads as "timed out after 0ms" to a caller who allowed 1.2s.
    let app = Router::new()
        .route("/tabs", post(create_tab))
        .route("/tabs/{id}/navigate", post(navigate))
        .route("/tabs/{id}/wait", post(wait_stalls))
        .route("/tabs/{id}/evaluate", post(evaluate))
        .route("/tabs/{id}", delete(close_tab));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let renderer = CamofoxRenderer::new(
        "camofox",
        &format!("http://{addr}"),
        None,
        Duration::from_secs(10),
    );

    let res = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::from_request_ms(1_200),
        )
        .await;
    assert!(
        matches!(res, Err(crw_core::error::CrwError::Timeout(1_200))),
        "expected Timeout(1200), got {res:?}"
    );
}

/// Firefox's own error page (port blocked, DNS failure, refused connection).
/// `location.href` keeps the requested URL; `document.documentURI` is the
/// `about:neterror` page, whose text must never ship as the scrape.
async fn evaluate_neterror(Path(_id): Path<String>, Json(body): Json<Value>) -> Json<Value> {
    if body["expression"]
        .as_str()
        .is_some_and(|e| e.contains("location.href"))
    {
        return Json(json!({
            "ok": true,
            "result": "about:neterror?e=deniedPortAccess&u=https%3A//1.1.1.1%3A9/&c=UTF-8",
            "resultType": "string",
            "truncated": false
        }));
    }
    Json(json!({
        "ok": true,
        "result": "<html><head><title>Problem loading page</title></head><body>This address is restricted</body></html>",
        "resultType": "string",
        "truncated": false
    }))
}

#[tokio::test]
async fn firefox_error_page_is_a_navigation_failure_not_content() {
    // Both shapes seen live: navigate answers 500 (sanitized) and the tab holds
    // the error page, or navigate answers 200 and the page later lands on one.
    for navigate_handler in [true, false] {
        let app = Router::new()
            .route("/tabs", post(create_tab))
            .route("/tabs/{id}/wait", post(wait))
            .route("/tabs/{id}/evaluate", post(evaluate_neterror))
            .route("/tabs/{id}", delete(close_tab));
        let app = if navigate_handler {
            app.route("/tabs/{id}/navigate", post(navigate_snapshot_timeout))
        } else {
            app.route("/tabs/{id}/navigate", post(navigate))
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let renderer = CamofoxRenderer::new(
            "camofox",
            &format!("http://{addr}"),
            None,
            Duration::from_secs(10),
        );
        let err = renderer
            .fetch("https://1.1.1.1:9/", &HashMap::new(), None, deadline())
            .await
            .expect_err("a Firefox error page must not be returned as the page");
        let msg = err.to_string();
        assert!(msg.contains("navigation failed"), "{msg}");
        assert!(msg.contains("deniedPortAccess"), "{msg}");
    }
}

fn probes(st: &ChallengeState) -> usize {
    st.probes.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test]
async fn challenge_clears_after_n_polls() {
    let (base, st) = spawn_challenge_mock(2, None, "Just a moment...").await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_challenge_poll_interval(Duration::from_millis(50));

    let result = renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("fetch succeeds once the challenge clears");

    assert!(
        result.html.contains("camofox rendered"),
        "got: {}",
        result.html
    );
    assert_eq!(probes(&st), 3, "two challenged probes then one clear probe");
}

#[tokio::test]
async fn challenge_loop_stops_at_deadline() {
    let (base, _st) = spawn_challenge_mock(usize::MAX, None, "Just a moment...").await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_challenge_poll_interval(Duration::from_millis(50));

    let started = std::time::Instant::now();
    let result = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_secs(3)),
        )
        .await
        .expect("a stuck challenge still yields the on-screen html");

    assert!(
        started.elapsed() < Duration::from_millis(3_500),
        "loop must not outlive the deadline, took {:?}",
        started.elapsed()
    );
    assert!(
        result.html.contains("challenge-platform/h/"),
        "got: {}",
        result.html
    );
}

#[tokio::test]
async fn challenge_loop_disabled_when_wait_is_zero() {
    let (base, st) = spawn_challenge_mock(usize::MAX, None, "Just a moment...").await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_challenge_wait(Duration::ZERO);

    let result = renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("fetch succeeds");

    assert_eq!(probes(&st), 0, "no probe when disabled");
    assert!(result.html.contains("challenge-platform/h/"));
}

/// A hard Cloudflare block never clears, so polling it only burns the budget.
/// Counted by probes, not wall clock.
#[tokio::test]
async fn attention_required_wall_is_not_polled() {
    let (base, st) =
        spawn_challenge_mock(usize::MAX, None, "Attention Required! | Cloudflare").await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_challenge_poll_interval(Duration::from_millis(50));

    renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("fetch returns the wall for the ladder to classify");

    assert_eq!(
        probes(&st),
        1,
        "a terminal wall is probed once, never polled"
    );
}

/// The challenge clears by reloading the tab, and an evaluate landing in that
/// window fails. One failure must not end the wait.
#[tokio::test]
async fn one_failed_probe_does_not_end_the_wait() {
    let (base, st) = spawn_challenge_mock(2, Some(1), "Just a moment...").await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_challenge_poll_interval(Duration::from_millis(50));

    let result = renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("fetch succeeds");

    assert!(
        result.html.contains("camofox rendered"),
        "got: {}",
        result.html
    );
    assert!(probes(&st) >= 3, "the loop kept probing after the failure");
}

const FIREFOX_UA: &str = "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0";

/// Evaluate that also answers `navigator.userAgent`.
async fn evaluate_with_ua(Path(id): Path<String>, Json(body): Json<Value>) -> Json<Value> {
    if body["expression"].as_str() == Some("navigator.userAgent") {
        return Json(
            json!({ "ok": true, "result": FIREFOX_UA, "resultType": "string", "truncated": false }),
        );
    }
    evaluate(Path(id), Json(body)).await
}

async fn cookies_with_clearance(
    Path(_id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> axum::response::Response {
    if !q.contains_key("userId") {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "userId required" })),
        )
            .into_response();
    }
    Json(json!([
        { "name": "cf_clearance", "value": "abc123", "domain": ".example.com", "path": "/", "expires": 4_102_444_800.0 },
        { "name": "__cf_bm", "value": "bm", "domain": ".example.com", "path": "/", "expires": -1 },
        { "name": "cf_clearance", "value": "elsewhere", "domain": ".other.test", "path": "/", "expires": -1 }
    ]))
    .into_response()
}

async fn cookies_without_clearance(Path(_id): Path<String>) -> Json<Value> {
    Json(json!([
        { "name": "session", "value": "s", "domain": "example.com", "path": "/", "expires": -1 }
    ]))
}

/// The browser context holds a `cf_clearance`, but for a different site.
async fn cookies_clearance_for_other_site(Path(_id): Path<String>) -> Json<Value> {
    Json(json!([
        { "name": "cf_clearance", "value": "elsewhere", "domain": ".other.test", "path": "/", "expires": -1 }
    ]))
}

/// Evaluate whose document is a challenge page (cookies present, but the html
/// must veto the capture).
async fn evaluate_challenge_html(Path(id): Path<String>, Json(body): Json<Value>) -> Json<Value> {
    let expr = body["expression"].as_str().unwrap_or_default();
    if expr == "navigator.userAgent" || expr.contains("location.href") {
        return evaluate_with_ua(Path(id), Json(body)).await;
    }
    Json(
        json!({ "ok": true, "result": CHALLENGE_HTML, "resultType": "string", "truncated": false }),
    )
}

async fn spawn_cookie_mock(
    evaluate_route: axum::routing::MethodRouter,
    cookies_route: axum::routing::MethodRouter,
) -> String {
    let app = Router::new()
        .route("/tabs", post(create_tab))
        .route("/tabs/{id}/navigate", post(navigate))
        .route("/tabs/{id}/wait", post(wait))
        .route("/tabs/{id}/evaluate", evaluate_route)
        .route("/tabs/{id}/cookies", cookies_route)
        .route("/tabs/{id}", delete(close_tab))
        .route("/health", get(health));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn clearance_cached_when_cf_clearance_present() {
    use crw_renderer::clearance::ClearanceCache;
    let base = spawn_cookie_mock(post(evaluate_with_ua), get(cookies_with_clearance)).await;
    let cache = std::sync::Arc::new(ClearanceCache::with_defaults());
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_clearance_cache(cache.clone());

    renderer
        .fetch(
            "https://www.example.com/page",
            &HashMap::new(),
            None,
            deadline(),
        )
        .await
        .expect("fetch succeeds");

    let entry = cache
        .get("example.com")
        .await
        .expect("cf_clearance cached for the host");
    assert_eq!(entry.user_agent, FIREFOX_UA);
    // Only this site's cookies: the jar is context-wide, measured live.
    assert_eq!(
        entry.cookie_header("www.example.com"),
        "cf_clearance=abc123; __cf_bm=bm"
    );
    assert_eq!(
        entry.cookies.len(),
        2,
        "other sites' cookies are not stored"
    );
}

#[tokio::test]
async fn clearance_not_cached_without_cf_clearance() {
    use crw_renderer::clearance::ClearanceCache;
    let base = spawn_cookie_mock(post(evaluate_with_ua), get(cookies_without_clearance)).await;
    let cache = std::sync::Arc::new(ClearanceCache::with_defaults());
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_clearance_cache(cache.clone());

    renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("fetch succeeds");

    assert!(cache.get("example.com").await.is_none());
}

/// The cookies endpoint returns every cookie in the browser context, so a
/// clearance earned on another site must not be taken as this host's.
#[tokio::test]
async fn clearance_for_another_site_is_not_cached_for_this_host() {
    use crw_renderer::clearance::ClearanceCache;
    let base = spawn_cookie_mock(
        post(evaluate_with_ua),
        get(cookies_clearance_for_other_site),
    )
    .await;
    let cache = std::sync::Arc::new(ClearanceCache::with_defaults());
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_clearance_cache(cache.clone());

    renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("fetch succeeds");

    assert!(cache.get("example.com").await.is_none());
}

#[tokio::test]
async fn clearance_not_cached_on_challenge_html() {
    use crw_renderer::clearance::ClearanceCache;
    let base = spawn_cookie_mock(post(evaluate_challenge_html), get(cookies_with_clearance)).await;
    let cache = std::sync::Arc::new(ClearanceCache::with_defaults());
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_clearance_cache(cache.clone())
        .with_challenge_wait(Duration::ZERO);

    renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("fetch returns the challenge html");

    assert!(
        cache.get("example.com").await.is_none(),
        "a challenge page must not seed the cache"
    );
}

/// Evaluate whose document answers 404 in its Navigation Timing entry.
async fn evaluate_not_found(Path(id): Path<String>, Json(body): Json<Value>) -> Json<Value> {
    if body["expression"]
        .as_str()
        .is_some_and(|e| e.contains("performance.getEntriesByType"))
    {
        return Json(
            json!({ "ok": true, "result": "404", "resultType": "string", "truncated": false }),
        );
    }
    evaluate(Path(id), Json(body)).await
}

/// Item 3b: a camofox render reports the document's real HTTP status, not a
/// synthetic 200.
#[tokio::test]
async fn fetch_reports_the_documents_real_status() {
    let base = spawn_cookie_mock(post(evaluate_not_found), get(cookies_without_clearance)).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    let result = renderer
        .fetch(
            "https://example.com/missing",
            &HashMap::new(),
            None,
            deadline(),
        )
        .await
        .expect("fetch succeeds");

    assert_eq!(result.status_code, 404);
}

/// Without a usable status probe (an HTML answer, as older servers give for
/// an unknown expression) the render keeps reporting 200, as before.
#[tokio::test]
async fn fetch_falls_back_to_200_when_the_status_probe_is_unusable() {
    let base = spawn_camofox_mock().await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    let result = renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("fetch succeeds");

    assert_eq!(result.status_code, 200);
}

/// Live: a real Camoufox browser waits out a challenge page that clears itself
/// after 5 s, and the clearance it earns is captured. Needs a camofox-browser
/// that can reach the challenge server, so it is ignored by default:
///
/// `CRW_ALLOW_LOOPBACK_FOR_TESTS=1` lets the final-URL guard accept the
/// private challenge host:
///
/// ```text
/// CRW_ALLOW_LOOPBACK_FOR_TESTS=1 \
/// CRW_CAMOFOX_LIVE_BASE=http://127.0.0.1:9377 CRW_CAMOFOX_LIVE_KEY=<key> \
/// CRW_CAMOFOX_LIVE_CHALLENGE_URL=http://host.docker.internal:18777/ \
/// cargo test -p crw-renderer --features camofox --test camofox_tests \
///   live_challenge -- --ignored
/// ```
#[tokio::test]
#[ignore = "needs a live camofox-browser and challenge server"]
async fn live_challenge_wait_clears_and_captures_clearance() {
    use crw_renderer::clearance::ClearanceCache;
    let base = std::env::var("CRW_CAMOFOX_LIVE_BASE").expect("CRW_CAMOFOX_LIVE_BASE");
    let key = std::env::var("CRW_CAMOFOX_LIVE_KEY").ok();
    let url = std::env::var("CRW_CAMOFOX_LIVE_CHALLENGE_URL").expect("challenge url");
    let host = url::Url::parse(&url)
        .unwrap()
        .host_str()
        .unwrap()
        .to_owned();
    let cache = std::sync::Arc::new(ClearanceCache::with_defaults());
    let renderer = CamofoxRenderer::new("camofox", &base, key, Duration::from_secs(60))
        .with_clearance_cache(cache.clone());

    let started = std::time::Instant::now();
    let result = renderer
        .fetch(
            &url,
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_secs(60)),
        )
        .await
        .expect("live fetch");

    assert!(
        result.html.contains("Cleared content"),
        "the challenge must clear during the wait; got {}",
        &result.html[..result.html.len().min(300)]
    );
    assert!(
        started.elapsed() >= Duration::from_secs(4),
        "the page only clears after 5 s, so the loop must have waited"
    );
    let entry = cache.get(&host).await.expect("cf_clearance captured");
    assert!(
        entry
            .cookie_header(&host)
            .contains("cf_clearance=live-token")
    );
    assert!(!entry.user_agent.is_empty());
}

// ---------------------------------------------------------------------------
// Tab lifecycle under failure. Two ways a tab outlives its fetch: the create
// response never arrives (the id is never learned), and the fetch future is
// cancelled with the tab open. The mock reproduces the server's real ordering —
// a tab is REGISTERED (so `GET /tabs` lists it) BEFORE the create responds with
// its id — which is what makes both leaks possible and what the reap must be
// safe against. Listings are DERIVED from registered-minus-closed rather than
// scripted, so `confirm_gone` behaves like the real server.
// ---------------------------------------------------------------------------

/// Shared state of the tab-lifecycle mock.
#[derive(Clone)]
struct TabLifeState {
    /// (tabId, url, listItemId) rows that already existed when the test
    /// started: another fetch's live tab, a tab under a DIFFERENT browser
    /// context (`listItemId`) that shares our `userId`, or extra candidates for
    /// the ambiguity guard.
    seed: std::sync::Arc<std::sync::Mutex<Vec<(String, String, String)>>>,
    /// Ids `POST /tabs` registered.
    created: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    /// Ids a `DELETE /tabs/{id}` arrived for — what every test asserts on.
    closed: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    /// Every id a DELETE was seen for, whether or not it took effect.
    seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    /// Answer `POST /tabs` only after 30 s, so the client's budget always fires
    /// first: the lost-create-response shape.
    create_stalls: bool,
    /// The FIRST `POST /tabs` answers 500 without registering: the client is
    /// left in its between-attempts backoff, holding an armed create guard that
    /// has provably nothing in flight.
    create_rejects_first: bool,
    /// Every `POST /tabs` the mock saw, registered or not — how a test knows a
    /// peer really is inside its create.
    create_hits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// False = the create registers nothing, modelling a failure raised before
    /// the server registers the tab.
    create_registers: bool,
    /// Answer 200 with a body that is not JSON: camofox registered the tab and
    /// we cannot read the id, the other `Lost` shape.
    create_bad_body: bool,
    /// `GET /tabs` answers 500, modelling its per-tab `page.title()` stall.
    list_fails: bool,
    /// Never answer `POST /tabs/{id}/navigate`, so a fetch can be aborted while
    /// holding an open tab.
    navigate_stalls: bool,
    /// Accept the DELETE (`{ok:true}`) but leave the tab listed — the server
    /// that claims a close it did not perform. What `close-noop` exists for.
    close_noop: bool,
    /// Never answer the DELETE, so the CLIENT budget fires while the server is
    /// still inside its own 5 s `safePageClose`. The tab stays listed, so a
    /// implementation that counted this would be caught.
    close_stalls: bool,
}

/// The `sessionKey` the renderer creates its tabs under — mirrored from the
/// module's `SESSION_KEY`, which the reap requires a candidate to match.
const RENDER_SESSION: &str = "render";

impl Default for TabLifeState {
    fn default() -> Self {
        Self {
            seed: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            created: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            closed: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            seen: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            create_stalls: false,
            create_rejects_first: false,
            create_hits: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            create_registers: true,
            create_bad_body: false,
            list_fails: false,
            navigate_stalls: false,
            close_noop: false,
            close_stalls: false,
        }
    }
}

impl TabLifeState {
    fn closed_ids(&self) -> Vec<String> {
        self.closed.lock().unwrap().clone()
    }

    /// Ids the DELETE handler saw, including ones that took no effect.
    fn seen_ids(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    /// The id of the tab that is still LISTED (the survivor), for the
    /// `close_noop` scenarios.
    fn open_ids(&self) -> Vec<String> {
        let closed = self.closed.lock().unwrap().clone();
        self.created
            .lock()
            .unwrap()
            .iter()
            .filter(|id| !closed.iter().any(|c| c == *id))
            .cloned()
            .collect()
    }
}

async fn life_create(
    axum::extract::State(st): axum::extract::State<TabLifeState>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let hit = st
        .create_hits
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        + 1;
    if st.create_rejects_first && hit == 1 {
        // A 5xx the server ANSWERED with, having registered nothing — which is
        // what puts the client into a backoff with an armed guard and no request
        // in flight.
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            json!({ "error": "profile context is starting" }).to_string(),
        );
    }
    // Every branch returns the same shape: status + a JSON content type + a raw
    // body string, because one branch deliberately sends a body that is NOT
    // valid JSON and must not be typed as `Json`.
    if body.get("sessionKey").and_then(|v| v.as_str()).is_none() {
        return (
            StatusCode::BAD_REQUEST,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            json!({ "error": "userId and sessionKey required" }).to_string(),
        );
    }
    let next = st.created.lock().unwrap().len() + 1;
    let id = format!("tab-{next}");
    if st.create_registers {
        st.created.lock().unwrap().push(id.clone());
    }
    if st.create_stalls {
        // Registered above; the reply the client is waiting on never comes.
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
    if st.create_bad_body {
        // Registered, then an answer the client cannot decode — the server knew
        // the id and we never did.
        return (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            "this is not json".to_string(),
        );
    }
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        json!({ "ok": true, "tabId": id }).to_string(),
    )
}

async fn life_list(
    axum::extract::State(st): axum::extract::State<TabLifeState>,
) -> impl IntoResponse {
    if st.list_fails {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "page.title() stalled" })),
        );
    }
    let closed = st.closed.lock().unwrap().clone();
    let mut rows: Vec<Value> = Vec::new();
    for (id, url, list_item_id) in st.seed.lock().unwrap().iter() {
        if !closed.iter().any(|c| c == id) {
            rows.push(json!({
                "tabId": id,
                "url": url,
                "title": "",
                "listItemId": list_item_id,
            }));
        }
    }
    for id in st.created.lock().unwrap().iter() {
        if !closed.iter().any(|c| c == id) {
            rows.push(json!({
                "tabId": id,
                "url": "about:blank",
                "title": "",
                // The real route tags every row with the sessionKey its
                // `tabGroups` entry lives under (`core.js:553-561`), which is
                // how the reap tells our context from another one sharing the
                // same userId.
                "listItemId": RENDER_SESSION,
            }));
        }
    }
    (
        StatusCode::OK,
        Json(json!({ "running": true, "tabs": rows })),
    )
}

async fn life_navigate(
    axum::extract::State(st): axum::extract::State<TabLifeState>,
    Path(_id): Path<String>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    if st.navigate_stalls {
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "url": body["url"] })),
    )
}

async fn life_close(
    axum::extract::State(st): axum::extract::State<TabLifeState>,
    Path(id): Path<String>,
    Json(_body): Json<Value>,
) -> Json<Value> {
    st.seen.lock().unwrap().push(id.clone());
    if st.close_stalls {
        // The server is still inside its own 5 s `safePageClose`; our client
        // gives up first and the tab is still listed. Note what is NOT mutated
        // here: `closed`, so a re-list still shows the tab.
        tokio::time::sleep(Duration::from_secs(30)).await;
    } else if !st.close_noop {
        st.closed.lock().unwrap().push(id.clone());
    }
    // Faithful to the real route: `{ok:true}` regardless of whether it found
    // the tab, which is precisely why crw verifies by re-listing.
    Json(json!({ "ok": true }))
}

async fn spawn_life_mock(st: TabLifeState) -> String {
    let app = Router::new()
        .route("/tabs", post(life_create).get(life_list))
        .route("/tabs/{id}/navigate", post(life_navigate))
        .route("/tabs/{id}/wait", post(wait))
        .route("/tabs/{id}/evaluate", post(evaluate))
        .route("/tabs/{id}", delete(life_close))
        .with_state(st);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// A fetch that gets as far as a tab must close exactly one DELETE, and must
/// take the inline path (the guard stays armed until then). Guards against the
/// `TabGuard` double-closing on top of the close it replaced.
#[tokio::test]
async fn tab_closes_exactly_once_on_the_normal_path() {
    let st = TabLifeState::default();
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    renderer
        .fetch("https://example.com", &HashMap::new(), None, deadline())
        .await
        .expect("happy path against the mock");

    assert_eq!(
        st.closed_ids(),
        vec!["tab-1".to_string()],
        "one create must mean exactly one close, inline"
    );
}

/// The documented leak: the create registers server-side but its response never
/// arrives, so crw never learns the id. The reap must find that one blank tab,
/// close it, and still report the caller's timeout.
#[tokio::test]
async fn lost_create_response_reaps_the_orphan_tab() {
    // The counter is process-global and this binary runs tests in parallel, so
    // every test that asserts a delta holds this instead of trusting that it is
    // the only incrementer.
    let _serial = counter_tests().await;
    let st = TabLifeState {
        create_stalls: true,
        ..TabLifeState::default()
    };
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));
    let leaked = || {
        crw_core::metrics::metrics()
            .camofox_tab_leak_total
            .with_label_values(&["crw", "create-orphan"])
            .get()
    };
    let before = leaked();

    let err = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(400)),
        )
        .await
        .expect_err("a create answer that never arrives must surface the timeout");
    assert!(
        matches!(err, crw_core::error::CrwError::Timeout(_)),
        "the caller must still see its own timeout, got {err:?}"
    );

    assert_eq!(
        st.closed_ids(),
        vec!["tab-1".to_string()],
        "the registered-but-unlearned tab must be reaped, nothing else"
    );
    assert_eq!(
        leaked(),
        before + 1,
        "a confirmed reap must count `create-orphan`, exactly once"
    );
}

/// A stalled `GET /tabs` is not evidence of anything: with no trustworthy list
/// the reap must close nothing, since a guess could kill a live tab.
#[tokio::test]
async fn lost_create_response_closes_nothing_when_the_list_fails() {
    // Reap-driving: it can move the shared counters the asserting tests read, so it
    // must not overlap them.
    let _serial = counter_tests().await;
    let st = TabLifeState {
        create_stalls: true,
        list_fails: true,
        ..TabLifeState::default()
    };
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    let _ = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(400)),
        )
        .await;

    assert!(
        st.closed_ids().is_empty(),
        "never close on a list crw could not read: {:?}",
        st.closed_ids()
    );
}

/// Two unknown tabs is ambiguous — one of them may belong to a fetch still
/// running — so the reap must stand down.
#[tokio::test]
async fn lost_create_response_closes_nothing_when_the_candidate_is_ambiguous() {
    // Reap-driving: it can move the shared counters the asserting tests read, so it
    // must not overlap them.
    let _serial = counter_tests().await;
    let st = TabLifeState {
        create_stalls: true,
        ..TabLifeState::default()
    };
    st.seed.lock().unwrap().push((
        "foreign-1".to_string(),
        "about:blank".to_string(),
        // Same browser context as ours, so it really is a second candidate the
        // reap cannot disambiguate — not something the session filter removes.
        RENDER_SESSION.to_string(),
    ));
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    let _ = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(400)),
        )
        .await;

    assert!(
        st.closed_ids().is_empty(),
        "with two candidates the reap cannot tell ours from a peer's: {:?}",
        st.closed_ids()
    );
}

/// A tab that has reached a real URL cannot be the blank one we just created,
/// so it is somebody else's and must be left alone.
#[tokio::test]
async fn lost_create_response_closes_nothing_when_the_unknown_tab_is_navigated() {
    // Reap-driving: it can move the shared counters the asserting tests read, so it
    // must not overlap them.
    let _serial = counter_tests().await;
    let st = TabLifeState {
        create_stalls: true,
        // Our create never registered, so the only unknown is the seeded
        // foreign tab — isolating the blank filter from the count filter.
        create_registers: false,
        ..TabLifeState::default()
    };
    st.seed.lock().unwrap().push((
        "foreign-1".to_string(),
        "https://example.org/live-page".to_string(),
        // Ours by context, so only the blank filter can save it: the reap must
        // never close a tab that has reached a real URL.
        RENDER_SESSION.to_string(),
    ));
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    let _ = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(400)),
        )
        .await;

    assert!(
        st.closed_ids().is_empty(),
        "a navigated tab is provably not ours: {:?}",
        st.closed_ids()
    );
}

/// The config kill switch must actually stop the reap.
#[tokio::test]
async fn orphan_reap_off_closes_nothing() {
    // Reap-driving: it can move the shared counters the asserting tests read, so it
    // must not overlap them.
    let _serial = counter_tests().await;
    let st = TabLifeState {
        create_stalls: true,
        ..TabLifeState::default()
    };
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10))
        .with_orphan_reap(false);

    let _ = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(400)),
        )
        .await;

    assert!(
        st.closed_ids().is_empty(),
        "reap disabled by config must send no DELETE at all: {:?}",
        st.closed_ids()
    );
}

/// The cancellation case an inline `.await` close can never cover: the crawl
/// cancel path aborts the running task, and the outer request timeout drops the
/// handler, both at an arbitrary await. The guard's `Drop` has to reap the tab
/// from a detached task.
#[tokio::test]
async fn cancelled_fetch_still_reaps_its_tab() {
    let _serial = counter_tests().await;
    let st = TabLifeState {
        navigate_stalls: true,
        ..TabLifeState::default()
    };
    let base = spawn_life_mock(st.clone()).await;
    let renderer = std::sync::Arc::new(CamofoxRenderer::new(
        "camofox",
        &base,
        None,
        Duration::from_secs(30),
    ));
    let cancelled = || {
        crw_core::metrics::metrics()
            .camofox_tab_leak_total
            .with_label_values(&["crw", "cancelled"])
            .get()
    };
    let before = cancelled();

    let handle = tokio::spawn({
        let renderer = std::sync::Arc::clone(&renderer);
        async move {
            renderer
                .fetch("https://example.com", &HashMap::new(), None, deadline())
                .await
        }
    });
    // Gate on the create having actually completed, NOT on a wall-clock guess:
    // aborting before `create_tab` returns would land in the create span, where
    // there is no `TabGuard` at all (that case is `CreateGuard`'s, and it is
    // covered by the reap tests). Without this gate the test is a race.
    for _ in 0..100 {
        if !st.created.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        st.created.lock().unwrap().len(),
        1,
        "precondition: the fetch must hold an open tab before it is aborted"
    );
    handle.abort();
    let _ = handle.await;

    // The detached reap runs on this (current-thread) runtime, so give it a
    // turn rather than asserting the instant we abort.
    let mut closed = st.closed_ids();
    for _ in 0..50 {
        if closed.iter().any(|id| id == "tab-1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        closed = st.closed_ids();
    }
    // One extra turn so a SECOND close (the bug this guards against: the inline
    // path and `Drop` both firing) lands before the snapshot is taken.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let closed = st.closed_ids();
    assert_eq!(
        closed,
        vec!["tab-1".to_string()],
        "exactly one close for the cancelled tab — a duplicate means the inline \
         close and `Drop` both fired, and an empty list means the reap never ran"
    );
    assert_eq!(
        cancelled(),
        before + 1,
        "the cancellation path must count `cancelled`, exactly once"
    );
}

/// The other side of the create guard: a create dropped while NOTHING is in
/// flight has no tab behind it. camofox answers every pre-registration failure
/// (bad profile, staged first use, per-tab cap) without ever committing one, so
/// a guard dropped in the backoff between attempts must neither move a counter
/// documented as empty in normal service — a crawl cancel is routine — nor point
/// a DELETE at whatever unregistered blank tab happened to be listed.
#[tokio::test]
async fn an_abort_during_the_create_backoff_is_not_counted_as_a_leak() {
    let _serial = counter_tests().await;
    let st = TabLifeState {
        // Attempt one is a 5xx the server answered without registering; the
        // retry after the backoff never answers, so only the abort ends this.
        create_rejects_first: true,
        create_stalls: true,
        create_registers: false,
        // A blank tab under OUR sessionKey that this process never registered:
        // exactly the prize a reap that fires without proof would seize.
        seed: std::sync::Arc::new(std::sync::Mutex::new(vec![(
            "unlabelled-blank".to_string(),
            "about:blank".to_string(),
            RENDER_SESSION.to_string(),
        )])),
        ..TabLifeState::default()
    };
    let base = spawn_life_mock(st.clone()).await;
    let renderer = std::sync::Arc::new(CamofoxRenderer::new(
        "camofox",
        &base,
        None,
        Duration::from_secs(30),
    ));
    let cancelled = || {
        crw_core::metrics::metrics()
            .camofox_tab_leak_total
            .with_label_values(&["crw", "cancelled"])
            .get()
    };
    let before = cancelled();
    let hits = || st.create_hits.load(std::sync::atomic::Ordering::SeqCst);

    let handle = tokio::spawn({
        let renderer = std::sync::Arc::clone(&renderer);
        async move {
            renderer
                .fetch("https://example.com", &HashMap::new(), None, deadline())
                .await
        }
    });
    // Gate on the 5xx having been answered: from that instant the client sits in
    // its 500 ms backoff with an armed guard and no request outstanding, which
    // is the state the guard must refuse to act on.
    for _ in 0..100 {
        if hits() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        hits() >= 1,
        "precondition: the first create must have been answered with a 5xx"
    );
    handle.abort();
    let _ = handle.await;
    // A detached reap, if one were wrongly scheduled, needs a turn to list and
    // close; give it several.
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(
        st.closed_ids().is_empty(),
        "an interrupted create that sent nothing must close nothing, least of \
         all someone else's blank tab: {:?}",
        st.closed_ids()
    );
    assert_eq!(
        cancelled(),
        before,
        "an interrupted create with nothing in flight is not a cancelled leak"
    );
}

// ---------------------------------------------------------------------------
// The counter-sensitive tests share one process-global registry and run in
// parallel, so they hold this rather than relying on being the only writer to
// a label — an assertion of `> before` passes under any stray increment.
// NOTHING enforces this: any new test that can move `cancelled`,
// `create-orphan` or `close-noop` has to take this lock too, or its
// `== before + 1` is quietly somebody else's increment.
// ---------------------------------------------------------------------------

async fn counter_tests() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// The second `Lost` shape: camofox answered 200 — so it DID register the tab —
/// but the body could not be decoded, leaving the id unknown. Only the outer
/// client timeout drove the reap before; this drives the decode arm.
#[tokio::test]
async fn lost_create_response_reaps_the_orphan_when_the_body_is_undecodable() {
    let _serial = counter_tests().await;
    let st = TabLifeState {
        create_bad_body: true,
        ..TabLifeState::default()
    };
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));
    let leaked = || {
        crw_core::metrics::metrics()
            .camofox_tab_leak_total
            .with_label_values(&["crw", "create-orphan"])
            .get()
    };
    let before = leaked();

    let err = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(2000)),
        )
        .await
        .expect_err("an unreadable create answer cannot produce a tab");
    assert!(
        format!("{err:?}").contains("bad response"),
        "the caller must see what went wrong, got {err:?}"
    );

    assert_eq!(
        st.closed_ids(),
        vec!["tab-1".to_string()],
        "the registered-but-unreadable tab must be reaped, nothing else"
    );
    assert_eq!(
        leaked(),
        before + 1,
        "a confirmed reap must count `create-orphan`, exactly once"
    );
}

/// `DELETE /tabs/{id}` answers `{ok:true}` whether or not it found the tab, so
/// an accepted close is not proof. When the tab is still listed afterwards this
/// is the one case that proves a real leak, and it must be counted.
#[tokio::test]
async fn close_noop_is_counted_when_the_accepted_close_did_nothing() {
    let _serial = counter_tests().await;
    let st = TabLifeState {
        create_stalls: true,
        // Accepts the DELETE, leaves the tab listed.
        close_noop: true,
        ..TabLifeState::default()
    };
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));
    let count = |cause: &'static str| {
        move || {
            crw_core::metrics::metrics()
                .camofox_tab_leak_total
                .with_label_values(&["crw", cause])
                .get()
        }
    };
    let noop = count("close-noop");
    let orphan = count("create-orphan");
    let (before_noop, before_orphan) = (noop(), orphan());

    let _ = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(400)),
        )
        .await;

    assert_eq!(
        st.seen_ids(),
        vec!["tab-1".to_string()],
        "the reap must have issued the close"
    );
    assert_eq!(
        st.open_ids(),
        vec!["tab-1".to_string()],
        "precondition: the server accepted the close and left the tab open"
    );
    assert_eq!(
        noop(),
        before_noop + 1,
        "a close that was accepted but did nothing is exactly what `close-noop` is for"
    );
    assert_eq!(
        orphan(),
        before_orphan,
        "the reap only counts `create-orphan` on proof the tab is gone"
    );
}

/// The mirror image, and the reason the survivor check is conditional: our close
/// budget is shorter than the server's own 5 s `safePageClose`, so a DELETE that
/// times out client-side usually completes anyway. The tab is still listed at
/// that instant — counting it would call a documented false alarm a leak, on a
/// counter documented as empty in normal operation.
#[tokio::test]
async fn a_close_that_only_timed_out_client_side_is_never_counted_as_a_leak() {
    let _serial = counter_tests().await;
    let st = TabLifeState {
        create_stalls: true,
        // The handler hangs: our client gives up while the server is still in
        // `safePageClose`, and the tab stays listed.
        close_stalls: true,
        ..TabLifeState::default()
    };
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));
    let noop = || {
        crw_core::metrics::metrics()
            .camofox_tab_leak_total
            .with_label_values(&["crw", "close-noop"])
            .get()
    };
    let before = noop();

    let _ = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(400)),
        )
        .await;

    assert!(
        st.seen_ids().iter().any(|id| id == "tab-1"),
        "precondition: the close must really have been attempted"
    );
    assert!(
        st.open_ids().iter().any(|id| id == "tab-1"),
        "precondition: the tab is still listed at the moment we gave up"
    );
    assert_eq!(
        noop(),
        before,
        "a client-side close timeout must not be counted as a leak, even though \
         the id is still listed — the server is likely still closing it"
    );
}

/// The reap's ledger of "tabs I registered" is per-process, but camofox keys
/// tabs by `userId` alone, so anything else sharing that userId is listable to
/// us. A tab from another browser context under the same `userId` must never be
/// a candidate — closing it would cost a live request somewhere else.
#[tokio::test]
async fn lost_create_closes_nothing_when_the_candidate_is_another_context() {
    // Reap-driving: it can move the shared counters the asserting tests read, so it
    // must not overlap them.
    let _serial = counter_tests().await;
    let st = TabLifeState {
        create_stalls: true,
        // Our create registered nothing, so the single unknown candidate is the
        // foreign tab: exactly the shape that would be closed without the
        // session filter.
        create_registers: false,
        ..TabLifeState::default()
    };
    st.seed.lock().unwrap().push((
        "peer-1".to_string(),
        "about:blank".to_string(),
        "some-other-context".to_string(),
    ));
    let base = spawn_life_mock(st.clone()).await;
    let renderer = CamofoxRenderer::new("camofox", &base, None, Duration::from_secs(10));

    let _ = renderer
        .fetch(
            "https://example.com",
            &HashMap::new(),
            None,
            Deadline::now_plus(Duration::from_millis(400)),
        )
        .await;

    assert!(
        st.closed_ids().is_empty(),
        "a blank tab in another context is not ours to close: {:?}",
        st.closed_ids()
    );
}
