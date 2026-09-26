//! Camofox renderer tier — drives the `camofox-browser` REST server
//! (`redf0x1/camofox-browser`, default port 9377) which wraps the Camoufox
//! (Firefox) anti-detect browser behind plain HTTP.
//!
//! Firefox does not speak CDP, so this tier does NOT use the `cdp` module.
//! It is a pure-`reqwest` client: per fetch it creates a tab, waits for the
//! page to settle, evaluates `document.documentElement.outerHTML`, and closes
//! the tab. It implements the same [`PageFetcher`] trait as the CDP renderers
//! so it slots into `FallbackRenderer`'s failover ladder unchanged.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use crw_core::Deadline;
use crw_core::error::{CrwError, CrwResult};
use crw_core::types::FetchResult;
use serde::Deserialize;
use serde_json::json;

use crate::clearance::{CLEARANCE_COOKIE, Clearance, ClearanceCache, Cookie, cookie_matches_host};
use crate::detector;
use crate::traits::PageFetcher;

/// Stable `userId` for all sessions opened by one renderer instance. The
/// camofox-browser server keys an isolated Firefox profile per `userId`, so a
/// constant value lets the browser reuse one warm profile across fetches.
const USER_ID: &str = "crw";

/// Browser-context key. `/tabs` requires both `userId` and `sessionKey`. A
/// fixed key reuses one context (tabs are created and deleted per fetch, so the
/// context never accumulates tabs and sessions don't leak toward MAX_SESSIONS).
const SESSION_KEY: &str = "render";

/// JS evaluated to extract the fully-rendered DOM after navigation.
const OUTER_HTML_EXPR: &str = "document.documentElement.outerHTML";

/// Grace budget for the best-effort tab cleanup DELETE. Deliberately NOT tied to
/// the request deadline: cleanup runs after the deadline may already be spent
/// (e.g. an evaluate that timed out), and a leaked tab drives the camofox
/// context toward MAX_SESSIONS, so the reap must still get a real chance to run.
const CLEANUP_BUDGET: Duration = Duration::from_secs(3);

/// `POST /tabs` fails transiently right after the context's last tab was
/// closed: camofox eagerly tears the context down and relaunches it, and a
/// create landing in that window fails with `window is null` (or, once the
/// breaker trips, `browser has been closed`). A relaunch takes a few seconds,
/// so a 5xx create is retried this many times in total with a growing pause
/// ([`CREATE_TAB_BACKOFF`] doubling each time), inside the request deadline.
/// The tab is created blank and navigated separately: camofox counts a failed
/// navigate-in-create toward its consecutive-failure breaker (3 by default),
/// so retrying creates that also navigate would trip the breaker faster.
const CREATE_TAB_ATTEMPTS: u32 = 4;
const CREATE_TAB_BACKOFF: Duration = Duration::from_millis(500);

/// Default cap on the passive Cloudflare-challenge wait. Mirrors
/// `CamofoxEndpoint::challenge_wait_ms`'s default.
const DEFAULT_CHALLENGE_WAIT: Duration = Duration::from_secs(20);

/// Interval between challenge probes while the interstitial is on screen.
const CHALLENGE_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Budget always held back from the challenge loop so the snapshot that
/// follows (final-URL check, status probe, outerHTML evaluate) still runs.
const MIN_EVAL_BUDGET: Duration = Duration::from_secs(2);

/// One cheap DOM probe, computed in the page so no markup crosses the wire:
/// the title and whether the managed challenge's own script is loaded. The
/// marker is `challenge-platform/h/`, never the bare
/// `/cdn-cgi/challenge-platform/` directory, whose telemetry loader Cloudflare
/// also injects into cleared pages (a false positive there costs the whole
/// wait). Measured live: a managed challenge carries the title and the script
/// but none of the old `#challenge-*` element ids.
const CHALLENGE_PROBE_EXPR: &str = "JSON.stringify({t:document.title,m:document.documentElement.outerHTML.includes('challenge-platform/h/')})";

/// Parsed [`CHALLENGE_PROBE_EXPR`] result.
#[derive(Deserialize)]
struct ChallengeProbe {
    #[serde(default)]
    t: String,
    #[serde(default)]
    m: bool,
}

/// What a challenge probe says is on screen.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChallengeState {
    /// A managed challenge that may clear if the tab is left alone.
    Challenge,
    /// Cloudflare's hard block ("Attention Required!"). It never clears, so it
    /// is never polled.
    Wall,
    /// Anything else, including an unparseable probe (an older server, a page
    /// that threw): proceed as before the loop existed.
    Clear,
}

pub(crate) fn probe_challenge_state(raw: &str) -> ChallengeState {
    let Ok(p) = serde_json::from_str::<ChallengeProbe>(raw) else {
        return ChallengeState::Clear;
    };
    let title = p.t.trim();
    if title.starts_with("Attention Required!") {
        ChallengeState::Wall
    } else if p.m || title.eq_ignore_ascii_case("just a moment...") {
        ChallengeState::Challenge
    } else {
        ChallengeState::Clear
    }
}

/// Renderer backed by a camofox-browser REST endpoint.
pub struct CamofoxRenderer {
    name: String,
    base_url: String,
    api_key: Option<String>,
    client: reqwest::Client,
    /// Serializes `POST /tabs`. Concurrent creates on a freshly (re)launched
    /// context race for camofox's reusable initial blank page and abort each
    /// other's navigation (`NS_BINDING_ABORTED`). A blank create is
    /// milliseconds when the context is warm, so holding this only across the
    /// create call costs nothing in steady state; navigation itself runs
    /// concurrently.
    ///
    /// This lock is ALSO what makes the create-orphan reap
    /// ([`CamofoxRenderer::reap_lost_create`] sound: every create, every
    /// registration into [`Self::known_ids`], and every reap runs inside it, so
    /// a tab the server has registered but we have not yet named provably has no
    /// in-process owner. Do not narrow the critical section without re-reading
    /// that method's docs.
    create_lock: Arc<tokio::sync::Mutex<()>>,
    /// Ids of tabs this process created and has not closed. The orphan reap
    /// lists camofox's view and treats anything NOT in here as a candidate;
    /// behind [`Self::create_lock`] that is exact, because camofox registers a
    /// tab before it responds with the id (so an in-flight peer create is
    /// already listable, and its id lands here before the lock is released).
    known_ids: TabRegistry,
    /// Kill switch for the create-orphan reap (config
    /// `renderer.camofox.reap_orphan_tabs`, env
    /// `CRW_RENDERER__CAMOFOX__REAP_ORPHAN_TABS`). On by default; it is the
    /// escape hatch if the reap ever misidentifies, since closing the wrong tab
    /// costs a request while leaking one costs a session slot for 30 minutes.
    reap_orphans: bool,
    /// Cap on the passive challenge wait; `ZERO` disables the loop.
    challenge_wait: Duration,
    /// Sleep between challenge probes ([`CHALLENGE_POLL_INTERVAL`]).
    challenge_poll_interval: Duration,
    /// Where a `cf_clearance` earned by a render is stored for the HTTP tier.
    /// `None` = capture disabled (config `clearance_reuse = false`).
    clearance: Option<Arc<ClearanceCache>>,
}

/// `GET /tabs/:id/cookies`: a bare array on current servers (measured live);
/// a `{cookies: [...]}` wrapper is accepted too so a server change does not
/// silently disable capture.
#[derive(Deserialize)]
#[serde(untagged)]
enum CookiesResponse {
    List(Vec<Cookie>),
    Wrapped { cookies: Vec<Cookie> },
}

impl CookiesResponse {
    fn into_cookies(self) -> Vec<Cookie> {
        match self {
            Self::List(c) | Self::Wrapped { cookies: c } => c,
        }
    }
}

/// `POST /tabs` response — we only need the tab id.
#[derive(Deserialize)]
struct CreateTabResponse {
    #[serde(rename = "tabId")]
    tab_id: String,
}

/// `GET /tabs` response — the server's view of the tabs live under one
/// `userId`. Verified live and against the server's own route handler: it
/// filters by `userId` and NOTHING else (`sessionKey` is accepted and
/// ignored), it needs no API key, and it builds each row by `await`ing
/// `page.url()` / `page.title()` — so it is NOT a cheap call and can stall
/// behind a wedged page. Every caller bounds it and treats failure as "do not
/// touch anything".
#[derive(Deserialize, Default)]
struct TabListResponse {
    #[serde(default)]
    tabs: Vec<TabInfo>,
}

/// One row of [`TabListResponse`]: the id we would close, plus the URL the
/// blank-tab filter needs in order to decide whether it is ours to close.
#[derive(Deserialize, Clone, Debug)]
struct TabInfo {
    #[serde(rename = "tabId")]
    tab_id: String,
    #[serde(default)]
    url: String,
    /// Which browser-context group the tab belongs to. `GET /tabs` merges every
    /// session of the `userId` (`core.js:553-561` iterates `session.tabGroups`,
    /// keyed by sessionKey) and tags each row with it, while the create that
    /// made our tab asked for [`SESSION_KEY`] — so this is the only attribution
    /// the endpoint offers, and the reap requires it to match.
    #[serde(rename = "listItemId", default)]
    list_item_id: Option<String>,
}

/// Outcome of a best-effort tab DELETE. Note deliberately what is NOT a case
/// here: proof.
#[derive(Debug)]
enum CloseOutcome {
    /// camofox accepted the request. NOT proof the tab closed: the route
    /// answers `{ok:true}` even when its lookup finds nothing (unknown id, or
    /// a `userId` mismatch), so `DELETE /tabs/{id}` never 404s. Only
    /// [`confirm_gone`] proves closure.
    Accepted,
    Rejected(u16),
    Failed(String),
    /// We gave up after [`CLEANUP_BUDGET`]. Usually a false alarm — the server
    /// gives its own `safePageClose` 5 s and Node finishes the handler after
    /// our client hangs up — so this is never a metric.
    TimedOut,
}

/// Fire the best-effort `DELETE /tabs/{id}`. Shared by the inline close, the
/// orphan reap, and the detached cancel-path close — the last of those runs
/// from a `Drop`, so it cannot borrow `self` and the pieces come in by value.
async fn send_tab_delete(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
    tab_id: &str,
) -> CloseOutcome {
    let mut req = client.delete(format!("{base_url}/tabs/{tab_id}"));
    if let Some(key) = api_key {
        req = req.bearer_auth(key);
    }
    // The userId rides in the JSON body on this route, exactly as it always
    // has. Do not "tidy" it into a query param: a DELETE whose body does not
    // match the tab's userId still answers `{ok:true}`, so a mistake here
    // leaks every tab while logging success.
    let fut = req.json(&json!({ "userId": USER_ID })).send();
    match tokio::time::timeout(CLEANUP_BUDGET, fut).await {
        Ok(Ok(resp)) => {
            let status = resp.status();
            // A 404 is not producible on this route today; kept so a server
            // that starts reporting one reads as "already gone", not failure.
            if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
                CloseOutcome::Accepted
            } else {
                CloseOutcome::Rejected(status.as_u16())
            }
        }
        Ok(Err(e)) => CloseOutcome::Failed(crw_core::error::reqwest_message(e)),
        Err(_) => CloseOutcome::TimedOut,
    }
}

/// `GET /tabs?userId=…`, bounded by [`CLEANUP_BUDGET`] because the server
/// builds it by awaiting `page.title()` once per tab. Any failure — transport,
/// timeout, non-2xx, undecodable body — is an error here, and every caller
/// treats "could not list" as "close nothing".
async fn list_tabs(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
) -> CrwResult<Vec<TabInfo>> {
    let mut req = client.get(format!("{base_url}/tabs?userId={USER_ID}"));
    if let Some(key) = api_key {
        req = req.bearer_auth(key);
    }
    let fut = async {
        let resp = req
            .send()
            .await
            .map_err(|e| CrwError::RendererError(crw_core::error::reqwest_message(e)))?;
        if !resp.status().is_success() {
            return Err(CrwError::RendererError(format!(
                "camofox /tabs list returned {}",
                resp.status()
            )));
        }
        resp.json::<TabListResponse>()
            .await
            .map_err(|e| CrwError::RendererError(format!("camofox /tabs list unreadable: {e}")))
    };
    match tokio::time::timeout(CLEANUP_BUDGET, fut).await {
        Ok(Ok(list)) => Ok(list.tabs),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(CrwError::Timeout(CLEANUP_BUDGET.as_millis() as u64)),
    }
}

/// Whether `tab_id` is absent from the server's list — the only real proof a
/// close worked. `None` means UNKNOWN (the list itself failed), never "gone":
/// a stalled list must not be reported as a successful close.
async fn confirm_gone(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
    tab_id: &str,
) -> Option<bool> {
    let tabs = list_tabs(client, base_url, api_key).await.ok()?;
    Some(!tabs.iter().any(|t| t.tab_id == tab_id))
}

/// Log a best-effort close identically in every caller: a client-side timeout
/// or transport failure is `debug` (the server gives its own close 5 s and
/// usually finishes anyway — counting those is noise), a rejection is `warn`
/// (a leak that will really burn a session slot for 30 minutes).
fn log_close(tab_id: &str, outcome: CloseOutcome) {
    match outcome {
        CloseOutcome::Accepted => {
            tracing::debug!(tab_id, "camofox: tab close accepted (not proof it closed)")
        }
        CloseOutcome::Rejected(status) => {
            tracing::warn!(tab_id, status, "camofox: tab close rejected; tab may leak")
        }
        CloseOutcome::Failed(error) => tracing::debug!(
            tab_id, %error,
            "camofox: tab close did not complete client-side; server likely still closed it"
        ),
        CloseOutcome::TimedOut => tracing::debug!(
            tab_id,
            budget_ms = CLEANUP_BUDGET.as_millis() as u64,
            "camofox: tab close timed out client-side; server likely still closed it"
        ),
    }
}

/// Whether a tab is still sitting on the blank page camofox creates it
/// on. The reap's second fail-safe: crw always creates blank and navigates
/// separately, so a tab that has reached a real URL is provably some other
/// fetch's and is never ours to close.
fn is_blank_url(url: &str) -> bool {
    let u = url.trim();
    u.is_empty() || u.eq_ignore_ascii_case("about:blank")
}

/// One aggregate budget for a whole reap, held against the caller's
/// `create_lock`. It has to cover the reap's own steps — the list (which awaits
/// `page.title()` per tab), the delete, and the confirming re-list, each of
/// which may use its full [`CLEANUP_BUDGET`] — so it is the sum of them, not a
/// smaller number that would give up before the last step ever ran and leave
/// every counter at zero. Without any cap, one wedged camofox could park every
/// other create behind that much cleanup; note the create-span guard can add a
/// SECOND reaper behind the same lock, doubling the worst-case queue. Running
/// out of budget is not a failure — it just means nothing was confirmed, and an
/// unconfirmed reap counts nothing.
const REAP_BUDGET: Duration = Duration::from_secs(9);

/// The body of [`CamofoxRenderer::reap_lost_create`], free so the create-span
/// guard can reach it from a detached task. `create_lock` is held by every
/// caller — pass it to the guard, which acquires it itself.
async fn reap_lost_tabs(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
    registry: &TabRegistry,
    cause: &str,
    detail: &str,
) {
    tracing::debug!(
        cause,
        detail,
        "camofox: create answer lost; checking for the orphan tab"
    );
    let fut = reap_unknown_blank_tab(client, base_url, api_key, registry, cause);
    match tokio::time::timeout(REAP_BUDGET, fut).await {
        Ok(()) => {}
        Err(_) => tracing::debug!(
            cause,
            "camofox: orphan reap ran out of budget; leaving every tab alone"
        ),
    }
}

/// List, decide, close, confirm — every "can't tell" branch closes nothing.
async fn reap_unknown_blank_tab(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
    registry: &TabRegistry,
    cause: &str,
) {
    let listed = match list_tabs(client, base_url, api_key).await {
        Ok(tabs) => tabs,
        // Blind: a guess could close another fetch's live tab.
        Err(e) => {
            tracing::debug!(
                cause,
                error = %e,
                "camofox: could not list tabs; not reaping"
            );
            return;
        }
    };
    let known = registry.snapshot();
    // Two filters, kept apart so the second one can be noticed: tabs the
    // registry does not name, and of those, the ones in our own browser context.
    let mut unknown: Vec<TabInfo> = Vec::new();
    let mut not_ours = 0usize;
    for tab in listed {
        if known.contains(&tab.tab_id) {
            continue;
        }
        // Our own browser context only. `GET /tabs` merges every session of the
        // `userId`, so without this a tab created under a different `sessionKey`
        // by anything else sharing that userId could be reaped; and if the
        // server stops reporting `listItemId` at all, this fails closed.
        if tab.list_item_id.as_deref() == Some(SESSION_KEY) {
            unknown.push(tab);
        } else {
            not_ours += 1;
        }
    }
    if unknown.is_empty() && not_ours > 0 {
        // Every candidate was rejected for not reporting our sessionKey. That is
        // correct when they really belong to another context — but it is also
        // exactly what a server that stopped emitting `listItemId` looks like,
        // and that would silently retire this whole mechanism. One warn, because
        // the alternative is discovering it only as a tab cap nobody can explain.
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                not_ours,
                session_key = SESSION_KEY,
                "camofox: tabs we do not recognise reported no sessionKey we could claim; not \
                 reaping. If they are ours, this endpoint no longer reports `listItemId` and \
                 orphan reaping cannot identify its own tabs"
            );
        });
    }
    let [orphan] = unknown.as_slice() else {
        // 0 = our create never registered a tab after all; >1 = ambiguous, and
        // one of them may be a live fetch.
        tracing::debug!(
            cause,
            unknown = unknown.len(),
            other_contexts = not_ours,
            registry = known.len(),
            "camofox: no single orphan candidate; not reaping"
        );
        return;
    };
    if !is_blank_url(&orphan.url) {
        tracing::debug!(
            tab_id = %orphan.tab_id,
            url = %orphan.url,
            "camofox: unknown tab is already navigated; not ours to close"
        );
        return;
    }
    tracing::warn!(
        tab_id = %orphan.tab_id,
        cause,
        "camofox: reaping tab whose create answer never arrived"
    );
    let outcome = send_tab_delete(client, base_url, api_key, &orphan.tab_id).await;
    // Only an answered close can leave a countable survivor — see `TabGuard`'s
    // `Drop` for why a client-side timeout is never a leak signal.
    let answered = matches!(&outcome, CloseOutcome::Accepted | CloseOutcome::Rejected(_));
    log_close(&orphan.tab_id, outcome);
    if !answered {
        return;
    }
    match confirm_gone(client, base_url, api_key, &orphan.tab_id).await {
        Some(true) => crw_core::metrics::metrics()
            .camofox_tab_leak_total
            .with_label_values(&[USER_ID, "create-orphan"])
            .inc(),
        Some(false) => {
            tracing::warn!(
                tab_id = %orphan.tab_id,
                "camofox: orphan survived the accepted close; it is leaking"
            );
            crw_core::metrics::metrics()
                .camofox_tab_leak_total
                .with_label_values(&[USER_ID, "close-noop"])
                .inc();
        }
        None => tracing::debug!(
            tab_id = %orphan.tab_id,
            "camofox: could not confirm whether the orphan close took effect"
        ),
    }
}

/// Armed for the whole span of a create, so the OTHER way a create can die is
/// covered too: the fetch future being *dropped* mid-await (the crawl cancel's
/// `handle.abort()` and the tower outer timeout both do this). camofox registers
/// the tab server-side before it replies, and a dropped future runs neither the
/// `Created` arm (nothing registered) nor the `Lost` arm (nothing reaped) — so
/// without this guard that tab is invisible to every mechanism in this module.
///
/// On drop-before-completion it hands the reap to a detached task, which
/// acquires `create_lock` itself: the reap must serialize against other creates
/// exactly as the inline one does, or it could close a peer create's blank tab.
struct CreateGuard {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
    registry: TabRegistry,
    create_lock: Arc<tokio::sync::Mutex<()>>,
    reap_orphans: bool,
    /// Cleared once camofox has answered (nothing to reap) or the inline reap
    /// has run, so `Drop` fires only for a genuinely interrupted create.
    armed: bool,
    /// Set the instant a `POST /tabs` is handed to the client, cleared again
    /// while the loop sleeps between attempts. Without it, a guard dropped
    /// while parked on the create lock or waiting out a backoff — where the last
    /// attempt was a 5xx that camofox answered BEFORE registering anything —
    /// would reap, and count, a tab that provably never existed.
    sent: bool,
}

impl CreateGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }

    /// The request is in flight: from here, dropping this future can strand a
    /// tab camofox already registered.
    fn mark_sent(&mut self) {
        self.sent = true;
    }

    /// Backoff between attempts, where nothing is in flight.
    fn mark_not_sent(&mut self) {
        self.sent = false;
    }
}

impl Drop for CreateGuard {
    fn drop(&mut self) {
        // Only a create actually in flight can have registered a tab. An armed
        // guard that never sent (parked on the lock, asleep between attempts)
        // has nothing behind it: every pre-registration failure is answered
        // before camofox commits the tab.
        if !self.armed || !self.sent {
            return;
        }
        let (client, base_url, api_key, registry, create_lock) = (
            self.client.clone(),
            self.base_url.clone(),
            self.api_key.clone(),
            self.registry.clone(),
            Arc::clone(&self.create_lock),
        );
        if !self.reap_orphans {
            // The only trace this leaves on an endpoint we are not allowed to
            // clean: without it, an operator sees a wedged tab cap and nothing
            // in the log explains it. Once per process, not once per abort.
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                tracing::warn!(
                    "camofox: an interrupted tab create may have left a tab open, and orphan reaping \
                     is off for this endpoint (it requires `manage = true`); such a tab holds one of \
                     the session's tab slots until camofox's session timeout"
                );
            });
            return;
        }
        tracing::warn!(
            "camofox: create interrupted by cancellation; reaping any tab it registered"
        );
        let Some(handle) = tokio::runtime::Handle::try_current().ok() else {
            tracing::warn!("camofox: no runtime to reap an interrupted create");
            return;
        };
        // Counted only now — with a runtime confirmed and a reaper about to run.
        // The count means "a create was aborted with a request in flight", not
        // merely "the guard was dropped", and it is emitted where the reap ran:
        // the tab itself is only counted, separately, if the reap confirms one.
        crw_core::metrics::metrics()
            .camofox_tab_leak_total
            .with_label_values(&[USER_ID, "cancelled"])
            .inc();
        handle.spawn(async move {
            // Same lock the creators hold. Dropped futures cannot take locks, so
            // this is the one place the reap is entered from outside `create_tab`
            // — and it still refuses to guess without it.
            let _serialized = create_lock.lock().await;
            reap_lost_tabs(
                &client,
                &base_url,
                api_key.as_deref(),
                &registry,
                "cancelled create",
                "the fetch was aborted before the create answer arrived",
            )
            .await;
        });
    }
}

/// The result of one `POST /tabs` attempt, told apart by whether camofox ever
/// ANSWERED — which is what decides if there can be an orphan tab at all.
enum CreateAttempt {
    /// Registered and we hold the id.
    Created(String),
    /// A 5xx read end to end, retried up to [`CREATE_TAB_ATTEMPTS`].
    Retryable(String),
    /// A non-2xx read end to end. camofox answered, and every create failure it
    /// reports (`window is null`, the consecutive-failure breaker, 429 over
    /// MAX_TABS_PER_SESSION) is raised BEFORE it registers a tab: nothing to
    /// reap.
    Failed(CrwError),
    /// No usable answer — transport failure, an unreadable body on a 2xx, or
    /// our own timeout. camofox registers the tab before it replies, so it may
    /// well exist under an id we never learned. THIS is the leak.
    Lost(CrwError),
}

/// Ownership ledger backing the create-orphan reap: the ids of tabs this
/// process created and has not closed.
///
/// Plain `std::sync::Mutex`, deliberately not `tokio::sync::Mutex`: every touch
/// point is synchronous, which makes it structurally impossible to hold the
/// guard across an `.await`. Lock ordering is always
/// [`CamofoxRenderer::create_lock`] → this, never the reverse. Poisoning is
/// recovered from: a panic between register and unregister can only ever
/// over-report a tab as live, which makes the reap skip, never mis-close.
#[derive(Clone, Default)]
struct TabRegistry(Arc<std::sync::Mutex<HashSet<String>>>);

impl TabRegistry {
    fn register(&self, tab_id: &str) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(tab_id.to_string());
    }

    fn unregister(&self, tab_id: &str) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(tab_id);
    }

    fn snapshot(&self) -> HashSet<String> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

/// Owns one open tab for the life of a fetch so the close survives
/// CANCELLATION, which is the case plain `.await` cleanup cannot cover.
///
/// The fetch future really is dropped in production, at arbitrary awaits: the
/// crawl cancel path calls `handle.abort()` on the running task
/// (`crw-server/src/routes/crawl.rs`, `routes/v2/crawl.rs`) and the tower outer
/// timeout layer drops the handler (`crw-server/src/app.rs`). Both can strand a
/// tab between `create_tab` and its close. A stranded tab is not self-healing:
/// camofox's idle cleanup only reaps ZERO-tab sessions, so it holds 1 of the
/// session's 10 tab slots until the 30 min session timeout — ten of them and
/// every later create hard-fails 429.
///
/// The normal path still closes INLINE via [`TabGuard::close`], never detached:
/// the create-before-close ordering is load-bearing against camofox's eager
/// zero-tab context teardown (see `crw-search::camofox_search`'s module docs),
/// and always-detaching would let a later create race an earlier fetch's close.
/// Only the cancellation path — where no one is left to await anything — goes
/// detached.
struct TabGuard {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
    tab_id: String,
    registry: TabRegistry,
    /// Disarmed by [`TabGuard::close`] once the inline DELETE has been issued,
    /// so `Drop` can tell "we already closed it" from "this future was
    /// cancelled — somebody still has to close it".
    consumed: bool,
}

impl TabGuard {
    fn new(
        client: reqwest::Client,
        base_url: String,
        api_key: Option<String>,
        tab_id: String,
        registry: TabRegistry,
    ) -> Self {
        Self {
            client,
            base_url,
            api_key,
            tab_id,
            registry,
            consumed: false,
        }
    }

    /// Normal path: close inline, preserving today's ordering exactly, and only
    /// then disarm. Both orderings matter:
    ///
    /// * the DELETE is issued while the tab is STILL registered, so a concurrent
    ///   reap can never mistake a tab with a close in flight for an unregistered
    ///   candidate (it would double-close it, then count the closing tab as an
    ///   orphan);
    /// * disarming happens last, so if this future is cancelled while awaiting
    ///   its own DELETE the request dies half-sent and the `Drop` impl (still
    ///   armed) reaps the tab from a detached task instead.
    async fn close(mut self) {
        let outcome = send_tab_delete(
            &self.client,
            &self.base_url,
            self.api_key.as_deref(),
            &self.tab_id,
        )
        .await;
        self.registry.unregister(&self.tab_id);
        log_close(&self.tab_id, outcome);
        self.consumed = true;
    }
}

impl Drop for TabGuard {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        tracing::warn!(
            tab_id = %self.tab_id,
            "camofox: fetch cancelled with the tab open; reaping it detached"
        );
        // Clones, not moves: `Drop` hands `&mut self`, and `reqwest::Client`
        // is internally `Arc`'d, so cloning it costs a refcount bump.
        let (client, base_url, api_key, tab_id) = (
            self.client.clone(),
            self.base_url.clone(),
            self.api_key.clone(),
            self.tab_id.clone(),
        );
        let registry = self.registry.clone();
        match tokio::runtime::Handle::try_current() {
            // Drop cannot await, so the close goes to a detached task. Safe
            // precisely because this is the cancellation path: no later create
            // can be ordered against a close nobody was waiting on anyway. The
            // corollary is the one ordering inversion this design accepts: the
            // detached DELETE can land AFTER a later fetch's create, so the
            // context can transiently hit zero tabs and camofox may tear its
            // window down under the next create — absorbed by
            // CREATE_TAB_ATTEMPTS rather than pretended away.
            Ok(handle) => {
                // Counted in this arm only: the branch below runs no reap, so
                // counting there would credit a leak-handling path that never
                // happened.
                crw_core::metrics::metrics()
                    .camofox_tab_leak_total
                    .with_label_values(&[USER_ID, "cancelled"])
                    .inc();
                handle.spawn(async move {
                    let outcome =
                        send_tab_delete(&client, &base_url, api_key.as_deref(), &tab_id).await;
                    // Released only now: the tab stays "ours" while its close is
                    // in flight, so no reap can pick it up as an unknown.
                    registry.unregister(&tab_id);
                    // Only an ANSWERED close can leave a survivor worth counting.
                    // On `TimedOut` the server is likely still inside its own 5 s
                    // `safePageClose`, so the id would legitimately still be
                    // listed — counting that would call a documented false alarm
                    // a leak, on a counter meant to stay empty.
                    let answered =
                        matches!(&outcome, CloseOutcome::Accepted | CloseOutcome::Rejected(_));
                    log_close(&tab_id, outcome);
                    if answered
                        && confirm_gone(&client, &base_url, api_key.as_deref(), &tab_id).await
                            == Some(false)
                    {
                        tracing::warn!(
                            tab_id,
                            "camofox: cancelled tab survived the accepted close; it is leaking"
                        );
                        crw_core::metrics::metrics()
                            .camofox_tab_leak_total
                            .with_label_values(&[USER_ID, "close-noop"])
                            .inc();
                    }
                });
            }
            // Runtime shutting down: nothing async can run, so this one tab
            // waits for the 30 min session sweep. Last resort, loud on purpose.
            // Nothing counted: no reap ran. The ledger is still released — we
            // are not going to close this tab, and holding the id would make it
            // invisible to any reap forever.
            Err(_) => {
                self.registry.unregister(&self.tab_id);
                tracing::warn!(
                    tab_id = %self.tab_id,
                    "camofox: no runtime to reap the cancelled tab; it waits for the session timeout"
                );
            }
        }
    }
}

/// Close the fetch's tab from a normal exit path and disarm the guard, so the
/// same close cannot fire again from `Drop`. `Option` rather than moving the
/// guard itself so several exit paths can share it — and an exit path added
/// later that forgets this call is still leak-free, because the undisarmed
/// guard reaps on drop.
async fn close_tab_guard(guard: &mut Option<TabGuard>) {
    if let Some(guard) = guard.take() {
        guard.close().await;
    }
}

/// `POST /tabs/:id/evaluate` response.
#[derive(Deserialize)]
struct EvaluateResponse {
    result: Option<String>,
    /// camofox caps one evaluate result at 1 MiB of serialized value and, over
    /// that, replaces it with a `[Truncated: …]` placeholder string and sets
    /// this flag. Absent on older servers, hence the default.
    #[serde(default)]
    truncated: bool,
}

/// Slice size for chunked document retrieval (UTF-16 units, JS `slice`
/// semantics). Kept well under camofox's 1 MiB serialized-result cap so a
/// slice never trips it even with heavy JSON escaping; halved on the spot
/// if one does.
const HTML_CHUNK_UNITS: usize = 256 * 1024;

/// Upper bound on chunked retrieval. Documents beyond this are cut, with a
/// warning; nothing downstream wants more than that from one page.
const MAX_CHUNKED_HTML_UNITS: usize = 16 * 1024 * 1024;

/// JS length of the document's outerHTML, as a string so the result is
/// always the `String` the response type expects.
const OUTER_HTML_LEN_EXPR: &str = "String(document.documentElement.outerHTML.length)";

/// Whether an evaluate result is camofox's truncation placeholder, for
/// servers that predate the `truncated` flag.
fn is_truncation_placeholder(result: &str) -> bool {
    result.starts_with("[Truncated: result was ")
}

/// `GET /health` response.
#[derive(Deserialize)]
struct HealthResponse {
    #[serde(rename = "browserConnected")]
    browser_connected: bool,
}

impl CamofoxRenderer {
    /// Build a renderer pointed at `base_url` (e.g. `http://camofox:9377`).
    /// `api_key`, when set, is sent as `Authorization: Bearer`. `timeout` caps
    /// each individual HTTP round-trip to the camofox-browser server.
    pub fn new(name: &str, base_url: &str, api_key: Option<String>, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|e| {
                tracing::error!("camofox: failed to build HTTP client: {e}; using default");
                reqwest::Client::new()
            });
        Self {
            name: name.to_string(),
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            client,
            create_lock: Arc::new(tokio::sync::Mutex::new(())),
            known_ids: TabRegistry::default(),
            reap_orphans: true,
            challenge_wait: DEFAULT_CHALLENGE_WAIT,
            challenge_poll_interval: CHALLENGE_POLL_INTERVAL,
            clearance: None,
        }
    }

    /// Enable clearance capture into `cache` (config `clearance_reuse`).
    pub fn with_clearance_cache(mut self, cache: Arc<ClearanceCache>) -> Self {
        self.clearance = Some(cache);
        self
    }

    /// Turn the create-orphan reap on or off (config
    /// `renderer.camofox.reap_orphan_tabs`, default on). Off means a tab whose
    /// `POST /tabs` response was lost survives until camofox's session timeout
    /// — the behaviour this reap exists to fix — so it is only for backing the
    /// reap out if it ever misidentifies a tab.
    pub fn with_orphan_reap(mut self, enabled: bool) -> Self {
        self.reap_orphans = enabled;
        self
    }

    /// The tab's cookie jar, the whole round-trip bounded by `budget`. The jar
    /// is the whole browser context's, every site this userId visited.
    async fn tab_cookies(&self, tab_id: &str, budget: Duration) -> CrwResult<Vec<Cookie>> {
        if budget.is_zero() {
            return Err(CrwError::Timeout(0));
        }
        let fut = async {
            let resp = self
                .auth(
                    // `USER_ID` is a fixed ASCII token, so it needs no encoding.
                    self.client.get(format!(
                        "{}/tabs/{tab_id}/cookies?userId={USER_ID}",
                        self.base_url
                    )),
                )
                .send()
                .await
                .map_err(|e| {
                    CrwError::RendererError(format!(
                        "camofox /cookies request failed: {}",
                        crw_core::error::reqwest_message(e)
                    ))
                })?;
            if !resp.status().is_success() {
                let status = resp.status();
                let detail = error_detail(resp).await;
                return Err(CrwError::RendererError(format!(
                    "camofox /cookies returned {status}{detail}"
                )));
            }
            resp.json::<CookiesResponse>()
                .await
                .map(CookiesResponse::into_cookies)
                .map_err(|e| {
                    CrwError::RendererError(format!(
                        "camofox /cookies bad response: {}",
                        crw_core::error::reqwest_message(e)
                    ))
                })
        };
        match tokio::time::timeout(budget, fut).await {
            Ok(r) => r,
            Err(_) => Err(CrwError::Timeout(budget.as_millis() as u64)),
        }
    }

    /// After a challenge-free render: if the tab holds a `cf_clearance` cookie
    /// for this host, store this host's cookies + the user agent for the HTTP
    /// tier. Best-effort: every failure logs at `debug` and returns.
    async fn capture_clearance(&self, tab_id: &str, url: &str, deadline: Deadline) {
        let Some(cache) = &self.clearance else {
            return;
        };
        let Some(host) = url::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned))
        else {
            return;
        };
        if deadline.remaining() < MIN_EVAL_BUDGET {
            tracing::debug!(url, "camofox: no budget left for clearance capture");
            return;
        }
        let cookies: Vec<Cookie> = match self.tab_cookies(tab_id, deadline.remaining()).await {
            // Only this host's cookies. The jar is context-wide, so a clearance
            // earned on another site would otherwise be cached for this one.
            Ok(c) => c
                .into_iter()
                .filter(|c| cookie_matches_host(&c.domain, &host) && !c.domain.trim().is_empty())
                .collect(),
            Err(e) => {
                tracing::debug!(url, error = %e, "camofox: cookie export failed");
                return;
            }
        };
        if !cookies.iter().any(|c| c.name == CLEARANCE_COOKIE) {
            return;
        }
        let ua = match self
            .post_decode_within::<EvaluateResponse>(
                &format!("/tabs/{tab_id}/evaluate"),
                json!({ "userId": USER_ID, "expression": "navigator.userAgent" }),
                deadline.remaining(),
                deadline,
            )
            .await
        {
            Ok(r) => r.result.unwrap_or_default(),
            Err(e) => {
                tracing::debug!(url, error = %e, "camofox: user agent read failed");
                return;
            }
        };
        if ua.is_empty() {
            return;
        }
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        if let Some(clearance) = Clearance::from_browser(cookies, ua, now_unix) {
            tracing::info!(host = %host, "camofox: cached cf_clearance for the HTTP tier");
            cache.insert(&host, clearance).await;
        }
    }

    /// Override the passive challenge wait cap (config `challenge_wait_ms`).
    pub fn with_challenge_wait(mut self, wait: Duration) -> Self {
        self.challenge_wait = wait;
        self
    }

    /// Override the sleep between challenge probes. For tests.
    #[doc(hidden)]
    pub fn with_challenge_poll_interval(mut self, interval: Duration) -> Self {
        self.challenge_poll_interval = interval;
        self
    }

    /// Wait, on the open tab, for a Cloudflare managed challenge to clear.
    ///
    /// Probes the DOM every `challenge_poll_interval` for up to
    /// `challenge_wait`, always leaving [`MIN_EVAL_BUDGET`] of the deadline for
    /// the snapshot that follows. Never fails the fetch: on give-up the caller
    /// snapshots whatever is on screen, exactly as before this loop existed,
    /// and the ladder's post-render check classifies it. One failed probe in a
    /// row is tolerated, because the challenge clears by reloading the tab and
    /// an evaluate in that window fails; a second ends the wait.
    async fn wait_out_challenge(&self, tab_id: &str, url: &str, deadline: Deadline) {
        if self.challenge_wait.is_zero() {
            return;
        }
        let path = format!("/tabs/{tab_id}/evaluate");
        let body = json!({ "userId": USER_ID, "expression": CHALLENGE_PROBE_EXPR });
        let started = Instant::now();
        let mut polls = 0u32;
        let mut failed_in_a_row = 0u32;
        loop {
            let left_for_loop = self
                .challenge_wait
                .saturating_sub(started.elapsed())
                .min(deadline.remaining().saturating_sub(MIN_EVAL_BUDGET));
            if left_for_loop.is_zero() {
                break;
            }
            let state = match self
                .post_decode_within::<EvaluateResponse>(
                    &path,
                    body.clone(),
                    left_for_loop,
                    deadline,
                )
                .await
            {
                Ok(r) => {
                    failed_in_a_row = 0;
                    probe_challenge_state(r.result.as_deref().unwrap_or_default())
                }
                Err(e) => {
                    failed_in_a_row += 1;
                    if failed_in_a_row >= 2 {
                        tracing::debug!(url, error = %e, "camofox: challenge probe failed twice; ending the wait");
                        break;
                    }
                    tracing::debug!(url, error = %e, "camofox: challenge probe failed; retrying once");
                    ChallengeState::Challenge
                }
            };
            match state {
                ChallengeState::Clear => {
                    if polls > 0 {
                        tracing::info!(
                            url,
                            polls,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "camofox: challenge cleared"
                        );
                        crw_core::metrics::metrics()
                            .render_route_decision_total
                            .with_label_values(&["camofox", "challengeCleared"])
                            .inc();
                    }
                    return;
                }
                ChallengeState::Wall => {
                    tracing::debug!(url, "camofox: Cloudflare block page on screen; not waiting");
                    return;
                }
                ChallengeState::Challenge => {}
            }
            if polls == 0 {
                tracing::info!(
                    url,
                    "camofox: Cloudflare challenge on screen, waiting for it to clear"
                );
            }
            polls += 1;
            let sleep = self.challenge_poll_interval.min(
                self.challenge_wait
                    .saturating_sub(started.elapsed())
                    .min(deadline.remaining().saturating_sub(MIN_EVAL_BUDGET)),
            );
            if sleep.is_zero() {
                break;
            }
            tokio::time::sleep(sleep).await;
        }
        if polls > 0 {
            tracing::warn!(
                url,
                polls,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "camofox: challenge did not clear within budget"
            );
            crw_core::metrics::metrics()
                .render_route_decision_total
                .with_label_values(&["camofox", "challengeStuck"])
                .inc();
        }
    }

    /// Attach the bearer header when an API key is configured.
    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.api_key {
            Some(key) => req.bearer_auth(key),
            None => req,
        }
    }

    async fn post_json(&self, path: &str, body: serde_json::Value) -> CrwResult<reqwest::Response> {
        self.auth(self.client.post(format!("{}{path}", self.base_url)))
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                // The camofox URL is internal; log it, return the stripped message.
                tracing::warn!("camofox {path} request failed: {e}");
                CrwError::RendererError(format!(
                    "camofox {path} request failed: {}",
                    crw_core::error::reqwest_message(e)
                ))
            })
    }

    /// Open a blank tab, retrying a 5xx create (see [`CREATE_TAB_ATTEMPTS`]).
    /// Each attempt's send + decode is bounded by the remaining deadline; a
    /// non-5xx failure surfaces at once.
    ///
    /// Everything here runs inside [`Self::create_lock`] and three invariants
    /// depend on that, so do not narrow the critical section:
    ///
    /// 1. A successful id is registered in [`Self::known_ids`] BEFORE the lock
    ///    is released. camofox registers a tab server-side before it responds
    ///    with the id, so an in-flight peer create is already listable —
    ///    registering under the same lock is what makes "listed but unknown"
    ///    mean "no IN-PROCESS fetch owns this" (see `reap_lost_create` for the
    ///    cross-process limitation).
    /// 2. The orphan reap for a lost create answer ([`Self::reap_lost_create`])
    ///    runs under that same lock, so no create can start or finish while the
    ///    reap is deciding.
    /// 3. [`CreateGuard`] spans the whole call, so the create being cancelled
    ///    mid-await reaps too — that case reaches no arm of the match below.
    async fn create_tab(&self, deadline: Deadline) -> CrwResult<String> {
        let _serialized = self.create_lock.lock().await;
        // Armed across the whole create, because the future can be DROPPED
        // inside the awaits below — an arm of the match cannot see that at all,
        // and camofox registers the tab before it replies.
        let mut guard = CreateGuard {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            api_key: self.api_key.clone(),
            registry: self.known_ids.clone(),
            create_lock: Arc::clone(&self.create_lock),
            reap_orphans: self.reap_orphans,
            armed: true,
            sent: false,
        };
        let body = json!({ "userId": USER_ID, "sessionKey": SESSION_KEY });
        let mut attempt = 1;
        let mut backoff = CREATE_TAB_BACKOFF;
        loop {
            let budget = deadline.remaining();
            if budget.is_zero() {
                // Nothing sent this iteration, and any earlier attempt was a
                // 5xx it answered with — a 5xx create registers no tab.
                guard.disarm();
                return Err(CrwError::Timeout(deadline.requested_ms()));
            }
            let can_retry = attempt < CREATE_TAB_ATTEMPTS;
            let fut = async {
                let resp = match self.post_json("/tabs", body.clone()).await {
                    Ok(resp) => resp,
                    // No answer at all: the tab may be registered under an id
                    // we never learned.
                    Err(e) => return CreateAttempt::Lost(e),
                };
                let status = resp.status();
                if status.is_success() {
                    return match resp.json::<CreateTabResponse>().await {
                        Ok(r) => CreateAttempt::Created(r.tab_id),
                        // A 2xx means camofox DID register the tab; a body we
                        // cannot read leaves its id unknown. Same leak shape.
                        Err(e) => CreateAttempt::Lost(CrwError::RendererError(format!(
                            "camofox /tabs bad response: {}",
                            crw_core::error::reqwest_message(e)
                        ))),
                    };
                }
                let detail = error_detail(resp).await;
                if status.is_server_error() && can_retry {
                    return CreateAttempt::Retryable(format!("{status}{detail}"));
                }
                CreateAttempt::Failed(CrwError::RendererError(format!(
                    "camofox /tabs returned {status}{detail}"
                )))
            };
            // From this statement the request is in flight, so a drop here is
            // the shape the guard exists for.
            guard.mark_sent();
            match tokio::time::timeout(budget, fut).await {
                Ok(CreateAttempt::Created(tab_id)) => {
                    // Invariant 1 above.
                    self.known_ids.register(&tab_id);
                    guard.disarm();
                    return Ok(tab_id);
                }
                Ok(CreateAttempt::Retryable(transient)) => {
                    tracing::info!(
                        attempt,
                        error = %transient,
                        "camofox: tab create failed, retrying"
                    );
                    // camofox ANSWERED this attempt, and an answered create
                    // registers no tab — so during the backoff there is nothing
                    // for the guard to reap.
                    guard.mark_not_sent();
                    tokio::time::sleep(backoff.min(deadline.remaining())).await;
                    backoff *= 2;
                    attempt += 1;
                }
                // camofox answered: no tab was registered, nothing to reap.
                Ok(CreateAttempt::Failed(e)) => {
                    guard.disarm();
                    return Err(e);
                }
                Ok(CreateAttempt::Lost(e)) => {
                    self.reap_lost_create("no response", &e).await;
                    // Only after the reap: a cancellation landing inside it must
                    // still hand the job to a detached task.
                    guard.disarm();
                    return Err(e);
                }
                Err(_) => {
                    let e = CrwError::Timeout(budget.as_millis() as u64);
                    self.reap_lost_create("client timeout", &e).await;
                    guard.disarm();
                    return Err(e);
                }
            }
        }
    }

    /// Reap the tab a create left registered-but-unnamed, when `POST /tabs`
    /// produced no usable answer (the leak `create_tab`'s old comment admitted
    /// to, and which camofox does NOT clean up: its idle reaper only takes
    /// ZERO-tab sessions, so the orphan holds 1 of the session's 10 tab slots
    /// for the full 30 min session timeout).
    ///
    /// Only called with [`Self::create_lock`] held, which serializes every
    /// create and every registration — so a listed id missing from
    /// [`Self::known_ids`] has no IN-PROCESS owner. Read that qualifier as the
    /// limitation it is: the ledger is per-process while camofox's tab set is
    /// per-`userId`, so a SECOND crw process pointed at the same endpoint is
    /// indistinguishable from us (same `userId`, same `sessionKey`). That is
    /// why this is off unless crw owns the endpoint — see the `reap_orphans`
    /// field and `CamofoxEndpoint::reap_orphan_tabs`.
    ///
    /// The operating requirement that makes it safe is one crw process per
    /// `userId` + `sessionKey` per endpoint. `manage` only APPROXIMATES that:
    /// the supervisor's fast path adopts whatever already answers `/health`
    /// without starting anything, so two processes both set to `manage = true`
    /// on the same endpoint both believe they own it, and share `USER_ID` and
    /// `SESSION_KEY` verbatim. The unknown-id, sessionKey and still-blank checks
    /// below are what stand in for the ownership we cannot actually prove.
    ///
    /// When both reapers run for one create — the inline one below and the
    /// guard's detached one, behind a cancellation that landed inside the first —
    /// the second finds the tab already gone and stands down: `create-orphan`
    /// then counts one LESS than the tabs reaped, and the abort is only visible
    /// in `cancelled`. Read the two together, never as a per-tab tally.
    ///
    /// Three fail-safes still gate the close, because closing the wrong tab
    /// costs a caller its request while leaving a real orphan only costs a slot:
    ///
    /// * exactly ONE unknown id — with two or more we cannot tell ours from a
    ///   peer's, so close nothing;
    /// * the tab reports our own `sessionKey` (`listItemId`), so tabs belonging
    ///   to a different browser context under the same `userId` are never
    ///   candidates, and a server that stops reporting it is never reaped from;
    /// * that tab is still BLANK ([`is_blank_url`]) — we create blank and
    ///   navigate separately, so a tab on a real URL cannot be the one we lost.
    ///
    /// Listing is never evidence of anything: `GET /tabs` awaits
    /// `page.title()` per tab, so a failed or timed-out list means "close
    /// nothing". And a DELETE being accepted is not proof either (the route
    /// answers `{ok:true}` even when it matched nothing), so closure is checked
    /// by re-listing and only a proven survivor is counted `close-noop`.
    async fn reap_lost_create(&self, cause: &str, err: &CrwError) {
        if !self.reap_orphans {
            tracing::debug!(
                cause,
                "camofox: orphan reap disabled (endpoint not exclusively owned, or configured off); \
                 tab may leak until the session timeout"
            );
            return;
        }
        reap_lost_tabs(
            &self.client,
            &self.base_url,
            self.api_key.as_deref(),
            &self.known_ids,
            cause,
            &err.to_string(),
        )
        .await;
    }

    /// Navigate an open tab to `url`, send + decode bounded by the remaining
    /// deadline. A non-2xx answer carries camofox's message.
    async fn navigate_tab(&self, tab_id: &str, url: &str, deadline: Deadline) -> CrwResult<()> {
        let budget = deadline.remaining();
        if budget.is_zero() {
            return Err(CrwError::Timeout(deadline.requested_ms()));
        }
        let path = format!("/tabs/{tab_id}/navigate");
        let fut = async {
            let resp = self
                .post_json(&path, json!({ "userId": USER_ID, "url": url }))
                .await?;
            let status = resp.status();
            if status.is_success() {
                return Ok(());
            }
            let detail = error_detail(resp).await;
            Err(CrwError::RendererError(format!(
                "camofox {path} returned {status}{detail}"
            )))
        };
        let e = match tokio::time::timeout(budget, fut).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => e,
            Err(_) => return Err(CrwError::Timeout(budget.as_millis() as u64)),
        };
        match (e, self.tab_location(tab_id, deadline).await.ok()) {
            (e, Some(TabLocation::Loaded(_))) => {
                // camofox's navigate route builds an ARIA snapshot of the
                // page after the navigation resolved (and after it recorded
                // the navigation as successful), with its own 10 s timeout
                // and no way to opt out. On very large documents that
                // snapshot times out and the route answers 500 (with a
                // sanitized body, so the cause is not visible here) although
                // the page is loaded. The tab holding a real document is the
                // tell that the navigation itself committed; we never use the
                // snapshot, so carry on.
                tracing::warn!(
                    url,
                    error = %e,
                    "camofox: navigate reported failure but the page committed; continuing"
                );
                Ok(())
            }
            // Firefox showed its own error page (DNS failure, refused, blocked
            // port). `location.href` still reads as the requested URL, so this
            // is only visible through `document.documentURI`.
            (_, Some(TabLocation::ErrorPage(code))) => Err(navigation_failed(&code)),
            // The browser answered and the tab never left about:blank: the page
            // did not load. camofox-browser sanitizes the Firefox error
            // (NS_ERROR_UNKNOWN_HOST, connection refused) to "Internal server
            // error", so this is the only evidence. Say "navigation failed" so
            // the ladder can pair it with the HTTP tier's `TargetUnreachable` and
            // attribute a dead origin to the caller.
            (CrwError::RendererError(msg), Some(TabLocation::Blank)) => {
                Err(navigation_failed(&msg))
            }
            (e, _) => Err(e),
        }
    }

    /// Fail unless the tab's current document is a destination the outbound
    /// policy allows. Same rules as the CDP tiers' per-request check
    /// (`crw_core::url_safety::classify_safe_host_resolved`): no-socket schemes
    /// pass, anything else must be http(s) to a public address. A Firefox error
    /// page fails as a navigation failure: its text is not the page.
    async fn check_final_url(&self, tab_id: &str, deadline: Deadline) -> CrwResult<()> {
        let href = match self.tab_location(tab_id, deadline).await? {
            TabLocation::Loaded(href) => href,
            TabLocation::ErrorPage(code) => return Err(navigation_failed(&code)),
            // Still on about:blank after navigate succeeded: nothing to check,
            // and the empty-document guard after the evaluate handles it.
            TabLocation::Blank => return Ok(()),
        };
        let Ok(parsed) = url::Url::parse(&href) else {
            return Err(CrwError::RendererError(
                "camofox: could not read the page's final URL".to_string(),
            ));
        };
        if matches!(parsed.scheme(), "about" | "data" | "blob") {
            return Ok(());
        }
        let verdict = if matches!(parsed.scheme(), "http" | "https") {
            match tokio::time::timeout(
                deadline.remaining(),
                crw_core::url_safety::classify_safe_host_resolved(&parsed),
            )
            .await
            {
                Ok(v) => v,
                Err(_) => return Err(CrwError::Timeout(deadline.requested_ms())),
            }
        } else {
            Err(crw_core::url_safety::HostRejection::Policy(
                "scheme".to_string(),
            ))
        };
        match verdict {
            Ok(()) => Ok(()),
            Err(crw_core::url_safety::HostRejection::Policy(_)) => {
                crw_core::metrics::metrics()
                    .chrome_blocked_requests_total
                    .with_label_values(&["camofox_final_url"])
                    .inc();
                tracing::warn!(tab_id, "camofox: page navigated to a blocked destination");
                Err(CrwError::RendererError(
                    "camofox: the page navigated to a blocked destination".to_string(),
                ))
            }
            // Our resolver could not answer. Fail closed, and say it is ours.
            Err(crw_core::url_safety::HostRejection::Unresolved(reason)) => {
                Err(CrwError::RendererError(format!(
                    "camofox: outbound destination check unavailable ({reason})"
                )))
            }
        }
    }

    /// Where the tab is. Errors when the probe itself fails or no budget remains.
    async fn tab_location(&self, tab_id: &str, deadline: Deadline) -> CrwResult<TabLocation> {
        let r = self
            .post_decode_within::<EvaluateResponse>(
                &format!("/tabs/{tab_id}/evaluate"),
                json!({ "userId": USER_ID, "expression": TAB_LOCATION_EXPR }),
                deadline.remaining().min(Duration::from_secs(5)),
                deadline,
            )
            .await?;
        Ok(TabLocation::from_probe(
            r.result.as_deref().unwrap_or_default(),
        ))
    }

    /// The HTTP status of the tab's document, or `None` when the probe fails
    /// or the browser does not report one.
    async fn nav_status(&self, tab_id: &str, deadline: Deadline) -> Option<u16> {
        match self
            .post_decode_within::<EvaluateResponse>(
                &format!("/tabs/{tab_id}/evaluate"),
                json!({ "userId": USER_ID, "expression": NAV_STATUS_EXPR }),
                deadline.remaining().min(Duration::from_secs(5)),
                deadline,
            )
            .await
        {
            Ok(r) => parse_nav_status(r.result.as_deref()),
            Err(e) => {
                tracing::debug!(tab_id, error = %e, "camofox: navigation status probe failed");
                None
            }
        }
    }

    /// Retrieve the document's outerHTML in slices, for pages whose HTML
    /// exceeds camofox's single-result cap. Slices are taken by UTF-16 offset
    /// (JS string semantics); the expression never ends a slice on a lone
    /// high surrogate, and the next offset advances by the received slice's
    /// UTF-16 length, so multibyte characters are never split.
    async fn evaluate_html_chunked(&self, tab_id: &str, deadline: Deadline) -> CrwResult<String> {
        let path = format!("/tabs/{tab_id}/evaluate");
        let total: usize = self
            .post_decode_within::<EvaluateResponse>(
                &path,
                json!({ "userId": USER_ID, "expression": OUTER_HTML_LEN_EXPR }),
                deadline.remaining(),
                deadline,
            )
            .await?
            .result
            .unwrap_or_default()
            .trim()
            .parse()
            .map_err(|e| CrwError::RendererError(format!("camofox: bad document length: {e}")))?;
        let end = total.min(MAX_CHUNKED_HTML_UNITS);
        if total > end {
            tracing::warn!(
                tab_id,
                total_units = total,
                cap_units = end,
                "camofox: document exceeds the chunked retrieval cap; cutting"
            );
        }
        let mut html = String::with_capacity(end);
        let mut start = 0usize;
        let mut chunk = HTML_CHUNK_UNITS;
        while start < end {
            let stop = (start + chunk).min(end);
            let expr = format!(
                "(function(s,a,b){{if(b<s.length){{var c=s.charCodeAt(b-1);\
                 if(c>=0xD800&&c<=0xDBFF)b--;}}return s.slice(a,b);}})\
                 (document.documentElement.outerHTML,{start},{stop})"
            );
            let r = self
                .post_decode_within::<EvaluateResponse>(
                    &path,
                    json!({ "userId": USER_ID, "expression": expr }),
                    deadline.remaining(),
                    deadline,
                )
                .await?;
            if r.truncated || r.result.as_deref().is_some_and(is_truncation_placeholder) {
                if chunk <= 4096 {
                    return Err(CrwError::RendererError(
                        "camofox: evaluate slice truncated even at the minimum chunk size".into(),
                    ));
                }
                chunk /= 2;
                continue;
            }
            let piece = r.result.unwrap_or_default();
            let advanced: usize = piece.chars().map(char::len_utf16).sum();
            if advanced == 0 {
                // The document shrank under us (navigation, script rewrite);
                // return what we have rather than spin.
                break;
            }
            html.push_str(&piece);
            start += advanced;
        }
        Ok(html)
    }

    /// Take ownership of a freshly created tab so its close survives
    /// CANCELLATION. Hand every exit path of `fetch` the same `Option` and close
    /// it with [`close_tab_guard`] before returning; if the future is dropped
    /// mid-await instead — crawl abort, outer request timeout — the guard's
    /// `Drop` still reaps the tab, which no `.await`-based cleanup can promise.
    fn tab_guard(&self, tab_id: String) -> TabGuard {
        TabGuard::new(
            self.client.clone(),
            self.base_url.clone(),
            self.api_key.clone(),
            tab_id,
            self.known_ids.clone(),
        )
    }

    /// Fire-and-discard POST bounded by `budget`. The response is dropped
    /// unread, so only the request send is bounded — used for `/wait`, whose
    /// body we never decode. The client's own `timeout` is a fixed per-op
    /// ceiling (config `chrome_timeout`, commonly 30s) far longer than a tight
    /// scrape deadline; without this each round-trip could run for that full
    /// ceiling and blow past the caller's deadline (the `PageFetcher` contract).
    /// Returns `Timeout` when the budget is already spent or the call outlives it.
    async fn post_discard_within(
        &self,
        path: &str,
        body: serde_json::Value,
        budget: Duration,
        deadline: Deadline,
    ) -> CrwResult<()> {
        if budget.is_zero() {
            // Report the caller's budget, not 0: nothing was awaited here.
            return Err(CrwError::Timeout(deadline.requested_ms()));
        }
        match tokio::time::timeout(budget, self.post_json(path, body)).await {
            Ok(r) => r.map(|_| ()),
            Err(_) => Err(CrwError::Timeout(budget.as_millis() as u64)),
        }
    }

    /// POST and decode the JSON body, the WHOLE round-trip (send, status check,
    /// body read) bounded by `budget`. Bounding only the send would let a
    /// stalled response body still overrun the deadline, so the decode is inside
    /// the timeout too. Returns `Timeout` when the budget is spent or exceeded.
    async fn post_decode_within<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: serde_json::Value,
        budget: Duration,
        deadline: Deadline,
    ) -> CrwResult<T> {
        if budget.is_zero() {
            // Report the caller's budget, not 0: nothing was awaited here.
            return Err(CrwError::Timeout(deadline.requested_ms()));
        }
        let fut = async {
            let resp = self.post_json(path, body).await?;
            if !resp.status().is_success() {
                let status = resp.status();
                let detail = error_detail(resp).await;
                return Err(CrwError::RendererError(format!(
                    "camofox {path} returned {status}{detail}"
                )));
            }
            resp.json::<T>().await.map_err(|e| {
                CrwError::RendererError(format!(
                    "camofox {path} bad response: {}",
                    crw_core::error::reqwest_message(e)
                ))
            })
        };
        match tokio::time::timeout(budget, fut).await {
            Ok(r) => r,
            Err(_) => Err(CrwError::Timeout(budget.as_millis() as u64)),
        }
    }
}

/// Cap on how much of camofox's `error` message is carried into the error.
const ERROR_BODY_CAP: usize = 300;

/// `: <message>` from a failed camofox response, or `""` when there is none.
/// camofox reports the real cause in the body's `error` field (e.g. a
/// profile/Camoufox version mismatch); only that field passes through, a
/// non-JSON body (a proxy's HTML page) is logged, not surfaced, since renderer
/// errors reach API responses.
async fn error_detail(resp: reqwest::Response) -> String {
    let status = resp.status().as_u16();
    let raw = match resp.text().await {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!(status, error = %e, "camofox: error body unreadable");
            String::new()
        }
    };
    let msg = serde_json::from_str::<serde_json::Value>(&raw)
        .ok()
        .and_then(|v| v.get("error")?.as_str().map(str::to_string))
        .unwrap_or_else(|| {
            if !raw.trim().is_empty() {
                tracing::debug!(status, body = %raw.trim(), "camofox: non-JSON error body");
            }
            String::new()
        });
    let msg: String = msg.trim().chars().take(ERROR_BODY_CAP).collect();
    if msg.is_empty() {
        String::new()
    } else {
        format!(": {msg}")
    }
}

/// `location.href`, or `document.documentURI` when Firefox is showing one of its
/// own error pages. Firefox keeps `location.href` at the requested URL on those,
/// so the error page is only visible through `documentURI`
/// (`about:neterror?e=dnsNotFound&u=…`).
const TAB_LOCATION_EXPR: &str = "(/^about:(neterror|certerror|blocked)/.test(document.documentURI) \
     ? document.documentURI : location.href)";

/// The document's HTTP status from its Navigation Timing entry. camofox-browser's
/// navigate route returns no status, but Firefox records it here: `404` for a
/// GitHub not-found page, measured live. `0` when there is no entry.
const NAV_STATUS_EXPR: &str =
    "String((performance.getEntriesByType('navigation')[0] || {}).responseStatus || 0)";

fn parse_nav_status(result: Option<&str>) -> Option<u16> {
    result?
        .trim()
        .parse::<u16>()
        .ok()
        .filter(|s| (100..=599).contains(s))
}

/// What the tab is showing, from [`TAB_LOCATION_EXPR`].
#[derive(Debug, PartialEq, Eq)]
enum TabLocation {
    /// No navigation committed.
    Blank,
    /// Firefox's own error page; carries its `e=` code (`dnsNotFound`,
    /// `connectionFailure`, `deniedPortAccess`, …).
    ErrorPage(String),
    /// A real document at this URL.
    Loaded(String),
}

impl TabLocation {
    fn from_probe(result: &str) -> Self {
        if result.is_empty() || result == "about:blank" {
            return Self::Blank;
        }
        if let Some(query) = ["about:neterror", "about:certerror", "about:blocked"]
            .iter()
            .find_map(|p| result.strip_prefix(p))
        {
            let code = query
                .trim_start_matches('?')
                .split('&')
                .find_map(|kv| kv.strip_prefix("e="))
                .filter(|c| !c.is_empty())
                .unwrap_or("unknown");
            return Self::ErrorPage(code.to_string());
        }
        Self::Loaded(result.to_string())
    }
}

/// The error the ladder reads as "the origin could not be loaded"
/// (`is_origin_navigation_failure` matches "navigation failed").
fn navigation_failed(detail: &str) -> CrwError {
    CrwError::RendererError(format!(
        "camofox: navigation failed, page did not load: {detail}"
    ))
}

#[async_trait]
impl PageFetcher for CamofoxRenderer {
    async fn fetch(
        &self,
        url: &str,
        _headers: &HashMap<String, String>,
        wait_for_ms: Option<u64>,
        deadline: Deadline,
    ) -> CrwResult<FetchResult> {
        if deadline.expired() {
            return Err(CrwError::RendererError(format!(
                "camofox: deadline expired before fetch of {url}"
            )));
        }
        let start = Instant::now();

        // 1. Open a blank tab, then navigate it separately. Two distinct leaks
        //    used to live here, and they need two mechanisms: a create whose
        //    ANSWER was lost leaked the tab it had already registered
        //    server-side (`create_tab` reaps that under `create_lock`), and a
        //    create whose FUTURE was dropped mid-await leaked one that no arm of
        //    that match ever ran (`CreateGuard` reaps that, detached). The tab we
        //    DID learn the id of goes straight into a [`TabGuard`], so every
        //    later exit — including this future being cancelled mid-await —
        //    still closes it.
        let tab_id = self.create_tab(deadline).await?;
        let mut tab = Some(self.tab_guard(tab_id.clone()));
        if let Err(e) = self.navigate_tab(&tab_id, url, deadline).await {
            close_tab_guard(&mut tab).await;
            return Err(e);
        }

        // 2. Wait for readiness, bounded by the smaller of the caller's
        //    `wait_for_ms` hint and the remaining request budget. The HTTP call
        //    itself is capped at the remaining budget too, so a server-side wait
        //    that ignores its `timeout` can't overrun the deadline.
        let budget_ms = deadline.remaining().as_millis() as u64;
        let wait_ms = wait_for_ms.unwrap_or(budget_ms).min(budget_ms);
        let _ = self
            .post_discard_within(
                &format!("/tabs/{tab_id}/wait"),
                json!({ "userId": USER_ID, "timeout": wait_ms }),
                deadline.remaining(),
                deadline,
            )
            .await;

        // 2b. If the page is a Cloudflare managed challenge, give Camoufox a
        //     bounded chance to clear it before snapshotting. Before the
        //     final-URL check and the status probe, because clearing reloads
        //     the tab.
        self.wait_out_challenge(&tab_id, url, deadline).await;

        // 3. Refuse to return a page that ended up somewhere internal. The route
        //    layer checked the URL the caller gave, but a redirect or a JS
        //    navigation inside the browser can land on the metadata endpoint or
        //    a compose service, and camofox renders whatever it lands on. Checked
        //    after the wait so client-side redirects have happened. The probe
        //    failing means we cannot tell where the page is, so it fails closed.
        //
        //    This guards what crw RETURNS. Requests the browser makes along the
        //    way (a redirect hop, subresources) still reach the network:
        //    camofox-browser's `CAMOFOX_ALLOW_PRIVATE_NETWORK=false` only checks
        //    the URL it is asked to open (verified live: a redirect to a compose
        //    service was followed). Network-level egress rules are the only
        //    complete control.
        if let Err(e) = self.check_final_url(&tab_id, deadline).await {
            close_tab_guard(&mut tab).await;
            return Err(e);
        }

        // Best-effort: without it the page is reported as a 200, as before.
        let status_code = self.nav_status(&tab_id, deadline).await.unwrap_or(200);

        // 4. Evaluate the rendered DOM, send + body decode bounded by the budget.
        //    A document larger than camofox's 1 MiB result cap comes back as a
        //    placeholder; fetch those in slices instead of treating the
        //    placeholder as the page.
        let html = match self
            .post_decode_within::<EvaluateResponse>(
                &format!("/tabs/{tab_id}/evaluate"),
                json!({ "userId": USER_ID, "expression": OUTER_HTML_EXPR }),
                deadline.remaining(),
                deadline,
            )
            .await
        {
            Ok(r) if r.truncated || r.result.as_deref().is_some_and(is_truncation_placeholder) => {
                self.evaluate_html_chunked(&tab_id, deadline).await
            }
            Ok(r) => Ok(r.result.unwrap_or_default()),
            Err(e) => Err(e),
        };

        // 4b. Clearance capture: only for a challenge-free document, only when a
        //     cache is wired. Never fails the fetch.
        if let Ok(h) = &html
            && !h.is_empty()
            && !detector::looks_like_cloudflare_challenge(h)
        {
            self.capture_clearance(&tab_id, url, deadline).await;
        }

        // 5. Best-effort close — never fail the fetch on cleanup. Disarms the
        //    guard; the `html?` below and every earlier exit are covered either
        //    way.
        close_tab_guard(&mut tab).await;

        let html = html?;
        if html.is_empty() {
            return Err(CrwError::RendererError(
                "camofox: evaluate returned empty document".to_string(),
            ));
        }

        // The camofox-browser REST API exposes only `tabId` and the evaluated
        // `result` — it returns no navigation status code, final URL, or response
        // content-type. The status comes from the page's Navigation Timing entry
        // (`nav_status`); final URL and content-type are synthetic, NOT observed
        // from the wire: a camofox-rendered redirect is reported without its
        // final URL. Downstream anti-bot/block classification still runs on the
        // returned `html` (see crw_crawl::single::classify_block).
        Ok(FetchResult {
            url: url.to_string(),
            final_url: None,
            status_code,
            html,
            content_type: Some("text/html".to_string()),
            raw_bytes: None,
            rendered_with: Some("camofox".to_string()),
            elapsed_ms: start.elapsed().as_millis() as u64,
            warning: None,
            render_decision: None,
            credit_cost: 0,
            warnings: Vec::new(),
            wall: None,
            truncated: false,
            deadline_exceeded: deadline.expired(),
            captured_responses: Vec::new(),
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn supports_js(&self) -> bool {
        true
    }

    async fn is_available(&self) -> bool {
        let req = self.auth(self.client.get(format!("{}/health", self.base_url)));
        match req.send().await {
            Ok(resp) if resp.status().is_success() => resp
                .json::<HealthResponse>()
                .await
                .map(|h| h.browser_connected)
                .unwrap_or(false),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ChallengeState, TabLocation, parse_nav_status, probe_challenge_state};

    #[test]
    fn probe_reads_title_and_marker() {
        use ChallengeState::*;
        assert_eq!(
            probe_challenge_state(r#"{"t":"Just a moment...","m":false}"#),
            Challenge
        );
        assert_eq!(probe_challenge_state(r#"{"t":"Site","m":true}"#), Challenge);
        assert_eq!(probe_challenge_state(r#"{"t":"Site","m":false}"#), Clear);
        // The hard block wins over the marker: it never clears.
        assert_eq!(
            probe_challenge_state(r#"{"t":"Attention Required! | Cloudflare","m":true}"#),
            Wall
        );
        assert_eq!(probe_challenge_state("<html>not json</html>"), Clear);
        assert_eq!(probe_challenge_state(""), Clear);
    }

    #[test]
    fn nav_status_reads_the_document_status() {
        assert_eq!(parse_nav_status(Some("404")), Some(404));
        assert_eq!(parse_nav_status(Some("200")), Some(200));
        // No navigation entry, or a browser without `responseStatus`.
        assert_eq!(parse_nav_status(Some("0")), None);
        assert_eq!(parse_nav_status(Some("undefined")), None);
        assert_eq!(parse_nav_status(None), None);
        assert_eq!(parse_nav_status(Some("<html></html>")), None);
    }

    #[test]
    fn tab_location_reads_firefox_error_pages() {
        assert_eq!(TabLocation::from_probe("about:blank"), TabLocation::Blank);
        assert_eq!(TabLocation::from_probe(""), TabLocation::Blank);
        assert_eq!(
            TabLocation::from_probe("about:neterror?e=dnsNotFound&u=https%3A//x.invalid/&c=UTF-8"),
            TabLocation::ErrorPage("dnsNotFound".into())
        );
        assert_eq!(
            TabLocation::from_probe("about:certerror?e=nssFailure2"),
            TabLocation::ErrorPage("nssFailure2".into())
        );
        assert_eq!(
            TabLocation::from_probe("about:neterror"),
            TabLocation::ErrorPage("unknown".into())
        );
        assert_eq!(
            TabLocation::from_probe("https://example.com/"),
            TabLocation::Loaded("https://example.com/".into())
        );
    }
}
