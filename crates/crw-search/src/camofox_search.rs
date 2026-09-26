//! Camofox-backed web-search client.
//!
//! Search SERPs trip anti-bot / consent walls immediately, so search does NOT
//! use the renderer failover ladder — it drives the camofox-browser (Firefox)
//! tier directly: navigate a tab to the engine's SERP, wait, then scrape the
//! result rows via `/evaluate`. Google navigates its SERP URL directly with
//! the locale pinned (`hl=en&gl=us`, so the extractor sees stable English
//! markup) and falls back to the browser's `@google_search` macro when the
//! direct URL renders no rows (consent wall); Bing/DuckDuckGo have no working
//! macro in camofox-browser, so they always navigate their search URL directly
//! (see [`navigate_body`]). Multiple engines requested in one call run
//! sequentially on the warm tab and their rows are merged (see
//! [`merge_results`]).
//!
//! Concurrency: camofox-browser keys one persistent context per `userId` and
//! eagerly tears that context down when its tab count hits zero, leaving a
//! ~9s relaunch window in which `newPage` throws `window is null`. Creating
//! and deleting a tab per query raced that teardown, so concurrent or
//! rapid-sequential searches failed with empty / 5xx results. We avoid the
//! race entirely: a single [`tokio::sync::Mutex`] serializes all browser
//! access and guards ONE long-lived warm tab that is reused across queries —
//! the context never sees concurrent tabs nor drops to zero. If the warm tab
//! goes stale (idle eviction / camofox restart) the next navigate fails — or
//! hangs, when camofox accepts a navigate for a tab it no longer has — and we
//! transparently recreate the tab and retry once. A tab abandoned after a
//! client-side timeout is closed only once its replacement has been adopted
//! (see [`CamofoxSearchClient::abandon_tab`]), so in the normal case the count
//! never hits zero; if the replacement's create is itself too slow to wait for,
//! we accept one retried create rather than keep a wedged tab in the count.
//!
//! Rows are mapped into the existing [`SearxngResponse`] shape so the entire
//! downstream transform / rerank pipeline (`transform.rs`, `rerank.rs`) is
//! reused unchanged — this client is a drop-in alternative upstream source, not
//! a new result format.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use crw_core::types::SearchEngine;

use crate::client::{
    MAX_ERROR_BODY_BYTES, SearchError, SearxngResponse, SearxngResult, read_capped,
};
use crate::params::SearxngParams;

/// Stable `userId` for the search client's camofox sessions (separate from the
/// renderer tier's so search and scrape don't share a profile).
const USER_ID: &str = "crw-search";

/// Browser-context key. `/tabs` requires both `userId` and `sessionKey`. Fixed
/// so the context is reused across queries; combined with the single warm tab
/// (see [`CamofoxSearchClient::tab`]) the context never accumulates tabs nor
/// drops to zero, and sessions don't leak toward MAX_SESSIONS.
const SESSION_KEY: &str = "search";

/// Pause before recreating a stale tab, giving any in-flight upstream context
/// relaunch a moment to settle before we retry. Only hit on the rare
/// stale-tab path (idle eviction / camofox restart), not the steady state.
const RETRY_BACKOFF: Duration = Duration::from_millis(750);

/// Cap on the best-effort `DELETE /tabs/{id}` sent when a tab is abandoned after
/// a timeout. It is NOT a latency guard — the close is detached and nobody waits
/// on it — so it is deliberately longer than the server's own 5 s allowance for
/// `safePageClose`: hanging up earlier is precisely how an abandoned tab gets
/// left listed with nobody left to close it.
const CLEANUP_BUDGET: Duration = Duration::from_secs(8);

/// Ceiling on how long [`CamofoxSearchClient::abandon_tab`] will WAIT for the
/// replacement tab. It bounds the wait only, never the request: the create runs
/// in a detached task (see [`CamofoxSearchClient::spawn_create_tab`]) because a
/// cancelled create is a permanent leak here, while a slow one costs nothing but
/// the round-trip the next search would have paid anyway. Without this bound one
/// flaky navigate could spend a full client timeout (`search.timeout_ms`, 60 s)
/// inside a recovery that only saves a later request one round-trip.
const PREWARM_BUDGET: Duration = Duration::from_secs(5);

/// How many unadopted, already-created tabs the shelf may hold before the oldest
/// is closed. One warm tab plus one in-flight replacement is the working set;
/// past that, whoever is failing is not going to adopt them, so they are closed
/// on the spot rather than aged toward the session timeout.
const PREWARM_SHELF: usize = 2;

/// Per-link cap on resolving a Google redirect link. Resolution runs while the
/// warm-tab mutex is held, so a slow answer must not stall the next search; a
/// link that misses it keeps its redirect URL.
const REDIRECT_RESOLVE_TIMEOUT: Duration = Duration::from_secs(3);

/// JS evaluated in the Google SERP to extract result rows. Returns a JSON
/// *string* (via `JSON.stringify`) so the camofox `/evaluate` `result` field
/// comes back as a string we can parse. Selectors are intentionally broad and
/// kept in this one place — Google rewrites its SERP DOM periodically, so this
/// is the single spot to fix when extraction drifts.
///
/// Two chrome guards, both learned live: the title may ONLY come from an `h3`
/// INSIDE the organic anchor (Google's AI-mode teaser heading sits loose in
/// the same `div.g` container and previously paired onto the next row's URL),
/// and chrome anchors pointing back at Google (`/search`, `/set/…` — the
/// AI-mode / related-search links) are skipped, so an interstitial never
/// contributes a row. A row whose anchor carries no inner `h3` keeps its
/// anchor text (possibly empty — the API client can still title it from the
/// URL).
const GOOGLE_SCRAPE_JS: &str = r#"JSON.stringify(Array.from(document.querySelectorAll('div.g, div.MjjYud')).map(function(el){var CHROME=/^https?:\/\/(?:www\.)?google\.[a-z0-9.]+\/(search|set|async|gen_204)($|[\/?#])/i;var links=el.querySelectorAll('a[href]');var s=el.querySelector('.VwiC3b, [data-sncf], .st');var snip=s?s.innerText:'';for(var i=0;i<links.length;i++){var a=links[i];if(CHROME.test(a.href||''))continue;var h=a.querySelector('h3');if(h)return{url:a.href,title:h.innerText,content:snip};}for(var j=0;j<links.length;j++){if(!CHROME.test(links[j].href||''))return{url:links[j].href,title:(links[j].innerText||'').trim(),content:snip};}return null;}).filter(Boolean))"#;

/// Bing SERP extractor. `li.b_algo` rows; `h2 a` for title/url, `.b_caption p`
/// for the snippet. Bing wraps result links in a `bing.com/ck/a?…&u=a1<base64>`
/// click-tracker — the inline `unwrap` decodes that `u` param back to the real
/// destination (and leaves already-direct links untouched).
const BING_SCRAPE_JS: &str = r#"JSON.stringify((function(){function unwrap(u){try{var m=u.match(/[?&]u=a1([^&]+)/);if(m){var b=m[1].replace(/-/g,'+').replace(/_/g,'/');while(b.length%4)b+='=';return decodeURIComponent(escape(atob(b)));}}catch(e){}return u;}return Array.from(document.querySelectorAll('li.b_algo')).map(function(el){var a=el.querySelector('h2 a[href]');var s=el.querySelector('.b_caption p, p');return a?{url:unwrap(a.href),title:a.innerText,content:s?s.innerText:''}:null;}).filter(Boolean);})())"#;

/// DuckDuckGo SERP extractor (the `duckduckgo.com/?q=` layout). Result blocks
/// are `article[data-testid="result"]` with `h2 a` and a snippet node; the
/// `div.result` / `a.result__a` fallbacks cover the lite/html layout.
const DDG_SCRAPE_JS: &str = r#"JSON.stringify(Array.from(document.querySelectorAll('article[data-testid="result"], div.result')).map(function(el){var a=el.querySelector('h2 a[href], a.result__a[href]');var s=el.querySelector('[data-result="snippet"], .result__snippet');return a?{url:a.href,title:a.innerText,content:s?s.innerText:''}:null;}).filter(Boolean))"#;

/// Wikipedia full-text search extractor. The `Special:Search` SERP lists hits
/// as `.mw-search-result-heading a` (absolute article hrefs, no inline snippet).
const WIKIPEDIA_SCRAPE_JS: &str = r#"JSON.stringify(Array.from(document.querySelectorAll('.mw-search-result-heading a')).map(function(a){var t=(a.innerText||'').trim();return (a.href&&t)?{url:a.href,title:t,content:''}:null;}).filter(Boolean))"#;

/// YouTube search extractor. Each video result is a `ytd-video-renderer` whose
/// `a#video-title` carries the watch URL and the full title in its `title`
/// attribute (the inner text is lazy/empty until hover).
const YOUTUBE_SCRAPE_JS: &str = r#"JSON.stringify(Array.from(document.querySelectorAll('ytd-video-renderer a#video-title')).map(function(a){var t=(a.getAttribute('title')||a.innerText||'').trim();return (a.href&&t)?{url:a.href,title:t,content:''}:null;}).filter(Boolean))"#;

/// Reddit search extractor. Post links are `a[href*="/comments/"]`; Reddit
/// renders several anchors per post (thumbnail + title), so we dedupe by the
/// query-stripped permalink and keep the first non-trivial link text.
const REDDIT_SCRAPE_JS: &str = r#"JSON.stringify((function(){var seen={};var out=[];document.querySelectorAll('a[href*="/comments/"]').forEach(function(a){var u=a.href.split('?')[0];var t=(a.innerText||'').trim();if(t.length>5&&!seen[u]){seen[u]=1;out.push({url:u,title:t,content:''});}});return out;})())"#;

/// Amazon product-search extractor. Each `[data-component-type="s-search-result"]`
/// card holds the product link (`a[href*="/dp/"]`) and title (`h2 span`/`h2`);
/// dedupe by the query-stripped `/dp/` URL.
const AMAZON_SCRAPE_JS: &str = r#"JSON.stringify((function(){var seen={};var out=[];document.querySelectorAll('[data-component-type="s-search-result"]').forEach(function(el){var a=el.querySelector('a[href*="/dp/"]');var h=el.querySelector('h2 span, h2');if(a&&h){var u=a.href.split('?')[0];var t=(h.innerText||'').trim();if(t&&!seen[u]){seen[u]=1;out.push({url:u,title:t,content:''});}}});return out;})())"#;

/// The extractor JS for a browser-driven engine. Each has dedicated selectors
/// tuned against its live SERP — this is the single place to fix when a DOM
/// drifts. GitHub is *not* browser-driven (it uses the REST Search API), so it
/// never reaches here.
fn scrape_js(engine: SearchEngine) -> &'static str {
    match engine {
        SearchEngine::Google => GOOGLE_SCRAPE_JS,
        SearchEngine::Bing => BING_SCRAPE_JS,
        SearchEngine::DuckDuckGo => DDG_SCRAPE_JS,
        SearchEngine::Wikipedia => WIKIPEDIA_SCRAPE_JS,
        SearchEngine::Youtube => YOUTUBE_SCRAPE_JS,
        SearchEngine::Reddit => REDDIT_SCRAPE_JS,
        SearchEngine::Amazon => AMAZON_SCRAPE_JS,
        SearchEngine::Github => unreachable!("github uses the REST Search API, not the browser"),
    }
}

/// The camofox `navigate` request body for a browser-driven engine + query.
/// Google navigates its search URL directly with `hl=en&gl=us`: the SERP
/// locale follows the exit IP otherwise (the request-level `lang` param is
/// SearXNG-only), and a localized SERP hands the extractor foreign-language
/// section headings. If the direct URL ever lands on a consent wall or other
/// interstitial ([`run_search`]), Google retries via the battle-tested
/// `@google_search` macro, which handles the consent/redirect dance itself.
/// Bing/DuckDuckGo have no working macro in camofox-browser, so they always
/// navigate their search URL directly — the macro is only URL shorthand.
/// `query` is form-url-encoded into the `q` parameter. GitHub is handled via
/// the REST Search API and never reaches here.
fn navigate_body(engine: SearchEngine, query: &str) -> serde_json::Value {
    let q: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
    match engine {
        SearchEngine::Google => json!({
            "userId": USER_ID,
            "url": format!("https://www.google.com/search?q={q}&hl=en&gl=us")
        }),
        SearchEngine::Bing => {
            json!({ "userId": USER_ID, "url": format!("https://www.bing.com/search?q={q}") })
        }
        SearchEngine::DuckDuckGo => {
            json!({ "userId": USER_ID, "url": format!("https://duckduckgo.com/?q={q}") })
        }
        SearchEngine::Wikipedia => {
            // `fulltext=1` forces the search-results page; without it Wikipedia
            // redirects an exact title match straight to the article.
            json!({ "userId": USER_ID, "url": format!("https://en.wikipedia.org/wiki/Special:Search?search={q}&fulltext=1") })
        }
        SearchEngine::Youtube => {
            json!({ "userId": USER_ID, "url": format!("https://www.youtube.com/results?search_query={q}") })
        }
        SearchEngine::Reddit => {
            json!({ "userId": USER_ID, "url": format!("https://www.reddit.com/search/?q={q}") })
        }
        SearchEngine::Amazon => {
            json!({ "userId": USER_ID, "url": format!("https://www.amazon.com/s?k={q}") })
        }
        SearchEngine::Github => {
            unreachable!("github uses the REST Search API, not the browser")
        }
    }
}

/// The camofox `@google_search` macro navigate body — Google's locale-agnostic
/// fallback when the direct SERP URL yields no rows (consent wall, interstitial
/// redirect). The macro handles the consent/redirect dance itself.
fn google_macro_body(query: &str) -> serde_json::Value {
    json!({ "userId": USER_ID, "macro": "@google_search", "query": query })
}

/// One scraped SERP row, as emitted by the per-engine extractors ([`scrape_js`]).
#[derive(Deserialize)]
struct ScrapedRow {
    url: String,
    title: String,
    #[serde(default)]
    content: String,
}

#[derive(Deserialize)]
struct EvaluateResponse {
    result: Option<String>,
}

/// GitHub REST Search API (`/search/repositories`) response — only the fields
/// we map into a result row.
#[derive(Deserialize)]
struct GithubSearchResponse {
    #[serde(default)]
    items: Vec<GithubRepo>,
}

#[derive(Deserialize)]
struct GithubRepo {
    html_url: String,
    full_name: String,
    #[serde(default)]
    description: Option<String>,
}

/// Attach the API key to a camofox request, if configured. Free function so the
/// detached tab tasks can authenticate without borrowing the client.
fn authed(req: reqwest::RequestBuilder, api_key: Option<&str>) -> reqwest::RequestBuilder {
    match api_key {
        Some(k) => req.bearer_auth(k),
        None => req,
    }
}

/// `POST /tabs` for one warm tab, returning its id. The single create path,
/// shared by [`CamofoxSearchClient::ensure_tab`] and the detached hand-off.
///
/// Takes the pieces rather than `&self` so a spawned task can run it after the
/// future that asked for it is gone.
async fn create_warm_tab(
    http: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
) -> Result<String, SearchError> {
    let req = authed(http.post(format!("{base_url}/tabs")), api_key)
        .json(&json!({ "userId": USER_ID, "sessionKey": SESSION_KEY }))
        .send();
    let create = req.await.map_err(|e: reqwest::Error| {
        if e.is_timeout() {
            SearchError::Timeout
        } else {
            SearchError::Transport(e.without_url().to_string())
        }
    })?;
    if !create.status().is_success() {
        // Every one of these is raised BEFORE the server commits a tab (profile
        // 403/409/400, staged first use, the per-tab cap, `window is null`), so
        // propagating the answer loses nothing.
        return Err(upstream_error("create tab", create).await);
    }
    // A 2xx means the tab EXISTS — camofox registers it before it replies — so
    // any way out of here that does not yield an id is a tab nobody will ever
    // close, and this client has no list-and-reap to discover it. Hence: parse
    // leniently (a `Value`, not the typed shape, so an added field or a
    // differently-typed sibling still gives up `tabId`), and say so loudly when
    // even that comes up empty, because that is a leak rather than a stall.
    let body = create.json::<serde_json::Value>().await.map_err(|e| {
        let msg = crw_core::error::reqwest_message(e);
        tracing::warn!(
            error = %msg,
            "camofox: /tabs answered 2xx with a body we cannot read; the tab it registered is \
             unaccounted for and holds a slot until the session timeout"
        );
        SearchError::InvalidResponse(format!("camofox: bad /tabs response: {msg}"))
    })?;
    let id = body
        .get("tabId")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    if id.is_empty() {
        tracing::warn!(
            body = %body,
            "camofox: /tabs answered 2xx without a usable tabId; the tab it registered is \
             unaccounted for and holds a slot until the session timeout"
        );
        return Err(SearchError::InvalidResponse(
            "camofox: /tabs response carried no tabId".into(),
        ));
    }
    Ok(id)
}

/// `DELETE /tabs/{id}`, best-effort, bounded by [`CLEANUP_BUDGET`]. Only ever
/// called from a detached task (see [`CamofoxSearchClient::abandon_tab`] and
/// [`CamofoxSearchClient::close_shelf_surplus`]), so nothing waits on it and no
/// caller is delayed by a wedged server.
///
/// Two status traps, both from the server's own route (`core.js`
/// `DELETE /tabs/:tabId`): it **never 404s** — a lookup miss (unknown id or
/// mismatched `userId`) still answers `{ok:true}` — so a 2xx here means the
/// request was ACCEPTED, not that the tab closed; only re-listing proves that,
/// and this client does not re-list (the render tier's `close-noop` counter is
/// where that check lives).
///
/// A close we abandon at [`CLEANUP_BUDGET`] is a REAL survivor, not a false
/// alarm, and is logged as one: the DELETE route takes no tab lock and no user
/// limiter (contrast navigate, which wraps `withUserLimit` around a
/// `withTabLock`), it only awaits `safePageClose`, which the server itself caps
/// at 5 s — so the budget is set above that cap, and missing it means the
/// server was genuinely wedged rather than merely busy closing. The backstop is
/// camofox's 30 min session timeout, which is why this is a `warn` and not an
/// error: the tab is stranded, not the search.
async fn close_warm_tab(
    http: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
    tab_id: &str,
) {
    let req = authed(http.delete(format!("{base_url}/tabs/{tab_id}")), api_key)
        .json(&json!({ "userId": USER_ID }))
        .send();
    match tokio::time::timeout(CLEANUP_BUDGET, req).await {
        // A 404 is kept for robustness against a future server that starts
        // reporting one, but this route cannot produce it today.
        Ok(Ok(resp)) if resp.status().is_success() || resp.status() == 404 => {}
        Ok(Ok(resp)) => tracing::warn!(
            tab_id,
            status = resp.status().as_u16(),
            "camofox: close of abandoned tab rejected; tab may leak"
        ),
        Ok(Err(e)) => tracing::warn!(
            tab_id,
            error = %e.without_url(),
            "camofox: close of abandoned tab never reached the server; it stays open until \
             the session timeout"
        ),
        Err(_) => tracing::warn!(
            tab_id,
            budget_ms = CLEANUP_BUDGET.as_millis() as u64,
            "camofox: close of abandoned tab outlasted our budget (which is above the server's \
             own 5 s close cap); it stays open until the session timeout"
        ),
    }
}

/// Search client backed by a camofox-browser REST endpoint. Returns the same
/// [`SearxngResponse`] shape as [`crate::client::SearxngClient`] so callers can
/// treat the two interchangeably.
pub struct CamofoxSearchClient {
    http: reqwest::Client,
    /// Never follows redirects: reads the `Location` of Google's result links.
    redirect_http: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
    /// Optional GitHub PAT for the `github` engine, which uses the GitHub REST
    /// Search API (not the browser) because GitHub web search rate-limits
    /// unauthenticated scraping. `None` falls back to the lower unauth quota.
    github_token: Option<String>,
    /// Base URL of the GitHub REST API for the `github` engine. Always
    /// `https://api.github.com` in production; overridden in tests to point at
    /// a mock server.
    github_api_base: String,
    timeout: Duration,
    /// How long `abandon_tab` waits for a replacement. A field rather than the
    /// constant so tests can exercise "the create outlived its waiter" without
    /// paying five seconds for it.
    prewarm_budget: Duration,
    /// The single warm tab id, lazily created and reused across queries. The
    /// mutex doubles as the search serializer: holding it for the whole `fetch`
    /// guarantees one navigation at a time on the one shared tab. `None` until
    /// the first search creates a tab; reset to `None` when a tab goes stale,
    /// or handed to a replacement by [`Self::abandon_tab`].
    ///
    /// `Arc`'d so the detached hand-off in `abandon_tab` can adopt-or-close its
    /// replacement against the SAME slot a concurrent search is filling — there
    /// is still exactly one warm tab, whoever won the race to create it.
    tab: Arc<tokio::sync::Mutex<Option<String>>>,
    /// Ids of tabs whose detached create finished but nobody adopted (see
    /// [`Self::spawn_create_tab`]). The one place a created id can go, so that
    /// no create can end with an id this process does not know.
    prewarmed: Arc<tokio::sync::Mutex<VecDeque<String>>>,
    /// Tests only: an origin whose `/goto` links are resolved like Google's, so
    /// the resolution step can be driven against a mock server.
    #[cfg(test)]
    redirect_origin: Option<String>,
}

impl CamofoxSearchClient {
    /// Build a client pointed at the camofox-browser base URL
    /// (e.g. `http://camofox:9377`). `timeout` caps each HTTP round-trip.
    /// `github_token` authenticates the `github` engine's Search-API calls.
    pub fn new(
        base_url: impl Into<String>,
        api_key: Option<String>,
        github_token: Option<String>,
        timeout: Duration,
    ) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let redirect_http = reqwest::Client::builder()
            .timeout(timeout.min(REDIRECT_RESOLVE_TIMEOUT))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            http,
            redirect_http,
            base_url,
            api_key,
            github_token,
            github_api_base: "https://api.github.com".to_string(),
            timeout,
            prewarm_budget: PREWARM_BUDGET,
            tab: Arc::new(tokio::sync::Mutex::new(None)),
            prewarmed: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            #[cfg(test)]
            redirect_origin: None,
        }
    }

    /// Test-only: shrink the wait in [`Self::abandon_tab`] so the "the create
    /// outlived its waiter" path is reachable without paying five seconds.
    #[doc(hidden)]
    #[must_use]
    pub fn with_prewarm_budget(mut self, budget: Duration) -> Self {
        self.prewarm_budget = budget;
        self
    }

    /// Configured base URL (trailing slash trimmed). Mirrors
    /// [`SearxngClient::base_url`](crate::client::SearxngClient::base_url) so the
    /// route layer can name the host in errors uniformly.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Base URL the `github` engine calls instead of the browser, so errors from
    /// that engine can name the host that actually failed.
    pub fn github_api_base(&self) -> &str {
        &self.github_api_base
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        authed(req, self.api_key.as_deref())
    }

    async fn post(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<reqwest::Response, SearchError> {
        self.auth(self.http.post(format!("{}{path}", self.base_url)))
            .json(&body)
            .send()
            .await
            .map_err(|e: reqwest::Error| {
                if e.is_timeout() {
                    SearchError::Timeout
                } else {
                    SearchError::Transport(e.without_url().to_string())
                }
            })
    }

    /// Run the requested engines via Camofox and map the merged rows into a
    /// [`SearxngResponse`]. Typed [`SearchError`]s match the SearXNG client so
    /// the route layer's existing error mapping applies unchanged.
    ///
    /// Serializes on the warm-tab mutex (see [`Self::tab`]) so only one search
    /// touches the browser at a time, reusing the one long-lived tab. The
    /// engines in `params.camofox_engines` are run sequentially on that tab
    /// (the single-tab design dodges camofox's teardown race, so fan-out is
    /// serial — N engines ≈ N× latency). A stale tab is recreated and the
    /// engine retried once. An engine that still fails is skipped; results from
    /// the engines that succeeded are merged and returned. Only when *every*
    /// engine fails is the last error surfaced.
    pub async fn fetch(&self, params: &SearxngParams) -> Result<SearxngResponse, SearchError> {
        let mut tab = self.tab.lock().await;
        let mut all: Vec<SearxngResult> = Vec::new();
        let mut last_err: Option<SearchError> = None;
        let mut any_ok = false;
        // Engines that failed OR returned zero rows, so a caller can tell a
        // blocked/consent-walled engine (0 rows despite HTTP 200) from a genuine
        // "no matches". Surfaced via SearxngResponse.unresponsive_engines →
        // response warnings; without it a hung/blocked engine looks like a clean
        // empty success (the exact Bing/Google-from-a-flagged-IP silent failure).
        let mut unresponsive: Vec<serde_json::Value> = Vec::new();

        for &engine in &params.camofox_engines {
            let label = engine.label();
            // GitHub uses the REST Search API, not the browser — no tab, no
            // stale-tab retry. Every other engine drives the warm camofox tab.
            let outcome = if matches!(engine, SearchEngine::Github) {
                self.github_search(&params.q).await
            } else {
                // Whether this attempt will reuse a cached id rather than mint a
                // fresh tab. Captured before the attempt because `ensure_tab`
                // populates `tab` as a side effect.
                let reused_tab = tab.is_some();
                let outcome = match self.attempt(&mut tab, engine, params).await {
                    // A timeout on a *reused* tab is the dead-tab signature, not
                    // a slow page: camofox does not 404 a navigate for a tab it
                    // no longer has, it accepts the request and hangs until its
                    // own (longer) navigate timeout, so we only ever see our
                    // client timeout. Without this the cached id is never
                    // dropped and every later search repeats the same stall —
                    // the endpoint stays broken until the process restarts.
                    // A fresh tab that times out really is a slow page, and is
                    // reported as-is (below): `reused_tab` is false on the
                    // retry, so this can recurse at most once.
                    Err(e)
                        if is_stale_tab(&e)
                            || (reused_tab && matches!(e, SearchError::Timeout)) =>
                    {
                        // Warm tab/context died (idle eviction or camofox
                        // restart). Let any in-flight relaunch settle, then
                        // recreate and retry this engine once. Whether the dead
                        // id is also DELETEd depends on who said the tab was
                        // gone (see [`tab_untrusted`]): the server's own 404/5xx
                        // means it is already reaped, but a timeout or a dropped
                        // connection leaves a tab camofox still holds — and one
                        // lost tab is 1/10 of the session's tab budget, which
                        // idle cleanup will NOT reclaim (it only reaps zero-tab
                        // sessions), so it survives to the 30 min session
                        // timeout.
                        if tab_untrusted(&e) {
                            self.abandon_tab(&mut tab).await;
                        } else {
                            *tab = None;
                        }
                        tokio::time::sleep(RETRY_BACKOFF).await;
                        self.attempt(&mut tab, engine, params).await
                    }
                    other => other,
                };
                // A fresh tab (first try or the retry above) timed out
                // client-side, but camofox is still working it, so reusing the
                // id would queue the next search behind it. Swap it out; no
                // retry — that would double the latency. A dropped connection
                // is treated the same way: caching that id means the next
                // search repeats the same stall, which is exactly what the
                // swap above exists to prevent.
                if outcome.as_ref().err().is_some_and(tab_untrusted) {
                    self.abandon_tab(&mut tab).await;
                }
                outcome
            };
            match outcome {
                Ok(rows) => {
                    any_ok = true;
                    if rows.is_empty() {
                        // Zero rows is either a genuine empty result or a
                        // bot-wall/consent page served as HTTP 200 — the scrape
                        // can't tell them apart, so word it neutrally.
                        unresponsive.push(serde_json::json!([
                            label,
                            "returned no results (no matches, or a bot wall / consent page)"
                        ]));
                    }
                    all.extend(rows);
                }
                Err(e) => {
                    // The API response gets only the stripped reason; the full
                    // error (with camofox's message) is only visible here.
                    tracing::warn!(engine = label, error = %e, "camofox: engine failed");
                    unresponsive.push(serde_json::json!([label, engine_failure_reason(&e)]));
                    last_err = Some(e);
                }
            }
        }

        if !any_ok {
            return Err(last_err.unwrap_or(SearchError::Timeout));
        }
        Ok(merge_results(params.q.clone(), all, unresponsive))
    }

    /// Ensure a warm tab exists, then run one engine's search against it. Caller
    /// holds the tab mutex, so this is the single in-flight search.
    async fn attempt(
        &self,
        tab: &mut Option<String>,
        engine: SearchEngine,
        params: &SearxngParams,
    ) -> Result<Vec<SearxngResult>, SearchError> {
        let tab_id = self.ensure_tab(tab).await?;
        self.run_search(&tab_id, engine, params).await
    }

    /// Return the warm tab id, creating one if we don't have it cached. The id
    /// is cached back into `tab` so subsequent searches reuse it.
    ///
    /// Adopts anything a detached create already published first (see
    /// [`Self::spawn_create_tab`]): that is the id of a replacement somebody
    /// stopped waiting for, and reusing it is both cheaper and the reason no tab
    /// is ever left unaccounted for.
    async fn ensure_tab(&self, tab: &mut Option<String>) -> Result<String, SearchError> {
        if let Some(id) = tab.as_ref() {
            // A cached warm tab makes every id still on the shelf surplus by
            // definition — nothing is waiting for them any more.
            self.close_shelf_surplus();
            return Ok(id.clone());
        }
        if let Some(id) = self.take_prewarmed().await {
            tracing::debug!(tab_id = %id, "camofox: adopting a pre-warmed tab");
            *tab = Some(id.clone());
            return Ok(id);
        }
        let id = self.create_tab_awaiting().await?;
        *tab = Some(id.clone());
        Ok(id)
    }

    /// Close everything on the pre-warm shelf, from a task so the search never
    /// waits on it. Called when a warm tab makes them all surplus — which, since
    /// the warm tab is cached, is every search: hence the empty check before the
    /// spawn rather than a no-op task per engine per search.
    ///
    /// The closes run one after another (up to [`CLEANUP_BUDGET`] each), so a
    /// wedged camofox leaves this one task alive for a while. It can only ever
    /// hold the shelf's own ids — the warm tab is never on the shelf, it was
    /// popped before it was cached — so a slow drain can never strand a tab
    /// somebody is using.
    fn close_shelf_surplus(&self) {
        if matches!(self.prewarmed.try_lock(), Ok(ids) if ids.is_empty()) {
            return;
        }
        let (http, base_url, api_key, shelf) = (
            self.http.clone(),
            self.base_url.clone(),
            self.api_key.clone(),
            Arc::clone(&self.prewarmed),
        );
        tokio::spawn(async move {
            let ids: VecDeque<String> = {
                let mut ids = shelf.lock().await;
                std::mem::take(&mut *ids)
            };
            for id in ids {
                close_warm_tab(&http, &base_url, api_key.as_deref(), &id).await;
            }
        });
    }

    /// `POST /tabs`, run in a task nobody can cancel, and wait for the id.
    ///
    /// The wait is cancellable; the create is not, which is the whole point —
    /// see [`Self::spawn_create_tab`]. On success the id is taken from the
    /// pre-warm shelf, the same single source every adopter uses.
    async fn create_tab_awaiting(&self) -> Result<String, SearchError> {
        let handle = self.spawn_create_tab();
        match handle.await {
            // An answered failure registered no tab: camofox raises each of them
            // (bad profile, staged first use, the per-tab cap, `window is null`)
            // before it commits one, so propagating loses nothing and keeps
            // camofox's own message for the caller. The one shape that DID
            // register a tab and still lands here — a 2xx whose body yielded no
            // id — is `create_warm_tab`'s to make noise about, since nothing
            // downstream can find that tab again.
            Ok(Err(e)) => return Err(e),
            Ok(Ok(_)) => {}
            Err(e) => {
                tracing::debug!(error = %e, "camofox: tab-create task did not run to completion");
                return Err(SearchError::Transport(format!("tab create failed: {e}")));
            }
        }
        // Published before the task returned, so it is there — unless a
        // concurrent adopter got it first, which is the same outcome as far as
        // this caller is concerned: some tab exists, but not one it can name.
        self.take_prewarmed()
            .await
            .ok_or_else(|| SearchError::Transport("tab create published no adoptable id".into()))
    }

    /// Spawn the `POST /tabs` so it outlives whoever asked for it, publishing the
    /// id to the pre-warm shelf on completion.
    ///
    /// This client has exactly ONE way to close a tab — `close_warm_tab`, by
    /// id — and
    /// no list-and-reap path. camofox registers a tab server-side BEFORE it
    /// replies with the id, so a create cancelled mid-request leaves a tab whose
    /// id this process can never learn: not a transient stall but a full slot of
    /// the session's `MAX_TABS_PER_SESSION` gone for the 30 min session timeout,
    /// ten times over and the search `userId` answers 429. So no `POST /tabs` is
    /// ever issued on a future that can be dropped: every create is detached,
    /// and every id it produces lands on the shelf for someone to adopt or close.
    fn spawn_create_tab(&self) -> tokio::task::JoinHandle<Result<String, SearchError>> {
        let (http, base_url, api_key, shelf) = (
            self.http.clone(),
            self.base_url.clone(),
            self.api_key.clone(),
            Arc::clone(&self.prewarmed),
        );
        tokio::spawn(async move {
            let id = create_warm_tab(&http, &base_url, api_key.as_deref()).await?;
            let mut ids = shelf.lock().await;
            ids.push_back(id.clone());
            // Bounded shelf: whoever is failing is not going to adopt these, so
            // the oldest is closed rather than left to age toward the session
            // timeout. Two is enough for one warm tab plus one in-flight
            // replacement.
            while ids.len() > PREWARM_SHELF {
                let old = ids.pop_front();
                drop(ids);
                if let Some(old) = old {
                    close_warm_tab(&http, &base_url, api_key.as_deref(), &old).await;
                }
                ids = shelf.lock().await;
            }
            Ok(id)
        })
    }

    /// Take the oldest unadopted pre-warmed id, if any.
    async fn take_prewarmed(&self) -> Option<String> {
        self.prewarmed.lock().await.pop_front()
    }

    /// Replace the warm tab after it stopped answering. The replacement is minted
    /// and adopted *before* the old tab is closed, so the context keeps a tab
    /// across the swap and never trips the eager zero-tab teardown described in
    /// the module docs (`window is null` on the next create).
    ///
    /// That ordering holds when the create answers inside [`PREWARM_BUDGET`].
    /// When it does not, this gives up waiting, leaves the slot empty, and closes
    /// the old tab regardless — deliberately: the alternative is keeping a tab we
    /// know is wedged in the count to protect against a teardown that costs one
    /// retried create. 5 s is far beyond a healthy camofox's create, so missing it
    /// means the endpoint is in trouble anyway.
    ///
    /// Waiting for the replacement is bounded by [`PREWARM_BUDGET`] so a wedged
    /// camofox cannot stall the search here for the full client timeout — but the
    /// WAIT being over is not the CREATE being cancelled: the request lives in a
    /// detached task, and its id still reaches the shelf, where the next
    /// `ensure_tab` adopts it (or it is closed on overflow). Before this, a
    /// bounded wait that also cancelled the request turned every slow create into
    /// a permanent leak, which this client has no way to detect, let alone fix.
    async fn abandon_tab(&self, tab: &mut Option<String>) {
        let Some(old) = tab.take() else { return };
        let handle = self.spawn_create_tab();
        if tokio::time::timeout(self.prewarm_budget, handle)
            .await
            .is_err()
        {
            tracing::debug!(
                budget_ms = self.prewarm_budget.as_millis() as u64,
                "camofox: replacement tab still being created; its id will be adopted off the shelf"
            );
        }
        // Adopt what the create published, HERE, before the old tab goes away:
        // that ordering is what keeps the context's tab count off zero, the whole
        // reason this function pre-warms instead of merely closing. A create that
        // did not answer inside the budget leaves the slot empty — its id is safe
        // on the shelf for whoever needs a tab next — exactly as a failed create
        // always did. The shelf is the single source of created ids — the task
        // publishes there and nowhere else — so adopting means taking from it,
        // never from what the task happened to return: an id could otherwise be
        // adopted AND still be sitting on the shelf to be closed as surplus.
        if let Some(id) = self.take_prewarmed().await {
            *tab = Some(id);
        }
        // The close goes to a task rather than being awaited. camofox works on
        // the tab its DELETE handler is closing, and a tab we just gave up on is
        // exactly the one whose navigate camofox may still be holding (its own
        // ceiling is 30 s) — so an inline close either makes the caller wait on
        // a tab that is already useless, or, with any budget shorter than the
        // server's, hangs up first and leaves that tab listed with nobody left in
        // this client to notice: there is no reaper here to rediscover it. A live
        // sighting of precisely that: a warm tab abandoned mid-navigate whose
        // DELETE was given up on at 2 s and never landed, sitting out the full
        // 30 min session timeout while the search reported clean results.
        let (http, base_url, api_key) = (
            self.http.clone(),
            self.base_url.clone(),
            self.api_key.clone(),
        );
        tokio::spawn(async move {
            close_warm_tab(&http, &base_url, api_key.as_deref(), &old).await;
        });
    }

    /// Navigate one tab with `body`, wait for the SERP, scrape the rows, and
    /// return them raw (no redirect resolution — that is the caller's, so a
    /// fallback attempt does not pay for it twice). Navigate/evaluate failures
    /// propagate unchanged: they are the stale-tab signal the caller's tab-swap
    /// retry keys on, and must not be swallowed by an engine-level fallback.
    async fn scrape_rows(
        &self,
        tab_id: &str,
        engine: SearchEngine,
        body: serde_json::Value,
    ) -> Result<Vec<ScrapedRow>, SearchError> {
        let nav = self.post(&format!("/tabs/{tab_id}/navigate"), body).await?;
        if !nav.status().is_success() {
            return Err(upstream_error("navigate", nav).await);
        }

        let _ = self
            .post(
                &format!("/tabs/{tab_id}/wait"),
                json!({ "userId": USER_ID, "timeout": self.timeout.as_millis() as u64 }),
            )
            .await;

        let eval = self
            .post(
                &format!("/tabs/{tab_id}/evaluate"),
                json!({ "userId": USER_ID, "expression": scrape_js(engine) }),
            )
            .await?;
        if !eval.status().is_success() {
            // A JSON error body (`{"error":"tab not found"}`) would otherwise
            // decode as `result: None` and pass for an empty SERP, bypassing
            // the stale-tab retry.
            return Err(upstream_error("evaluate", eval).await);
        }
        let raw = eval
            .json::<EvaluateResponse>()
            .await
            .map_err(|e| {
                SearchError::InvalidResponse(format!(
                    "camofox: bad evaluate response: {}",
                    crw_core::error::reqwest_message(e)
                ))
            })?
            .result
            .unwrap_or_default();

        let rows: Vec<ScrapedRow> = if raw.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&raw)
                .map_err(|e| SearchError::InvalidResponse(format!("camofox: scrape JSON: {e}")))?
        };
        Ok(rows)
    }

    async fn run_search(
        &self,
        tab_id: &str,
        engine: SearchEngine,
        params: &SearxngParams,
    ) -> Result<Vec<SearxngResult>, SearchError> {
        let mut rows = self
            .scrape_rows(tab_id, engine, navigate_body(engine, &params.q))
            .await?;
        // The pinned-English direct URL can land on a consent wall or another
        // interstitial on a fresh browser profile (a successful navigate that
        // renders no organic rows). Fall back to the macro, which walks the
        // consent dance itself. A failed navigate/evaluate is NOT retried here:
        // that is the stale-tab path, handled by the caller's tab swap.
        if rows.is_empty() && matches!(engine, SearchEngine::Google) {
            tracing::debug!(
                "camofox: empty Google SERP via direct URL; retrying via @google_search macro"
            );
            rows = self
                .scrape_rows(tab_id, engine, google_macro_body(&params.q))
                .await?;
        }

        // Drop SERP-chrome rows that survived the extractor: an anchor still
        // pointing back at Google's own `/search` / `/set/…` is an AI-mode or
        // related-search chip, never an organic result. Organic links always
        // carry an off-origin URL or one of the wrapper paths.
        if matches!(engine, SearchEngine::Google) {
            rows.retain(|r| !is_google_chrome_link(&r.url));
        }

        // Google now links every result through `/goto?url=<opaque token>`, and
        // the real URL is nowhere else in the result markup. Resolve them all at
        // once; a link that cannot be resolved keeps its redirect URL.
        if matches!(engine, SearchEngine::Google) {
            let resolved = futures::future::join_all(rows.iter().map(|r| async {
                if self.is_result_redirect(&r.url) {
                    self.resolve_redirect(&r.url).await
                } else {
                    None
                }
            }))
            .await;
            for (row, target) in rows.iter_mut().zip(resolved) {
                if let Some(target) = target {
                    row.url = target;
                }
            }
        }

        let n = rows.len();
        let label = engine.label();
        let results = rows
            .into_iter()
            .enumerate()
            .map(|(i, r)| SearxngResult {
                url: Some(r.url),
                title: Some(r.title),
                engine: Some(label.to_string()),
                content: (!r.content.is_empty()).then_some(r.content),
                // Synthesize a descending score from SERP position so the
                // existing score-sort in transform.rs preserves engine order.
                score: Some((n - i) as f64),
                engines: vec![label.to_string()],
                positions: vec![(i + 1) as u32],
                category: Some("general".to_string()),
                template: None,
                published_date: None,
                img_src: None,
                thumbnail_src: None,
                img_format: None,
                resolution: None,
            })
            .collect();

        Ok(results)
    }

    /// Whether a Google result link must be resolved to its target.
    fn is_result_redirect(&self, url: &str) -> bool {
        #[cfg(test)]
        if let Some(origin) = &self.redirect_origin
            && url.starts_with(&format!("{origin}/goto"))
        {
            return true;
        }
        is_google_redirect(url)
    }

    /// The absolute http(s) `Location` a redirect link answers with, without
    /// following it. `None` when the link does not redirect or cannot be reached.
    async fn resolve_redirect(&self, url: &str) -> Option<String> {
        let resp = match self.redirect_http.get(url).send().await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::debug!(error = %e.without_url(), "camofox: redirect link not resolved");
                return None;
            }
        };
        if !resp.status().is_redirection() {
            return None;
        }
        let location = resp
            .headers()
            .get(reqwest::header::LOCATION)?
            .to_str()
            .ok()?;
        let target = url::Url::parse(location).ok()?;
        // Google answers a rate-limited or cookieless request with a redirect to
        // its own interstitial, which is not the result.
        let interstitial = match target.host_str() {
            Some("consent.google.com") => true,
            Some("www.google.com" | "google.com") => target.path().starts_with("/sorry"),
            _ => false,
        };
        (matches!(target.scheme(), "http" | "https") && !interstitial).then(|| target.to_string())
    }

    /// Search GitHub repositories via the REST Search API. Used instead of the
    /// browser because GitHub's web search rate-limits unauthenticated scraping
    /// almost immediately; the API gives clean JSON and a token lifts the quota.
    /// GitHub requires a `User-Agent`; the token (when set) is sent as a bearer.
    async fn github_search(&self, query: &str) -> Result<Vec<SearxngResult>, SearchError> {
        let q: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
        let url = format!(
            "{}/search/repositories?q={q}&per_page=10",
            self.github_api_base
        );
        let mut req = self
            .http
            .get(url)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "crw-search");
        if let Some(token) = &self.github_token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.map_err(|e: reqwest::Error| {
            if e.is_timeout() {
                SearchError::Timeout
            } else {
                SearchError::Transport(e.without_url().to_string())
            }
        })?;
        if !resp.status().is_success() {
            return Err(SearchError::Upstream {
                status: resp.status().as_u16(),
                body: "github: search failed".to_string(),
            });
        }
        let data = resp.json::<GithubSearchResponse>().await.map_err(|e| {
            SearchError::InvalidResponse(format!(
                "github: bad search response: {}",
                crw_core::error::reqwest_message(e)
            ))
        })?;

        let n = data.items.len();
        Ok(data
            .items
            .into_iter()
            .enumerate()
            .map(|(i, r)| SearxngResult {
                url: Some(r.html_url),
                title: Some(r.full_name),
                engine: Some("github".to_string()),
                content: r.description.filter(|d| !d.is_empty()),
                score: Some((n - i) as f64),
                engines: vec!["github".to_string()],
                positions: vec![(i + 1) as u32],
                category: Some("general".to_string()),
                template: None,
                published_date: None,
                img_src: None,
                thumbnail_src: None,
                img_format: None,
                resolution: None,
            })
            .collect())
    }
}

/// A Google result link that redirects to the real result (`/goto?url=` or
/// `/url?q=`). The host is matched exactly: a scraped href is page content, and
/// a pattern such as `google.<tld>` would send crw's own request to any domain
/// a result can name.
fn is_google_redirect(url: &str) -> bool {
    let Ok(u) = url::Url::parse(url) else {
        return false;
    };
    u.scheme() == "https"
        && matches!(u.host_str(), Some("www.google.com" | "google.com"))
        && matches!(u.path(), "/goto" | "/url")
}

/// True for a scraped Google row that is SERP chrome rather than an organic
/// result: an https link on Google itself pointing at its own UI
/// (`/search` — the AI-mode teaser and related-search chips; `/set/…`,
/// `/async`, `/gen_204`). Organic rows always carry an off-origin URL or one
/// of the wrapper paths [`is_google_redirect`] recognizes, so those stay.
fn is_google_chrome_link(url: &str) -> bool {
    let Ok(u) = url::Url::parse(url) else {
        return false;
    };
    if u.scheme() != "https" || !matches!(u.host_str(), Some("www.google.com") | Some("google.com"))
    {
        return false;
    }
    let path = u.path();
    !is_google_redirect(url)
        && (path.starts_with("/search") || path.starts_with("/set/") || path.starts_with("/async"))
}

/// Merge per-engine result rows into one response, deduped by URL. A URL seen
/// by multiple engines accumulates their `engines`/`positions` and sums their
/// position-scores, so cross-engine agreement ranks higher. First-appearance
/// order is preserved; downstream `rerank` does the final ordering.
/// Concise, user-facing reason for an engine failure — no internal detail, just
/// enough to tell a timeout/block apart. Feeds `unresponsive_engines`.
fn engine_failure_reason(e: &SearchError) -> String {
    match e {
        SearchError::Timeout => "timed out".to_string(),
        SearchError::Upstream { status, .. } => format!("upstream error (HTTP {status})"),
        SearchError::InvalidResponse(_) => "unreadable response".to_string(),
        _ => "request failed".to_string(),
    }
}

fn merge_results(
    query: String,
    rows: Vec<SearxngResult>,
    unresponsive_engines: Vec<serde_json::Value>,
) -> SearxngResponse {
    use std::collections::HashMap;
    let mut order: Vec<String> = Vec::new();
    let mut by_url: HashMap<String, SearxngResult> = HashMap::new();
    for r in rows {
        let key = r.url.clone().unwrap_or_default();
        if let Some(existing) = by_url.get_mut(&key) {
            existing.engines.extend(r.engines);
            existing.positions.extend(r.positions);
            existing.score = Some(existing.score.unwrap_or(0.0) + r.score.unwrap_or(0.0));
        } else {
            order.push(key.clone());
            by_url.insert(key, r);
        }
    }
    let results: Vec<SearxngResult> = order
        .into_iter()
        .filter_map(|k| by_url.remove(&k))
        .collect();
    SearxngResponse {
        query,
        number_of_results: results.len() as u64,
        results,
        unresponsive_engines,
        ..Default::default()
    }
}

/// Cap on how much of camofox's `error` message is carried into the error.
/// The route layer trims `Upstream.body` again (to 200 chars) before it reaches
/// an HTTP client; this cap only bounds what lands in logs.
const UPSTREAM_BODY_CAP: usize = 300;

/// Turn a non-2xx camofox response into an `Upstream` error carrying camofox's
/// own `error` message (e.g. a profile/Camoufox version mismatch), so the
/// cause is visible instead of a bare status. Only that JSON field passes
/// through: a non-JSON body (a proxy's HTML error page, a crash trace) is
/// logged, not surfaced, since `Upstream.body` reaches API responses. `what`
/// names the step (`create tab`, `navigate`).
async fn upstream_error(what: &str, resp: reqwest::Response) -> SearchError {
    let status = resp.status().as_u16();
    let raw = match read_capped(resp, MAX_ERROR_BODY_BYTES).await {
        Ok(b) => String::from_utf8_lossy(&b).into_owned(),
        Err(e) => {
            tracing::debug!(status, step = what, error = %e, "camofox: error body unreadable");
            String::new()
        }
    };
    let detail = serde_json::from_str::<serde_json::Value>(&raw)
        .ok()
        .and_then(|v| v.get("error")?.as_str().map(str::to_string))
        .unwrap_or_else(|| {
            if !raw.trim().is_empty() {
                tracing::debug!(status, step = what, body = %raw.trim(), "camofox: non-JSON error body");
            }
            String::new()
        });
    let detail: String = detail.trim().chars().take(UPSTREAM_BODY_CAP).collect();
    let body = if detail.is_empty() {
        format!("camofox: {what} failed")
    } else {
        format!("camofox: {what} failed: {detail}")
    };
    SearchError::Upstream { status, body }
}

/// Whether an error means the warm tab/context is gone and recreating it could
/// recover — a missing tab (404), a server-side fault like the upstream
/// `window is null` (5xx), or a dropped connection during a relaunch. A
/// malformed-response parse error won't be helped by recreating, so it is
/// reported as-is.
///
/// Timeouts are deliberately *not* classified here, because the same error
/// means different things depending on the tab: on a reused one it is the
/// dead-tab signature (recreate and retry once), on a freshly minted tab it is
/// a slow page (drop the tab, no retry). `fetch` applies that
/// context-dependent rule at the call site and swaps out the abandoned tab in
/// both cases.
fn is_stale_tab(e: &SearchError) -> bool {
    match e {
        SearchError::Upstream { status, .. } => *status == 404 || *status >= 500,
        SearchError::Transport(_) => true,
        SearchError::Timeout | SearchError::InvalidResponse(_) => false,
    }
}

/// Whether the cached tab must be assumed STILL ALIVE and therefore deleted,
/// as opposed to already reaped by the server. Orthogonal to
/// [`is_stale_tab`], which decides whether to *retry*; this decides whether to
/// `DELETE` the abandoned id — the two disagree exactly on the transport error
/// (stale enough to retry, yet the tab is almost certainly still there).
///
/// A 404 or 5xx is the server's own word that the tab/context is gone, so no
/// DELETE is due (pinned by `stale_5xx_recreates_tab_without_delete`). A client
/// timeout or a dropped connection is not: camofox keeps the page — nothing in
/// it closes a tab when the HTTP client hangs up mid-navigate — so forgetting
/// the id silently strands it. That is not self-healing: camofox's idle cleanup
/// only reaps ZERO-tab sessions and its lifecycle controller refuses to run
/// while any tab lives, so the orphan pins the context until the 30 min session
/// timeout while consuming 1 of `MAX_TABS_PER_SESSION` (10 by default), after
/// which creates hard-fail 429.
///
/// Deleting an already-gone tab is cheap, but not for the reason the old
/// comment gave: camofox's `DELETE /tabs/{id}` never 404s (it answers
/// `{ok:true}` even when it cannot find the tab), so treat the status as
/// submission-accepted, never as proof of closure.
fn tab_untrusted(e: &SearchError) -> bool {
    matches!(e, SearchError::Timeout | SearchError::Transport(_))
}

#[cfg(test)]
mod extractor_tests {
    use super::*;
    use crw_core::types::SearchEngine;

    #[test]
    fn browser_engines_have_dedicated_extractors() {
        // GitHub is excluded: it uses the REST Search API, not the browser.
        assert!(scrape_js(SearchEngine::Google).contains("div.g"));
        assert!(scrape_js(SearchEngine::Bing).contains("li.b_algo"));
        assert!(scrape_js(SearchEngine::DuckDuckGo).contains("article"));
        assert!(scrape_js(SearchEngine::Wikipedia).contains("mw-search-result-heading"));
        assert!(scrape_js(SearchEngine::Youtube).contains("ytd-video-renderer"));
        assert!(scrape_js(SearchEngine::Reddit).contains("/comments/"));
        assert!(scrape_js(SearchEngine::Amazon).contains("s-search-result"));
    }

    #[test]
    fn all_browser_engines_navigate_by_url_google_pinned_english() {
        // Google navigates directly with the locale pinned (hl/gl), so the
        // extractor sees stable English UI markup regardless of exit IP.
        let g = navigate_body(SearchEngine::Google, "rust lang");
        assert_eq!(
            g["url"],
            "https://www.google.com/search?q=rust+lang&hl=en&gl=us"
        );
        assert!(g.get("macro").is_none());

        // The macro remains the consent-wall fallback body.
        let gm = google_macro_body("rust lang");
        assert_eq!(gm["macro"], "@google_search");
        assert_eq!(gm["query"], "rust lang");

        // Non-macro browser engines navigate a search URL, query url-encoded.
        let b = navigate_body(SearchEngine::Bing, "rust lang");
        assert_eq!(b["url"], "https://www.bing.com/search?q=rust+lang");
        let d = navigate_body(SearchEngine::DuckDuckGo, "rust lang");
        assert_eq!(d["url"], "https://duckduckgo.com/?q=rust+lang");
        let w = navigate_body(SearchEngine::Wikipedia, "rust lang");
        assert_eq!(
            w["url"],
            "https://en.wikipedia.org/wiki/Special:Search?search=rust+lang&fulltext=1"
        );
        let y = navigate_body(SearchEngine::Youtube, "rust lang");
        assert_eq!(
            y["url"],
            "https://www.youtube.com/results?search_query=rust+lang"
        );
        let rd = navigate_body(SearchEngine::Reddit, "rust lang");
        assert_eq!(rd["url"], "https://www.reddit.com/search/?q=rust+lang");
        let am = navigate_body(SearchEngine::Amazon, "rust lang");
        assert_eq!(am["url"], "https://www.amazon.com/s?k=rust+lang");
    }

    #[test]
    fn engine_failure_reason_is_concise_and_leaks_no_internals() {
        assert_eq!(engine_failure_reason(&SearchError::Timeout), "timed out");
        assert_eq!(
            engine_failure_reason(&SearchError::Upstream {
                status: 503,
                body: "secret internal detail".into(),
            }),
            "upstream error (HTTP 503)"
        );
        // The upstream body (potential internal detail) must not leak through.
        assert!(
            !engine_failure_reason(&SearchError::Upstream {
                status: 500,
                body: "stacktrace".into(),
            })
            .contains("stacktrace")
        );
    }

    #[test]
    fn merge_results_propagates_unresponsive_engines() {
        // A zero-row / errored engine is carried on the response so the route can
        // warn instead of returning a silent empty success.
        let resp = merge_results(
            "q".into(),
            vec![],
            vec![serde_json::json!(["bing", "timed out"])],
        );
        assert!(resp.results.is_empty());
        assert_eq!(resp.unresponsive_engines.len(), 1);
        assert_eq!(resp.unresponsive_engines[0][0], "bing");
        assert_eq!(resp.unresponsive_engines[0][1], "timed out");
    }
}

#[cfg(test)]
mod google_redirect_tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Item 12: the github engine's errors name the host it actually calls.
    #[test]
    fn github_api_base_is_the_rest_api_not_the_browser() {
        let client =
            CamofoxSearchClient::new("http://camofox:9377", None, None, Duration::from_secs(5));
        assert_eq!(client.github_api_base(), "https://api.github.com");
        assert_eq!(client.base_url(), "http://camofox:9377");
    }

    #[test]
    fn google_redirect_links_are_recognised() {
        assert!(is_google_redirect(
            "https://www.google.com/goto?url=CAESVwHrOzAV"
        ));
        assert!(is_google_redirect(
            "https://www.google.com/url?q=https://x.dev/"
        ));
        assert!(!is_google_redirect("https://google.co.xyz/goto?url=x"));
        assert!(!is_google_redirect("http://www.google.com/goto?url=x"));
        assert!(!is_google_redirect("https://corrode.dev/blog/async/"));
        assert!(!is_google_redirect("https://www.google.com/search?q=rust"));
        assert!(!is_google_redirect("https://evil.example/goto?url=x"));
        assert!(!is_google_redirect(
            "https://www.google.evil.example/goto?url=x"
        ));
    }

    #[test]
    fn google_chrome_links_are_recognised() {
        // The AI-mode teaser heading links a NEW query on Google itself.
        assert!(is_google_chrome_link(
            "https://www.google.com/search?q=deepseek+harness&ai=APn0"
        ));
        assert!(is_google_chrome_link("https://www.google.com/set/ai_mode"));
        assert!(is_google_chrome_link(
            "https://www.google.com/async/ctx?x=1"
        ));
        // Organic wrapper links are NOT chrome (they hide the real target).
        assert!(!is_google_chrome_link(
            "https://www.google.com/url?q=https://x.dev/"
        ));
        assert!(!is_google_chrome_link(
            "https://www.google.com/goto?url=CAESVwHrOzAV"
        ));
        assert!(!is_google_chrome_link("https://deepseek.com/harness"));
        assert!(!is_google_chrome_link(
            "https://www.google.evil.example/search?q=x"
        ));
        assert!(!is_google_chrome_link("not a url"));
    }

    /// Google's `/goto?url=<opaque token>` answers a plain 302 whose `Location`
    /// is the result's real URL (measured live: corrode.dev/blog/async/).
    #[tokio::test]
    async fn resolve_redirect_reads_location_without_following() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/goto"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", "https://corrode.dev/blog/async/"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/relative"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", "/elsewhere"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/script"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("location", "javascript:alert(1)"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/ok"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/sorry"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                "https://www.google.com/sorry/index?continue=https://www.google.com/goto",
            ))
            .mount(&server)
            .await;

        let client = CamofoxSearchClient::new("http://unused", None, None, Duration::from_secs(5));
        let base = server.uri();
        assert_eq!(
            client
                .resolve_redirect(&format!("{base}/goto?url=x"))
                .await
                .as_deref(),
            Some("https://corrode.dev/blog/async/")
        );
        assert_eq!(
            client.resolve_redirect(&format!("{base}/relative")).await,
            None
        );
        assert_eq!(
            client.resolve_redirect(&format!("{base}/script")).await,
            None
        );
        assert_eq!(client.resolve_redirect(&format!("{base}/ok")).await, None);
        // A rate-limit interstitial is not the result.
        assert_eq!(
            client.resolve_redirect(&format!("{base}/sorry")).await,
            None
        );
    }
}

#[cfg(test)]
mod github_api_tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// `github_search` hits the REST Search API and maps `items[]` into result
    /// rows: `html_url` → url, `full_name` → title, `description` → content,
    /// with the engine tagged `github` and descending position scores.
    #[tokio::test]
    async fn github_search_maps_items_to_rows() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search/repositories"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "items": [
                    { "html_url": "https://github.com/a/one", "full_name": "a/one", "description": "first" },
                    { "html_url": "https://github.com/b/two", "full_name": "b/two", "description": null },
                ]
            })))
            .mount(&server)
            .await;

        let mut client = CamofoxSearchClient::new(
            "http://unused",
            None,
            Some("tok".into()),
            Duration::from_secs(5),
        );
        client.github_api_base = server.uri();

        let rows = client
            .github_search("rust")
            .await
            .expect("github search ok");
        assert_eq!(rows.len(), 2);

        let first = &rows[0];
        assert_eq!(first.url.as_deref(), Some("https://github.com/a/one"));
        assert_eq!(first.title.as_deref(), Some("a/one"));
        assert_eq!(first.content.as_deref(), Some("first"));
        assert_eq!(first.engine.as_deref(), Some("github"));
        // A null description maps to no content.
        assert_eq!(rows[1].content, None);
        // Descending score by position so the merge ranks earlier hits higher.
        assert!(rows[0].score.unwrap() > rows[1].score.unwrap());
    }

    /// A non-2xx GitHub response surfaces as an `Upstream` error (not a panic or
    /// silent empty), so the fetch loop records it and skips the engine.
    #[tokio::test]
    async fn github_search_maps_error_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search/repositories"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let mut client =
            CamofoxSearchClient::new("http://unused", None, None, Duration::from_secs(5));
        client.github_api_base = server.uri();

        let err = client.github_search("rust").await.unwrap_err();
        assert!(matches!(err, SearchError::Upstream { status: 403, .. }));
    }

    /// Partial-failure skip: a multi-engine fetch where one engine fails must
    /// still return the others' rows (the failed engine is skipped, not fatal).
    /// Here Google (browser) succeeds and GitHub (API) 500s; the response holds
    /// only Google's row.
    #[tokio::test]
    async fn fetch_skips_failed_engine_and_returns_partial() {
        let server = MockServer::start().await;
        // Camofox browser flow for the Google engine → one row.
        let rows = serde_json::to_string(&json!([
            { "url": "https://rust-lang.org", "title": "Rust", "content": "" },
        ]))
        .unwrap();
        Mock::given(method("POST"))
            .and(path("/tabs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tabId": "t1" })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t1/navigate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t1/wait"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t1/evaluate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": rows })))
            .mount(&server)
            .await;
        // GitHub API fails.
        Mock::given(method("GET"))
            .and(path("/search/repositories"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let mut client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_secs(5));
        client.github_api_base = server.uri();

        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Google, SearchEngine::Github],
            ..Default::default()
        };
        let resp = client
            .fetch(&params)
            .await
            .expect("partial success, not error");
        assert_eq!(resp.results.len(), 1);
        assert_eq!(
            resp.results[0].url.as_deref(),
            Some("https://rust-lang.org")
        );
        assert_eq!(resp.results[0].engine.as_deref(), Some("google"));
    }

    /// When *every* engine fails, the fetch surfaces an error (not an empty Ok).
    #[tokio::test]
    async fn fetch_errors_when_all_engines_fail() {
        let server = MockServer::start().await;
        // Tab creation fails → the Google engine errors; no other engine.
        Mock::given(method("POST"))
            .and(path("/tabs"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Google],
            ..Default::default()
        };
        assert!(client.fetch(&params).await.is_err());
    }

    /// A timeout on a *reused* warm tab is the dead-tab signature (camofox
    /// accepts a navigate for a tab it no longer has and hangs instead of
    /// returning 404), so the cached id is dropped and the search retried on a
    /// fresh tab. Regression guard: without it the client keeps addressing the
    /// dead tab and every later search stalls identically, leaving the endpoint
    /// broken until the process restarts.
    #[tokio::test]
    async fn timeout_on_reused_tab_recreates_it_and_recovers() {
        let server = MockServer::start().await;
        let rows = r#"[{"url":"https://rust-lang.org","title":"Rust","content":"lang"}]"#;

        // `t1` is minted first and later "dies"; `t2` is its replacement.
        Mock::given(method("POST"))
            .and(path("/tabs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tabId": "t1" })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tabId": "t2" })))
            .mount(&server)
            .await;

        // t1 answers the first navigate (warming the cache), then hangs well
        // past the client timeout on every later one.
        Mock::given(method("POST"))
            .and(path("/tabs/t1/navigate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t1/navigate"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t2/navigate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
            .mount(&server)
            .await;
        // The abandoned t1 is closed, not just forgotten.
        let closed_t1 = Mock::given(method("DELETE"))
            .and(path("/tabs/t1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount_as_scoped(&server)
            .await;

        for tab in ["t1", "t2"] {
            Mock::given(method("POST"))
                .and(path(format!("/tabs/{tab}/wait")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path(format!("/tabs/{tab}/evaluate")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": rows })))
                .mount(&server)
                .await;
        }

        let client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_millis(300));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Google],
            ..Default::default()
        };

        // First search warms the cache with t1.
        let first = client.fetch(&params).await.expect("first search succeeds");
        assert_eq!(first.results.len(), 1);

        // Second search stalls on the now-dead t1, recreates as t2, and still
        // returns results — transparently, with no engine reported unresponsive.
        let second = client
            .fetch(&params)
            .await
            .expect("stale-tab timeout recovers on a fresh tab");
        assert_eq!(second.results.len(), 1);
        assert!(
            second.unresponsive_engines.is_empty(),
            "recovery should be transparent, got {:?}",
            second.unresponsive_engines
        );
        await_close(&closed_t1).await;
        server.verify().await;
    }

    /// A failed camofox call carries the server's own `error` message in the
    /// `Upstream` body (truncated), not a fixed label — the message is what
    /// tells a profile-version pin apart from a crashed browser.
    #[tokio::test]
    async fn create_tab_error_surfaces_camofox_message() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/tabs"))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "error": "Profile for user \"crw-search\" was created with Camoufox 135.0.1-beta.24, but the current version is 152.0.4-beta.28"
            })))
            .mount(&server)
            .await;

        let client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Google],
            ..Default::default()
        };
        let err = client.fetch(&params).await.unwrap_err();
        match err {
            SearchError::Upstream { status, body } => {
                assert_eq!(status, 500);
                assert!(
                    body.starts_with("camofox: create tab failed: Profile for user"),
                    "{body}"
                );
                assert!(body.contains("152.0.4-beta.28"), "{body}");
            }
            other => panic!("expected Upstream, got {other:?}"),
        }
    }

    /// A non-JSON error body (a proxy's HTML page, a crash trace) stays out of
    /// the message — `Upstream.body` reaches API responses — so the error
    /// degrades to the bare step label.
    #[tokio::test]
    async fn navigate_error_with_non_json_body_keeps_label() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/tabs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tabId": "t1" })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t1/navigate"))
            .respond_with(
                ResponseTemplate::new(502)
                    .set_body_string("<html><body>Bad Gateway at /internal/x</body></html>"),
            )
            .mount(&server)
            .await;

        let client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };
        match client.fetch(&params).await.unwrap_err() {
            SearchError::Upstream { status, body } => {
                assert_eq!(status, 502);
                assert_eq!(body, "camofox: navigate failed");
            }
            other => panic!("expected Upstream, got {other:?}"),
        }
    }

    /// A stale tab signalled by a 5xx is recreated and retried, and — unlike
    /// the timeout path — not DELETEd: the server already dropped it.
    #[tokio::test]
    async fn stale_5xx_recreates_tab_without_delete() {
        let server = MockServer::start().await;
        let rows = r#"[{"url":"https://rust-lang.org","title":"Rust","content":"lang"}]"#;
        mount_tab_sequence(&server, &["t1", "t2"]).await;
        Mock::given(method("POST"))
            .and(path("/tabs/t1/navigate"))
            .respond_with(
                ResponseTemplate::new(500).set_body_json(json!({ "error": "window is null" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t2/navigate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t2/wait"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t2/evaluate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": rows })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/tabs/t1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };
        let resp = client
            .fetch(&params)
            .await
            .expect("5xx stale tab recovers on a fresh tab");
        assert_eq!(resp.results.len(), 1);
        assert_eq!(client.tab.lock().await.as_deref(), Some("t2"));
        server.verify().await;
    }

    /// A failed `/evaluate` (camofox answers a dead tab with a JSON error, not
    /// an empty result) is an `Upstream` error, so the stale-tab retry runs
    /// instead of reporting a clean empty SERP.
    #[tokio::test]
    async fn evaluate_error_status_triggers_stale_retry() {
        let server = MockServer::start().await;
        let rows = r#"[{"url":"https://rust-lang.org","title":"Rust","content":"lang"}]"#;
        mount_tab_sequence(&server, &["t1", "t2"]).await;
        for tab in ["t1", "t2"] {
            Mock::given(method("POST"))
                .and(path(format!("/tabs/{tab}/navigate")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path(format!("/tabs/{tab}/wait")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
                .mount(&server)
                .await;
        }
        Mock::given(method("POST"))
            .and(path("/tabs/t1/evaluate"))
            .respond_with(
                ResponseTemplate::new(500).set_body_json(json!({ "error": "tab not found" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t2/evaluate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": rows })))
            .mount(&server)
            .await;

        let client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };
        let resp = client
            .fetch(&params)
            .await
            .expect("evaluate 5xx recovers on a fresh tab");
        assert_eq!(resp.results.len(), 1);
        assert!(
            resp.unresponsive_engines.is_empty(),
            "{:?}",
            resp.unresponsive_engines
        );
        assert_eq!(client.tab.lock().await.as_deref(), Some("t2"));
    }

    /// An empty error body degrades to the bare step label.
    #[tokio::test]
    async fn navigate_error_without_body_keeps_label() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/tabs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tabId": "t1" })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t1/navigate"))
            .respond_with(ResponseTemplate::new(502))
            .mount(&server)
            .await;

        let client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };
        let err = client.fetch(&params).await.unwrap_err();
        match err {
            SearchError::Upstream { status, body } => {
                assert_eq!(status, 502);
                assert_eq!(body, "camofox: navigate failed");
            }
            other => panic!("expected Upstream, got {other:?}"),
        }
    }

    /// Mount `POST /tabs` so each call mints the next id in `ids`, the last one
    /// repeating.
    async fn mount_tab_sequence(server: &MockServer, ids: &[&str]) {
        for (i, id) in ids.iter().enumerate() {
            let mock = Mock::given(method("POST"))
                .and(path("/tabs"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tabId": id })));
            let mock = if i + 1 < ids.len() {
                mock.up_to_n_times(1)
            } else {
                mock
            };
            mock.mount(server).await;
        }
    }

    /// The abandoned-tab close is detached — nothing on the search path waits for
    /// it — so a test that asserts the DELETE arrived has to let it land before
    /// `MockServer::verify()` runs. Bounded: a test that never sees it falls
    /// through to `verify()` and fails there, as it should.
    async fn await_close(mock: &wiremock::MockGuard) {
        for _ in 0..100 {
            if !mock.received_requests().await.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// A client-side timeout abandons the warm tab: a replacement is minted
    /// first (so the context never drops to zero tabs), then the old tab is
    /// closed (DELETE), and the next search reuses the replacement instead of
    /// queueing behind the call camofox is still working on.
    #[tokio::test]
    async fn timeout_replaces_and_closes_warm_tab() {
        let server = MockServer::start().await;
        mount_tab_sequence(&server, &["t1", "t2"]).await;
        Mock::given(method("POST"))
            .and(path("/tabs/t1/navigate"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "ok": true }))
                    .set_delay(Duration::from_millis(1500)),
            )
            .mount(&server)
            .await;
        let closed_t1 = Mock::given(method("DELETE"))
            .and(path("/tabs/t1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount_as_scoped(&server)
            .await;

        let client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_millis(300));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };
        let err = client.fetch(&params).await.unwrap_err();
        assert!(matches!(err, SearchError::Timeout), "{err:?}");
        assert_eq!(
            client.tab.lock().await.as_deref(),
            Some("t2"),
            "the replacement must be cached, not the abandoned tab"
        );
        await_close(&closed_t1).await;
        server.verify().await;
    }

    /// When the stale-tab retry itself times out, the retry's fresh tab is
    /// abandoned too — it is closed and not left cached for the next search.
    #[tokio::test]
    async fn retry_timeout_abandons_fresh_tab() {
        let server = MockServer::start().await;
        let rows = r#"[{"url":"https://rust-lang.org","title":"Rust","content":"lang"}]"#;
        mount_tab_sequence(&server, &["t1", "t2", "t3"]).await;

        // t1 warms the cache, then every navigate hangs past the client timeout.
        Mock::given(method("POST"))
            .and(path("/tabs/t1/navigate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        let mut closed = Vec::new();
        for tab in ["t1", "t2"] {
            Mock::given(method("POST"))
                .and(path(format!("/tabs/{tab}/navigate")))
                .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
                .mount(&server)
                .await;
            closed.push(
                Mock::given(method("DELETE"))
                    .and(path(format!("/tabs/{tab}")))
                    .respond_with(ResponseTemplate::new(200))
                    .expect(1)
                    .mount_as_scoped(&server)
                    .await,
            );
        }
        Mock::given(method("POST"))
            .and(path("/tabs/t1/wait"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tabs/t1/evaluate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": rows })))
            .mount(&server)
            .await;

        let client = CamofoxSearchClient::new(server.uri(), None, None, Duration::from_millis(300));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };
        client.fetch(&params).await.expect("first search warms t1");
        let err = client.fetch(&params).await.unwrap_err();
        assert!(matches!(err, SearchError::Timeout), "{err:?}");
        assert_eq!(
            client.tab.lock().await.as_deref(),
            Some("t3"),
            "the retry's timed-out tab must be swapped out too"
        );
        for mock in &closed {
            await_close(mock).await;
        }
        server.verify().await;
    }
}

#[cfg(test)]
mod google_resolution_tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Item 4 end to end: a Google search whose result links are `/goto`
    /// redirects comes back with the real destinations, a link that does not
    /// redirect keeps its URL, and non-redirect rows are untouched.
    #[tokio::test]
    async fn google_search_returns_resolved_result_urls() {
        let server = MockServer::start().await;
        let base = server.uri();
        let rows = serde_json::to_string(&json!([
            { "url": format!("{base}/goto?url=a"), "title": "A", "content": "" },
            { "url": format!("{base}/goto?url=dead"), "title": "Dead", "content": "" },
            { "url": "https://direct.example/c", "title": "C", "content": "" },
        ]))
        .unwrap();
        for (p, body) in [
            ("/tabs", json!({ "tabId": "t1" })),
            ("/tabs/t1/navigate", json!({ "ok": true })),
            ("/tabs/t1/wait", json!({ "ok": true })),
            ("/tabs/t1/evaluate", json!({ "result": rows })),
        ] {
            Mock::given(method("POST"))
                .and(path(p))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/goto"))
            .and(wiremock::matchers::query_param("url", "a"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("location", "https://real.example/a"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/goto"))
            .and(wiremock::matchers::query_param("url", "dead"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut client = CamofoxSearchClient::new(&base, None, None, Duration::from_secs(5));
        client.redirect_origin = Some(base.clone());
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Google],
            ..Default::default()
        };
        let resp = client.fetch(&params).await.expect("search ok");
        let urls: Vec<_> = resp.results.iter().filter_map(|r| r.url.clone()).collect();
        assert!(
            urls.contains(&"https://real.example/a".to_string()),
            "{urls:?}"
        );
        assert!(urls.contains(&format!("{base}/goto?url=dead")), "{urls:?}");
        assert!(
            urls.contains(&"https://direct.example/c".to_string()),
            "{urls:?}"
        );
    }
}

#[cfg(test)]
mod tab_trust_tests {
    use super::*;

    /// The two recovery questions are deliberately NOT the same question, and
    /// the whole rule is checkable without a socket: `is_stale_tab` decides
    /// whether recreating could help (RETRY), `tab_untrusted` decides whether
    /// the abandoned id must be DELETEd. They disagree exactly where it matters
    /// — a dropped connection is stale enough to retry, yet the server almost
    /// certainly still holds the tab.
    #[test]
    fn only_the_servers_own_word_skips_the_delete() {
        let gone = |status: u16| SearchError::Upstream {
            status,
            body: String::new(),
        };

        // The server itself said the tab/context is gone: recreate, but a
        // DELETE would be pointless. Pinned end-to-end by
        // `stale_5xx_recreates_tab_without_delete`.
        for e in [gone(404), gone(500), gone(503)] {
            assert!(is_stale_tab(&e), "{e:?} must retry");
            assert!(!tab_untrusted(&e), "{e:?} must not be DELETEd");
        }

        // The server never answered, so nothing closed the tab: DELETE it.
        assert!(tab_untrusted(&SearchError::Timeout));
        assert!(tab_untrusted(&SearchError::Transport(
            "connection reset".into()
        )));
        // `Timeout` is not `is_stale_tab` because it means different things
        // depending on the tab; `fetch` applies that at the call site.
        assert!(!is_stale_tab(&SearchError::Timeout));
        // The transport error is the classification this change is about: it
        // was already stale (so it retried) but was silently forgetting tabs.
        assert!(is_stale_tab(&SearchError::Transport("reset".into())));

        // A malformed answer recovers neither way: reported as-is, tab kept.
        let bad = SearchError::InvalidResponse("not json".into());
        assert!(!is_stale_tab(&bad));
        assert!(!tab_untrusted(&bad));
    }

    /// The end-to-end half of the rule above: a warm tab whose navigate dies
    /// mid-flight (connection dropped, camofox restart) must be CLOSED, not
    /// just forgotten, and the search must still recover on a fresh tab.
    ///
    /// Deliberately not a wiremock test: wiremock has no clean way to drop a
    /// connection, and tearing the mock server down would make the DELETE being
    /// asserted unobservable. A hand-rolled stub (precedent:
    /// `crw-renderer/tests/waf_challenge_proxy_hang.rs`) answers `/tabs`
    /// normally while closing the socket on `tab-1`'s navigate, which is what
    /// makes reqwest fail with something that is NOT a timeout and so reach the
    /// `SearchError::Transport` arm of `post`.
    #[tokio::test]
    async fn transport_error_closes_the_abandoned_warm_tab() {
        let state = StubState::default();
        let base = spawn_stub(state.clone()).await;
        let client = CamofoxSearchClient::new(base, None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };

        let resp = client
            .fetch(&params)
            .await
            .expect("the dropped connection must recover on a replacement tab");
        assert_eq!(resp.results.len(), 1, "the retry must still return rows");
        assert!(
            resp.unresponsive_engines.is_empty(),
            "recovery is transparent: {:?}",
            resp.unresponsive_engines
        );
        assert_eq!(
            client.tab.lock().await.as_deref(),
            Some("tab-2"),
            "and the cache must hold the replacement"
        );
        assert!(
            state.wait_deleted("tab-1").await,
            "the tab whose connection dropped must be DELETEd, not just dropped; \
             deletes so far: {:?}",
            state.deletes()
        );
        // One settle window so a SECOND close (a surplus tab wrongly closed, say)
        // is in the snapshot too, before asserting that nothing else was.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            state.deletes(),
            vec!["tab-1".to_string()],
            "exactly the abandoned tab is closed — the replacement it just \
             adopted must stay open"
        );
    }

    /// The invariant this client exists to uphold, in one sentence: every tab
    /// camofox minted for us is either the warm tab or closed. A create that
    /// answers later than we were willing to wait is the case that used to break
    /// it — the request was cancelled while the id was still in flight, and
    /// nothing in this process could ever learn that id to close it.
    #[tokio::test(flavor = "current_thread")]
    async fn a_create_that_outlives_its_waiter_is_never_lost() {
        let state = StubState {
            // Slower than the wait below, so the waiter always gives up first.
            stall_create_ms: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(250)),
            ..StubState::default()
        };
        let base = spawn_stub(state.clone()).await;
        let client = CamofoxSearchClient::new(base, None, None, Duration::from_secs(5))
            .with_prewarm_budget(Duration::from_millis(60));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };

        let started = std::time::Instant::now();
        // A heartbeat on the same runtime: if the stalled create blocked the
        // runtime instead of awaiting, the client's own 60 ms timer could not
        // fire either and this test would silently stop exercising the race it is
        // named for. Ticking proves the waiter genuinely gave up first.
        let beats = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let beat_handle = {
            let beats = std::sync::Arc::clone(&beats);
            tokio::spawn(async move {
                loop {
                    beats.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        client
            .fetch(&params)
            .await
            .expect("the search must survive a create slower than its budget");
        beat_handle.abort();
        assert!(
            beats.load(std::sync::atomic::Ordering::SeqCst) > 5,
            "the runtime kept running while the create was stalled — without this, \
             the waiter and the stalled create cannot be racing at all"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the recovery is bounded by the WAIT, not by the create: {:?}",
            started.elapsed()
        );

        // A second search settles the shelf: with the warm tab cached, anything
        // still unadopted is surplus and must go away.
        client.fetch(&params).await.expect("second search");
        let mut settled = false;
        for _ in 0..50 {
            let cached = client.tab.lock().await.clone();
            let deleted = state.deletes();
            let outstanding: Vec<String> = state
                .minted
                .lock()
                .unwrap()
                .iter()
                .filter(|id| Some(*id) != cached.as_ref() && !deleted.contains(id))
                .cloned()
                .collect();
            if outstanding.is_empty() {
                settled = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            settled,
            "tabs nobody owns and nobody closed: {:?}",
            state.minted.lock().unwrap()
        );
        assert!(
            state.deletes().contains(&"tab-1".to_string()),
            "the abandoned tab must still be closed: {:?}",
            state.deletes()
        );
    }

    /// A tab the server already reported gone (404/5xx) must NOT be DELETEd —
    /// the same contract as `stale_5xx_recreates_tab_without_delete`, driven
    /// through the same stub so the two paths are told apart by one instrument.
    #[tokio::test]
    async fn server_reported_stale_tab_is_not_deleted() {
        let state = StubState {
            fail_navigate_with_status: Some(500),
            ..StubState::default()
        };
        let base = spawn_stub(state.clone()).await;
        let client = CamofoxSearchClient::new(base, None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };

        let resp = client
            .fetch(&params)
            .await
            .expect("a 5xx on the warm tab must recover on a fresh one");
        assert_eq!(resp.results.len(), 1);
        // Prove the recovery actually ran, rather than this engine having
        // quietly dropped out: a replacement must have been minted and cached.
        assert_eq!(
            state.minted.lock().unwrap().as_slice(),
            &["tab-1".to_string(), "tab-2".to_string()],
            "the stale tab must be replaced, not reused"
        );
        assert_eq!(
            client.tab.lock().await.as_deref(),
            Some("tab-2"),
            "the replacement must be the cached warm tab"
        );
        // Closes are detached, so "nothing was deleted" is only meaningful after
        // a window in which one could have arrived.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            state.deletes().is_empty(),
            "camofox already reaped it; nothing to close: {:?}",
            state.deletes()
        );
    }

    /// The shelf's own bound: nobody is going to adopt an unadopted tab, so the
    /// oldest goes away rather than ageing toward the session timeout. The two
    /// newest must survive — a trim that ate its own creator's id would leave
    /// `create_tab_awaiting` with nothing to adopt.
    #[tokio::test]
    async fn an_unadopted_shelf_trims_its_oldest_and_keeps_the_rest() {
        let state = StubState::default();
        let base = spawn_stub(state.clone()).await;
        let client = CamofoxSearchClient::new(base, None, None, Duration::from_secs(5));

        // Serialized deliberately: each create publishes and trims in turn, so
        // this pins the trim's policy rather than its timing.
        for _ in 0..3 {
            client
                .spawn_create_tab()
                .await
                .expect("create task runs")
                .expect("create succeeds");
        }

        assert_eq!(
            state.deletes(),
            vec!["tab-1".to_string()],
            "the third unadopted tab closes the oldest"
        );
        let shelf = client.prewarmed.lock().await.clone();
        assert_eq!(
            shelf,
            vec!["tab-2".to_string(), "tab-3".to_string()],
            "the two newest stay"
        );
    }

    /// The property the whole shelf rests on, which until now only reasoning
    /// protected: a drain closes the shelf's tabs, never the warm tab. Possible
    /// only because an id is popped before it is cached, so the two sets are
    /// disjoint by construction — this makes the claim a test.
    #[tokio::test]
    async fn a_drain_closes_the_shelf_and_never_the_warm_tab() {
        let state = StubState::default();
        let base = spawn_stub(state.clone()).await;
        let client = CamofoxSearchClient::new(base, None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };

        // A first search that recovers, leaving tab-2 as the cached warm tab.
        client.fetch(&params).await.expect("recovery search");
        let warm = client.tab.lock().await.clone().expect("warm tab cached");
        assert_eq!(warm, "tab-2", "the recovered tab is the cached one");

        // A detached create that finished after nobody wanted it.
        client.prewarmed.lock().await.push_back("tab-3".to_string());

        client.fetch(&params).await.expect("cached-tab search");
        assert!(
            state.wait_deleted("tab-3").await,
            "the surplus tab must be closed: {:?}",
            state.deletes()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !state.deletes().contains(&warm),
            "the drain closed the warm tab it was sitting next to: {:?}",
            state.deletes()
        );
        assert_eq!(
            client.tab.lock().await.as_deref(),
            Some("tab-2"),
            "and the warm tab is still the one this client is using"
        );
    }

    /// A 2xx carrying fields we never asked about must still yield the id, and
    /// must not be mistaken for the failure that IS a leak — a 2xx we cannot read
    /// leaves a tab camofox had ALREADY registered (`core.js` registers before it
    /// replies) with no id in hand, and this client has no list-and-reap to ever
    /// find it. The two cases are told apart here: unrelated fields are fine, an
    /// absent id is not (see `a_2xx_create_without_an_id_is_reported_as_such`).
    #[tokio::test]
    async fn a_2xx_create_with_extra_fields_still_yields_its_id() {
        let state = StubState {
            create_extra_fields: true,
            ..StubState::default()
        };
        let base = spawn_stub(state.clone()).await;
        let client = CamofoxSearchClient::new(base, None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };

        let resp = client
            .fetch(&params)
            .await
            .expect("the extra fields must not break the create");
        assert_eq!(resp.results.len(), 1);
        assert_eq!(
            client.tab.lock().await.as_deref(),
            Some("tab-2"),
            "the id was read off a body a typed parse would have rejected"
        );
    }

    /// A 2xx with NO id is the one create failure that is a leak rather than a
    /// stall, and it must be surfaced as its own error rather than dressed up as
    /// an upstream status.
    #[tokio::test]
    async fn a_2xx_create_without_an_id_is_reported_as_such() {
        let state = StubState {
            create_no_tab_id: true,
            ..StubState::default()
        };
        let base = spawn_stub(state.clone()).await;
        let client = CamofoxSearchClient::new(base, None, None, Duration::from_secs(5));
        let params = SearxngParams {
            q: "rust".to_string(),
            camofox_engines: vec![SearchEngine::Bing],
            ..Default::default()
        };

        let err = client.fetch(&params).await.unwrap_err();
        assert!(
            matches!(err, SearchError::InvalidResponse(_)),
            "an unreadable-but-successful create is an InvalidResponse, not a timeout: {err:?}"
        );
        assert!(
            state.deletes().is_empty(),
            "nothing here can close a tab whose id was never returned: {:?}",
            state.deletes()
        );
    }

    // ---- the stub ----------------------------------------------------------

    /// What the stub does, and what it saw.
    #[derive(Clone, Default)]
    struct StubState {
        /// Tab ids minted by `POST /tabs`, in order.
        minted: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        /// Tab ids a `DELETE /tabs/{id}` arrived for.
        deleted: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        /// When set, every navigate answers this status instead of dropping the
        /// connection — the "server said the tab is gone" shape.
        fail_navigate_with_status: Option<u16>,
        /// How long `POST /tabs` stalls before answering, in milliseconds. Models
        /// a create slower than any budget we wait on — the cold-context and
        /// profile-relaunch shapes that make an abandoned create likely.
        stall_create_ms: std::sync::Arc<std::sync::atomic::AtomicU64>,
        /// Answer `POST /tabs` with a 2xx that carries no `tabId`. The tab is
        /// registered server-side; this client never learns it exists.
        create_no_tab_id: bool,
        /// Answer `POST /tabs` with the id PLUS unrelated fields — an answer a
        /// typed parser would choke on and a lenient one still reads.
        create_extra_fields: bool,
    }

    impl StubState {
        fn deletes(&self) -> Vec<String> {
            self.deleted.lock().unwrap().clone()
        }

        /// Wait for a DELETE for `id` to arrive. The abandoned-tab close is
        /// detached, so it lands on its own schedule, not the search's.
        async fn wait_deleted(&self, id: &str) -> bool {
            for _ in 0..100 {
                if self.deletes().iter().any(|d| d == id) {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            false
        }
    }

    fn http_ok(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn find_headers_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    fn content_length(head: &[u8]) -> usize {
        String::from_utf8_lossy(head)
            .lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0)
    }

    /// Read one request far enough to answer it correctly — including its body,
    /// so closing the socket cannot be misreported as a write failure on a
    /// request the stub meant to serve.
    async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<(String, String)> {
        use tokio::io::AsyncReadExt;
        let mut acc: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 1024];
        let head_end = loop {
            match sock.read(&mut chunk).await {
                Ok(0) => return None,
                Ok(n) => {
                    acc.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = find_headers_end(&acc) {
                        break pos;
                    }
                }
                Err(_) => return None,
            }
        };
        let want = head_end + 4 + content_length(&acc[..head_end]);
        while acc.len() < want {
            match sock.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => acc.extend_from_slice(&chunk[..n]),
            }
        }
        let request_line = String::from_utf8_lossy(&acc[..head_end])
            .lines()
            .next()?
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let mut request_line = request_line.into_iter();
        let method = request_line.next()?;
        let path = request_line.next()?;
        Some((method, path))
    }

    async fn spawn_stub(state: StubState) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stub binds");
        let addr = listener.local_addr().expect("stub addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    let Some((method, path)) = read_request(&mut sock).await else {
                        return;
                    };
                    let rows =
                        r#"[{"url":"https://rust-lang.org","title":"Rust","content":"lang"}]"#;
                    // Only the FIRST tab fails, whichever way the test asked
                    // for: the replacement must succeed or the recovery itself
                    // would never be exercised.
                    let first_tab = path.contains("tab-1");
                    let reply = match (method.as_str(), path.as_str()) {
                        ("POST", "/tabs") => {
                            let stall = state
                                .stall_create_ms
                                .load(std::sync::atomic::Ordering::SeqCst);
                            if stall > 0 {
                                // Awaited, NOT `std::thread::sleep`: this handler
                                // runs as a task on the test's current-thread
                                // runtime, so blocking here parks the runtime too —
                                // including the very client timer the test is
                                // trying to beat, and the race it names would
                                // never actually happen.
                                tokio::time::sleep(Duration::from_millis(stall)).await;
                            }
                            let next = state.minted.lock().unwrap().len() + 1;
                            let id = format!("tab-{next}");
                            state.minted.lock().unwrap().push(id.clone());
                            if state.create_no_tab_id {
                                // Registered server-side, id never handed back —
                                // the 2xx-without-an-id leak shape.
                                http_ok(r#"{"ok":true}"#)
                            } else if state.create_extra_fields {
                                // A server that has grown fields we never asked
                                // about: the id is still there for a parser that
                                // goes and gets it.
                                http_ok(&format!(
                                    "{{\"ok\":true,\"tabId\":\"{id}\",\"meta\":{{\"profile\":\"default\",\"at\":1700000000}}}}"
                                ))
                            } else {
                                http_ok(&format!("{{\"ok\":true,\"tabId\":\"{id}\"}}"))
                            }
                        }
                        ("POST", p) if p.ends_with("/navigate") && first_tab => {
                            match state.fail_navigate_with_status {
                                // The server's own word that the tab is gone.
                                Some(status) => format!(
                                    "HTTP/1.1 {status} Failed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                                ),
                                // The connection dies mid-request, which reqwest
                                // reports as a transport error, not a timeout.
                                None => {
                                    drop(sock);
                                    return;
                                }
                            }
                        }
                        ("POST", p) if p.ends_with("/navigate") => http_ok(r#"{"ok":true}"#),
                        ("POST", p) if p.ends_with("/wait") => http_ok(r#"{"ok":true}"#),
                        ("POST", p) if p.ends_with("/evaluate") => {
                            http_ok(&format!("{{\"ok\":true,\"result\":{}}}", json!(rows)))
                        }
                        ("DELETE", p) => {
                            if let Some(id) = p.rsplit('/').next() {
                                state.deleted.lock().unwrap().push(id.to_string());
                            }
                            // Like the real route: it accepts whatever it is
                            // asked to close, so an accepted DELETE proves
                            // nothing on its own.
                            http_ok(r#"{"ok":true}"#)
                        }
                        _ => http_ok(r#"{"ok":true}"#),
                    };
                    let _ = sock.write_all(reply.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        format!("http://{addr}")
    }
}
