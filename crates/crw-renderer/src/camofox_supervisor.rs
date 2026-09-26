//! On-demand lifecycle for a host-local camofox-browser server.
//!
//! The Docker path ships camofox as its own container; on a single machine
//! (the macOS/Apple-Silicon local path) that is exactly the cost we want to
//! avoid: a whole VM for one REST server. With `[renderer.camofox]
//! manage = true`, this module takes over the endpoint's lifecycle:
//!
//! * **Lazy wake** — the server is spawned the first time the Camofox tier
//!   or `/v1/search` actually needs it, not at boot. An idle stack pays
//!   nothing for Firefox.
//! * **Adoption before spawn** — a server already answering `GET /health` on
//!   the configured port is adopted (a second crw instance, a leftover
//!   daemon), never duplicated.
//! * **Crash/idle respawn** — camofox's two-stage idle policy closes idle
//!   sessions and can exit the daemon between bursts; a crashed or exited
//!   server is respawned on the next request that needs it (the request
//!   that wakes it pays the few seconds, subsequent ones do not).
//! * **Reaping** — the spawned process leads its own process group,
//!   registered in `browser::BROWSER_PGIDS`, so the existing shutdown and
//!   signal paths (`kill_all_browsers`) reap the Node server *and* its
//!   Camoufox/Firefox grandchildren.
//!
//! Health probes deliberately distinguish the two liveness questions:
//! * **TCP connect** (cheap, no side effects) — "is the server process
//!   there at all?", used by [`CamofoxSupervisor::alive_hint`].
//! * **`GET /health`** — "can we render?" Note camofox's `/health`
//!   *pre-launches the browser* by design; it is therefore only called on
//!   the request path ([`CamofoxSupervisor::ensure_ready`]), never from a
//!   background loop, so an idle managed server is free to let Firefox go.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};
use tracing::info;

use crw_core::error::{CrwError, CrwResult};

use crate::browser::{command_exists, find_in_path, register_child};

/// The camofox-browser npm version this build manages. Kept in lockstep with
/// the `ghcr.io/redf0x1/camofox-browser` pin in `docker-compose.yml` so the
/// native and containerized tiers speak the same REST dialect. Override with
/// `CRW_CAMOFOX_BROWSER_VERSION`.
pub const DEFAULT_CAMOFOX_BROWSER_VERSION: &str = "2.4.6";

/// `ensure_ready` budget when the caller has no tighter deadline: the first
/// spawn on a fresh machine downloads the pinned Camoufox engine (a few
/// hundred MB) before the server answers `/health`.
pub const DEFAULT_COLD_START: Duration = Duration::from_secs(180);

/// Readiness poll interval while a freshly spawned server boots.
const PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// Timeout for a single `/health` probe. The server answers instantly when
/// up; anything slower is treated as down and retried against the budget.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// TCP-connect timeout for the no-side-effect liveness hint.
const TCP_PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// Environment overrides honored by the supervisor.
const ENV_BROWSER_BIN: &str = "CRW_CAMOFOX_BROWSER_BIN";
const ENV_BROWSER_VERSION: &str = "CRW_CAMOFOX_BROWSER_VERSION";

/// Resolved launch command for the camofox-browser server (foreground: the
/// supervised child *is* the server, so `try_wait` is the liveness oracle —
/// unlike the detached `server start --background` daemon mode, which would
/// escape the process-group registry as a double-fork).
#[derive(Debug, Clone, PartialEq, Eq)]
struct LaunchCommand {
    program: String,
    args: Vec<String>,
}

/// Supervisor for one camofox-browser endpoint. Cheap to share; internal
/// state is behind a mutex so concurrent `ensure_ready` callers serialize
/// on a single spawn (the loser of the race finds the server already
/// healthy and returns immediately).
pub struct CamofoxSupervisor {
    base_url: String,
    port: u16,
    api_key: Option<String>,
    cold_start: Duration,
    probe: reqwest::Client,
    /// Test seam: bypass runner resolution (PATH/npx).
    runner_override: Option<LaunchCommand>,
    /// Test seam / operator override for the npm pin.
    version: String,
    state: tokio::sync::Mutex<State>,
}

#[derive(Default)]
struct State {
    child: Option<Child>,
    pgid: Option<i32>,
}

impl CamofoxSupervisor {
    pub fn new(base_url: impl Into<String>, api_key: Option<String>) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let port = parse_http_port(&base_url).unwrap_or(9377);
        let version = std::env::var(ENV_BROWSER_VERSION)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_CAMOFOX_BROWSER_VERSION.to_string());
        let probe = reqwest::Client::builder()
            .timeout(PROBE_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            base_url,
            port,
            api_key,
            cold_start: DEFAULT_COLD_START,
            probe,
            runner_override: None,
            version,
            state: tokio::sync::Mutex::new(State::default()),
        }
    }

    /// Override the cold-start readiness budget (tests, tight deadlines).
    pub fn with_cold_start(mut self, budget: Duration) -> Self {
        self.cold_start = budget;
        self
    }

    /// Inject the launch command (tests). Production leaves this `None` so
    /// [`Self::resolve_runner`] decides: PATH → npx.
    pub fn with_runner(mut self, program: impl Into<String>, args: Vec<String>) -> Self {
        self.runner_override = Some(LaunchCommand {
            program: program.into(),
            args,
        });
        self
    }

    /// The configured endpoint, e.g. `http://127.0.0.1:9377`.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Where the server's stdout/stderr go — surfaced in every spawn failure
    /// so an operator can diagnose an engine download or a Node error without
    /// hunting for the process.
    fn log_path() -> Option<PathBuf> {
        dirs::home_dir().map(|h| h.join(".crw").join("camofox").join("server.log"))
    }

    /// Resolve how to launch the server.
    ///
    /// Precedence: test override → `CRW_CAMOFOX_BROWSER_BIN` (a foreground
    /// server executable) → `camofox-browser` on PATH → `npx -y
    /// camofox-browser@<pin>` (fetches the pinned package on demand).
    fn resolve_runner(&self) -> CrwResult<LaunchCommand> {
        if let Some(rc) = &self.runner_override {
            return Ok(rc.clone());
        }
        if let Ok(bin) = std::env::var(ENV_BROWSER_BIN)
            && !bin.trim().is_empty()
        {
            return Ok(LaunchCommand {
                program: bin,
                args: Vec::new(),
            });
        }
        if find_in_path("camofox-browser").is_some() {
            return Ok(LaunchCommand {
                program: "camofox-browser".into(),
                args: Vec::new(),
            });
        }
        // The bare `camofox-browser` invocation starts the REST server in the
        // foreground — what we supervise. NOT `server start --background`,
        // which double-forks a daemon outside our process group.
        if find_in_path("npx").is_some() || command_exists("npx") {
            return Ok(LaunchCommand {
                program: "npx".into(),
                args: vec!["-y".into(), format!("camofox-browser@{}", self.version)],
            });
        }
        Err(CrwError::ConfigError(format!(
            "managed camofox needs a runner: install camofox-browser (npm), \
             put `camofox-browser` on PATH, or set {ENV_BROWSER_BIN}; \
             npx was not found either"
        )))
    }

    /// `GET {base}/health`. Camofox answers 2xx `{ok,browserConnected,…}`;
    /// any 2xx counts as up — `browserConnected:false` still means the REST
    /// server is live and launches the browser lazily on first tab.
    pub async fn health_probe(&self) -> bool {
        let mut req = self.probe.get(format!("{}/health", self.base_url));
        if let Some(k) = &self.api_key {
            req = req.bearer_auth(k);
        }
        matches!(req.send().await, Ok(r) if r.status().is_success())
    }

    /// No-side-effect liveness hint: does anything accept TCP connections on
    /// the endpoint's port? Deliberately NOT an HTTP probe — `/health`
    /// pre-launches the browser in camofox, so a background keep-alive that
    /// used it would pin ~0.5 GB of Firefox forever. Used by `ensure_ready`
    /// to reap an exited child without touching the browser.
    async fn tcp_alive(&self) -> bool {
        let host = parse_http_host(&self.base_url).unwrap_or_else(|| "127.0.0.1".to_string());
        let addr = format!("{host}:{}", self.port);
        matches!(
            tokio::time::timeout(TCP_PROBE_TIMEOUT, tokio::net::TcpStream::connect(&addr),).await,
            Ok(Ok(_))
        )
    }

    /// Bring the endpoint to a renderable state, spawning the server when
    /// nothing is listening. Callers pass their remaining request budget;
    /// it is clamped to `cold_start` from the constructor.
    ///
    /// Serializes on an internal mutex: concurrent callers either find the
    /// server healthy or queue behind the one spawn.
    pub async fn ensure_ready(&self, budget: Duration) -> CrwResult<()> {
        let budget = budget.min(self.cold_start);
        let mut st = self.state.lock().await;

        // Fast path: a server (ours or an adopted one) already answers.
        // Adopted children stay unmanaged — we never kill what we did not
        // start; `stop` only tears down the child we spawned.
        if self.health_probe().await {
            self.reap_if_exited_locked(&mut st);
            return Ok(());
        }

        // A child we started: if it died (crash, camofox's idle daemon exit)
        // clear it so we respawn below; if it is merely booting, do not
        // double-spawn — fall through to the poll loop below.
        self.reap_if_exited_locked(&mut st);
        let spawned_now = st.child.is_none();
        if spawned_now {
            self.spawn_locked(&mut st).await?;
        }

        let started = Instant::now();
        loop {
            if self.health_probe().await {
                if spawned_now {
                    info!(
                        base_url = %self.base_url,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "managed camofox server is up",
                    );
                }
                return Ok(());
            }
            // Our child died mid-poll: stop paying its budget and report —
            // the next call will respawn with a fresh budget.
            if st.child.is_some() && !self.tcp_alive().await {
                self.reap_if_exited_locked(&mut st);
                if st.child.is_none() {
                    return Err(CrwError::RendererError(format!(
                        "managed camofox server exited during startup (runner: {}); \
                         see log: {}",
                        self.runner_label(),
                        Self::log_path()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "~/.crw/camofox/server.log".into()),
                    )));
                }
            }
            if started.elapsed() >= budget {
                // Never leave an orphan behind a failed launch: group-kill
                // and deregister before reporting.
                self.kill_child_locked(&mut st).await;
                return Err(CrwError::RendererError(format!(
                    "managed camofox server not healthy within {}s at {} \
                     (engine download on first run can exceed this; run \
                     `crw-server setup --camofox` once, or raise the budget); \
                     see log: {}",
                    budget.as_secs(),
                    self.base_url,
                    Self::log_path()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "~/.crw/camofox/server.log".into()),
                )));
            }
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
    }

    /// Tear down a server we spawned (tests, shutdown). An adopted server is
    /// left running: its owner keeps ownership.
    pub async fn stop(&self) {
        let mut st = self.state.lock().await;
        self.kill_child_locked(&mut st).await;
    }

    fn runner_label(&self) -> String {
        match &self.runner_override {
            Some(rc) => format!("{} {:?}", rc.program, rc.args),
            None => format!("camofox-browser@{} (PATH or npx)", self.version),
        }
    }

    fn reap_if_exited_locked(&self, st: &mut State) {
        let Some(child) = st.child.as_mut() else {
            return;
        };
        let exited = match child.try_wait() {
            Ok(Some(_)) => true,
            Ok(None) => false,
            // try_wait on a reaped/foreign child: treat as gone; the group
            // (if any) is already ours to clean via the registry.
            Err(_) => true,
        };
        if exited {
            #[cfg(unix)]
            if let Some(pgid) = st.pgid.take() {
                crate::browser::deregister_pgid(pgid);
            }
            st.child = None;
        }
    }

    async fn spawn_locked(&self, st: &mut State) -> CrwResult<()> {
        let LaunchCommand { program, args } = self.resolve_runner()?;

        let log = Self::log_path();
        let (stdout, stderr) = match log.as_ref().and_then(|p| {
            std::fs::create_dir_all(p.parent()?).ok()?;
            let f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .ok()?;
            let f2 = f.try_clone().ok()?;
            Some((Stdio::from(f), Stdio::from(f2)))
        }) {
            Some(pair) => pair,
            None => (Stdio::null(), Stdio::null()),
        };

        info!(
            program = %program,
            ?args,
            port = self.port,
            "spawning managed camofox server (lazy wake)",
        );

        let mut cmd = Command::new(&program);
        cmd.args(&args)
            // Our contract, not the operator's environment: loopback-only
            // bind, auth disabled (loopback makes that safe; see camofox
            // README on CAMOFOX_AUTH_MODE), SSRF guard on, headless.
            .env("CAMOFOX_HOST", "127.0.0.1")
            .env("CAMOFOX_PORT", self.port.to_string())
            .env("CAMOFOX_AUTH_MODE", "disabled")
            .env("CAMOFOX_ALLOW_PRIVATE_NETWORK", "false")
            .env("CAMOFOX_HEADLESS", "true")
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr);
        // Own process group: killpg on this pgid reaps the Node server plus
        // every Camoufox/Firefox grandchild (see browser.rs rationale).
        #[cfg(unix)]
        cmd.process_group(0);

        let child = cmd.spawn().map_err(|e| {
            CrwError::RendererError(format!(
                "failed to spawn managed camofox via {}: {e}",
                self.runner_label()
            ))
        })?;

        st.pgid = register_child(&child);
        st.child = Some(child);
        Ok(())
    }

    async fn kill_child_locked(&self, st: &mut State) {
        let mut child_taken = st.child.take();
        let pgid = st.pgid.take();
        #[cfg(unix)]
        if let Some(pgid) = pgid {
            crate::browser::kill_pgid(pgid);
            crate::browser::deregister_pgid(pgid);
        }
        if let Some(child) = child_taken.as_mut() {
            #[cfg(not(unix))]
            {
                let _ = child.start_kill();
            }
            // Reap so we never leave a zombie; killpg already SIGKILLed the
            // tree on unix, start_kill above on other platforms.
            let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
        }
    }
}

/// Extract the port from an `http://host:port` base URL; default 9377 only
/// when the URL carries no explicit port.
fn parse_http_port(base_url: &str) -> Option<u16> {
    let rest = base_url.split("://").nth(1)?;
    let authority = rest.split('/').next()?;
    authority.rsplit(':').next()?.parse().ok()
}

/// Extract the host from an `http://host:port` base URL.
fn parse_http_host(base_url: &str) -> Option<String> {
    let rest = base_url.split("://").nth(1)?;
    let authority = rest.split('/').next()?;
    // rsplit on ':' yields the port when one is present; take everything
    // before the colon instead.
    let host = authority
        .rsplit_once(':')
        .map(|(h, p)| {
            if p.chars().all(|c| c.is_ascii_digit()) {
                h
            } else {
                authority
            }
        })
        .unwrap_or(authority);
    Some(host.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_host_and_port() {
        assert_eq!(parse_http_port("http://127.0.0.1:19377"), Some(19377));
        assert_eq!(parse_http_port("http://localhost:9377/"), Some(9377));
        assert_eq!(parse_http_port("http://no-port.example"), None);
        assert_eq!(
            parse_http_host("http://127.0.0.1:19377").as_deref(),
            Some("127.0.0.1")
        );
        assert_eq!(
            parse_http_host("http://no-port.example").as_deref(),
            Some("no-port.example")
        );
    }

    #[test]
    fn runner_resolution_precedence() {
        let sup = CamofoxSupervisor::new("http://127.0.0.1:1", None)
            .with_runner("/usr/bin/true", vec!["--x".into()]);
        assert_eq!(
            sup.resolve_runner().unwrap(),
            LaunchCommand {
                program: "/usr/bin/true".into(),
                args: vec!["--x".into()]
            }
        );
    }

    #[tokio::test]
    async fn ensure_ready_adopts_healthy_server_without_spawning() {
        // A wiremock standing in for an already-running camofox (a leftover
        // daemon or a second crw instance): ensure_ready must adopt, not
        // spawn, and leave `child` empty.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/health"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_string(r#"{"ok":true,"browserConnected":true}"#),
            )
            .mount(&server)
            .await;

        let sup = CamofoxSupervisor::new(server.uri(), None)
            // A runner that would *fail loudly* if spawned: proves adoption
            // never reaches spawn.
            .with_runner("/definitely/not/here", Vec::new());
        sup.ensure_ready(Duration::from_secs(2))
            .await
            .expect("adopted server is ready");
        let st = sup.state.lock().await;
        assert!(st.child.is_none(), "adoption must not spawn a child");
    }

    #[tokio::test]
    async fn ensure_ready_respawns_after_child_exit_and_reports_on_timeout() {
        // Nothing answers on the port; the injected runner is a long sleep,
        // so the supervisor must: spawn it, poll health, hit the budget,
        // group-kill (no orphan) and report a RendererError.
        let port = spare_port().await;
        let sup = CamofoxSupervisor::new(format!("http://127.0.0.1:{port}"), None)
            .with_runner("/bin/sh", vec!["-c".to_string(), "sleep 300".to_string()])
            .with_cold_start(Duration::from_millis(1500));

        let err = sup
            .ensure_ready(Duration::from_secs(2))
            .await
            .expect_err("no server on the port must fail");
        assert!(
            matches!(&err, CrwError::RendererError(m) if m.contains("not healthy")),
            "unexpected error: {err}"
        );
        // The budget-exhausted child was group-killed and deregistered.
        let st = sup.state.lock().await;
        assert!(
            st.child.is_none() && st.pgid.is_none(),
            "failed spawn must clean up"
        );
    }

    #[tokio::test]
    async fn ensure_ready_spawns_then_adopts_the_server_that_comes_up_mid_budget() {
        // The port answers nothing for the first seconds (listener bound but
        // not accepting; connects hang until the probe timeout), so the
        // first `/health` probes fail and the supervisor must spawn the
        // runner; then the canned responder starts accepting and the next
        // probe succeeds → Ok, with OUR child (not an adoption).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            // Longer than PROBE_TIMEOUT so at least one probe fails for sure
            // before the first connection is served out of the backlog.
            tokio::time::sleep(PROBE_TIMEOUT + Duration::from_secs(1)).await;
            while let Ok((mut sock, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let body = r#"{"ok":true,"browserConnected":false}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let sup = CamofoxSupervisor::new(format!("http://127.0.0.1:{port}"), None)
            .with_runner("/bin/sh", vec!["-c".to_string(), "sleep 300".to_string()])
            .with_cold_start(Duration::from_secs(15));
        sup.ensure_ready(Duration::from_secs(10))
            .await
            .expect("server that comes up mid-budget is adopted");
        {
            let st = sup.state.lock().await;
            assert!(
                st.child.is_some(),
                "the dark first probes must have triggered a spawn, not an adoption"
            );
        }
        sup.stop().await;
    }

    #[tokio::test]
    async fn stop_kills_the_managed_child() {
        let port = spare_port().await;
        let sup = CamofoxSupervisor::new(format!("http://127.0.0.1:{port}"), None)
            .with_runner("/bin/sh", vec!["-c".to_string(), "sleep 300".to_string()]);
        // Spawn directly (bypassing the budget loop); stop must reap.
        let mut st = sup.state.lock().await;
        sup.spawn_locked(&mut st).await.expect("spawn");
        assert!(st.child.is_some());
        let pid = st.child.as_ref().unwrap().id().unwrap() as i32;
        drop(st);

        sup.stop().await;

        // The process group must be gone: a fresh killpg finds no victims.
        #[cfg(unix)]
        {
            let rc = unsafe { libc::killpg(pid, 0) };
            assert_ne!(rc, 0, "process group survived stop()");
        }
    }

    /// Reserve a port no server is bound to, by binding and releasing it.
    async fn spare_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        port
    }
}
