# Native mode (Apple Silicon, no Docker)

The Docker Compose stack is the production contract: sidecar isolation
(`read_only`, `cap_drop ALL`, pinned images), and it runs unchanged on any
Linux server. On a single machine — a developer laptop, a work Mac — the VM
that Compose requires is pure overhead: on Apple Silicon it reserves several
GB of RAM, and worse, the published `camofox-browser` image is `linux/amd64`
only, so the stealth/search tier runs a fully **emulated x86 Firefox** inside
it (~1.1 GB measured, vs ~460 MB for the same work natively).

Native mode deletes the VM from the picture:

| Component | Native form | Obtained by |
|---|---|---|
| `crw-server` | arm64 Rust binary | `cargo build --release -p crw-server --features cdp,camofox,impersonated` |
| LightPanda (light JS tier) | `lightpanda-aarch64-macos` binary | auto-downloaded to `~/.crw/lightpanda` (or PATH) |
| Camofox (heavy/stealth tier + `/v1/search`) | `npx camofox-browser@<pin>` + the pinned Camoufox engine (`mac.arm64`) in `~/Library/Caches/camoufox` | spawned on demand by crw |
| Byparr (Cloudflare solver) | *no native build exists* | optional; keep remote — see below |

Idle footprint becomes `crw-server` (~50 MB) + LightPanda (~64 MB); Camofox
starts **only when a request first needs it** and its own idle policy closes
the browser between bursts.

## Setup

```sh
brew install node          # Node 20+ (npx ships with it)
cargo build --release -p crw-server --features cdp,camofox,impersonated
./target/release/crw-server setup --camofox   # pre-download LightPanda + Camoufox engine (~300 MB)
```

`config.local.toml` (picked up automatically):

```toml
# crw-native.example.toml — copy to config.local.toml (git-ignored) and run
# `crw-server` (or `make native-run`).
[renderer]
mode = "auto"
manage_browsers = true      # crw spawns the light tier (LightPanda) itself.
                            # The stock [renderer.lightpanda] default URL
                            # counts as "not explicitly configured": nothing
                            # listening on 9222 ⇒ managed; a live server
                            # there (or a custom ws_url) ⇒ external.

[renderer.camofox]
base_url = "http://127.0.0.1:9377"
manage = true               # crw spawns/adopts camofox-browser on demand
challenge_wait_ms = 20000
clearance_reuse = true

[search]
enabled = true              # served through the managed Camofox, no SearXNG
```

```sh
make native-run    # == cargo run --release -p crw-server --features cdp,camofox,impersonated
curl -s localhost:3000/healthz
```

## What `manage = true` actually does

* **Lazy wake.** No Camofox process exists until the first request that needs
  the stealth tier or a `/v1/search`. That request waits for readiness
  (seconds; only the very first run pays the engine download — that's what
  `setup --camofox` is for).
* **Adoption.** If something already answers `/health` on the endpoint (a
  second crw instance, a leftover daemon), it is adopted, never duplicated.
* **Respawn.** A crashed or idle-exited server is respawned by the next
  request that needs it. The breaker reports the failure like any tier
  connection error.
* **Reaping.** Spawned servers lead their own process group, registered in
  crw's browser-registry; Ctrl-C, SIGTERM, and the early-exit paths all
  group-kill the Node server *and* its Firefox grandchildren. `~/.crw/camofox/server.log`
  captures their output for debugging.
* **Loopback only.** `manage = true` requires a loopback `base_url` (the
  spawned server binds `127.0.0.1`, auth disabled — safe only on loopback).
  Anything else is rejected at boot.
* **Defaults are safe.** Both switches default `false`; container
  deployments and the published image behave exactly as before.

## Byparr

There is no native Byparr build. Three supported postures:

1. **Omit it** (default here). Camofox's own "Just a moment" wait plus the
   per-host `cf_clearance` cache still clear many Cloudflare sites; the
   ladder degrades gracefully without the solver.
2. **Remote solver**: point `[renderer.byparr].base_url` at a Byparr on a
   cheap server (never publish it publicly — it has no auth).
3. **Containerized, alone**: the Byparr image is multi-arch (native arm64),
   so running just that one service (OrbStack/colima) is cheap if you want
   the solver locally.

## Fingerprint note (measure before trusting)

Native Camofox renders on macOS: its fonts, GPU and OS claims differ from
the containerized Linux profile. Pin the generation OS to match the platform
so the fingerprint is internally consistent:

```sh
export CAMOFOX_OS=macos   # passed through to the spawned server
```

If a site behaves worse than the Docker baseline, either drop that
`[renderer.camofox]` section back to a containerized endpoint (compose
still works side-by-side) or run the whole stack in Docker for that workload.

## Comparison (measured, M-series, pinned versions)

| | Compose stack | Native |
|---|---|---|
| Host RAM reserved by container VM | 8 GiB VM (≈2.2 GiB guest-in-use when idle) | none |
| Camofox Firefox | ~1.13 GiB, emulated amd64 | ~460 MB, native arm64 |
| Byparr resident | ~456 MB | — (omit/remote) |
| Idle before first heavy request | all containers up | ~110 MB (crw + LightPanda) |
