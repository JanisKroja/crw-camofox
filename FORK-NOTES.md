# Fork notes

This is a private working fork of [adambenhassen/crw-camofox](https://github.com/adambenhassen/crw-camofox)
(which itself is a camofox-first fork of [us/crw](https://github.com/fastcrw/crw)), maintained for
use as the search/scrape backend behind the DeepSeek Harness `web_search` tool
(via `dsh-web-search-crw`). Licensed **AGPL-3.0** — the license and all upstream
copyright notices are retained.

## Local changes

### `fix(search)`: English-pinned Google SERP + chrome-proof extraction

`crates/crw-search/src/camofox_search.rs`:

- **Locale pinned.** Google navigates
  `https://www.google.com/search?q=<q>&hl=en&gl=us` directly instead of the
  `@google_search` macro. Without this the SERP follows the exit-IP locale, so
  the JS extractor scraped localized section headings as result titles and the
  request-level `lang` param (SearXNG-only) could not correct it. If the direct
  URL ever renders zero rows (consent wall on a fresh profile), the search
  retries via the original macro body (`google_macro_body`), preserving the
  battle-tested consent dance.
- **Chrome-proof title extraction.** The title may only come from an `h3`
  *inside* the organic anchor; Google's AI-mode teaser heading sits loose in
  the same `div.g` container and previously paired onto the next row's URL
  (e.g. "AI Mode replied:" on a deepseek.com result). Anchors pointing back at
  Google's own `/search`, `/set/…`, `/async`, `/gen_204` are skipped.
- **Chrome row filter.** `is_google_chrome_link()` drops any surviving row
  whose URL is a Google-internal UI link; organic `/url` + `/goto` wrapper
  links are untouched.

Tests: `cargo test -p crw-search` (105 pass, incl. new cases for the navigate
bodies and the chrome predicate); `cargo fmt --check` and
`cargo clippy -p crw-search --all-features` clean.

## Building / running locally

The compose stack normally uses the published image; this fork runs the local
patch. Copy [`docker-compose.override.example.yml`](docker-compose.override.example.yml)
to `docker-compose.override.yml` (auto-loaded by compose; the plain filename is
git-ignored upstream) and:

```sh
docker compose build crw && docker compose up -d
```

## Tracking upstream

The upstream clone remotes are kept but not named `origin` (so nothing
accidentally pushes there):

| Remote | Repo | Use |
|---|---|---|
| `camofox-fork` | `github.com/adambenhassen/crw-camofox` | the fork this was cloned from |
| `crw-upstream` | `github.com/fastcrw/crw` | the root upstream |

To pull new changes: `git fetch camofox-fork && git merge camofox-fork/feat/camofox-renderer`,
then re-apply/rebase the local commits above (expect a conflict in
`camofox_search.rs` if upstream touches the SERP extractor).

## License / attribution

- AGPL-3.0 license text (`LICENSE`) retained unmodified, as required.
- Upstream copyright and attribution retained in `LICENSE`, `README.md`, and source headers.
- Copyright (c) 2026 Jānis Kroja for the local changes described above.
