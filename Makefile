.PHONY: hooks fmt clippy test build check sync-docs-changelog native-run native-build

# Install git pre-commit hook
hooks:
	@cp scripts/pre-commit .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed."

# Individual CI-equivalent targets
fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

build:
	cargo build --workspace --all-targets

# Run all checks (same as CI)
check: fmt-check clippy test

# Native (Docker-free) local mode: build and run the server with the
# full tier set. Requires Node 20+ on PATH for the managed camofox runner.
# See docs/docs/native-macos.md; enable manage flags in config.local.toml.
native-build:
	cargo build --release -p crw-server --features cdp,camofox,impersonated

native-run: native-build
	./target/release/crw-server

sync-docs-changelog:
	python3 scripts/sync-docs-changelog.py
