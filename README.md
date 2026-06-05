# moth

Experimental Rust core for a minimal agent harness. Distils ideas from
the upstream TypeScript library it descended from (sandbox provider ×
branch strategy, two-phase prompt, completion signal, structured
output) and from Flue (instance/harness/session hierarchy, virtual
sandbox default, skills + roles, declarative triggers) into a build
with a small dependency footprint.

## Quickstart

New here? [`QUICKSTART.md`](./QUICKSTART.md) walks `git clone` to first
prompt in about five minutes — install, provider switch, MCP, sessions,
`agent serve`, and Claude Desktop wiring. Runnable shell examples live
under [`examples/`](./examples/).

## Install

**Cargo (from source).** Requires Rust ≥ 1.85 and a C toolchain
(`build-essential` on Debian, `xcode-select --install` on macOS,
`pkg install gmake perl5` on FreeBSD):

```bash
cargo install --locked --offline --frozen --path cli --root ~/.local
# binary at ~/.local/bin/agent
```

The vendored tree at `vendor/` is the build's only source of
dependencies — `--offline --frozen --locked` together guarantee no
network fetches and pin every transitive crate to the lockfile.

**Docker.** Multi-stage build, distroless runtime, 48 MB final image:

```bash
docker build -t agent .
docker run --rm -e ANTHROPIC_API_KEY agent run "hello"

# Mount a workdir if the prompt should see local files:
docker run --rm -e ANTHROPIC_API_KEY \
  -v "$PWD:/work" -w /work \
  agent run --sessions /work/.agent "summarise README.md"
```

**Release tarballs.** Planned. None published yet — build from source
for now.

## Platforms

Linux (x86_64, aarch64) and macOS (x86_64, aarch64) are the primary
targets and run in CI. **FreeBSD** (x86_64) builds and runs natively —
`pkg install gmake perl5 ca_root_nss` for the vendored OpenSSL/libcurl C
build (`gmake` because OpenSSL needs GNU make; `ca_root_nss` *before*
building so libcurl bakes in a CA-bundle path — otherwise real API
calls fail TLS verification, fixable at runtime with
`SSL_CERT_FILE=/etc/ssl/cert.pem`), then the usual `cargo install`. A
`freebsd` CI job builds + tests the binary inside a native FreeBSD VM
(the vendored OpenSSL C sources don't cross-compile from Linux, so BSD
is built natively rather than cross-targeted). NetBSD / OpenBSD aren't
tested but the code is `std`-and-`libc`-only with no Linux-specific
syscalls, so they're expected to work with the same build
prerequisites.

## Status

Production-grade. 19 crates, 592 inline tests across the workspace;
one direct external dependency (`curl-sys`), all transitive deps fully
vendored. Streaming + cancellation through the Session iteration loop;
runlog audit trail; auto-compaction hook between turns; HTTP server
hardened + streaming SSE; sessions persist; MCP client (stdio + HTTP)
*and* MCP server (`agent mcp-serve`); fstools symlink + hard-link safe;
git branch strategies; Flue-style `subagent::spawn_task` with an
LLM-callable `task` tool; Aho-Corasick `audit` scanner; microbench
suite with measured numbers; cross-crate integration suite (16 default
+ 3 gated real-network).

## Layout

| Crate | Purpose | Direct deps |
|---|---|---|
| `actor/` | OS-thread actor primitive (`Send` / `ask` / `stopped`). | — |
| `wire/` | SIMD byte/pair scanners + `SseFramer` + `NdjsonSplitter` + `find_tag`. NEON (aarch64) + AVX2 (x86_64, runtime-detected) + scalar. | — |
| `vshell/` | In-process POSIX shell subset. Quoting, `$VAR`/`${VAR}`, `$(cmd)`, pipelines, redirects, sequencing, env prefix, built-ins. | — |
| `anthropic/` | Streaming Messages API client. libcurl + OpenSSL via `curl-sys`. Hand-rolled JSON. Tool-use / tool-result content blocks. | `wire`, `curl-sys` |
| `openai/` | Streaming `/v1/chat/completions` client for any OpenAI-compatible endpoint (OpenAI, OpenRouter, LM Studio, Ollama). | `wire`, `curl-sys` |
| `audit/` | Literal-pattern shell scanner. Blocks shai-hulud-class payloads. | `wire` |
| `fstools/` | `read_file` / `write_file` / `edit_file` tools. Per-component canonicalisation + leaf `symlink_metadata` check makes them symlink-safe when a `root` is set. | `harness`, `anthropic` |
| `tmpl/` | `{{KEY}}` substitution + `.agents/skills/<name>.md` + `.agents/roles/<name>.md` markdown loading. | `wire` |
| `server/` | Hand-rolled HTTP/1.1 + SSE. Read/write timeouts, configurable connection cap (atomic counter), HTTP/1.1 keep-alive on non-streaming responses. | — |
| `mcp/` | Model Context Protocol client + server. Client: stdio + streamable-HTTP transports, each remote tool implements `harness::Tool`. Server (`mcp::server::Server`): exposes a local `harness::Tool` registry over stdio JSON-RPC. | `actor`, `harness`, `wire`, `anthropic`, `curl-sys` |
| `git/` | Branch strategies for code-editing agents: `HeadStrategy` (works in repo_root, refuses dirty tree), `MergeToHeadStrategy` (temp worktree + merge back on success, leave on failure), `Branch{name}` (named persistent branch). Shells out to `git(1)`. | — |
| `integration/` | Cross-crate scenario tests. 12 whole-stack tests covering tool routing, audit blocking, fstools sandboxing, session persistence, MCP wiring, completion signal, turn cap, structured output extraction. | every crate it tests |
| `benches/` | Microbenchmarks for the SIMD/parsing hot paths. `std::time::Instant`-based, no criterion. `cargo test -p benches --release -- --nocapture`. | `wire`, `audit`, `anthropic`, `vshell` |
| `runlog/` | File-backed JSONL audit trail. Subscribes to `harness::StreamEvent` and writes one record per event. Atomic appends, mutex-guarded; ms-precision UNIX timestamps. | `harness`, `anthropic` |
| `metrics/` | DogStatsD-over-UDP emitter. Load-bearing observability: counters/gauges/histograms/timers for prompt outcome, tool calls, and audit blocks, wired through `HarnessState` across `run`/`serve` and inherited by subagents. Opt-in via `--metrics`/`DOGSTATSD_ADDR`; a zero-cost no-op when disabled. | — |
| `compact/` | Message-history compaction. Pure layer (`estimate_chars`, `split_for_compaction`) + model-driven `Compactor` that replaces older turns with a single synthetic summary message. Wires into `HarnessState::with_compactor`. | `harness` |
| `subagent/` | Flue-style `session.task()`: spawn a focused child agent that shares the parent's sandbox + tools but starts with empty history. Optional role overlay. Streaming variant available. | `harness`, `actor` |
| `persist/` | File-backed `SessionStore`. Atomic writes via tmp + rename. Version-tagged JSON; key validation rejects path-traversal. | `harness`, `anthropic` |
| `harness/` | Instance + Session actors. `Model` / `Sandbox` / `Tool` / `SessionStore` traits. `AnthropicModel`, `OpenAiModel`, `AuditedShell<S>`, `BashTool`. Iteration loop with tool-use, completion signal, structured output, 16-turn cap, optional persistence hook. | `actor`, `wire`, `anthropic`, `openai`, `audit`, `vshell` |
| `cli/` | `agent run` / `agent serve` binary. Wires every other crate. | all of the above |

## Dependencies & supply chain

One direct external dep: `curl-sys 0.4` with `static-curl` + `static-ssl`
features. `cargo vendor` was run at the workspace root; the resulting
`vendor/` tree (12 crates including OpenSSL + libcurl source for static
builds) is committed. `.cargo/config.toml` redirects crates.io to
`vendored-sources`. Builds with `--locked --frozen --offline` succeed.

`.gitignore` is anchored so `target/` only matches Cargo build
dirs, not `vendor/**/target/` source subdirectories (cc 1.2 reorganised
its layout — the unanchored rule used to swallow real source files).

## Run

```bash
# All tests + lints
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --target aarch64-unknown-linux-gnu

# One-shot agent
ANTHROPIC_API_KEY=... cargo run --bin agent -- run "list /tmp"

# OpenAI-compatible (with OPENAI_BASE_URL pointed at OpenRouter,
# LM Studio, Ollama, etc.)
OPENAI_API_KEY=... cargo run --bin agent -- run --openai "hello"

# Skill-driven prompt (loads .agents/skills/triage.md, substitutes {{KEY}}s)
cargo run --bin agent -- run --skill triage --arg issue_number=42

# Persist conversations across runs
cargo run --bin agent -- run --sessions ./.sessions "first turn"
cargo run --bin agent -- run --sessions ./.sessions "second turn — remembers the first"

# Register external MCP tools (e.g. an MCP-spec'd filesystem server)
cargo run --bin agent -- run --mcp 'npx -y @modelcontextprotocol/server-filesystem /tmp' "list /tmp"

# Tee every StreamEvent to a JSONL log for audit / debugging
cargo run --bin agent -- run --runlog ./.runs "do something"

# Emit DogStatsD metrics over UDP (run + serve); also via DOGSTATSD_ADDR.
# Opt-in, no-op when unset. See QUICKSTART.md "Metrics & observability".
cargo run --bin agent -- run --metrics 127.0.0.1:8125 "do something"

# Let the LLM spawn subagents via a 'task' tool
cargo run --bin agent -- run --task-tool "research three approaches and summarise"

# Compact history older than 50k chars before each model call
cargo run --bin agent -- run --compact-budget 50000 "long conversation continues..."

# Expose our tools as an MCP server over stdio (other agents can call us)
cargo run --bin agent -- mcp-serve

# HTTP server: POST /agents/chat/<id>, SSE response (now actually streams),
# sessions persist by id
cargo run --bin agent -- serve --addr 0.0.0.0:3583 --sessions ./.sessions
```

## Measured numbers

From `cargo test -p benches --release -- --nocapture` on x86_64 + AVX2:

```
wire::scan_for_byte 4 KiB (absent):       59 ns/op   68.9 GB/s
wire::scan_for_byte 64 KiB (absent):    1032 ns/op   63.5 GB/s
wire::scan_for_pair 4 KiB (absent):      146 ns/op   28.0 GB/s
wire::find_tag 4 KiB (tag at end):       100 ns/op   40.7 GB/s
audit::Scanner::scan benign:              25 ns/op  (was 133 — Aho-Corasick win)
audit::Scanner::scan malicious:           94 ns/op  (was 448)
audit::Scanner::scan 1 KiB no patterns: 2359 ns/op  (was 5501)
anthropic::json::parse SSE event:        835 ns/op
vshell::execute("echo $X"):              566 ns/op
```

SIMD byte scan saturates L1 bandwidth (~65 GB/s). Audit replaced its
per-pattern scan with a flat Aho-Corasick automaton; one walk now
finds every match regardless of pattern count.

## What's left

(Nothing actively deferred. The crate set covers the workflows we set
out to support — runtime, models, tools, sandbox, audit, VCS branch
strategy, persistence, MCP, HTTP service, CLI. Future work is feature
growth rather than gap-closing.)
