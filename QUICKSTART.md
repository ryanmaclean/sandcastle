# Quickstart

Get from `git clone` to a running agent in about five minutes.

## Prereqs

- Rust ≥ 1.85 (edition 2024). `rustup toolchain install stable`, or on
  FreeBSD `pkg install rust` (must be ≥ 1.85) — though `rustup` is
  preferred since it honours this repo's `rust-toolchain.toml` pin.
- A C toolchain for `curl-sys`'s static-OpenSSL build:
  - **Debian / Ubuntu:** `sudo apt install build-essential pkg-config perl`
  - **macOS:** `xcode-select --install`
  - **Fedora / RHEL:** `sudo dnf install gcc gcc-c++ make perl pkgconfig`
  - **FreeBSD:** `pkg install gmake perl5 ca_root_nss git`. OpenSSL's
    build needs GNU make, hence `gmake`. Install `ca_root_nss`
    **before** building: libcurl bakes a CA-bundle path in at build
    time, so a real `agent run` fails TLS verification if no CA store
    was present when you compiled (see Troubleshooting). `git` is only
    needed for the branch-strategy tools.
- An API key for either Anthropic or any OpenAI-compatible provider.

If you'd rather not install Rust, see the Docker path in the
[Install](./README.md#install) section of the README — it bundles the
build toolchain in a multi-stage image and exposes a 48 MB runtime.

## Install

```bash
git clone <this-repo>
cd <this-repo>
cargo install --locked --offline --frozen --path cli --root ~/.local
# binary lands in ~/.local/bin/agent
```

Add `~/.local/bin` to your `PATH` if it isn't already:

```bash
export PATH="$HOME/.local/bin:$PATH"
agent --version    # agent 0.0.1
```

## First prompt

```bash
export ANTHROPIC_API_KEY=sk-ant-…
agent run "Write a haiku about Rust ownership."
```

The response streams to stdout. Ctrl-C cancels mid-stream (the SIGINT
handler flips a flag the iteration loop checks each event).

Don't have an Anthropic key? Use any OpenAI-compatible endpoint:

```bash
export OPENAI_API_KEY=sk-…
agent run --openai --model gpt-4o-mini "hello"
```

## OpenRouter / LM Studio / Ollama

`agent run --openai` speaks the OpenAI Chat Completions wire format.
Point at any compatible endpoint via `OPENAI_BASE_URL`:

```bash
# OpenRouter (multi-model gateway)
export OPENAI_BASE_URL=https://openrouter.ai/api/v1
export OPENAI_API_KEY=sk-or-v1-…
agent run --openai --model anthropic/claude-sonnet-4 "hi"

# LM Studio (local, no key)
export OPENAI_BASE_URL=http://localhost:1234/v1
export OPENAI_API_KEY=lm-studio          # any non-empty string works
agent run --openai --model qwen2.5-coder "hi"

# Ollama
export OPENAI_BASE_URL=http://localhost:11434/v1
export OPENAI_API_KEY=ollama
agent run --openai --model llama3 "hi"
```

## Adding tools via MCP

Mount any [MCP](https://modelcontextprotocol.io)-spec server's tools
into the agent's tool registry by passing `--mcp 'CMD ARGS'`. The CLI
spawns the child, runs the `initialize` handshake, and registers every
advertised tool alongside the built-ins (`bash`, `read_file`,
`write_file`, `edit_file`):

```bash
agent run \
  --mcp 'npx -y @modelcontextprotocol/server-filesystem /tmp' \
  "list every file in /tmp larger than 1 MB"
```

`--mcp` is repeatable. Tool name collisions take the last spec to win.

## Persistent sessions

```bash
agent run --sessions ~/.agent/sessions "remember my favourite colour is teal"
agent run --sessions ~/.agent/sessions "what's my favourite colour?"
# → "teal"
```

Each session id (default: `default`) gets a `<key>.snapshot.json` +
`<key>.log.jsonl` pair. The log appends per turn; an automatic
snapshot rolls every 256 records or 1 MiB. To resume a named session,
pass `--session-id NAME` (see `agent run --help`).

## Running as an HTTP service

```bash
agent serve --addr 127.0.0.1:3583 --sessions ~/.agent/sessions
```

Then from another terminal:

```bash
curl -N -X POST -H 'Content-Type: application/json' \
  -d '{"text":"hello"}' \
  http://127.0.0.1:3583/agents/chat/demo
```

The response is `text/event-stream`. Each model event becomes one SSE
frame. `GET /healthz` and `/readyz` return 200 once the server is
draining-ready. Send SIGTERM to drain in-flight requests with a 30 s
deadline.

## Metrics & observability

The agent emits [DogStatsD](https://docs.datadoghq.com/developers/dogstatsd/)
over UDP — fire-and-forget, errors swallowed, never on the critical path.
It's opt-in and a no-op until you point it at a sink:

```bash
# Flag (highest priority) — applies to run and serve:
agent run  --metrics 127.0.0.1:8125 "do something"
agent serve --metrics 127.0.0.1:8125 --addr 127.0.0.1:3583

# Or via env var (used when --metrics is absent):
export DOGSTATSD_ADDR=127.0.0.1:8125
agent run "do something"
```

**Resolution order:** `--metrics <HOST:PORT>` > `DOGSTATSD_ADDR` >
disabled. When neither is set the emitter is a no-op (no socket bound,
every emit returns immediately), so leaving metrics off costs nothing.

Any DogStatsD-speaking receiver works: a Datadog Agent, or
[`statsd_exporter`](https://github.com/prometheus/statsd_exporter) to
scrape into Prometheus. Quick local sink to eyeball the wire format:

```bash
nc -u -l 8125          # or: socat -u UDP-RECV:8125 -
```

Metric names below are emitted as listed, with a
`provider:anthropic|openai` constant tag on every line. Emitted from the
Session iteration loop and per tool call:

| Metric | Type | Tags |
|---|---|---|
| `agent.prompt.started` | counter | — |
| `agent.prompt.turns` | histogram | — |
| `agent.prompt.duration_ms` | timer | `outcome` (`ok` / `model_error` / `turn_limit` / `cancelled` / `dropped`) |
| `agent.tool.calls` | counter | `name`, `outcome` (`ok` / `error` / `unknown`) |
| `agent.tool.duration_ms` | timer | `name`, `outcome` |
| `agent.audit.blocked` | counter | `name` |

`agent.prompt.duration_ms` is emitted on every exit path via an RAII
guard — success, model error, turn-limit, cancellation, even a panic
(tagged `dropped`) — so no prompt goes unmeasured.

Subagents spawned via the `task` tool inherit the parent's emitter, so
their prompt/tool/audit metrics land in the same sink with the same
metric names and provider tag. `agent doctor` prints the configured sink under
`paths:` (the `DOGSTATSD_ADDR:` line) so you can confirm wiring before a
run.

## Exposing your tools to other agents

`agent mcp-serve` makes the agent itself an MCP server over stdio.
Wire it into Claude Desktop by editing
`~/Library/Application Support/Claude/claude_desktop_config.json` (or
`%APPDATA%\Claude\claude_desktop_config.json` on Windows):

```json
{
  "mcpServers": {
    "agent": {
      "command": "agent",
      "args": ["mcp-serve"]
    }
  }
}
```

Claude Desktop now calls into `bash`, `read_file`, `write_file`, and
`edit_file` through the same audit + sandbox path the CLI uses.

## Troubleshooting

**`error: ANTHROPIC_API_KEY is not set.`** — Set the env var, or pass
`--openai` with `OPENAI_API_KEY` (or any non-empty string + a
`OPENAI_BASE_URL` pointing at a local model).

**`curl error 6: Couldn't resolve host 'api.anthropic.com'`** — DNS
failure. Check `/etc/resolv.conf`; behind a corporate proxy, set
`HTTPS_PROXY`. Note: the static-curl build does NOT consult `curl`'s
runtime CA bundle; the bundled CA store from the openssl-src build is
used.

**`curl error 60: SSL certificate problem` (often on FreeBSD)** — the
statically-linked libcurl/OpenSSL couldn't find a CA bundle. The CA
path is detected at *build* time, so the durable fix is `pkg install
ca_root_nss` **before** `cargo install`. If the binary is already
built, point OpenSSL at the system store at runtime instead:
`export SSL_CERT_FILE=/etc/ssl/cert.pem` (FreeBSD; the path
`ca_root_nss` populates). `--mock` runs make no network calls, so this
only surfaces on a real prompt.

**`session store append failed: …`** — Check that `--sessions DIR`
points at a writable path. Errors are best-effort logged; the prompt
continues.

**Slow first build** — `cargo install` rebuilds `openssl-src` + libcurl
from C source on first run (one-time, ~3 minutes). Subsequent builds
hit the cargo cache.
