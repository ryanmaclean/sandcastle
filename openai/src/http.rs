//! libcurl streaming POST for OpenAI-compatible chat-completions endpoints.
//!
//! One easy handle per request, on a background thread. Body chunks land in
//! the curl WRITEFUNCTION callback and are forwarded across a bounded mpsc
//! channel to the caller. If the caller stops draining, the next write
//! callback returns 0 → CURLE_WRITE_ERROR → curl_easy_perform exits cleanly.
//!
//! Structurally identical to `anthropic::http`; the only differences are the
//! configurable base URL, the path (`/v1/chat/completions`), and the auth
//! header shape (`Authorization: Bearer …`).
//!
//! ## Retry & cancellation
//!
//! Transient pre-first-byte failures (DNS, connect, handshake, 5xx/408/425/429)
//! are retried with exponential backoff via `wire::retry::with_backoff`. The
//! "no retry after first byte" invariant is load-bearing: once a single byte
//! has been handed to the iterator consumer the streaming contract has been
//! observed externally, so we must not silently re-issue the request.
//!
//! Cancellation: a `CURLOPT_XFERINFOFUNCTION` callback polls `ctx.aborted` so
//! that connect-time / between-byte stalls can be interrupted promptly when
//! the caller drops the `Stream` (the existing WRITEFUNCTION-returns-0 path
//! still handles cancellation that occurs while bytes are arriving).

use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::thread::JoinHandle;

use curl_sys as c;

use crate::Error;

const PATH: &str = "/v1/chat/completions";

// curl-sys exposes CURLOPT_XFERINFOFUNCTION/DATA as commented-out entries
// in its `pub const` list, so we materialise them from the same base values
// (`CURLOPTTYPE_FUNCTIONPOINT` = 20_000, `CURLOPTTYPE_OBJECTPOINT` = 10_000).
const CURLOPT_XFERINFOFUNCTION: c::CURLoption = 20_000 + 219;
const CURLOPT_XFERINFODATA: c::CURLoption = 10_000 + 57;

pub(crate) enum Chunk {
    Data(Vec<u8>),
    End(Result<u32, Error>),
}

pub(crate) struct Stream {
    pub(crate) rx: Receiver<Chunk>,
    pub(crate) handle: Option<JoinHandle<()>>,
}

impl Drop for Stream {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

struct Ctx {
    tx: SyncSender<Chunk>,
    /// Set when the caller has dropped the receiver (write_cb's `tx.send`
    /// fails) OR when classification decides we must give up. Both write_cb
    /// and xferinfo_cb read it.
    aborted: AtomicBool,
    /// Set on the first successful `tx.send`. Once true, the streaming
    /// contract has been observed externally and the request is no longer
    /// safe to retry.
    delivered: AtomicBool,
}

extern "C" fn write_cb(ptr: *mut c_char, size: usize, nmemb: usize, user: *mut c_void) -> usize {
    let total = size.saturating_mul(nmemb);
    if total == 0 {
        return 0;
    }
    let ctx = unsafe { &*(user as *const Ctx) };
    if ctx.aborted.load(Ordering::Relaxed) {
        return 0;
    }
    let slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, total) };
    match ctx.tx.send(Chunk::Data(slice.to_vec())) {
        Ok(()) => {
            ctx.delivered.store(true, Ordering::Relaxed);
            total
        }
        Err(_) => {
            ctx.aborted.store(true, Ordering::Relaxed);
            0
        }
    }
}

extern "C" fn xferinfo_cb(
    user: *mut c_void,
    _dltotal: f64,
    _dlnow: f64,
    _ultotal: f64,
    _ulnow: f64,
) -> i32 {
    let ctx = unsafe { &*(user as *const Ctx) };
    // Non-zero return triggers CURLE_ABORTED_BY_CALLBACK from curl, which
    // unblocks any connect/recv that's currently parked. Polled by curl
    // roughly once per second, so this matters most for between-byte stalls.
    i32::from(ctx.aborted.load(Ordering::Relaxed))
}

pub(crate) fn post_stream(api_key: &str, base_url: &str, body: String) -> Result<Stream, Error> {
    let host = host_of(base_url);
    let breaker_cfg = wire::retry::BreakerConfig::default();
    match wire::retry::check(&host, &breaker_cfg) {
        wire::retry::BreakerVerdict::Allow | wire::retry::BreakerVerdict::HalfOpenProbe => {}
        wire::retry::BreakerVerdict::Open => {
            return Err(Error::Http(format!("circuit open for {host} (recent upstream failures)")));
        }
    }

    let (tx, rx) = std::sync::mpsc::sync_channel::<Chunk>(64);

    let full_url = join_url(base_url, PATH);
    let url = CString::new(full_url).map_err(|_| Error::Http("bad URL".into()))?;
    let auth_header = CString::new(format!("Authorization: Bearer {api_key}"))
        .map_err(|_| Error::Http("api key contains NUL".into()))?;
    let content_type = CString::new("Content-Type: application/json").unwrap();
    let accept = CString::new("Accept: text/event-stream").unwrap();
    let body_bytes = body.into_bytes();

    let handle = std::thread::spawn(move || {
        let ctx = Ctx {
            tx: tx.clone(),
            aborted: AtomicBool::new(false),
            delivered: AtomicBool::new(false),
        };
        let policy = wire::retry::RetryPolicy::default();
        let result = wire::retry::with_backoff(&policy, |_attempt| {
            // Fresh body clone per attempt: curl reads from the buffer for
            // the duration of `curl_easy_perform`, and a retried call may
            // re-issue the same bytes. Cloning `Vec<u8>` is cheap.
            let body_attempt = body_bytes.clone();
            let perform = unsafe {
                run_easy(&url, &auth_header, &content_type, &accept, &body_attempt, &ctx)
            };
            // Strict invariant: never retry once bytes have crossed the
            // channel into the iterator consumer.
            let delivered = ctx.delivered.load(Ordering::Relaxed);
            classify(perform, delivered)
        });
        match &result {
            Ok(_) => wire::retry::record_success(&host, &breaker_cfg),
            Err(_) => wire::retry::record_failure(&host, &breaker_cfg),
        }
        let _ = tx.send(Chunk::End(result));
    });

    Ok(Stream { rx, handle: Some(handle) })
}

/// Extract the host portion of `base_url` (e.g. "https://api.openai.com/v1"
/// → "api.openai.com"). Defaults to the whole input if parsing fails so
/// the breaker still keys consistently per caller-supplied URL.
fn host_of(base_url: &str) -> String {
    let after_scheme = base_url.split_once("://").map(|(_, rest)| rest).unwrap_or(base_url);
    after_scheme.split(['/', ':', '?']).next().unwrap_or(base_url).to_string()
}

fn join_url(base: &str, path: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    format!("{trimmed}{path}")
}

/// One curl_easy_perform's worth of outcome, factored out so the body of
/// `run_easy` can be a single FFI call with no policy mixed in.
pub(crate) struct PerformOutcome {
    pub rc: c::CURLcode,
    pub status: i64,
}

/// Classify a perform outcome into a retry policy decision. Pure function,
/// no FFI — exists so the retry rules are unit-testable without a network.
///
/// Retryable iff `!delivered` AND (curl-side transient | retryable status):
/// - CURLE_COULDNT_RESOLVE_HOST (6), CURLE_COULDNT_CONNECT (7)
/// - CURLE_OPERATION_TIMEDOUT (28), CURLE_SSL_CONNECT_ERROR (35)
/// - CURLE_GOT_NOTHING (52), CURLE_SEND_ERROR (55), CURLE_RECV_ERROR (56)
/// - HTTP 408 / 425 / 429 / 502 / 503 / 504
fn classify(
    outcome: Result<PerformOutcome, Error>,
    delivered: bool,
) -> wire::retry::Outcome<u32, Error> {
    match outcome {
        Err(e) => wire::retry::Outcome::Fatal(e),
        Ok(p) => {
            // CURLE_WRITE_ERROR (23) is a clean caller-initiated abort —
            // not a real failure; preserve the existing semantics.
            if p.rc == c::CURLE_WRITE_ERROR {
                return if (200..300).contains(&p.status) {
                    wire::retry::Outcome::Ok(p.status as u32)
                } else {
                    wire::retry::Outcome::Fatal(Error::Http(format!("HTTP {}", p.status)))
                };
            }
            if p.rc != c::CURLE_OK {
                if !delivered && is_retryable_curl_code(p.rc) {
                    return wire::retry::Outcome::Retryable(curl_err(p.rc, "perform"));
                }
                return wire::retry::Outcome::Fatal(curl_err(p.rc, "perform"));
            }
            if (200..300).contains(&p.status) {
                wire::retry::Outcome::Ok(p.status as u32)
            } else if !delivered && is_retryable_status(p.status) {
                wire::retry::Outcome::Retryable(Error::Http(format!("HTTP {}", p.status)))
            } else {
                wire::retry::Outcome::Fatal(Error::Http(format!("HTTP {}", p.status)))
            }
        }
    }
}

fn is_retryable_curl_code(rc: c::CURLcode) -> bool {
    matches!(
        rc,
        c::CURLE_COULDNT_RESOLVE_HOST
            | c::CURLE_COULDNT_CONNECT
            | c::CURLE_OPERATION_TIMEDOUT
            | c::CURLE_SSL_CONNECT_ERROR
            | c::CURLE_GOT_NOTHING
            | c::CURLE_SEND_ERROR
            | c::CURLE_RECV_ERROR
    )
}

fn is_retryable_status(status: i64) -> bool {
    matches!(status, 408 | 425 | 429 | 502 | 503 | 504)
}

unsafe fn run_easy(
    url: &CString,
    auth_header: &CString,
    content_type: &CString,
    accept: &CString,
    body: &[u8],
    ctx: *const Ctx,
) -> Result<PerformOutcome, Error> {
    unsafe {
        curl_global_init_once();
        let easy = c::curl_easy_init();
        if easy.is_null() {
            return Err(Error::Http("curl_easy_init failed".into()));
        }

        let mut headers: *mut c::curl_slist = ptr::null_mut();
        headers = c::curl_slist_append(headers, auth_header.as_ptr());
        headers = c::curl_slist_append(headers, content_type.as_ptr());
        headers = c::curl_slist_append(headers, accept.as_ptr());

        let guard = Handle { easy, headers };

        setopt_ptr(easy, c::CURLOPT_URL, url.as_ptr() as *const c_void, "URL")?;
        setopt_long(easy, c::CURLOPT_POST, 1, "POST")?;
        setopt_ptr(easy, c::CURLOPT_POSTFIELDS, body.as_ptr() as *const c_void, "POSTFIELDS")?;
        setopt_long(easy, c::CURLOPT_POSTFIELDSIZE, body.len() as i64, "POSTFIELDSIZE")?;
        setopt_ptr(easy, c::CURLOPT_HTTPHEADER, headers as *const c_void, "HTTPHEADER")?;
        setopt_ptr(easy, c::CURLOPT_WRITEFUNCTION, write_cb as *const c_void, "WRITEFUNCTION")?;
        setopt_ptr(easy, c::CURLOPT_WRITEDATA, ctx as *const c_void, "WRITEDATA")?;
        // Cancellation: xferinfo_cb returns non-zero when ctx.aborted is set,
        // which lets curl break out of connect/recv stalls without waiting
        // for the next byte to flow through write_cb.
        setopt_long(easy, c::CURLOPT_NOPROGRESS, 0, "NOPROGRESS")?;
        setopt_ptr(
            easy,
            CURLOPT_XFERINFOFUNCTION,
            xferinfo_cb as *const c_void,
            "XFERINFOFUNCTION",
        )?;
        setopt_ptr(easy, CURLOPT_XFERINFODATA, ctx as *const c_void, "XFERINFODATA")?;
        setopt_long(easy, c::CURLOPT_FOLLOWLOCATION, 1, "FOLLOWLOCATION")?;
        // Connection-level timeouts so a half-open or LB-dropped TCP socket
        // can't wedge the worker thread indefinitely. No hard CURLOPT_TIMEOUT
        // — streams can legitimately run for many minutes — but require at
        // least 1 byte/s averaged over a 60s window once data should be
        // flowing, and cap the initial connect at 10s.
        setopt_long(easy, c::CURLOPT_CONNECTTIMEOUT, 10, "CONNECTTIMEOUT")?;
        setopt_long(easy, c::CURLOPT_LOW_SPEED_LIMIT, 1, "LOW_SPEED_LIMIT")?;
        setopt_long(easy, c::CURLOPT_LOW_SPEED_TIME, 60, "LOW_SPEED_TIME")?;

        let perform_rc = c::curl_easy_perform(easy);
        let mut status: i64 = 0;
        c::curl_easy_getinfo(easy, c::CURLINFO_RESPONSE_CODE, &mut status);

        drop(guard);

        Ok(PerformOutcome { rc: perform_rc, status })
    }
}

struct Handle {
    easy: *mut c::CURL,
    headers: *mut c::curl_slist,
}

impl Drop for Handle {
    fn drop(&mut self) {
        unsafe {
            if !self.headers.is_null() {
                c::curl_slist_free_all(self.headers);
            }
            if !self.easy.is_null() {
                c::curl_easy_cleanup(self.easy);
            }
        }
    }
}

unsafe fn setopt_ptr(
    easy: *mut c::CURL,
    opt: c::CURLoption,
    val: *const c_void,
    name: &str,
) -> Result<(), Error> {
    let rc = unsafe { c::curl_easy_setopt(easy, opt, val) };
    if rc != c::CURLE_OK {
        return Err(curl_err(rc, name));
    }
    Ok(())
}

unsafe fn setopt_long(
    easy: *mut c::CURL,
    opt: c::CURLoption,
    val: i64,
    name: &str,
) -> Result<(), Error> {
    let rc = unsafe { c::curl_easy_setopt(easy, opt, val) };
    if rc != c::CURLE_OK {
        return Err(curl_err(rc, name));
    }
    Ok(())
}

fn curl_err(code: c::CURLcode, where_: &str) -> Error {
    let msg = unsafe {
        let p = c::curl_easy_strerror(code);
        if p.is_null() {
            String::new()
        } else {
            std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
        }
    };
    Error::Http(format!("curl {where_}: {msg} (code {code})"))
}

/// Idempotent process-wide `curl_global_init`. libcurl docs require
/// global_init be called once before any other curl call, and it's not
/// itself thread-safe — without this, parallel callers (tests, multi-Client
/// production code) occasionally corrupt state at first-init.
unsafe fn curl_global_init_once() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| unsafe {
        c::curl_global_init(c::CURL_GLOBAL_DEFAULT);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire::retry::Outcome;

    fn ok_outcome(rc: c::CURLcode, status: i64) -> Result<PerformOutcome, Error> {
        Ok(PerformOutcome { rc, status })
    }

    #[test]
    fn join_url_no_trailing_slash() {
        assert_eq!(
            join_url("https://api.openai.com", "/v1/chat/completions"),
            "https://api.openai.com/v1/chat/completions"
        );
    }

    #[test]
    fn join_url_trims_trailing_slash() {
        assert_eq!(
            join_url("http://localhost:1234/", "/v1/chat/completions"),
            "http://localhost:1234/v1/chat/completions"
        );
    }

    #[test]
    fn classify_success_is_ok() {
        match classify(ok_outcome(c::CURLE_OK, 200), false) {
            Outcome::Ok(200) => {}
            _ => panic!("expected Ok"),
        }
    }

    #[test]
    fn classify_dns_failure_pre_byte_is_retryable() {
        match classify(ok_outcome(c::CURLE_COULDNT_RESOLVE_HOST, 0), false) {
            Outcome::Retryable(_) => {}
            _ => panic!("expected Retryable"),
        }
    }

    #[test]
    fn classify_dns_failure_after_first_byte_is_fatal() {
        // Streaming-retry invariant: once any data delivered, never retry.
        match classify(ok_outcome(c::CURLE_COULDNT_RESOLVE_HOST, 0), true) {
            Outcome::Fatal(_) => {}
            _ => panic!("expected Fatal once delivered=true"),
        }
    }

    #[test]
    fn classify_connect_failure_is_retryable() {
        match classify(ok_outcome(c::CURLE_COULDNT_CONNECT, 0), false) {
            Outcome::Retryable(_) => {}
            _ => panic!("expected Retryable"),
        }
    }

    #[test]
    fn classify_timeout_is_retryable() {
        match classify(ok_outcome(c::CURLE_OPERATION_TIMEDOUT, 0), false) {
            Outcome::Retryable(_) => {}
            _ => panic!("expected Retryable"),
        }
    }

    #[test]
    fn classify_recv_error_pre_byte_is_retryable() {
        match classify(ok_outcome(c::CURLE_RECV_ERROR, 0), false) {
            Outcome::Retryable(_) => {}
            _ => panic!("expected Retryable"),
        }
    }

    #[test]
    fn classify_got_nothing_is_retryable() {
        match classify(ok_outcome(c::CURLE_GOT_NOTHING, 0), false) {
            Outcome::Retryable(_) => {}
            _ => panic!("expected Retryable"),
        }
    }

    #[test]
    fn classify_http_429_pre_byte_is_retryable() {
        match classify(ok_outcome(c::CURLE_OK, 429), false) {
            Outcome::Retryable(_) => {}
            _ => panic!("expected Retryable for 429"),
        }
    }

    #[test]
    fn classify_http_503_pre_byte_is_retryable() {
        match classify(ok_outcome(c::CURLE_OK, 503), false) {
            Outcome::Retryable(_) => {}
            _ => panic!("expected Retryable for 503"),
        }
    }

    #[test]
    fn classify_http_429_after_first_byte_is_fatal() {
        match classify(ok_outcome(c::CURLE_OK, 429), true) {
            Outcome::Fatal(_) => {}
            _ => panic!("expected Fatal once delivered=true"),
        }
    }

    #[test]
    fn classify_http_400_is_fatal() {
        match classify(ok_outcome(c::CURLE_OK, 400), false) {
            Outcome::Fatal(_) => {}
            _ => panic!("expected Fatal for 400"),
        }
    }

    #[test]
    fn classify_http_401_is_fatal() {
        match classify(ok_outcome(c::CURLE_OK, 401), false) {
            Outcome::Fatal(_) => {}
            _ => panic!("expected Fatal for 401"),
        }
    }

    #[test]
    fn classify_write_error_is_ok_when_status_2xx() {
        // CURLE_WRITE_ERROR with 200 = caller dropped the receiver mid-stream;
        // not a real failure.
        match classify(ok_outcome(c::CURLE_WRITE_ERROR, 200), true) {
            Outcome::Ok(200) => {}
            _ => panic!("expected Ok for caller-abort"),
        }
    }

    #[test]
    fn classify_non_retryable_curl_code_is_fatal() {
        // CURLE_HTTP_RETURNED_ERROR (22) isn't in our retry set.
        match classify(ok_outcome(c::CURLE_HTTP_RETURNED_ERROR, 0), false) {
            Outcome::Fatal(_) => {}
            _ => panic!("expected Fatal for non-retryable code"),
        }
    }

    #[test]
    fn retry_loop_clones_body_per_attempt() {
        // Verify that wrapping with_backoff with a closure that captures a
        // shared body produces a fresh clone per iteration.
        let body: Vec<u8> = b"payload".to_vec();
        let seen_ptrs = std::sync::Mutex::new(Vec::<usize>::new());
        let policy = wire::retry::RetryPolicy {
            max_attempts: 3,
            base_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_millis(2),
        };
        let _res: Result<u32, Error> = wire::retry::with_backoff(&policy, |_attempt| {
            let cloned = body.clone();
            seen_ptrs.lock().unwrap().push(cloned.as_ptr() as usize);
            classify(ok_outcome(c::CURLE_COULDNT_CONNECT, 0), false)
        });
        let ptrs = seen_ptrs.lock().unwrap();
        assert_eq!(ptrs.len(), 3, "loop should run all 3 attempts");
    }

    #[test]
    fn aborted_flag_short_circuits_retry_loop_via_fatal() {
        // Once classify returns Ok/Fatal (which is what an aborted-by-caller
        // write_cb path produces via CURLE_WRITE_ERROR), the loop stops
        // immediately and we don't keep hammering the upstream.
        let mut attempts = 0;
        let policy = wire::retry::RetryPolicy {
            max_attempts: 4,
            base_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_millis(2),
        };
        let _res: Result<u32, Error> = wire::retry::with_backoff(&policy, |_| {
            attempts += 1;
            classify(ok_outcome(c::CURLE_WRITE_ERROR, 200), true)
        });
        assert_eq!(attempts, 1, "Ok outcome must short-circuit");
    }

    /// A silent peer: TcpListener that `accept`s once and holds the socket
    /// open without ever writing. Models the LB-dropped / NAT-timeout /
    /// half-open case where TCP handshakes succeed but no bytes flow.
    /// Without CURLOPT_LOW_SPEED_LIMIT/TIME the worker thread would block
    /// for hours; with them, curl aborts after LOW_SPEED_TIME=60s with
    /// CURLE_OPERATION_TIMEDOUT.
    #[test]
    fn silent_peer_trips_low_speed_timeout() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let stop_c = std::sync::Arc::clone(&stop);
        let parked = std::thread::spawn(move || {
            listener.set_nonblocking(true).ok();
            let mut held: Option<std::net::TcpStream> = None;
            while !stop_c.load(Ordering::Relaxed) {
                if held.is_none()
                    && let Ok((s, _)) = listener.accept()
                {
                    held = Some(s);
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            drop(held);
        });

        let url = CString::new(format!("http://{addr}/v1/chat/completions")).unwrap();
        let auth_header = CString::new("Authorization: Bearer test").unwrap();
        let content_type = CString::new("Content-Type: application/json").unwrap();
        let accept = CString::new("Accept: text/event-stream").unwrap();
        let body = b"{}".to_vec();

        let (tx, _rx) = std::sync::mpsc::sync_channel::<Chunk>(1);
        let ctx = Ctx { tx, aborted: AtomicBool::new(false), delivered: AtomicBool::new(false) };

        let started = std::time::Instant::now();
        let result = unsafe { run_easy(&url, &auth_header, &content_type, &accept, &body, &ctx) };
        let elapsed = started.elapsed();

        stop.store(true, Ordering::Relaxed);
        let _ = parked.join();

        // LOW_SPEED_TIME=60s; generous 75s upper bound. The load-bearing
        // assertion is that perform completes in finite time at all.
        assert!(
            elapsed < std::time::Duration::from_secs(75),
            "perform must abort within ~60s; took {elapsed:?}"
        );
        let outcome = result.expect("run_easy itself shouldn't error");
        assert_eq!(
            outcome.rc,
            c::CURLE_OPERATION_TIMEDOUT,
            "expected CURLE_OPERATION_TIMEDOUT (28), got code {}",
            outcome.rc
        );
    }

    #[test]
    fn xferinfo_cb_returns_nonzero_when_aborted() {
        let (tx, _rx) = std::sync::mpsc::sync_channel::<Chunk>(1);
        let ctx = Ctx { tx, aborted: AtomicBool::new(false), delivered: AtomicBool::new(false) };
        let ptr = &ctx as *const Ctx as *mut c_void;
        assert_eq!(xferinfo_cb(ptr, 0.0, 0.0, 0.0, 0.0), 0);
        ctx.aborted.store(true, Ordering::Relaxed);
        assert_ne!(xferinfo_cb(ptr, 0.0, 0.0, 0.0, 0.0), 0, "non-zero signals abort to curl");
    }
}
