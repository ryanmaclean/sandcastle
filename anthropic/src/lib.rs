//! Streaming client for the Anthropic Messages API.
//!
//! `Client::stream` issues a POST, streams the response through `wire::SseFramer`,
//! and yields parsed `Event` values via an `Iterator`. Tools are advertised by
//! raw JSON schema — the caller chose to skip serde, so we don't re-parse.
//!
//! libcurl + OpenSSL via FFI is the only network dep. No async runtime.

mod http;
pub mod json;
mod parse;

pub use http::{TlsProbe, probe_tls};

use std::sync::Arc;
use std::sync::mpsc::TryRecvError;

use wire::SseFramer;

use http::{Chunk, Stream, post_stream};

pub struct Client {
    api_key: String,
}

impl Client {
    pub fn new(api_key: String) -> Self {
        Self { api_key }
    }

    pub fn stream(&self, req: Request) -> Result<EventStream, Error> {
        let body = serialize_request(&req);
        let stream = post_stream(&self.api_key, body)?;
        Ok(EventStream { stream, framer: SseFramer::new(), done: false, terminal: None })
    }
}

pub struct Request {
    pub model: String,
    pub max_tokens: u32,
    pub messages: Vec<Message>,
    pub system: Option<String>,
    pub tools: Vec<Tool>,
}

pub struct Message {
    pub role: Role,
    pub content: Vec<ContentBlock>,
}

impl Message {
    pub fn user_text(s: impl Into<Arc<str>>) -> Self {
        Self { role: Role::User, content: vec![ContentBlock::Text(s.into())] }
    }

    pub fn assistant_text(s: impl Into<Arc<str>>) -> Self {
        Self { role: Role::Assistant, content: vec![ContentBlock::Text(s.into())] }
    }
}

/// One block in a `Message::content` list. `ToolUse::input` is a raw JSON
/// value spliced into the wire payload verbatim; the caller is responsible
/// for it being valid JSON (typically `{"command":"..."}` for the bash tool).
///
/// Payloads are `Arc<str>` so callers (the harness in particular) can clone
/// a `Vec<Message>` cheaply between turns — each block clone is an atomic
/// refcount bump rather than a deep String copy.
pub enum ContentBlock {
    Text(Arc<str>),
    ToolUse { id: Arc<str>, name: Arc<str>, input: Arc<str> },
    ToolResult { tool_use_id: Arc<str>, content: Arc<str>, is_error: bool },
}

#[derive(Clone, Copy)]
pub enum Role {
    User,
    Assistant,
}

pub struct Tool {
    pub name: String,
    pub description: String,
    pub input_schema: String,
}

#[derive(Debug, PartialEq)]
pub enum Event {
    MessageStart { id: String, model: String },
    TextDelta(String),
    ToolUseStart { id: String, name: String },
    ToolUseInputDelta(String),
    ContentBlockStop,
    MessageDelta { stop_reason: Option<String> },
    MessageStop,
    Ping,
    Other(String),
}

#[derive(Debug)]
pub enum Error {
    Http(String),
    InvalidResponse(String),
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Http(m) => write!(f, "http: {m}"),
            Error::InvalidResponse(m) => write!(f, "invalid response: {m}"),
            Error::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

pub struct EventStream {
    stream: Stream,
    framer: SseFramer,
    done: bool,
    terminal: Option<Error>,
}

impl Iterator for EventStream {
    type Item = Result<Event, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(frame) = self.framer.pop_frame() {
                return Some(parse::parse_frame(&frame));
            }
            if self.done {
                return self.terminal.take().map(Err);
            }
            match self.stream.rx.recv() {
                Ok(Chunk::Data(b)) => {
                    if let Err(e) = self.framer.push(&b) {
                        self.done = true;
                        self.terminal =
                            Some(Error::InvalidResponse(format!("frame exceeds 4 MiB cap: {e}")));
                    }
                }
                Ok(Chunk::End(Ok(_))) => {
                    self.done = true;
                }
                Ok(Chunk::End(Err(e))) => {
                    self.done = true;
                    self.terminal = Some(e);
                }
                Err(_) => {
                    self.done = true;
                }
            }
        }
    }
}

impl EventStream {
    /// Non-blocking peek. Returns `None` if there's no event yet but the
    /// stream is still open.
    pub fn try_next(&mut self) -> Option<Result<Event, Error>> {
        loop {
            if let Some(frame) = self.framer.pop_frame() {
                return Some(parse::parse_frame(&frame));
            }
            if self.done {
                return self.terminal.take().map(Err);
            }
            match self.stream.rx.try_recv() {
                Ok(Chunk::Data(b)) => {
                    if let Err(e) = self.framer.push(&b) {
                        self.done = true;
                        self.terminal =
                            Some(Error::InvalidResponse(format!("frame exceeds 4 MiB cap: {e}")));
                    }
                }
                Ok(Chunk::End(Ok(_))) => {
                    self.done = true;
                }
                Ok(Chunk::End(Err(e))) => {
                    self.done = true;
                    self.terminal = Some(e);
                }
                Err(TryRecvError::Empty) => return None,
                Err(TryRecvError::Disconnected) => {
                    self.done = true;
                }
            }
        }
    }
}

fn serialize_request(req: &Request) -> String {
    let mut s = String::with_capacity(256);
    s.push('{');
    s.push_str("\"model\":\"");
    json::escape_into(&mut s, &req.model);
    s.push_str("\",\"max_tokens\":");
    s.push_str(&req.max_tokens.to_string());
    s.push_str(",\"stream\":true,\"messages\":[");
    for (i, m) in req.messages.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str("{\"role\":\"");
        s.push_str(match m.role {
            Role::User => "user",
            Role::Assistant => "assistant",
        });
        s.push_str("\",\"content\":");
        serialize_content(&mut s, &m.content);
        s.push('}');
    }
    s.push(']');
    if let Some(sys) = &req.system {
        s.push_str(",\"system\":\"");
        json::escape_into(&mut s, sys);
        s.push('"');
    }
    if !req.tools.is_empty() {
        s.push_str(",\"tools\":[");
        for (i, t) in req.tools.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            s.push_str("{\"name\":\"");
            json::escape_into(&mut s, &t.name);
            s.push_str("\",\"description\":\"");
            json::escape_into(&mut s, &t.description);
            // input_schema is a raw JSON value, not a JSON string.
            s.push_str("\",\"input_schema\":");
            s.push_str(&t.input_schema);
            s.push('}');
        }
        s.push(']');
    }
    s.push('}');
    s
}

fn serialize_content(s: &mut String, blocks: &[ContentBlock]) {
    // Single text block stays as a plain string for readability and to match
    // the canonical request shape most users will see. Anything else goes as
    // an array of typed blocks.
    if let [ContentBlock::Text(t)] = blocks {
        s.push('"');
        json::escape_into(s, t);
        s.push('"');
        return;
    }
    s.push('[');
    for (i, b) in blocks.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        match b {
            ContentBlock::Text(t) => {
                s.push_str(r#"{"type":"text","text":""#);
                json::escape_into(s, t);
                s.push_str("\"}");
            }
            ContentBlock::ToolUse { id, name, input } => {
                s.push_str(r#"{"type":"tool_use","id":""#);
                json::escape_into(s, id);
                s.push_str(r#"","name":""#);
                json::escape_into(s, name);
                s.push_str(r#"","input":"#);
                s.push_str(input);
                s.push('}');
            }
            ContentBlock::ToolResult { tool_use_id, content, is_error } => {
                s.push_str(r#"{"type":"tool_result","tool_use_id":""#);
                json::escape_into(s, tool_use_id);
                s.push_str(r#"","content":""#);
                json::escape_into(s, content);
                s.push_str(r#"","is_error":"#);
                s.push_str(if *is_error { "true" } else { "false" });
                s.push('}');
            }
        }
    }
    s.push(']');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_minimal() {
        let req = Request {
            model: "claude-opus-4-7".into(),
            max_tokens: 64,
            messages: vec![Message::user_text("hi")],
            system: None,
            tools: vec![],
        };
        let body = serialize_request(&req);
        assert_eq!(
            body,
            r#"{"model":"claude-opus-4-7","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#
        );
    }

    #[test]
    fn serialize_with_system_and_tools() {
        let req = Request {
            model: "claude-opus-4-7".into(),
            max_tokens: 1,
            messages: vec![Message::assistant_text("ok\nthen")],
            system: Some("be terse".into()),
            tools: vec![Tool {
                name: "get_weather".into(),
                description: "weather lookup".into(),
                input_schema: r#"{"type":"object","properties":{}}"#.into(),
            }],
        };
        let body = serialize_request(&req);
        assert!(body.contains(r#""system":"be terse""#));
        assert!(body.contains(r#""role":"assistant""#));
        assert!(body.contains(r#""content":"ok\nthen""#));
        assert!(body.contains(r#""tools":[{"name":"get_weather""#));
        assert!(body.contains(r#""input_schema":{"type":"object""#));
    }

    #[test]
    fn serialize_tool_use_assistant_message() {
        let req = Request {
            model: "m".into(),
            max_tokens: 1,
            messages: vec![Message {
                role: Role::Assistant,
                content: vec![
                    ContentBlock::Text("ok let me look".into()),
                    ContentBlock::ToolUse {
                        id: "toolu_1".into(),
                        name: "bash".into(),
                        input: r#"{"command":"ls"}"#.into(),
                    },
                ],
            }],
            system: None,
            tools: vec![],
        };
        let body = serialize_request(&req);
        assert!(body.contains(r#""content":[{"type":"text","text":"ok let me look"},{"type":"tool_use","id":"toolu_1","name":"bash","input":{"command":"ls"}}]"#));
    }

    #[test]
    fn serialize_tool_result_user_message() {
        let req = Request {
            model: "m".into(),
            max_tokens: 1,
            messages: vec![Message {
                role: Role::User,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "toolu_1".into(),
                    content: "file1\nfile2\n".into(),
                    is_error: false,
                }],
            }],
            system: None,
            tools: vec![],
        };
        let body = serialize_request(&req);
        assert!(body.contains(r#""content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"file1\nfile2\n","is_error":false}]"#));
    }

    #[test]
    fn serialize_escapes_dangerous_chars() {
        let req = Request {
            model: "m".into(),
            max_tokens: 1,
            messages: vec![Message::user_text("a\"b\\c")],
            system: None,
            tools: vec![],
        };
        let body = serialize_request(&req);
        assert!(body.contains(r#""content":"a\"b\\c""#));
    }
}
