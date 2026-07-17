//! Tiny JSON: just the slice of the spec the Messages API uses.
//!
//! Two halves:
//!   - `escape_into`: writes a JSON string body (no surrounding quotes) into
//!     a `String`, escaping `"`, `\`, control chars per RFC 8259.
//!   - `Json` + `parse`: a value tree with by-key lookup. Numbers are kept
//!     as their source bytes — we only need string/bool/null discrimination.
//!
//! The Messages API responses we parse are small (one SSE frame); recursive
//! descent is fine because `MAX_DEPTH` caps nesting, so a hostile or
//! malformed frame errors cleanly instead of overflowing the stack.

use crate::Error;

pub fn escape_into(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(kv) => kv.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }
}

/// Max nesting depth for `[`/`{`. Recursive descent turns each level of
/// nesting into a stack frame, so an unbounded depth lets a single frame
/// (up to the 4 MiB `SseFramer` cap) of `[[[[…` overflow the stack and
/// abort the process — an uncatchable DoS reachable from any untrusted
/// JSON source (model API, OpenAI-compatible endpoint, MCP peer). The
/// Messages API nests only a handful deep; 128 is far above real payloads
/// and far below the frame budget that would overflow.
const MAX_DEPTH: usize = 128;

pub fn parse(input: &[u8]) -> Result<Json, Error> {
    let mut p = Parser { bytes: input, pos: 0 };
    p.skip_ws();
    let v = p.value(0)?;
    p.skip_ws();
    if p.pos != p.bytes.len() {
        return Err(Error::InvalidResponse(format!("trailing bytes at {}", p.pos)));
    }
    Ok(v)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Some(b)
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            if matches!(b, b' ' | b'\t' | b'\n' | b'\r') {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn expect(&mut self, b: u8) -> Result<(), Error> {
        match self.bump() {
            Some(x) if x == b => Ok(()),
            other => Err(Error::InvalidResponse(format!(
                "expected {:?} at {}, got {:?}",
                b as char, self.pos, other
            ))),
        }
    }

    fn keyword(&mut self, word: &[u8]) -> Result<(), Error> {
        if self.bytes.get(self.pos..self.pos + word.len()) == Some(word) {
            self.pos += word.len();
            Ok(())
        } else {
            Err(Error::InvalidResponse(format!("expected keyword at {}", self.pos)))
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json, Error> {
        if depth > MAX_DEPTH {
            return Err(Error::InvalidResponse(format!(
                "nesting exceeds max depth {MAX_DEPTH} at {}",
                self.pos
            )));
        }
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => self.string().map(Json::Str),
            Some(b't') => {
                self.keyword(b"true")?;
                Ok(Json::Bool(true))
            }
            Some(b'f') => {
                self.keyword(b"false")?;
                Ok(Json::Bool(false))
            }
            Some(b'n') => {
                self.keyword(b"null")?;
                Ok(Json::Null)
            }
            Some(b) if b == b'-' || b.is_ascii_digit() => self.number(),
            other => Err(Error::InvalidResponse(format!("unexpected {:?} at {}", other, self.pos))),
        }
    }

    fn object(&mut self, depth: usize) -> Result<Json, Error> {
        self.expect(b'{')?;
        let mut out = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Json::Obj(out));
        }
        loop {
            self.skip_ws();
            let key = self.string()?;
            self.skip_ws();
            self.expect(b':')?;
            let v = self.value(depth + 1)?;
            out.push((key, v));
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b'}') => return Ok(Json::Obj(out)),
                other => {
                    return Err(Error::InvalidResponse(format!(
                        "expected ',' or '}}' at {}, got {:?}",
                        self.pos, other
                    )));
                }
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<Json, Error> {
        self.expect(b'[')?;
        let mut out = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Json::Arr(out));
        }
        loop {
            let v = self.value(depth + 1)?;
            out.push(v);
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b']') => return Ok(Json::Arr(out)),
                other => {
                    return Err(Error::InvalidResponse(format!(
                        "expected ',' or ']' at {}, got {:?}",
                        self.pos, other
                    )));
                }
            }
        }
    }

    fn string(&mut self) -> Result<String, Error> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            match self.bump() {
                Some(b'"') => return Ok(out),
                Some(b'\\') => match self.bump() {
                    Some(b'"') => out.push('"'),
                    Some(b'\\') => out.push('\\'),
                    Some(b'/') => out.push('/'),
                    Some(b'n') => out.push('\n'),
                    Some(b'r') => out.push('\r'),
                    Some(b't') => out.push('\t'),
                    Some(b'b') => out.push('\x08'),
                    Some(b'f') => out.push('\x0c'),
                    Some(b'u') => {
                        let cp = self.hex4()?;
                        if (0xD800..=0xDBFF).contains(&cp) {
                            if self.bump() != Some(b'\\') || self.bump() != Some(b'u') {
                                return Err(Error::InvalidResponse("lone high surrogate".into()));
                            }
                            let lo = self.hex4()?;
                            if !(0xDC00..=0xDFFF).contains(&lo) {
                                return Err(Error::InvalidResponse("invalid low surrogate".into()));
                            }
                            let c = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                            match char::from_u32(c) {
                                Some(ch) => out.push(ch),
                                None => {
                                    return Err(Error::InvalidResponse(
                                        "invalid surrogate pair".into(),
                                    ));
                                }
                            }
                        } else {
                            match char::from_u32(cp) {
                                Some(ch) => out.push(ch),
                                None => {
                                    return Err(Error::InvalidResponse(
                                        "invalid \\u escape".into(),
                                    ));
                                }
                            }
                        }
                    }
                    other => {
                        return Err(Error::InvalidResponse(format!("bad escape {:?}", other)));
                    }
                },
                Some(b) => {
                    // Multi-byte UTF-8: accumulate raw bytes and let
                    // String::from_utf8 below catch invalid sequences.
                    if b < 0x20 {
                        return Err(Error::InvalidResponse("control char in string".into()));
                    }
                    let start = self.pos - 1;
                    let mut end = self.pos;
                    while let Some(nb) = self.peek() {
                        if nb == b'"' || nb == b'\\' || nb < 0x20 {
                            break;
                        }
                        end += 1;
                        self.pos += 1;
                    }
                    match std::str::from_utf8(&self.bytes[start..end]) {
                        Ok(s) => out.push_str(s),
                        Err(_) => {
                            return Err(Error::InvalidResponse("invalid utf-8".into()));
                        }
                    }
                }
                None => {
                    return Err(Error::InvalidResponse("unterminated string".into()));
                }
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, Error> {
        let mut v = 0u32;
        for _ in 0..4 {
            let b = self.bump().ok_or_else(|| Error::InvalidResponse("short \\u escape".into()))?;
            let d = match b {
                b'0'..=b'9' => (b - b'0') as u32,
                b'a'..=b'f' => (b - b'a' + 10) as u32,
                b'A'..=b'F' => (b - b'A' + 10) as u32,
                _ => return Err(Error::InvalidResponse("bad hex".into())),
            };
            v = (v << 4) | d;
        }
        Ok(v)
    }

    fn number(&mut self) -> Result<Json, Error> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while let Some(b) = self.peek() {
            if b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-') {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(Error::InvalidResponse("empty number".into()));
        }
        Ok(Json::Num(
            std::str::from_utf8(&self.bytes[start..self.pos])
                .map_err(|_| Error::InvalidResponse("bad number utf-8".into()))?
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_basic() {
        let mut s = String::new();
        escape_into(&mut s, "hi \"world\"\n");
        assert_eq!(s, r#"hi \"world\"\n"#);
    }

    #[test]
    fn escape_control() {
        let mut s = String::new();
        escape_into(&mut s, "\x01");
        assert_eq!(s, "\\u0001");
    }

    #[test]
    fn parse_string() {
        let v = parse(br#""hello""#).unwrap();
        assert_eq!(v, Json::Str("hello".into()));
    }

    #[test]
    fn parse_obj_nested() {
        let v = parse(br#"{"a":{"b":"c"},"n":42}"#).unwrap();
        assert_eq!(v.get("a").unwrap().get("b").unwrap().as_str(), Some("c"));
        assert!(matches!(v.get("n"), Some(Json::Num(_))));
    }

    #[test]
    fn parse_null_and_bool() {
        let v = parse(br#"{"a":null,"b":true,"c":false}"#).unwrap();
        assert_eq!(v.get("a"), Some(&Json::Null));
        assert_eq!(v.get("b"), Some(&Json::Bool(true)));
        assert_eq!(v.get("c"), Some(&Json::Bool(false)));
    }

    #[test]
    fn parse_string_escapes() {
        let v = parse(br#""a\nb\"c\\d""#).unwrap();
        assert_eq!(v.as_str(), Some("a\nb\"c\\d"));
    }

    #[test]
    fn parse_unicode_escape() {
        let v = parse(b"\"\\u00e9\"").unwrap();
        assert_eq!(v.as_str(), Some("\u{00e9}"));
    }

    #[test]
    fn parse_surrogate_pair() {
        let v = parse(b"\"\\uD83D\\uDE00\"").unwrap();
        assert_eq!(v.as_str(), Some("\u{1F600}"));
    }

    #[test]
    fn parse_raw_utf8_string() {
        let v = parse("\"é😀\"".as_bytes()).unwrap();
        assert_eq!(v.as_str(), Some("é😀"));
    }

    #[test]
    fn parse_array() {
        let v = parse(br#"[1,"two",null]"#).unwrap();
        if let Json::Arr(items) = v {
            assert_eq!(items.len(), 3);
        } else {
            panic!("not array");
        }
    }

    #[test]
    fn parse_rejects_trailing_garbage() {
        assert!(parse(br#"{}garbage"#).is_err());
    }

    #[test]
    fn parse_rejects_unterminated_string() {
        assert!(parse(br#""hello"#).is_err());
    }

    #[test]
    fn parse_rejects_deeply_nested_without_overflow() {
        // A single frame of ~200k `[` used to overflow the stack and abort
        // the process (uncatchable DoS). It must now be a clean error.
        let deep = vec![b'['; 200_000];
        assert!(parse(&deep).is_err());
        // Objects too.
        let mut obj = Vec::new();
        for _ in 0..200_000 {
            obj.extend_from_slice(br#"{"a":"#);
        }
        assert!(parse(&obj).is_err());
    }

    #[test]
    fn parse_allows_nesting_up_to_the_cap() {
        // Real payloads nest only a handful deep; a modest depth still parses.
        let depth = 64;
        let mut s = Vec::new();
        s.extend(std::iter::repeat_n(b'[', depth));
        s.push(b'0');
        s.extend(std::iter::repeat_n(b']', depth));
        assert!(parse(&s).is_ok());
    }
}
