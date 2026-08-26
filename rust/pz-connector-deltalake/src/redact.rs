//! Secret hygiene for wrapped delta-rs/object_store error strings: those can embed a table's own
//! URI complete with a presigned query string (`X-Amz-Signature=...`, SAS tokens) or a bare
//! `key=value`/`key: value` credential the object-store client logged verbatim. Every error this
//! connector hands back to the host goes through [`redact`] first -- the same duty
//! `PzConnectorException`'s doc places on every connector author, just carried out here instead of
//! by the host, since the host cannot redact bytes a subprocess already decided to send.
//!
//! Two independent passes, deliberately conservative (over-redacting a harmless value is a rounding
//! error; leaking one secret is not):
//!   1. Strip every URL query string down to `?<redacted>` -- a presigned S3/Azure URL puts its
//!      entire signature there, not in a recognizable key name.
//!   2. Blank the value half of any `key=value`/`key: value` pair whose key looks credential-shaped
//!      (secret, token, password, key, credential, authorization, sas, sig...), for the plain
//!      environment-variable-style strings (`AWS_SECRET_ACCESS_KEY=...`) object_store's own error
//!      messages sometimes include outside of a URL.

/// Case-insensitive key fragments that mark a `key=value`/`key: value` pair as secret-shaped. Matched
/// against the key with ASCII-lowercasing, not a full Unicode fold -- every real-world instance of
/// these (AWS/Azure env var names, HTTP headers) is ASCII.
const SECRET_KEY_FRAGMENTS: &[&str] = &[
    "secret",
    "password",
    "passwd",
    "token",
    "credential",
    "authorization",
    "sas",
    "sig",
    "apikey",
    "api_key",
    "access_key",
];

/// Redacts a delta-rs/object_store error message before it ever becomes a [`crate::PzError`] the
/// host logs, persists in `plan.json`, or renders in a run event -- see the module doc.
pub fn redact(message: &str) -> String {
    let after_query_strip = strip_query_strings(message);
    strip_secret_pairs(&after_query_strip)
}

/// Replaces every `?...` run (up to the next whitespace/quote/bracket/paren) with `?<redacted>`.
/// Deliberately blunt: a `?` that is not actually a URL query string (rare in an error message) just
/// loses a few harmless characters, which costs nothing next to leaking a real signature.
fn strip_query_strings(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut chars = message.char_indices().peekable();
    while let Some((_, c)) = chars.next() {
        if c != '?' {
            out.push(c);
            continue;
        }

        out.push('?');
        out.push_str("<redacted>");
        while let Some(&(_, next)) = chars.peek() {
            if is_query_terminator(next) {
                break;
            }
            chars.next();
        }
    }
    out
}

fn is_query_terminator(c: char) -> bool {
    c.is_whitespace() || matches!(c, '"' | '\'' | ')' | ']' | '>' | ',')
}

/// Replaces the value half of any `key=value` or `key: value` pair whose key contains one of
/// [`SECRET_KEY_FRAGMENTS`] (case-insensitively) with `<redacted>`. A "value" runs to the next
/// whitespace/quote/bracket/paren/comma, matching [`strip_query_strings`]'s own terminator rule so
/// the two passes read consistently.
fn strip_secret_pairs(message: &str) -> String {
    let bytes = message.as_bytes();
    let mut out = String::with_capacity(message.len());
    let mut i = 0;
    while i < bytes.len() {
        let c = message[i..].chars().next().unwrap();
        let key_start = i;
        if is_key_char(c) {
            let mut j = i;
            while j < bytes.len() && is_key_char(message[j..].chars().next().unwrap()) {
                j += message[j..].chars().next().unwrap().len_utf8();
            }

            let key = &message[key_start..j];
            let mut k = j;
            while k < bytes.len() && (bytes[k] == b' ' || bytes[k] == b'\t') {
                k += 1;
            }

            if k < bytes.len() && (bytes[k] == b'=' || bytes[k] == b':') && looks_secret(key) {
                k += 1;
                while k < bytes.len() && (bytes[k] == b' ' || bytes[k] == b'\t') {
                    k += 1;
                }

                out.push_str(key);
                out.push_str(&message[j..k]); // whitespace + separator, verbatim
                out.push_str("<redacted>");

                while k < bytes.len() && !is_query_terminator(message[k..].chars().next().unwrap())
                {
                    k += message[k..].chars().next().unwrap().len_utf8();
                }

                i = k;
                continue;
            }

            out.push_str(key);
            i = j;
            continue;
        }

        out.push(c);
        i += c.len_utf8();
    }
    out
}

fn is_key_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn looks_secret(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    SECRET_KEY_FRAGMENTS.iter().any(|frag| lower.contains(frag))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_a_presigned_url_query_string() {
        let msg = "object store request failed: GET https://bucket.s3.amazonaws.com/root/_delta_log/00000000000000000000.json?X-Amz-Signature=abcdef1234&X-Amz-Credential=AKIA_EXAMPLE%2F20260824 returned 403";
        let redacted = redact(msg);

        assert!(
            !redacted.contains("X-Amz-Signature=abcdef1234"),
            "signature leaked: {redacted}"
        );
        assert!(
            !redacted.contains("AKIA_EXAMPLE"),
            "credential leaked: {redacted}"
        );
        assert!(
            redacted.contains("https://bucket.s3.amazonaws.com/root/_delta_log/00000000000000000000.json?<redacted>"),
            "path was not preserved: {redacted}"
        );
        assert!(
            redacted.contains("returned 403"),
            "trailing context lost: {redacted}"
        );
    }

    #[test]
    fn redacts_a_bare_secret_key_value_pair() {
        let msg = "storage backend rejected the request: AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY was invalid";
        let redacted = redact(msg);

        assert!(
            !redacted.contains("wJalrXUtnFEMI"),
            "secret leaked: {redacted}"
        );
        assert!(redacted.contains("AWS_SECRET_ACCESS_KEY=<redacted>"));
        assert!(redacted.contains("was invalid"));
    }

    #[test]
    fn leaves_a_message_with_no_secrets_untouched() {
        let msg = "table not found at the given location";
        assert_eq!(redact(msg), msg);
    }

    #[test]
    fn does_not_touch_ordinary_key_value_pairs() {
        let msg = "partition_by=region,year is not a valid column list";
        assert_eq!(redact(msg), msg);
    }
}
