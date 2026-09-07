use std::fmt::Write as _;

const UNTRUSTED_BEGIN: &str = "----- BEGIN UNTRUSTED CONTENT -----";
const UNTRUSTED_END: &str = "----- END UNTRUSTED CONTENT -----";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rendered {
    pub text: String,
    pub non_ascii: bool,
    pub clamped: bool,
}

/// Escape terminal-active Unicode before displaying peer- or worker-authored text.
///
/// `max_chars` counts input Unicode scalar values. Flags describe the complete input, including
/// text beyond the clamp, so a trusted caller can always surface the reason for truncation.
pub fn escape_untrusted(s: &str, max_chars: usize) -> Rendered {
    let mut text = String::with_capacity(s.len().min(max_chars));
    let mut non_ascii = false;
    let mut clamped = false;

    for (index, ch) in s.chars().enumerate() {
        non_ascii |= !ch.is_ascii();
        if index >= max_chars {
            clamped = true;
            continue;
        }
        if ch.is_control() || is_format_control(ch) {
            push_unicode_escape(&mut text, ch);
        } else {
            text.push(ch);
        }
    }

    Rendered {
        text,
        non_ascii,
        clamped,
    }
}

/// Wrap already-escaped content in markers emitted by a2a rather than by the untrusted source.
pub fn fence_untrusted(text: &str) -> String {
    let needs_newline = !text.is_empty() && !text.ends_with('\n');
    let mut fenced = String::with_capacity(
        UNTRUSTED_BEGIN.len() + text.len() + UNTRUSTED_END.len() + 3,
    );
    fenced.push_str(UNTRUSTED_BEGIN);
    fenced.push('\n');
    fenced.push_str(text);
    if needs_newline {
        fenced.push('\n');
    }
    fenced.push_str(UNTRUSTED_END);
    fenced
}

fn push_unicode_escape(out: &mut String, ch: char) {
    let codepoint = ch as u32;
    if codepoint <= 0xffff {
        write!(out, "\\u{codepoint:04X}").expect("writing to a String cannot fail");
        return;
    }

    let astral = codepoint - 0x1_0000;
    let high = 0xd800 + (astral >> 10);
    let low = 0xdc00 + (astral & 0x3ff);
    write!(out, "\\u{high:04X}\\u{low:04X}").expect("writing to a String cannot fail");
}

fn is_format_control(ch: char) -> bool {
    matches!(
        ch as u32,
        0x00ad
            | 0x0600..=0x0605
            | 0x061c
            | 0x06dd
            | 0x070f
            | 0x0890..=0x0891
            | 0x08e2
            | 0x180e
            | 0x200b..=0x200f
            | 0x202a..=0x202e
            | 0x2060..=0x2064
            | 0x2066..=0x206f
            | 0xfeff
            | 0xfff9..=0xfffb
            | 0x110bd
            | 0x110cd
            | 0x13430..=0x1343f
            | 0x1bca0..=0x1bca3
            | 0x1d173..=0x1d17a
            | 0xe0001
            | 0xe0020..=0xe007f
    )
}
