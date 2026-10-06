//! Python-exact text handling: `stage1_extract_positions.iter_san_moves` and
//! python-chess's `SAN_REGEX`.
//!
//! Exactness is the whole point, so nothing here uses Rust's notion of a digit
//! or of whitespace directly:
//!   * `str.split()` splits on Rust's `char::is_whitespace` PLUS U+001C..U+001F
//!     (Python counts the information separators as whitespace);
//!   * `re`'s `\d` is Unicode 14 category Nd, 660 code points, embedded from
//!     Python itself in `pydigits.rs`.
//! Both are checked code point by code point against fixtures generated from
//! Python (`python/gen_rust_fixtures.py`).

use crate::pydigits::PY_DIGIT_RANGES;

/// Python's `str.isspace()` / `str.split()` separator set.
#[inline]
pub fn is_py_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python `re`'s `\d` for a `str` pattern.
#[inline]
pub fn is_py_digit(c: char) -> bool {
    let cp = c as u32;
    if cp < 0x80 {
        return c.is_ascii_digit();
    }
    PY_DIGIT_RANGES
        .binary_search_by(|&(lo, hi)| {
            if hi < cp {
                std::cmp::Ordering::Less
            } else if lo > cp {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

const RESULT_TOKENS: [&str; 4] = ["1-0", "0-1", "1/2-1/2", "*"];

/// `MOVE_NUM_RE = ^\d+\.+$` on a token (which never contains a newline, so
/// Python's `$`-before-final-newline case cannot arise).
fn is_move_number(tok: &str) -> bool {
    let mut digits = 0usize;
    let mut dots = 0usize;
    for c in tok.chars() {
        if dots == 0 && is_py_digit(c) {
            digits += 1;
        } else if c == '.' && digits > 0 {
            dots += 1;
        } else {
            return false;
        }
    }
    digits > 0 && dots > 0
}

/// `re.sub(r"\{[^}]*\}", "", s)` (and the same with parentheses): a match runs
/// from an opener to the FIRST closer after it; an opener with no closer after
/// it matches nothing, and neither can any later opener.
fn strip_pairs(s: &str, open: char, close: char) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find(open) {
        match rest[i + 1..].find(close) {
            Some(j) => {
                out.push_str(&rest[..i]);
                rest = &rest[i + 1 + j + 1..];
            }
            None => break,
        }
    }
    out.push_str(rest);
    out
}

/// `re.sub(r"\$\d+", "", s)` with Python's `\d`.
fn strip_nags(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.char_indices().peekable();
    while let Some((_, c)) = it.next() {
        if c == '$' && it.peek().is_some_and(|&(_, d)| is_py_digit(d)) {
            while it.peek().is_some_and(|&(_, d)| is_py_digit(d)) {
                it.next();
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// The comment/variation/NAG passes, in Python's order, or None when the text
/// has none of `{`, `(`, `$` -- then no pass can match and the text is used as is.
pub fn clean(movetext: &str) -> Option<String> {
    if !movetext.bytes().any(|b| b == b'{' || b == b'(' || b == b'$') {
        return None;
    }
    Some(strip_nags(&strip_pairs(&strip_pairs(movetext, '{', '}'), '(', ')')))
}

/// `iter_san_moves`: tokens of `text` (already cleaned), each passed to `f`
/// until it returns false.
fn for_each_filtered(text: &str, mut f: impl FnMut(&str) -> bool) {
    for raw in text.split(is_py_space) {
        if raw.is_empty() || RESULT_TOKENS.contains(&raw) || is_move_number(raw) {
            continue;
        }
        let tok = raw.trim_end_matches(['?', '!']);
        if tok.is_empty() {
            continue;
        }
        if !f(tok) {
            return;
        }
    }
}

/// A reusable token buffer: the first `limit` tokens of a movetext, copied into
/// one string so a game costs no per-token allocation.
#[derive(Default)]
pub struct Tokens {
    text: String,
    spans: Vec<(u32, u32)>,
}

impl Tokens {
    pub fn fill(&mut self, movetext: Option<&str>, limit: usize) {
        self.text.clear();
        self.spans.clear();
        let Some(mt) = movetext else { return };
        let cleaned = clean(mt);
        let src = cleaned.as_deref().unwrap_or(mt);
        let (text, spans) = (&mut self.text, &mut self.spans);
        for_each_filtered(src, |tok| {
            if spans.len() >= limit {
                return false;
            }
            let s = text.len() as u32;
            text.push_str(tok);
            spans.push((s, text.len() as u32));
            true
        });
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    #[inline]
    pub fn get(&self, i: usize) -> &str {
        let (s, e) = self.spans[i];
        &self.text[s as usize..e as usize]
    }
}

/// Every token of a movetext, as `list(iter_san_moves(movetext))`.
pub fn all_tokens(movetext: &str) -> Vec<String> {
    let cleaned = clean(movetext);
    let mut out = Vec::new();
    for_each_filtered(cleaned.as_deref().unwrap_or(movetext), |t| {
        out.push(t.to_string());
        true
    });
    out
}

// ── SAN_REGEX ────────────────────────────────────────────────────────────────

/// The groups of
/// `^([NBKRQ])?([a-h])?([1-8])?[\-x]?([a-h][1-8])(=?[nbrqkNBRQK])?[\+#]?\Z`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SanGroups {
    pub piece: Option<u8>,
    pub file: Option<u8>,
    pub rank: Option<u8>,
    pub to_file: u8,
    pub to_rank: u8,
    /// The promotion letter, and whether `=` preceded it.
    pub promo: Option<u8>,
    pub promo_eq: bool,
}

impl SanGroups {
    /// Python's `match.groups()`, for the fuzz comparison.
    pub fn groups(&self) -> [Option<String>; 5] {
        let ch = |b: u8| (b as char).to_string();
        [
            self.piece.map(ch),
            self.file.map(ch),
            self.rank.map(ch),
            Some(format!("{}{}", self.to_file as char, self.to_rank as char)),
            self.promo
                .map(|p| format!("{}{}", if self.promo_eq { "=" } else { "" }, p as char)),
        ]
    }
}

#[inline]
fn is_file(b: u8) -> bool {
    (b'a'..=b'h').contains(&b)
}

#[inline]
fn is_rank(b: u8) -> bool {
    (b'1'..=b'8').contains(&b)
}

#[inline]
fn is_promo_letter(b: u8) -> bool {
    matches!(b, b'n' | b'b' | b'r' | b'q' | b'k' | b'N' | b'B' | b'R' | b'Q' | b'K')
}

/// `([a-h][1-8])(=?[nbrqkNBRQK])?[\+#]?\Z` from `p`. Deterministic: '=' can only
/// be consumed by the promotion group, a letter only by it, and +/# only by the
/// check group, so there is exactly one way this can match.
fn match_tail(t: &[u8], p: usize) -> Option<(u8, u8, Option<u8>, bool)> {
    if p + 2 > t.len() || !is_file(t[p]) || !is_rank(t[p + 1]) {
        return None;
    }
    let mut q = p + 2;
    let (mut promo, mut eq) = (None, false);
    if q < t.len() && t[q] == b'=' {
        if q + 1 < t.len() && is_promo_letter(t[q + 1]) {
            promo = Some(t[q + 1]);
            eq = true;
            q += 2;
        } else {
            return None;
        }
    } else if q < t.len() && is_promo_letter(t[q]) {
        promo = Some(t[q]);
        q += 1;
    }
    if q < t.len() && (t[q] == b'+' || t[q] == b'#') {
        q += 1;
    }
    (q == t.len()).then_some((t[p], t[p + 1], promo, eq))
}

/// `SAN_REGEX.match(tok)`, with the capture groups Python's backtracking engine
/// assigns. The optional file, rank and separator are greedy, so combinations
/// are tried in exactly the order a backtracking matcher tries them and the
/// first full match wins. Byte-wise is exact: every class is ASCII, so no byte
/// of a multi-byte character can match anything.
pub fn match_san(tok: &str) -> Option<SanGroups> {
    let t = tok.as_bytes();
    let n = t.len();
    // An uppercase piece letter can match nothing else in the pattern, so if the
    // piece group fails to include it the whole match fails.
    let (piece, p) = match t.first() {
        Some(&b) if matches!(b, b'N' | b'B' | b'K' | b'R' | b'Q') => (Some(b), 1),
        _ => (None, 0),
    };
    for f_inc in [true, false] {
        if f_inc && !(p < n && is_file(t[p])) {
            continue;
        }
        let p1 = p + f_inc as usize;
        for r_inc in [true, false] {
            if r_inc && !(p1 < n && is_rank(t[p1])) {
                continue;
            }
            let p2 = p1 + r_inc as usize;
            for s_inc in [true, false] {
                if s_inc && !(p2 < n && (t[p2] == b'-' || t[p2] == b'x')) {
                    continue;
                }
                let p3 = p2 + s_inc as usize;
                if let Some((to_file, to_rank, promo, promo_eq)) = match_tail(t, p3) {
                    return Some(SanGroups {
                        piece,
                        file: f_inc.then(|| t[p]),
                        rank: r_inc.then(|| t[p1]),
                        to_file,
                        to_rank,
                        promo,
                        promo_eq,
                    });
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_and_nags() {
        assert_eq!(strip_pairs("a {b} c {d", '{', '}'), "a  c {d");
        assert_eq!(strip_pairs("a {x {y} z} b", '{', '}'), "a  z} b");
        assert_eq!(strip_nags("$$1 $ $12a $\u{661}"), "$ $ a ");
        assert!(is_move_number("12...") && is_move_number("\u{661}.") && !is_move_number("1.e4"));
        assert!(!is_move_number(".") && !is_move_number("1") && !is_move_number("\u{b2}."));
    }

    #[test]
    fn regex_groups() {
        let g = match_san("e4").unwrap();
        assert_eq!((g.file, g.rank, g.to_file, g.to_rank), (None, None, b'e', b'4'));
        let g = match_san("e2e4").unwrap();
        assert_eq!((g.file, g.rank), (Some(b'e'), Some(b'2')));
        let g = match_san("Nbd7").unwrap();
        assert_eq!((g.piece, g.file, g.rank), (Some(b'N'), Some(b'b'), None));
        let g = match_san("a8=Q+").unwrap();
        assert_eq!((g.promo, g.promo_eq), (Some(b'Q'), true));
        assert!(match_san("Pe4").is_none() && match_san("e4?").is_none() && match_san("O-O").is_none());
        assert!(match_san("a8==Q").is_none() && match_san("").is_none());
    }
}
