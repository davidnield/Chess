//! Port of the pinned fen2cdb encoding (SSDB BSD-3-Clause / ChessDB Unlicense).
//! See the crate's THIRD_PARTY-fen2cdb.txt. Archive queries are standard-chess FENs;
//! preserve the caller's four position fields, including pseudo-legal EP spelling.
use anyhow::{ensure, Result};

pub fn normalize(fen: &str) -> Result<String> {
    ensure!(fen.is_ascii(), "non-ASCII FEN");
    let f: Vec<_> = fen.split_whitespace().collect();
    ensure!(f.len() == 4 || f.len() == 6, "FEN needs four or six fields");
    ensure!(f[1] == "w" || f[1] == "b", "invalid side to move");
    let ranks: Vec<_> = f[0].split('/').collect();
    ensure!(ranks.len() == 8, "FEN needs eight ranks");
    for rank in ranks {
        let mut squares = 0;
        for c in rank.bytes() {
            if (b'1'..=b'8').contains(&c) {
                // Historical source keys preserve adjacent empty-square runs.
                // Recanonicalizing them would query a different binary key.
                squares += (c - b'0') as usize;
            } else {
                ensure!(b"pnbrqkPNBRQK".contains(&c), "invalid piece");
                squares += 1;
            }
        }
        ensure!(squares == 8, "invalid rank width");
    }
    ensure!(
        f[2] == "-" || (!f[2].is_empty() && f[2].bytes().all(|c| b"KQkq".contains(&c))),
        "only standard-chess castling fields are supported"
    );
    if f[2] != "-" {
        let mut c = f[2].as_bytes().to_vec();
        c.sort_unstable();
        c.dedup();
        ensure!(c.len() == f[2].len(), "duplicate castling right");
    }
    let ep = f[3].as_bytes();
    ensure!(
        f[3] == "-"
            || (ep.len() == 2
                && (b'a'..=b'h').contains(&ep[0])
                && (ep[1] == b'3' || ep[1] == b'6')),
        "invalid en passant square"
    );
    if f.len() == 6 {
        f[4].parse::<u32>()?;
        ensure!(f[5].parse::<u32>()? > 0, "invalid fullmove number");
    }
    Ok(f[..4].join(" "))
}

pub fn mirror(fen: &str) -> Result<String> {
    let fen = normalize(fen)?;
    let f: Vec<_> = fen.split(' ').collect();
    let board = f[0]
        .split('/')
        .rev()
        .map(|r| {
            r.chars()
                .map(|c| {
                    if c.is_ascii_uppercase() {
                        c.to_ascii_lowercase()
                    } else {
                        c.to_ascii_uppercase()
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("/");
    let mut castle = String::new();
    for c in f[2].chars().filter(|c| !c.is_ascii_uppercase()) {
        castle.push(c.to_ascii_uppercase());
    }
    for c in f[2].chars().filter(|c| c.is_ascii_uppercase()) {
        castle.push(c.to_ascii_lowercase());
    }
    let ep = if f[3] == "-" {
        "-".to_string()
    } else {
        format!(
            "{}{}",
            f[3].as_bytes()[0] as char,
            (b'9' - f[3].as_bytes()[1] + b'0') as char
        )
    };
    Ok(format!(
        "{} {} {} {}",
        board,
        if f[1] == "w" { "b" } else { "w" },
        castle,
        ep
    ))
}

fn encoded(fen: &str) -> Vec<u8> {
    let f: Vec<_> = fen.split(' ').collect();
    let mut n = Vec::<u8>::new();
    for c in f[0].bytes().filter(|c| *c != b'/') {
        if (b'1'..=b'3').contains(&c) {
            n.push(c - b'1');
        } else if (b'4'..=b'8').contains(&c) {
            n.extend([8, c - b'4']);
        } else {
            n.push(match c {
                b'p' => 3,
                b'n' => 4,
                b'b' => 5,
                b'r' => 6,
                b'q' => 7,
                b'k' => 9,
                b'P' => 10,
                b'N' => 11,
                b'B' => 12,
                b'R' => 13,
                b'Q' => 14,
                b'K' => 15,
                _ => unreachable!(),
            });
        }
    }
    n.push(if f[1] == "w" { 0 } else { 1 });
    for c in f[2].bytes() {
        n.push(match c {
            b'-' => 0,
            b'K' => 10,
            b'Q' => 11,
            b'k' => 12,
            b'q' => 13,
            _ => unreachable!(),
        });
    }
    n.push(9);
    if f[3] == "-" {
        n.push(0);
    } else {
        n.extend([f[3].as_bytes()[0] - b'a' + 1, f[3].as_bytes()[1] - b'0']);
    }
    if n.len() % 2 == 1 {
        if n.last() == Some(&0) {
            n.pop();
        } else {
            n.push(0);
        }
    }
    let mut out = vec![b'h'];
    out.extend(n.chunks_exact(2).map(|p| (p[0] << 4) | p[1]));
    out
}

pub fn key(fen: &str) -> Result<(String, Vec<u8>, bool)> {
    let fen = normalize(fen)?;
    let other = mirror(&fen)?;
    let direct = encoded(&fen);
    let inverse = encoded(&other);
    let mirrored = direct >= inverse;
    Ok((fen, if mirrored { inverse } else { direct }, mirrored))
}

pub fn mirror_move(uci: &str) -> Result<String> {
    let mut b = uci.as_bytes().to_vec();
    ensure!(b.len() == 4 || b.len() == 5, "invalid archive move length");
    ensure!(
        (b'a'..=b'h').contains(&b[0]) && (b'a'..=b'h').contains(&b[2]),
        "invalid move file"
    );
    for i in [1, 3] {
        ensure!((b'1'..=b'8').contains(&b[i]), "invalid move rank");
        b[i] = b'9' - b[i] + b'0';
    }
    if b.len() == 5 {
        ensure!(b"qrbn".contains(&b[4]), "invalid promotion");
    }
    Ok(String::from_utf8(b)?)
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn unhex(s: &str) -> Result<Vec<u8>> {
    ensure!(s.len() % 2 == 0 && s.is_ascii(), "invalid hex key");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(Into::into))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_mirror() {
        for f in [
            "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -",
            "4k3/8/8/3pP3/8/8/8/4K3 w - d6",
        ] {
            assert_eq!(mirror(&mirror(f).unwrap()).unwrap(), f);
            let a = key(f).unwrap();
            let b = key(&mirror(f).unwrap()).unwrap();
            assert_eq!(a.1, b.1);
            assert_ne!(a.2, b.2);
            assert_eq!(key(&format!("{f} 0 1")).unwrap().1, a.1);
        }
        assert_eq!(mirror_move("b7a8n").unwrap(), "b2a1n");
    }
    #[test]
    fn invalid_is_error_not_missing() {
        for f in [
            "x",
            "8/8 w - -",
            "8/8/8/8/8/8/8/8 x - -",
            "8/8/8/8/8/8/8/8 w - z9",
            "8/8/8/8/8/8/8/8 w KK -",
        ] {
            assert!(key(f).is_err());
        }
    }
    #[test]
    fn encode_expected() {
        let (_, k, _) = key("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -").unwrap();
        assert_eq!(hex(&k), "68645795463333333384848484aaaaaaaadbcefcbd0abcd9");
    }
}
