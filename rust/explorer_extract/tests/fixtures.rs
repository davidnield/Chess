//! `cargo test`: every fixture python/gen_rust_fixtures.py writes.

use std::path::PathBuf;

use serde_json::Value;
use shakmaty::{Chess, Position};

use explorer_extract::chesspos::{hash, pack, shakmaty_epd};
use explorer_extract::pyre::match_san;
use explorer_extract::san::{parse_san, Parsed};
use explorer_extract::selftest;

fn fixture(name: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

fn ok(r: selftest::Check) {
    match r {
        Ok(s) => println!("{s}"),
        Err(e) => panic!("{e}"),
    }
}

#[test]
fn polyglot_keys_and_start() {
    ok(selftest::check_keys(&fixture("polyglot_keys.json")));
}

#[test]
fn unicode_whitespace_and_digits() {
    ok(selftest::check_unicode(&fixture("pyunicode.json")));
}

#[test]
fn san_table() {
    ok(selftest::check_san_table(&fixture("san_table.json")));
}

#[test]
fn pinned_en_passant() {
    ok(selftest::check_pinned_ep());
}

#[test]
fn castling_cleanup() {
    // Kg1 / Kc1 / Kg8 / Kc8 never castle: the regex path drops Move::Castle.
    let v: Value = serde_json::from_str(&fixture("san_table.json")).unwrap();
    let mut seen = 0;
    for c in v["cases"].as_array().unwrap() {
        let san = c["san"].as_str().unwrap();
        if ["Kg1", "Kc1", "Kg8", "Kc8"].contains(&san) {
            assert_eq!(c["expect"]["ok"], false, "{san} must fail in the fixture");
            let pos = selftest::setup(c["fen"].as_str().unwrap(), c["moves"].as_array().unwrap()).unwrap();
            assert!(parse_san(&pos, san).is_err(), "{san} parsed in {}", c["label"]);
            seen += 1;
        }
    }
    assert!(seen >= 4, "only {seen} castling-cleanup cases");
}

#[test]
fn tokenizer() {
    ok(selftest::check_tokenizer(&fixture("tokenizer.json")));
}

#[test]
fn mini_extract() {
    ok(selftest::check_mini(&fixture("mini_extract.json")));
}

/// Real games: every ply's parent hash and EPD, and parse_san's verdict on
/// each token variant, against python-chess.
#[test]
fn san_real_games() {
    let v: Value = serde_json::from_str(&fixture("san_games.json")).unwrap();
    let (mut plies, mut variants) = (0, 0);
    for g in v["games"].as_array().unwrap() {
        let mut pos = Chess::default();
        for p in g["plies"].as_array().unwrap() {
            assert_eq!(hash(&pos), p["parent_hash"].as_i64().unwrap(), "{} ply {plies}", g["source"]);
            let e = pack(&pos).render();
            assert_eq!(e, p["parent_epd"].as_str().unwrap());
            assert_eq!(e, shakmaty_epd(&pos));
            for (tok, want) in p["variants"].as_object().unwrap() {
                selftest::verdict_matches(&pos, tok, want, false)
                    .unwrap_or_else(|e| panic!("{}: {e}", g["source"]));
                variants += 1;
            }
            match parse_san(&pos, p["token"].as_str().unwrap()).unwrap() {
                Parsed::Move(m) => pos.play_unchecked(m),
                Parsed::Null => pos = pos.swap_turn().unwrap(),
            }
            plies += 1;
        }
        assert_eq!(hash(&pos), g["final_hash"].as_i64().unwrap());
        assert_eq!(pack(&pos).render(), g["final_epd"].as_str().unwrap());
    }
    println!("{plies} real plies, {variants} token variants");
    assert!(plies > 1000 && variants > 5000);
}

// ── the SAN_REGEX fuzz ───────────────────────────────────────────────────────

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

struct Fuzz {
    alpha: Vec<char>,
    pieces: Vec<char>,
    files: Vec<char>,
    ranks: Vec<char>,
    sq_ranks: Vec<char>,
    promos: Vec<char>,
}

impl Fuzz {
    /// gen_rust_fixtures.fuzz_token, draw for draw.
    fn token(&self, r: &mut SplitMix64) -> String {
        if r.below(2) == 0 {
            let mut s: Vec<char> = Vec::new();
            if r.below(3) == 0 {
                s.push(self.pieces[r.below(self.pieces.len())]);
            }
            if r.below(3) == 0 {
                s.push(self.files[r.below(self.files.len())]);
            }
            if r.below(3) == 0 {
                s.push(self.ranks[r.below(self.ranks.len())]);
            }
            if r.below(3) == 0 {
                s.push(['-', 'x'][r.below(2)]);
            }
            let f = self.files[r.below(self.files.len())];
            let k = self.sq_ranks[r.below(self.sq_ranks.len())];
            s.push(f);
            s.push(k);
            match r.below(6) {
                0 => {
                    s.push('=');
                    s.push(self.promos[r.below(self.promos.len())]);
                }
                1 => s.push(self.promos[r.below(self.promos.len())]),
                _ => {}
            }
            match r.below(4) {
                0 => s.push('+'),
                1 => s.push('#'),
                _ => {}
            }
            for _ in 0..r.below(3) {
                match r.below(3) {
                    0 => {
                        let i = r.below(s.len() + 1);
                        let c = self.alpha[r.below(self.alpha.len())];
                        s.insert(i, c);
                    }
                    1 => {
                        if !s.is_empty() {
                            let i = r.below(s.len());
                            s.remove(i);
                        }
                    }
                    _ => {
                        if !s.is_empty() {
                            let i = r.below(s.len());
                            let c = self.alpha[r.below(self.alpha.len())];
                            s[i] = c;
                        }
                    }
                }
            }
            return s.into_iter().collect();
        }
        let n = 1 + r.below(8);
        (0..n).map(|_| self.alpha[r.below(self.alpha.len())]).collect()
    }
}

fn record(tok: &str) -> String {
    match match_san(tok) {
        None => format!("{tok}\u{0}0\n"),
        Some(g) => {
            let mut s = format!("{tok}\u{0}1");
            for x in g.groups() {
                s.push_str(x.as_deref().unwrap_or("\u{1}"));
                s.push('\u{0}');
            }
            s.push('\n');
            s
        }
    }
}

fn fnv1a(data: &[u8], mut h: u64) -> u64 {
    for &b in data {
        h = (h ^ u64::from(b)).wrapping_mul(0x100_0000_01B3);
    }
    h
}

#[test]
fn san_regex_fuzz() {
    let v: Value = serde_json::from_str(&fixture("fuzz_regex.json")).unwrap();
    let chars = |k: &str| -> Vec<char> { v[k].as_str().unwrap().chars().collect() };
    let fz = Fuzz {
        alpha: v["alpha"].as_array().unwrap().iter().map(|c| c.as_str().unwrap().chars().next().unwrap()).collect(),
        pieces: chars("pieces"),
        files: chars("files"),
        ranks: chars("ranks"),
        sq_ranks: chars("sq_ranks"),
        promos: chars("promos"),
    };
    // The regex crate as a second oracle on the same tokens.
    let re = regex::Regex::new(r"^([NBKRQ])?([a-h])?([1-8])?[\-x]?([a-h][1-8])(=?[nbrqkNBRQK])?[\+#]?$")
        .unwrap();
    let mut r = SplitMix64(v["seed"].as_u64().unwrap());
    let per = v["per_chunk"].as_u64().unwrap() as usize;
    let explicit = v["explicit"].as_array().unwrap();
    let (mut n, mut matched) = (0usize, 0u64);
    for (ci, want) in v["digests"].as_array().unwrap().iter().enumerate() {
        let mut h = 0xCBF2_9CE4_8422_2325u64;
        for _ in 0..per {
            let tok = fz.token(&mut r);
            let ours = match_san(&tok);
            matched += u64::from(ours.is_some());
            let theirs = re.captures(&tok).map(|c| {
                [1, 2, 3, 4, 5].map(|i| c.get(i).map(|m| m.as_str().to_string()))
            });
            assert_eq!(ours.map(|g| g.groups()), theirs, "regex crate disagrees on {tok:?}");
            if let Some(e) = explicit.get(n) {
                assert_eq!(e["tok"].as_str().unwrap(), tok, "token stream diverged at {n}");
                let want_g: Option<Vec<Option<String>>> = e["groups"]
                    .as_array()
                    .map(|a| a.iter().map(|g| g.as_str().map(str::to_string)).collect());
                assert_eq!(ours.map(|g| g.groups().to_vec()), want_g, "{tok:?}");
            }
            h = fnv1a(record(&tok).as_bytes(), h);
            n += 1;
        }
        assert_eq!(format!("{h:016x}"), want.as_str().unwrap(), "fuzz chunk {ci} differs from Python");
    }
    assert_eq!(matched, v["matched"].as_u64().unwrap());
    println!("{n} fuzzed tokens, {matched} matching SAN_REGEX, all groups equal to Python's");
}
