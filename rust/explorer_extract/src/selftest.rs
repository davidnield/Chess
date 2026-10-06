//! Checks against fixtures generated from python-chess and the Python extract
//! (`python/gen_rust_fixtures.py`). The small ones are embedded, so `selftest`
//! runs on any machine in well under a second before real use; `cargo test`
//! runs these plus the heavy ones (real-game SAN variants, the 1M-token regex
//! fuzz) from the fixture files.

use serde_json::Value;
use shakmaty::fen::Fen;
use shakmaty::uci::UciMove;
use shakmaty::zobrist::{Zobrist64, ZobristValue};
use shakmaty::{CastlingMode, CastlingSide, Chess, Color, File, Piece, Position, Role, Square};

use crate::chesspos::{hash, pack, shakmaty_epd, START_HASH};
use crate::game::Filters;
use crate::keys::{san_str, BANDS};
use crate::partials::ChunkAgg;
use crate::pyre::{all_tokens, is_py_digit, is_py_space, Tokens};
use crate::san::{parse_san, py_uci, Parsed};

pub const KEYS_JSON: &str = include_str!("../tests/fixtures/polyglot_keys.json");
pub const UNICODE_JSON: &str = include_str!("../tests/fixtures/pyunicode.json");
pub const SAN_TABLE_JSON: &str = include_str!("../tests/fixtures/san_table.json");
pub const TOKENIZER_JSON: &str = include_str!("../tests/fixtures/tokenizer.json");
pub const MINI_JSON: &str = include_str!("../tests/fixtures/mini_extract.json");

pub type Check = Result<String, String>;

fn parse(json: &str) -> Value {
    serde_json::from_str(json).expect("embedded fixture parses")
}

/// The 781 polyglot keys, by polyglot index, against shakmaty's Zobrist64.
pub fn check_keys(json: &str) -> Check {
    let v = parse(json);
    let keys: Vec<u64> = v["keys"].as_array().unwrap().iter().map(|k| k.as_u64().unwrap()).collect();
    if keys.len() != 781 {
        return Err(format!("{} keys, want 781", keys.len()));
    }
    let roles = [Role::Pawn, Role::Knight, Role::Bishop, Role::Rook, Role::Queen, Role::King];
    for (i, &want) in keys.iter().enumerate() {
        let got = match i {
            0..=767 => {
                let kind = i / 64;
                let color = if kind % 2 == 1 { Color::White } else { Color::Black };
                let piece = Piece { color, role: roles[kind / 2] };
                Zobrist64::zobrist_for_piece(Square::new((i % 64) as u32), piece)
            }
            768 => Zobrist64::zobrist_for_castling_right(Color::White, CastlingSide::KingSide),
            769 => Zobrist64::zobrist_for_castling_right(Color::White, CastlingSide::QueenSide),
            770 => Zobrist64::zobrist_for_castling_right(Color::Black, CastlingSide::KingSide),
            771 => Zobrist64::zobrist_for_castling_right(Color::Black, CastlingSide::QueenSide),
            772..=779 => Zobrist64::zobrist_for_en_passant_file(File::new((i - 772) as u32)),
            _ => Zobrist64::zobrist_for_white_turn(),
        };
        if got.0 != want {
            return Err(format!("key {i}: shakmaty {:#x}, polyglot {want:#x}", got.0));
        }
    }
    let start = Chess::default();
    let sh = v["start_hash"].as_i64().unwrap();
    let se = v["start_epd"].as_str().unwrap();
    if hash(&start) != sh || START_HASH != sh {
        return Err(format!("start hash {} / const {START_HASH}, want {sh}", hash(&start)));
    }
    if pack(&start).render() != se || shakmaty_epd(&start) != se {
        return Err(format!("start EPD {:?}, want {se:?}", pack(&start).render()));
    }
    Ok(format!("781 polyglot keys; start hash {sh}; start EPD"))
}

/// Python's whitespace and \d sets, over every code point.
pub fn check_unicode(json: &str) -> Check {
    let v = parse(json);
    let set = |k: &str| -> std::collections::HashSet<u32> {
        v[k].as_array().unwrap().iter().map(|c| c.as_u64().unwrap() as u32).collect()
    };
    let (ws, ds) = (set("whitespace"), set("digits"));
    for cp in 0..0x11_0000u32 {
        let Some(c) = char::from_u32(cp) else { continue };
        if is_py_space(c) != ws.contains(&cp) {
            return Err(format!("U+{cp:04X}: whitespace {} vs Python {}", is_py_space(c), ws.contains(&cp)));
        }
        if is_py_digit(c) != ds.contains(&cp) {
            return Err(format!("U+{cp:04X}: digit {} vs Python {}", is_py_digit(c), ds.contains(&cp)));
        }
    }
    Ok(format!("{} whitespace and {} digit code points, all 1,114,112 agree", ws.len(), ds.len()))
}

pub fn setup(fen: &str, moves: &[Value]) -> Result<Chess, String> {
    let mut pos: Chess = Fen::from_ascii(fen.as_bytes())
        .map_err(|e| format!("{fen}: {e}"))?
        .into_position(CastlingMode::Standard)
        .map_err(|e| format!("{fen}: {e}"))?;
    for m in moves {
        let u = m.as_str().unwrap();
        let mv = UciMove::from_ascii(u.as_bytes())
            .map_err(|e| format!("{u}: {e}"))?
            .to_move(&pos)
            .map_err(|e| format!("{u}: {e}"))?;
        pos.play_unchecked(mv);
    }
    Ok(pos)
}

/// parse_san's verdict on `san` at `pos`, in the fixtures' terms.
pub fn verdict_matches(pos: &Chess, san: &str, want: &Value, with_epd: bool) -> Result<(), String> {
    let got = parse_san(pos, san);
    let ok = want["ok"].as_bool().unwrap();
    match (got, ok) {
        (Err(_), false) => Ok(()),
        (Err(e), true) => Err(format!("{san:?}: rejected ({e:?}), python accepts {want}")),
        (Ok(p), false) => Err(format!("{san:?}: accepted ({p:?}), python rejects ({})", want["error"])),
        (Ok(Parsed::Null), true) => {
            if !want["null"].as_bool().unwrap() {
                return Err(format!("{san:?}: null, python {want}"));
            }
            if pos.is_check() {
                // python-chess pushes it anyway; the extract refuses such a game.
                return Ok(());
            }
            let child = pos.clone().swap_turn().map_err(|e| format!("{san:?}: {e}"))?;
            compare_child(san, &child, want, with_epd)
        }
        (Ok(Parsed::Move(m)), true) => {
            if want["null"].as_bool().unwrap() || py_uci(&m) != want["uci"].as_str().unwrap() {
                return Err(format!("{san:?}: {} vs python {}", py_uci(&m), want["uci"]));
            }
            let mut child = pos.clone();
            child.play_unchecked(m);
            compare_child(san, &child, want, with_epd)
        }
    }
}

fn compare_child(san: &str, child: &Chess, want: &Value, with_epd: bool) -> Result<(), String> {
    if hash(child) != want["child_hash"].as_i64().unwrap() {
        return Err(format!("{san:?}: child hash {} vs python {}", hash(child), want["child_hash"]));
    }
    if with_epd {
        let e = pack(child).render();
        if e != want["child_epd"].as_str().unwrap() || e != shakmaty_epd(child) {
            return Err(format!("{san:?}: child EPD {e:?} vs python {}", want["child_epd"]));
        }
    }
    Ok(())
}

/// The hand-built SAN table: every row of the spec's 2f table and more.
pub fn check_san_table(json: &str) -> Check {
    let v = parse(json);
    let cases = v["cases"].as_array().unwrap();
    for c in cases {
        let pos = setup(c["fen"].as_str().unwrap(), c["moves"].as_array().unwrap())?;
        let label = c["label"].as_str().unwrap();
        if hash(&pos) != c["parent_hash"].as_i64().unwrap() {
            return Err(format!("{label}: setup hash {} vs python {}", hash(&pos), c["parent_hash"]));
        }
        let e = pack(&pos).render();
        if e != c["parent_epd"].as_str().unwrap() || e != shakmaty_epd(&pos) {
            return Err(format!("{label}: setup EPD {e:?} vs python {}", c["parent_epd"]));
        }
        verdict_matches(&pos, c["san"].as_str().unwrap(), &c["expect"], true)
            .map_err(|e| format!("{label}: {e}"))?;
    }
    Ok(format!("{} SAN table cases", cases.len()))
}

/// The pinned en-passant position: the hash keeps the ep file (a pawn is
/// adjacent), the EPD drops it (the capture would expose the king).
pub fn check_pinned_ep() -> Check {
    let moves: Vec<Value> = "e2e4 a7a6 e4e5 a6a5 e1e2 a8a6 e2d3 a6h6 d3c4 h6h5 c4c5 f7f5"
        .split(' ')
        .map(|m| Value::String(m.into()))
        .collect();
    let pos = setup(shakmaty::fen::Fen::default().to_string().as_str(), &moves)?;
    let epd = pack(&pos).render();
    let rebuilt: Chess = Fen::from_ascii(format!("{epd} 0 1").as_bytes())
        .unwrap()
        .into_position(CastlingMode::Standard)
        .unwrap();
    if !epd.ends_with(" -") || hash(&rebuilt) == hash(&pos) || pos.pseudo_legal_ep_square().is_none() {
        return Err(format!("pinned ep: EPD {epd:?}, hash {} vs rebuilt {}", hash(&pos), hash(&rebuilt)));
    }
    Ok("pinned en passant: hash keeps the file, EPD drops it".into())
}

pub fn check_tokenizer(json: &str) -> Check {
    let v = parse(json);
    let cases = v["cases"].as_array().unwrap();
    let mut toks = Tokens::default();
    for c in cases {
        let mt = c["movetext"].as_str().unwrap();
        let want: Vec<&str> = c["tokens"].as_array().unwrap().iter().map(|t| t.as_str().unwrap()).collect();
        let got = all_tokens(mt);
        if got != want {
            return Err(format!("{mt:?}: {got:?} vs python {want:?}"));
        }
        toks.fill(Some(mt), 31);
        let n = want.len().min(31);
        if toks.len() != n || (0..n).any(|i| toks.get(i) != want[i]) {
            return Err(format!("{mt:?}: the 31-token buffer disagrees"));
        }
    }
    Ok(format!("{} tokenizer vectors", cases.len()))
}

fn opt_str(v: &Value) -> Option<&str> {
    v.as_str()
}

/// The mini game set through ChunkAgg, against extract_file's own rows.
pub fn check_mini(json: &str) -> Check {
    let v = parse(json);
    let excl: Vec<String> = v["excluded_terminations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t.as_str().unwrap().to_string())
        .collect();
    let filters = Filters {
        min_elo: v["min_elo"].as_i64().unwrap() as i32,
        exclude_bots: true,
        excluded_terminations: excl,
    };
    let max_ply = v["max_ply"].as_u64().unwrap() as usize;
    let event = v["event"].as_str().unwrap();
    let games = v["games"].as_array().unwrap();
    let mut summary = Vec::new();
    for epd_max in [16u32, 30] {
        let mut agg = ChunkAgg::new(epd_max);
        let mut toks = Tokens::default();
        let start = Chess::default();
        for (row, g) in games.iter().enumerate() {
            let ws = g["white_score"].as_str().map(|s| if s == "nan" { f64::NAN } else { s.parse().unwrap() });
            let gr = crate::game::GameRow {
                movetext: opt_str(&g["movetext"]),
                white_score: ws,
                termination: opt_str(&g["termination"]),
                mean_elo: g["mean_elo"].as_i64().map(|x| x as i32),
                white_title: opt_str(&g["white_title"]),
                black_title: opt_str(&g["black_title"]),
            };
            agg.game(row as u64, &gr, &filters, max_ply, 0, &mut toks, &start)
                .map_err(|e| format!("epd {epd_max}: {e}"))?;
        }
        let mut ps: Vec<_> = agg.ps.iter().collect();
        ps.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let want = v[format!("ps_epd{epd_max}")].as_array().unwrap();
        if ps.len() != want.len() {
            return Err(format!("epd {epd_max}: {} ps rows vs python {}", ps.len(), want.len()));
        }
        for ((k, val), w) in ps.iter().zip(want) {
            let epd = val.epd.map(|p| p.render());
            let got = serde_json::json!([
                k.hash, san_str(&k.san), event, BANDS[k.band as usize], epd, val.child,
                null, val.ply, val.counts.w, val.counts.d, val.counts.b, val.counts.t
            ]);
            if &got != w {
                return Err(format!("epd {epd_max}: ps row {got} vs python {w}"));
            }
        }
        let mut term: Vec<_> = agg.term.iter().collect();
        term.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let want_t = v[format!("term_epd{epd_max}")].as_array().unwrap();
        let got_t: Vec<Value> = term
            .iter()
            .map(|(k, c)| serde_json::json!([k.hash, k.kind, k.reason, c.w, c.d, c.b, c.t]))
            .collect();
        if &got_t != want_t {
            return Err(format!("epd {epd_max}: term rows {got_t:?} vs python {want_t:?}"));
        }
        let cnt = &v[format!("counts_epd{epd_max}")];
        let d = &cnt["drop"];
        let c = agg.c;
        let want_c = [
            cnt["games"].as_u64(), cnt["kept"].as_u64(), cnt["failed"].as_u64(),
            d["elo"].as_u64(), d["no_score"].as_u64(), d["termination"].as_u64(), d["bot"].as_u64(),
        ];
        let got_c = [c.games, c.kept, c.failed, c.drop_elo, c.drop_no_score, c.drop_termination, c.drop_bot]
            .map(Some);
        if want_c != got_c {
            return Err(format!("epd {epd_max}: counters {got_c:?} vs python {want_c:?}"));
        }
        summary.push(format!("epd {epd_max}: {} ps + {} term rows", ps.len(), term.len()));
    }
    Ok(format!("mini extract, {} games: {}", games.len(), summary.join(", ")))
}

/// Every embedded check, in order: (name, result).
pub fn run_embedded() -> Vec<(&'static str, Check)> {
    vec![
        ("polyglot keys", check_keys(KEYS_JSON)),
        ("unicode tables", check_unicode(UNICODE_JSON)),
        ("SAN table", check_san_table(SAN_TABLE_JSON)),
        ("pinned en passant", check_pinned_ep()),
        ("tokenizer", check_tokenizer(TOKENIZER_JSON)),
        ("mini extract", check_mini(MINI_JSON)),
    ]
}
