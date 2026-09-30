//! `cargo test --test evals`: the eval DB builder. Position identity against
//! python-chess on real FENs from both datasets (tests/fixtures/
//! eval_positions.json, written by python/gen_eval_fixtures.py); the choice
//! rules; and `explorer-extract evals` end to end on a synthetic book and
//! synthetic sources that carry the spec's three real cloud examples.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int16Type, Int32Type, Int64Type, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use shakmaty::fen::Fen;
use shakmaty::uci::UciMove;
use shakmaty::{CastlingMode, Chess, Position};

use explorer_extract::chesspos::{hash, pack, shakmaty_epd};
use explorer_extract::evals::{
    self, choose, cloud_pick, eval_cp, fish_pick, fishnet_disagrees, identify, key_cp_mate, lower_median,
    score_key, tier_of, CloudCand, CloudPick, FishPick, MATE_BASE,
};
use explorer_extract::keys::bucket_of;
use explorer_extract::merge::book_ps_schema;

const EXE: &str = env!("CARGO_BIN_EXE_explorer-extract");

fn fixture() -> Value {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/eval_positions.json");
    serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap()
}

// ── position identity vs python-chess ────────────────────────────────────────

#[test]
fn identity_matches_python_chess() {
    let v = fixture();
    let cases = v["cases"].as_array().unwrap();
    assert!(cases.len() > 1000, "{} cases", cases.len());
    let (mut n_var, mut n_played) = (0, 0);
    for c in cases {
        let fen = c["fen"].as_str().unwrap();
        let id = identify(fen.as_bytes()).unwrap_or_else(|e| panic!("{fen}: {e:?}"));
        let epd = c["epd"].as_str().unwrap();
        assert_eq!(id.packed.render(), epd, "{fen}: EPD");
        let want: Vec<i64> = c["hashes"].as_array().unwrap().iter().map(|h| h.as_i64().unwrap()).collect();
        let got = id.hashes.as_slice();
        assert_eq!(got[0], want[0], "{fen}: canonical hash");
        let mut g: Vec<i64> = got[1..].to_vec();
        g.sort();
        assert_eq!(g, want[1..].to_vec(), "{fen}: ep-variant hashes");
        n_var += usize::from(want.len() > 1);
        // The canonical hash is the book's hash of the EPD's own position.
        let canon: Chess = Fen::from_ascii(epd.as_bytes()).unwrap().into_position(CastlingMode::Standard).unwrap();
        assert_eq!(hash(&canon), want[0], "{fen}: chesspos::hash of the EPD");
        assert_eq!(shakmaty_epd(&canon), epd);
        if let Some(ph) = c.get("played_hash").and_then(Value::as_i64) {
            assert!(got.contains(&ph), "{fen}: the played board's hash {ph} is not among {got:?}");
            n_played += 1;
        }
    }
    assert!(n_var >= 3 && n_played >= 5, "{n_var} variant cases, {n_played} played");
    for f in v["fails"].as_array().unwrap() {
        let fen = f.as_str().unwrap();
        assert!(identify(fen.as_bytes()).is_err(), "{fen} should be rejected");
    }
}

/// Play `uci` from `fen`.
fn play(fen: &str, uci: &[&str]) -> Chess {
    let mut pos: Chess = Fen::from_ascii(fen.as_bytes()).unwrap().into_position(CastlingMode::Standard).unwrap();
    for u in uci {
        let m = u.parse::<UciMove>().unwrap().to_move(&pos).unwrap();
        pos.play_unchecked(m);
    }
    pos
}

#[test]
fn ep_variants_both_directions() {
    // Pinned: the capture is pseudo-legal but illegal. The book hashes the
    // played board with the ep file; the EPD omits it; both hashes are emitted,
    // whatever the source FEN printed.
    let p = play("8/8/8/8/k2p3R/8/4P3/4K3 w - - 0 1", &["e2e4"]);
    let e = pack(&p).render();
    assert!(e.ends_with(" b - -"));
    for src in [e.clone(), format!("{} e3 0 1", &e[..e.len() - 2])] {
        let id = identify(src.as_bytes()).unwrap();
        assert_eq!(id.hashes.as_slice().len(), 2, "{src}");
        assert!(id.hashes.as_slice().contains(&hash(&p)), "{src}");
        assert_eq!(id.packed.render(), e);
    }
    // Legal: the EPD shows the ep square; no variant (the ep-less board is
    // another EPD). And the same board without its ep square gains none.
    let l = play("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1", &["e2e4", "d7d5", "e4e5", "f7f5"]);
    let le = pack(&l).render();
    assert!(le.ends_with(" f6"), "{le}");
    let id = identify(le.as_bytes()).unwrap();
    assert_eq!(id.hashes.as_slice(), &[hash(&l)]);
    let no_ep = format!("{} -", &le[..le.len() - 3]);
    let id = identify(no_ep.as_bytes()).unwrap();
    assert_eq!(id.hashes.as_slice().len(), 1, "a legal ep square is never a variant of the ep-less EPD");
    assert_ne!(id.hashes.canonical(), hash(&l));
    // An adjacent pawn that is not pinned: no variant, one hash.
    let q = play("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1", &["e2e4", "a7a6", "e4e5", "d7d5"]);
    assert_eq!(identify(pack(&q).render().as_bytes()).unwrap().hashes.as_slice(), &[hash(&q)]);
}

// ── the choice rules ─────────────────────────────────────────────────────────

fn k(cp: Option<i64>, mate: Option<i64>, white: bool) -> i32 {
    score_key(cp, mate, white).unwrap()
}

#[test]
fn score_order_and_eval_cp() {
    // White POV: mate > 0 above every cp, a shorter mate higher; mate < 0
    // below, a shorter mate lower; mate 0 by the mated side.
    let order = [
        k(None, Some(0), true),  // White to move, mated
        k(None, Some(-1), true),
        k(None, Some(-7), true),
        k(Some(-30000), None, true),
        k(Some(-5), None, true),
        k(Some(0), None, true),
        k(Some(40000), None, true),
        k(None, Some(9), true),
        k(None, Some(1), true),
        k(None, Some(0), false), // Black to move, mated
    ];
    assert!(order.windows(2).all(|w| w[0] < w[1]), "{order:?}");
    for (cp, mate, white) in [(Some(69), None, false), (None, Some(15), true), (None, Some(-3), false), (None, Some(0), true), (None, Some(0), false), (Some(-2500), None, true)] {
        assert_eq!(key_cp_mate(k(cp, mate, white)), (cp.map(|x| x as i32), mate.map(|x| x as i32)));
    }
    assert!(score_key(Some(1), Some(1), true).is_err() && score_key(None, None, true).is_err());
    assert_eq!(eval_cp(k(Some(69), None, true)), 69);
    assert_eq!(eval_cp(k(Some(2500), None, true)), 2000);
    assert_eq!(eval_cp(k(Some(-2001), None, true)), -2000);
    assert_eq!(eval_cp(k(None, Some(15), true)), 2000);
    assert_eq!(eval_cp(k(None, Some(-2), false)), -2000);
    assert_eq!(eval_cp(k(None, Some(0), true)), -2000);
    assert_eq!(eval_cp(k(None, Some(0), false)), 2000);
    assert_eq!(MATE_BASE, 1 << 24);
}

#[test]
fn fishnet_tiers_and_lower_median() {
    assert_eq!((tier_of(2013, 1), tier_of(2015, 12), tier_of(2016, 1), tier_of(2020, 12), tier_of(2021, 1)), (2, 2, 1, 1, 0));
    assert_eq!(lower_median(&[(1, 1), (2, 1)]), Some(1));
    assert_eq!(lower_median(&[(1, 1), (2, 1), (3, 1)]), Some(2));
    assert_eq!(lower_median(&[(5, 4)]), Some(5));
    assert_eq!(lower_median(&[(1, 2), (9, 2)]), Some(1));
    assert_eq!(lower_median(&[]), None);
    // Mixed mate and cp in the classical tier; early ignored, no nnue.
    let m3 = k(None, Some(3), false);
    let mm2 = k(None, Some(-2), false);
    let rows = [(2u8, 50, 1u64), (2, 60, 1), (1, 40, 1), (1, m3, 1), (1, 30, 1), (1, mm2, 1)];
    assert_eq!(fish_pick(&rows), Some(FishPick { tier: 1, key: 30, n_tier: 4, n: 6 }));
    // An nnue row beats any number of older ones.
    let rows = [(1u8, 500, 100u64), (0, -7, 1)];
    assert_eq!(fish_pick(&rows), Some(FishPick { tier: 0, key: -7, n_tier: 1, n: 101 }));
    // Mates only: two mates for White and two against -> the lower middle one.
    let rows = [(0u8, k(None, Some(-1), true), 2u64), (0, k(None, Some(4), true), 2)];
    assert_eq!(key_cp_mate(fish_pick(&rows).unwrap().key), (None, Some(-1)));
}

fn cand(depth: i16, knodes: i64, key: i32, file: u32, row: u64) -> CloudCand {
    CloudCand { depth, knodes, key, line: format!("l{file}_{row}"), file, row, npv: 1, bad: false }
}

#[test]
fn cloud_pick_rule() {
    let v = [cand(40, 9, 1, 0, 5), cand(46, 3, 2, 1, 0), cand(46, 7, 3, 1, 9), cand(46, 7, 4, 0, 99), cand(46, 7, 5, 0, 100)];
    let p = cloud_pick(&v).unwrap();
    assert_eq!((p.cand.key, p.n_evals), (4, 3), "max depth, max knodes, then the first (file, row)");
}

#[test]
fn disagrees_flag() {
    let sat_neg = k(None, Some(-1), true);
    assert!(fishnet_disagrees(69, sat_neg, 5));
    assert!(!fishnet_disagrees(69, sat_neg, 4), "needs >= 5 best-tier rows");
    assert!(!fishnet_disagrees(-69, sat_neg, 9), "same sign");
    assert!(fishnet_disagrees(0, k(Some(2400), None, true), 5), "old DB: sign(0) != sign(+2000)");
    assert!(!fishnet_disagrees(-300, k(Some(-1999), None, true), 50), "not saturated");
    let cloud = CloudPick { cand: cand(30, 1, 69, 0, 0), n_evals: 1 };
    let fish = FishPick { tier: 0, key: sat_neg, n_tier: 5, n: 5 };
    let c = choose(Some(&cloud), Some(&fish)).unwrap();
    assert_eq!((c.source, c.eval_cp, c.disagrees), ("cloud", 69, true));
    let c = choose(None, Some(&fish)).unwrap();
    assert_eq!((c.source, c.eval_cp, c.disagrees), ("fishnet", -2000, false), "only cloud rows are flagged");
    assert!(choose(None, None).is_none());
}

// ── end to end ───────────────────────────────────────────────────────────────

const EX1: &str = "7r/1p3k2/p1bPR3/5p2/2B2P1p/8/PP4P1/3K4 b - -";
const EX2: &str = "8/4r3/2R2pk1/6pp/3P4/6P1/5K1P/8 b - -";
const EX3: &str = "6k1/6p1/8/4K3/4NN2/8/8/8 w - -";
const POS_D: &str = "rnbqkbnr/pppp1ppp/8/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq -";
const POS_E: &str = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq -";
const POS_R: &str = "4k3/8/8/8/8/8/4P3/4K3 w - -";
const POS_X: &str = "rnbqkbnr/pppp1ppp/8/4p3/6P1/5P2/PPPPP2P/RNBQKBNR b KQkq -";
const POS_Q: &str = "rnb1kbnr/pppp1ppp/8/4p3/6Pq/5P2/PPPPP2P/RNBQKBNR w KQkq -";
const POS_Z: &str = "4k3/8/8/8/8/8/8/4K3 w - -";

fn canon(fen: &str) -> i64 {
    identify(fen.as_bytes()).unwrap().hashes.canonical()
}

fn pinned() -> Chess {
    play("8/8/8/8/k2p3R/8/4P3/4K3 w - - 0 1", &["e2e4"])
}

fn write(path: &Path, batch: RecordBatch) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// (hash, epd, san, child, ply, total) into slice (event, band).
type BookRow = (i64, String, &'static str, i64, i32, i64);

fn write_book(root: &Path, slices: &[(&str, i64, Vec<BookRow>)]) {
    for (ev, band, rows) in slices {
        let mut by: BTreeMap<u32, Vec<BookRow>> = BTreeMap::new();
        for r in rows {
            by.entry(bucket_of(r.0, 512)).or_default().push(r.clone());
        }
        for (b, mut v) in by {
            v.sort_by(|a, c| (a.0, &a.1, a.2, a.4).cmp(&(c.0, &c.1, c.2, c.4)));
            let n = v.len();
            let cols: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(v.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(v.iter().map(|r| r.2))),
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(*ev, n))),
                Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(*band, n))),
                Arc::new(StringArray::from_iter_values(v.iter().map(|r| r.1.as_str()))),
                Arc::new(Int64Array::from_iter_values(v.iter().map(|r| r.3))),
                Arc::new(Int32Array::from_iter_values(v.iter().map(|r| r.4))),
                Arc::new(Int64Array::from_iter_values(v.iter().map(|r| r.5))),
                Arc::new(Int64Array::from_iter_values(v.iter().map(|_| 0))),
                Arc::new(Int64Array::from_iter_values(v.iter().map(|_| 0))),
                Arc::new(Int64Array::from_iter_values(v.iter().map(|r| r.5))),
                Arc::new(Float64Array::from_iter_values(v.iter().map(|_| 1.0))),
            ];
            let p = root.join("ps").join(format!("event={ev}")).join(format!("elo_band={band}")).join(format!("bkt{b:03}.parquet"));
            write(&p, RecordBatch::try_new(book_ps_schema(), cols).unwrap());
        }
    }
}

type CloudSrc = (String, String, i64, i64, Option<i64>, Option<i64>);

fn write_cloud(path: &Path, rows: &[CloudSrc]) {
    // The dataset's own types: depth uint8, knodes int32, cp int16, mate int8.
    let schema = Arc::new(Schema::new(vec![
        Field::new("fen", DataType::Utf8, true),
        Field::new("line", DataType::Utf8, true),
        Field::new("depth", DataType::UInt8, true),
        Field::new("knodes", DataType::Int32, true),
        Field::new("cp", DataType::Int16, true),
        Field::new("mate", DataType::Int8, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1.as_str()))),
        Arc::new(arrow::array::UInt8Array::from_iter_values(rows.iter().map(|r| r.2 as u8))),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.3 as i32))),
        Arc::new(Int16Array::from(rows.iter().map(|r| r.4.map(|x| x as i16)).collect::<Vec<_>>())),
        Arc::new(Int8Array::from(rows.iter().map(|r| r.5.map(|x| x as i8)).collect::<Vec<_>>())),
    ];
    write(path, RecordBatch::try_new(schema, cols).unwrap());
}

fn write_fish(path: &Path, rows: &[(String, Option<i32>, Option<i32>)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("fen", DataType::Utf8, false),
        Field::new("cp", DataType::Int32, true),
        Field::new("mate", DataType::Int32, true),
        Field::new("move", DataType::Utf8, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0.as_str()))),
        Arc::new(Int32Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
        Arc::new(Int32Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|_| "e2e4"))),
    ];
    write(path, RecordBatch::try_new(schema, cols).unwrap());
}

fn f6(epd: &str) -> String {
    format!("{epd} 0 12")
}

struct Case {
    dir: PathBuf,
    buckets: String,
    sources: String,
}

impl Drop for Case {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn build_case(name: &str) -> Case {
    let dir = std::env::temp_dir().join(format!("ee_evals_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let fx = fixture();
    // Cloud: the spec's three examples verbatim, then R (a block whose first
    // PV is not the best), in data_0000; a tie with EX1's block and a
    // position not in the book in data_0001.
    let mut c0: Vec<CloudSrc> = fx["cloud_rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (r["fen"].as_str().unwrap().to_string(), r["line"].as_str().unwrap().to_string(), r["depth"].as_i64().unwrap(),
             r["knodes"].as_i64().unwrap(), r["cp"].as_i64(), r["mate"].as_i64())
        })
        .collect();
    c0.push((POS_R.into(), "e2e3 e8e7".into(), 30, 100, Some(10), None));
    c0.push((POS_R.into(), "e2e4 e8e7".into(), 30, 100, Some(50), None));
    write_cloud(&dir.join("evals/cloud/data_0000.parquet"), &c0);
    write_cloud(
        &dir.join("evals/cloud/data_0001.parquet"),
        &[
            (EX1.into(), "f7f6 e6e2".into(), 46, 4189972, Some(70), None),
            (POS_Z.into(), "e1e2".into(), 10, 5, Some(0), None),
        ],
    );
    let fdir = dir.join("evals/fishnet");
    let pin = pack(&pinned()).render();
    write_fish(&fdir.join("standard_rated_2015_01.parquet"), &[(f6(POS_D), Some(50), None), (f6(POS_D), Some(60), None)]);
    write_fish(
        &fdir.join("standard_rated_2019_05.parquet"),
        &[(f6(POS_D), Some(40), None), (f6(POS_D), None, Some(3)), (f6(POS_D), Some(30), None), (f6(POS_D), None, Some(-2)), (f6(POS_Z), Some(1), None)],
    );
    let mut nn = vec![(f6(POS_E), Some(20), None), (f6(POS_E), Some(10), None), (f6(&pin), Some(-5), None)];
    for _ in 0..5 {
        nn.push((f6(EX1), None, Some(-1)));
        nn.push((f6(EX2), None, Some(2)));
    }
    for _ in 0..3 {
        nn.push((f6(POS_Q), None, Some(0)));
    }
    nn.push(("garbage".into(), Some(1), None));
    write_fish(&fdir.join("standard_rated_2022_03.parquet"), &nn);
    // The book: parents in two slices (EX1 at two plies), X's child is Q.
    let ph = hash(&pinned());
    let p = |f: &str| -> (i64, String) { (canon(f), f.to_string()) };
    let other = 12345 * 512 + 7;
    let blitz: Vec<BookRow> = vec![
        (p(EX1).0, p(EX1).1, "Kg7", other, 30, 3),
        (p(EX1).0, p(EX1).1, "Kg7", other, 28, 1),
        (p(EX2).0, p(EX2).1, "Ra7", other, 30, 2),
        (p(EX3).0, p(EX3).1, "Ke6", other, 29, 1),
        (p(POS_D).0, p(POS_D).1, "Nc6", other, 4, 10),
        (p(POS_E).0, p(POS_E).1, "e5", other, 2, 20),
        (p(POS_R).0, p(POS_R).1, "e4", other, 29, 1),
        (p(POS_X).0, p(POS_X).1, "Qh4#", canon(POS_Q), 4, 2),
        (ph, pin.clone(), "Kb4", other, 30, 1),
    ];
    let rapid: Vec<BookRow> = vec![(p(EX1).0, p(EX1).1, "Kg7", other, 30, 5), (p(POS_E).0, p(POS_E).1, "c5", other, 2, 7)];
    write_book(&dir.join("book"), &[("Blitz", 1600, blitz), ("Rapid", 1800, rapid)]);
    let mut bs: BTreeSet<u32> = [EX1, EX2, EX3, POS_D, POS_E, POS_R, POS_X, POS_Q, POS_Z]
        .iter()
        .map(|f| bucket_of(canon(f), 512))
        .collect();
    bs.insert(bucket_of(ph, 512));
    bs.insert(bucket_of(canon(&pin), 512));
    bs.insert(bucket_of(other, 512));
    let buckets = bs.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(",");
    Case { dir, buckets: buckets.clone(), sources: buckets }
}

fn run(c: &Case, out: &str, work: &str, extra: &[&str]) -> Output {
    let d = &c.dir;
    let mut cmd = Command::new(EXE);
    cmd.args(["evals", "--test-inputs"]);
    if !extra.contains(&"--mem-gb") {
        cmd.args(["--mem-gb", "2"]);
    }
    if !extra.contains(&"--threads") {
        cmd.args(["--threads", "2"]);
    }
    cmd.arg("--book").arg(d.join("book"));
    cmd.arg("--cloud").arg(d.join("evals/cloud"));
    cmd.arg("--fishnet").arg(d.join("evals/fishnet"));
    cmd.arg("--work").arg(d.join(work));
    cmd.arg("--out").arg(d.join(out));
    cmd.args(["--buckets", &c.buckets, "--child-sources", &c.sources]);
    cmd.args(extra);
    cmd.output().unwrap()
}

fn log(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

#[derive(Debug, Clone, PartialEq)]
struct OutRow {
    hash: i64,
    in_book: String,
    source: String,
    cp: Option<i32>,
    mate: Option<i32>,
    eval_cp: i16,
    depth: Option<i16>,
    knodes: Option<i64>,
    line: Option<String>,
    n_evals: Option<i32>,
    fcp: Option<i32>,
    fmate: Option<i32>,
    tier: Option<String>,
    n_tier: Option<i32>,
    n: Option<i32>,
    amb: bool,
    dis: bool,
}

fn read_out(out: &Path) -> BTreeMap<String, OutRow> {
    let mut m = BTreeMap::new();
    for e in std::fs::read_dir(out).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        if !(name.starts_with("bkt") && name.ends_with(".parquet")) {
            continue;
        }
        let r = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap()).unwrap().build().unwrap();
        for b in r {
            let b = b.unwrap();
            let c = |i: usize| b.column(i);
            let o32 = |i: usize, j: usize| {
                let a = c(i).as_primitive::<Int32Type>();
                (!a.is_null(j)).then(|| a.value(j))
            };
            let os = |i: usize, j: usize| {
                let a = c(i).as_string::<i32>();
                (!a.is_null(j)).then(|| a.value(j).to_string())
            };
            for j in 0..b.num_rows() {
                let epd = c(1).as_string::<i32>().value(j).to_string();
                let r = OutRow {
                    hash: c(0).as_primitive::<Int64Type>().value(j),
                    in_book: os(2, j).unwrap(),
                    source: os(3, j).unwrap(),
                    cp: o32(4, j),
                    mate: o32(5, j),
                    eval_cp: c(6).as_primitive::<Int16Type>().value(j),
                    depth: { let a = c(7).as_primitive::<Int16Type>(); (!a.is_null(j)).then(|| a.value(j)) },
                    knodes: { let a = c(8).as_primitive::<Int64Type>(); (!a.is_null(j)).then(|| a.value(j)) },
                    line: os(11, j),
                    n_evals: o32(12, j),
                    fcp: o32(13, j),
                    fmate: o32(14, j),
                    tier: os(15, j),
                    n_tier: o32(16, j),
                    n: o32(17, j),
                    amb: c(18).as_boolean().value(j),
                    dis: c(19).as_boolean().value(j),
                };
                assert!(bucket_of(r.hash, 512).to_string().len() <= 3);
                assert!(m.insert(epd, r).is_none());
            }
        }
    }
    m
}

fn out_files(out: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut m = BTreeMap::new();
    for e in std::fs::read_dir(out).unwrap() {
        let p = e.unwrap().path();
        let n = p.file_name().unwrap().to_string_lossy().into_owned();
        if n.starts_with("bkt") || n == "_coverage.parquet" || n == "_ambiguous.parquet" || n == "README.md" {
            m.insert(n, std::fs::read(&p).unwrap());
        }
    }
    m
}

fn assert_expected(out: &Path) {
    assert!(out.join("_DONE").is_file());
    let m = read_out(out);
    let pin = pack(&pinned()).render();
    let keys: BTreeSet<&str> = m.keys().map(String::as_str).collect();
    let want: BTreeSet<&str> = [EX1, EX2, EX3, POS_D, POS_E, POS_R, POS_Q, pin.as_str()].into_iter().collect();
    assert_eq!(keys, want, "Z (not in the book) and X (no eval) are absent; P only under its book hash");
    let r = &m[EX1];
    assert_eq!((r.in_book.as_str(), r.source.as_str(), r.cp, r.mate, r.eval_cp), ("parent", "cloud", Some(69), None, 69));
    assert_eq!((r.depth, r.knodes, r.n_evals), (Some(46), Some(4189972), Some(1)));
    assert!(r.line.as_deref().unwrap().starts_with("f7g7 e6e2 h8d8"), "{:?}", r.line);
    assert_eq!((r.fmate, r.tier.as_deref(), r.n_tier, r.n, r.dis), (Some(-1), Some("nnue"), Some(5), Some(5), true));
    let r = &m[EX2];
    assert_eq!((r.depth, r.knodes, r.cp, r.n_evals, r.dis), (Some(58), Some(491568), Some(0), Some(2), true));
    assert!(r.line.as_deref().unwrap().starts_with("e7a7 "), "{:?}", r.line);
    let r = &m[EX3];
    assert_eq!((r.depth, r.mate, r.cp, r.eval_cp, r.n_evals, r.fcp, r.dis), (Some(95), Some(15), None, 2000, Some(3), None, false));
    assert!(r.line.as_deref().unwrap().starts_with("e5e6 "));
    let r = &m[POS_D];
    assert_eq!((r.source.as_str(), r.cp, r.eval_cp, r.tier.as_deref(), r.n_tier, r.n, r.depth), ("fishnet", Some(30), 30, Some("classical"), Some(4), Some(6), None));
    let r = &m[POS_E];
    assert_eq!((r.cp, r.tier.as_deref(), r.n_tier, r.n), (Some(10), Some("nnue"), Some(2), Some(2)));
    let r = &m[POS_R];
    assert_eq!((r.cp, r.line.as_deref()), (Some(10), Some("e2e3 e8e7")), "the first PV even when it is not the best");
    let r = &m[POS_Q];
    assert_eq!((r.in_book.as_str(), r.source.as_str(), r.cp, r.mate, r.eval_cp, r.fmate), ("child", "fishnet", None, Some(0), -2000, Some(0)));
    let r = &m[pin.as_str()];
    assert_eq!((r.hash, r.in_book.as_str(), r.cp), (hash(&pinned()), "parent", Some(-5)));
    assert!(m.values().all(|r| !r.amb));
    // Manifest, coverage and meta.
    let meta: Value = serde_json::from_str(&std::fs::read_to_string(out.join("_build.meta.json")).unwrap()).unwrap();
    let t = &meta["totals"];
    assert_eq!((t["rows"].as_u64(), t["parents"].as_u64(), t["children"].as_u64(), t["ep_variant_rows"].as_u64(), t["fishnet_disagrees"].as_u64()),
               (Some(8), Some(7), Some(1), Some(1), Some(2)));
    assert_eq!(t["book_parents"].as_u64(), Some(8));
    assert_eq!(t["book_parents_with_eval"].as_u64(), Some(7));
    let ce = &meta["phase_e"]["cloud"];
    assert_eq!((ce["order_bad"].as_u64(), ce["runs"].as_u64()), (Some(1), Some(6)));
    assert_eq!(meta["phase_e"]["fishnet"]["fails"]["fen_parse"].as_u64(), Some(1));
    let cov = ParquetRecordBatchReaderBuilder::try_new(File::open(out.join("_coverage.parquet")).unwrap()).unwrap().build().unwrap();
    let mut got = BTreeMap::new();
    for b in cov {
        let b = b.unwrap();
        for j in 0..b.num_rows() {
            let v: Vec<i64> = (1..5).map(|i| b.column(i).as_primitive::<Int64Type>().value(j)).collect();
            got.insert(b.column(0).as_primitive::<Int32Type>().value(j), v);
        }
    }
    // ply 30: EX1 (3 + 5 games), EX2 (2), P (1); EX1 again at 28; X (no eval) at 4.
    assert_eq!(got[&30], vec![3, 3, 11, 11]);
    assert_eq!(got[&28], vec![1, 1, 1, 1]);
    assert_eq!(got[&4], vec![2, 1, 12, 10]);
    assert_eq!(got[&2], vec![1, 1, 27, 27]);
    let man = ParquetRecordBatchReaderBuilder::try_new(File::open(out.join("_manifest.parquet")).unwrap()).unwrap().build().unwrap();
    let rows: usize = man.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, meta["buckets"].as_array().map_or(0, Vec::len));
}

#[test]
fn end_to_end_resume_lock_determinism() {
    let c = build_case("e2e");
    let o = run(&c, "out", "work", &[]);
    assert!(o.status.success(), "{}", log(&o));
    assert_expected(&c.dir.join("out"));
    let base = out_files(&c.dir.join("out"));

    // The lock: other settings refuse (exit 5); a plain rerun is a no-op.
    let o = run(&c, "out", "work", &["--mem-gb", "3"]);
    assert_eq!(o.status.code(), Some(5), "{}", log(&o));
    let o = run(&c, "out", "work", &[]);
    assert!(o.status.success() && log(&o).contains("_DONE exists"), "{}", log(&o));

    // Phase J refuses before E and C are done.
    let o = run(&c, "out_j", "work_j", &["--phases", "j"]);
    assert_eq!(o.status.code(), Some(5), "{}", log(&o));

    // Threads: byte-identical output.
    let o = run(&c, "out_t1", "work_t1", &["--threads", "1"]);
    assert!(o.status.success(), "{}", log(&o));
    let t1 = out_files(&c.dir.join("out_t1"));
    assert_eq!(t1.keys().collect::<Vec<_>>(), base.keys().collect::<Vec<_>>());
    assert!(t1 == base, "--threads 1 and 2 differ");

    // Resume after a kill in each phase: identical output, nothing left over.
    let b1 = bucket_of(canon(EX1), 512);
    for (i, at) in [format!("e:1"), format!("e:3"), "c:0".into(), format!("j:{b1}"), format!("j-publish:{b1}")].iter().enumerate() {
        let (out, work) = (format!("out_r{i}"), format!("work_r{i}"));
        let o = run(&c, &out, &work, &["--test-crash-at", at]);
        assert_eq!(o.status.code(), Some(86), "{at}: {}", log(&o));
        assert!(!c.dir.join(&out).join("_DONE").exists());
        let o = run(&c, &out, &work, &[]);
        assert!(o.status.success(), "{at}: {}", log(&o));
        assert!(out_files(&c.dir.join(&out)) == base, "{at}: resumed output differs");
        for e in std::fs::read_dir(c.dir.join(&work).join("e")).unwrap() {
            assert!(!e.unwrap().file_name().to_string_lossy().starts_with("_tmp_"), "{at}: a _tmp_ unit is left");
        }
        assert!(std::fs::read_dir(c.dir.join(&out)).unwrap().all(|e| !e.unwrap().file_name().to_string_lossy().ends_with(".tmp")));
    }
    let _ = evals::BUCKETS;
}
