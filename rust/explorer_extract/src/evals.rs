//! `evals`: the two Lichess eval datasets -> one eval per position of the
//! banded explorer book (the blog repo's docs/eval-db-spec.md is the contract;
//! the output's README.md, `evals_readme.md` here, is the read contract).
//!
//! INPUT  --book     the merged book (ps/event=E/elo_band=B/bkt<iii>.parquet,
//!                   _BOOK.DONE), read-only;
//!        --cloud    Lichess/chess-position-evaluations: 4-field `fen`, one row
//!                   per PV, a position's rows contiguous, PVs best-first;
//!        --fishnet  Lichess/fishnet-evals, standard_rated_YYYY_MM.parquet: one
//!                   row per position occurrence per analysed game.
//! OUTPUT --out      bkt<iii>.parquet sorted by (position_hash, epd), plus
//!                   _manifest, _ambiguous, _coverage, _build.meta.json,
//!                   README.md and _DONE written LAST.
//!
//! POSITION IDENTITY (the book's own functions): shakmaty parses the FEN
//! (standard castling, strict); `epd` = `chesspos::pack(..).render()`, the
//! legal-ep EPD; the hash is `chesspos::hash`, polyglot with PseudoLegal ep.
//! A position that could carry an ep square whose capture is pseudo-legal but
//! illegal (a pinned capturer) is the same position under a second hash; the
//! eval is emitted under every such hash ("ep variants").
//!
//! PHASE E (per source file): parse, hash, route to 512 bucket shards under
//!        <work>/e/<unit>/; cloud keeps one row per contiguous block of one
//!        eval's PVs (its first PV), fishnet one row per (hash, EPD, score).
//! PHASE C (per group of book buckets): the distinct book child_hash values,
//!        routed to <work>/c/g<ggg>/bkt<iii>.parquet by bucket_of(child_hash).
//! PHASE J (per output bucket): reduce the bucket's evals (cloud: max depth,
//!        max knodes, first row in file order; fishnet: the newest tier's lower
//!        median), stream the book's parents for exact (hash, EPD) matches,
//!        match the rest against child hashes that are not parent hashes,
//!        write, re-read, and write the sentinel.
//!
//! Every unit is resumable: a unit is done iff its sentinel exists; an
//! unfinished unit's files are deleted on start. `_evals_params.json` locks the
//! paths, threads, memory budget, bucket sets, source files and the build.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtOrd};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
};
use arrow::array::RecordBatchReader;
use arrow::datatypes::{DataType, Field, Int16Type, Int32Type, Int64Type, Int8Type, Schema, SchemaRef};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use shakmaty::fen::Fen;
use shakmaty::{
    attacks, zobrist::Zobrist64, CastlingMode, Chess, Color, EnPassantMode, File as BFile, FromSetup,
    Position, PositionErrorKinds, Rank, Role, Setup, Square,
};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::chesspos::{hash as pos_hash, pack, Packed, PACKED_BYTES};
use crate::fasthash::{FastMap, FastSet};
use crate::keys::{bucket_of, rename_retry, tmp_of};
use crate::merge::{
    fmt_n, footer_rows, read_json, rm_file, rmtree, utc_now, write_json_atomic, Exit, EXIT_FAIL,
    EXIT_REFUSED,
};
use crate::sys;

pub const BUCKETS: u32 = 512;
pub const SCHEMA: &str = "eval-db-v1";
pub const LOCK_FILE: &str = "_evals_params.json";
/// Per-bucket sentinels. Not `_done`: NTFS is case-insensitive and `_DONE`
/// (written last) sits in the same directory.
pub const J_DONE_DIR: &str = "_bucket_done";
pub const EVAL_CAP: i32 = 2000;
pub const ZSTD_LEVEL: i32 = 3;
pub const OUT_ROW_GROUP: usize = 1 << 20;
/// Not enough free commit to start: nothing was done, retry later.
pub const EXIT_COMMIT: u8 = 6;

/// Score keys: White-POV and totally ordered. A cp value is itself; a mate
/// sits beyond every cp, a shorter mate further out; mate 0 (the side to move
/// is mated) is the extreme on the mated side's end.
pub const MATE_BASE: i32 = 1 << 24;
const SCORE_LIMIT: i64 = 1 << 20;

pub const TIERS: [&str; 3] = ["nnue", "classical", "early"];
/// fishnet switched to SF12 NNUE on 2020-12-17: 2021-01 is the first NNUE month.
pub const NNUE_FROM: (i32, u32) = (2021, 1);
pub const CLASSICAL_FROM: (i32, u32) = (2016, 1);
/// fishnet_disagrees: the old DB's fishnet-decisive replicate floor.
pub const DECISIVE_MIN_N: u64 = 5;

const READ_BATCH: usize = 65_536;
const OUT_BATCH: usize = 65_536;

fn refused(msg: impl Into<String>) -> anyhow::Error {
    Exit { code: EXIT_REFUSED, msg: msg.into() }.into()
}

/// The exit status for an error from `run`: 5 refused, 6 short of commit, else 1.
pub fn exit_code(e: &anyhow::Error) -> u8 {
    e.chain().find_map(|c| c.downcast_ref::<Exit>()).map_or(EXIT_FAIL, |x| x.code)
}

// ── position identity ────────────────────────────────────────────────────────

/// Why a source row was skipped. Rows are counted under the first reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fail {
    Fen,
    Castling,
    EpSquare,
    MissingKing,
    TooManyKings,
    PawnsOnBackrank,
    OppositeCheck,
    ImpossibleCheck,
    TooMuchMaterial,
    Illegal,
    Score,
    Depth,
    Knodes,
    Line,
}

pub const FAIL_NAMES: [&str; 14] = [
    "fen_parse",
    "castling",
    "ep_square",
    "missing_king",
    "too_many_kings",
    "pawns_on_backrank",
    "opposite_check",
    "impossible_check",
    "too_much_material",
    "illegal_other",
    "score",
    "depth",
    "knodes",
    "line",
];

impl Fail {
    pub fn index(self) -> usize {
        self as usize
    }
}

fn fail_of(k: PositionErrorKinds) -> Fail {
    for (flag, f) in [
        (PositionErrorKinds::INVALID_CASTLING_RIGHTS, Fail::Castling),
        (PositionErrorKinds::INVALID_EP_SQUARE, Fail::EpSquare),
        (PositionErrorKinds::MISSING_KING, Fail::MissingKing),
        (PositionErrorKinds::TOO_MANY_KINGS, Fail::TooManyKings),
        (PositionErrorKinds::PAWNS_ON_BACKRANK, Fail::PawnsOnBackrank),
        (PositionErrorKinds::OPPOSITE_CHECK, Fail::OppositeCheck),
        (PositionErrorKinds::IMPOSSIBLE_CHECK, Fail::ImpossibleCheck),
        (PositionErrorKinds::TOO_MUCH_MATERIAL, Fail::TooMuchMaterial),
    ] {
        if k.contains(flag) {
            return f;
        }
    }
    Fail::Illegal
}

/// A position's hashes: the canonical one first (the position its legal EPD
/// describes), then each distinct ep-variant hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hashes {
    n: u8,
    h: [i64; 9],
}

impl Hashes {
    fn one(h: i64) -> Hashes {
        let mut a = [0i64; 9];
        a[0] = h;
        Hashes { n: 1, h: a }
    }

    fn push_unique(&mut self, h: i64) {
        if !self.as_slice().contains(&h) {
            self.h[self.n as usize] = h;
            self.n += 1;
        }
    }

    pub fn as_slice(&self) -> &[i64] {
        &self.h[..self.n as usize]
    }

    pub fn canonical(&self) -> i64 {
        self.h[0]
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Ident {
    pub packed: Packed,
    pub hashes: Hashes,
}

impl Ident {
    pub fn white(&self) -> bool {
        self.packed.white_to_move()
    }
}

/// Parse one source FEN (4 or 6 fields) the way the book sees positions.
///
/// The canonical hash is the book's hash of the position its legal EPD
/// describes: polyglot with the ep file only if the capture is legal, which is
/// `zobrist(Legal)` of the parsed position (the PseudoLegal hash of that
/// position once an illegal ep square is dropped). A variant sets an ep square
/// the position could carry -- an enemy pawn that could just have
/// double-pushed, its skipped and origin squares empty -- and is kept only if
/// shakmaty accepts it and its legal EPD is byte-identical (Packed-equal): the
/// capture is then pseudo-legal but illegal, and `chesspos::hash` keeps the
/// file where the EPD cannot show it. A candidate with no pseudo-legal capture
/// has the canonical hash and adds nothing.
pub fn identify(fen: &[u8]) -> std::result::Result<Ident, Fail> {
    let f = Fen::from_ascii(fen).map_err(|_| Fail::Fen)?;
    let setup: Setup = f.into();
    let pos = Chess::from_setup(setup, CastlingMode::Standard).map_err(|e| fail_of(e.kinds()))?;
    let packed = pack(&pos);
    let canon: Zobrist64 = pos.zobrist_hash(EnPassantMode::Legal);
    let mut hashes = Hashes::one(canon.0 as i64);
    if pos.legal_ep_square().is_none() {
        let turn = pos.turn();
        let theirs = pos.board().pawns() & pos.board().by_color(!turn);
        let ours = pos.our(Role::Pawn);
        let occ = pos.board().occupied();
        let (to_r, ep_r, from_r) = if turn == Color::White {
            (Rank::Fifth, Rank::Sixth, Rank::Seventh)
        } else {
            (Rank::Fourth, Rank::Third, Rank::Second)
        };
        for file in BFile::ALL {
            let to = Square::from_coords(file, to_r);
            let ep = Square::from_coords(file, ep_r);
            let from = Square::from_coords(file, from_r);
            if !theirs.contains(to) || occ.contains(ep) || occ.contains(from) {
                continue;
            }
            if (attacks::pawn_attacks(!turn, ep) & ours).is_empty() {
                continue;
            }
            let mut s = pos.to_setup(EnPassantMode::Legal);
            s.ep_square = Some(ep);
            let Ok(v) = Chess::from_setup(s, CastlingMode::Standard) else { continue };
            if pack(&v) == packed {
                hashes.push_unique(pos_hash(&v));
            }
        }
    }
    Ok(Ident { packed, hashes })
}

// ── scores and the choice rules ─────────────────────────────────────────────

/// The White-POV sort key of (cp, mate). Exactly one must be present.
pub fn score_key(cp: Option<i64>, mate: Option<i64>, white_to_move: bool) -> std::result::Result<i32, Fail> {
    match (cp, mate) {
        (Some(c), None) if c.abs() < SCORE_LIMIT => Ok(c as i32),
        (None, Some(0)) => Ok(if white_to_move { -MATE_BASE } else { MATE_BASE }),
        (None, Some(m)) if m > 0 && m < SCORE_LIMIT => Ok(MATE_BASE - m as i32),
        (None, Some(m)) if m < 0 && -m < SCORE_LIMIT => Ok(-MATE_BASE - m as i32),
        _ => Err(Fail::Score),
    }
}

/// (cp, mate) back from a key. Mate 0 comes back as mate 0.
pub fn key_cp_mate(k: i32) -> (Option<i32>, Option<i32>) {
    let lim = SCORE_LIMIT as i32;
    if k >= MATE_BASE - lim {
        (None, Some(MATE_BASE - k))
    } else if k <= -MATE_BASE + lim {
        (None, Some(-MATE_BASE - k))
    } else {
        (Some(k), None)
    }
}

/// The old DB's scale: cp clamped to +-2000, mate -> sign * 2000, mate 0 by the
/// mated side (the key already carries it).
pub fn eval_cp(k: i32) -> i16 {
    let lim = SCORE_LIMIT as i32;
    if k >= MATE_BASE - lim {
        EVAL_CAP as i16
    } else if k <= -MATE_BASE + lim {
        -(EVAL_CAP as i16)
    } else {
        k.clamp(-EVAL_CAP, EVAL_CAP) as i16
    }
}

/// The old DB's `fishnet-decisive` condition (build_fishnet_eval_db.py): the
/// fishnet median saturated at +-2000 with >= 5 best-tier replicates, and
/// `sign(fishnet) != sign(eval_cp)` -- DuckDB's `!=`, so a cloud 0 against a
/// saturated fishnet counts. The caller applies it only to cloud-sourced rows.
pub fn fishnet_disagrees(chosen_eval_cp: i16, fish_key: i32, fish_n_tier: u64) -> bool {
    let f = eval_cp(fish_key);
    i32::from(f).abs() == EVAL_CAP && fish_n_tier >= DECISIVE_MIN_N && f.signum() != chosen_eval_cp.signum()
}

pub fn tier_of(y: i32, m: u32) -> u8 {
    if (y, m) >= NNUE_FROM {
        0
    } else if (y, m) >= CLASSICAL_FROM {
        1
    } else {
        2
    }
}

/// The lower median of a multiset given as ascending (key, count) pairs:
/// the element at 0-based rank (n - 1) / 2, always an observed value.
pub fn lower_median(sorted: &[(i32, u64)]) -> Option<i32> {
    let n: u64 = sorted.iter().map(|x| x.1).sum();
    if n == 0 {
        return None;
    }
    let want = (n - 1) / 2;
    let mut seen = 0u64;
    for &(k, c) in sorted {
        seen += c;
        if seen > want {
            return Some(k);
        }
    }
    unreachable!()
}

/// One fishnet position's reduction: the best tier present, its lower median,
/// its count, and the count over all tiers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FishPick {
    pub tier: u8,
    pub key: i32,
    pub n_tier: u64,
    pub n: u64,
}

/// `rows` are (tier, key, count) in any order.
pub fn fish_pick(rows: &[(u8, i32, u64)]) -> Option<FishPick> {
    let best = rows.iter().map(|r| r.0).min()?;
    let n = rows.iter().map(|r| r.2).sum();
    let mut v: Vec<(i32, u64)> = rows.iter().filter(|r| r.0 == best).map(|r| (r.1, r.2)).collect();
    v.sort_unstable();
    let n_tier = v.iter().map(|x| x.1).sum();
    Some(FishPick { tier: best, key: lower_median(&v)?, n_tier, n })
}

/// A cloud eval block (one eval's PVs), as its first row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudCand {
    pub depth: i16,
    pub knodes: i64,
    pub key: i32,
    pub line: String,
    pub file: u32,
    pub row: u64,
    /// PVs in the block, and whether its first PV is not the best score for
    /// the side to move among them (the row-order check).
    pub npv: u32,
    pub bad: bool,
}

/// The owner's rule: max depth, then max knodes, then the first row in file
/// order ((file, row)).
pub fn cloud_better(a: &CloudCand, b: &CloudCand) -> bool {
    (a.depth, a.knodes, Reverse((a.file, a.row))) > (b.depth, b.knodes, Reverse((b.file, b.row)))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudPick {
    pub cand: CloudCand,
    /// Distinct (depth, knodes) among the position's rows.
    pub n_evals: u32,
}

pub fn cloud_pick(cands: &[CloudCand]) -> Option<CloudPick> {
    let mut best: Option<&CloudCand> = None;
    for c in cands {
        if best.is_none_or(|b| cloud_better(c, b)) {
            best = Some(c);
        }
    }
    let mut pairs: Vec<(i16, i64)> = cands.iter().map(|c| (c.depth, c.knodes)).collect();
    pairs.sort_unstable();
    pairs.dedup();
    Some(CloudPick { cand: best?.clone(), n_evals: pairs.len() as u32 })
}

/// The chosen eval of one position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chosen {
    pub source: &'static str,
    pub key: i32,
    pub eval_cp: i16,
    pub disagrees: bool,
}

/// Cloud when there is one, else fishnet.
pub fn choose(cloud: Option<&CloudPick>, fish: Option<&FishPick>) -> Option<Chosen> {
    match (cloud, fish) {
        (Some(c), f) => {
            let e = eval_cp(c.cand.key);
            Some(Chosen {
                source: "cloud",
                key: c.cand.key,
                eval_cp: e,
                disagrees: f.is_some_and(|f| fishnet_disagrees(e, f.key, f.n_tier)),
            })
        }
        (None, Some(f)) => Some(Chosen { source: "fishnet", key: f.key, eval_cp: eval_cp(f.key), disagrees: false }),
        (None, None) => None,
    }
}

// ── configuration ────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashPhase {
    E,
    C,
    J,
    JPublish,
}

pub fn parse_crash(s: &str) -> Result<(CrashPhase, u32)> {
    let (p, n) = s.split_once(':').ok_or_else(|| anyhow!("--test-crash-at wants PHASE:N"))?;
    let ph = match p {
        "e" => CrashPhase::E,
        "c" => CrashPhase::C,
        "j" => CrashPhase::J,
        "j-publish" => CrashPhase::JPublish,
        _ => bail!("--test-crash-at: unknown phase {p:?}"),
    };
    Ok((ph, n.parse()?))
}

pub struct Config {
    pub book: PathBuf,
    pub cloud: PathBuf,
    pub fishnet: PathBuf,
    pub work: PathBuf,
    pub out: PathBuf,
    pub threads: usize,
    pub mem_gb: f64,
    /// Output buckets (locked): all 512 for the full build.
    pub buckets: Vec<u32>,
    /// Book buckets whose child_hash Phase C reads (locked): all 512 for the
    /// full build. A subset gives a pilot whose child-only matches are partial.
    pub child_sources: Vec<u32>,
    /// Phases to run in this invocation (e, c, j); finalize follows J.
    pub phases: (bool, bool, bool),
    /// Test only: no _DOWNLOAD.DONE / .ok markers / _BOOK.DONE / commit check.
    pub test_inputs: bool,
    pub crash_at: Option<(CrashPhase, u32)>,
}

impl Config {
    fn mem_bytes(&self) -> u64 {
        (self.mem_gb * 1e9) as u64
    }

    /// Book buckets per Phase C group: ~1.2 GB of distinct child hashes each,
    /// up to half the budget.
    fn child_group(&self) -> usize {
        ((self.mem_gb * 0.5 / 1.2) as usize).clamp(1, 16)
    }
}

#[derive(Clone, Debug)]
pub struct SrcFile {
    pub name: String,
    pub path: PathBuf,
    pub bytes: u64,
    /// fishnet only: (year, month)
    pub ym: Option<(i32, u32)>,
}

fn list_sources(dir: &Path, fishnet: bool, need_ok: bool) -> Result<Vec<SrcFile>> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".parquet") || !e.file_type()?.is_file() {
            continue;
        }
        let ym = if fishnet {
            let stem = name.trim_end_matches(".parquet");
            let rest = stem
                .strip_prefix("standard_rated_")
                .ok_or_else(|| refused(format!("{name}: not standard_rated_YYYY_MM.parquet")))?;
            let (y, m) = rest.split_once('_').ok_or_else(|| refused(format!("{name}: no _MM")))?;
            Some((y.parse::<i32>()?, m.parse::<u32>()?))
        } else {
            None
        };
        if need_ok && !dir.join(format!("{name}.ok")).is_file() {
            return Err(refused(format!("{}: no {name}.ok marker (not sha256-verified)", dir.display())));
        }
        v.push(SrcFile { name, path: e.path(), bytes: e.metadata()?.len(), ym });
    }
    v.sort_by(|a, b| a.name.cmp(&b.name));
    if v.is_empty() {
        return Err(refused(format!("no .parquet files in {}", dir.display())));
    }
    Ok(v)
}

/// A book bucket's ps files, sorted by path.
fn book_bucket_files(book: &Path, b: u32) -> Result<Vec<PathBuf>> {
    let name = format!("bkt{b:03}.parquet");
    let mut v = Vec::new();
    let ps = book.join("ps");
    for ev in std::fs::read_dir(&ps).with_context(|| format!("listing {}", ps.display()))? {
        let ev = ev?;
        if !ev.file_type()?.is_dir() {
            continue;
        }
        for band in std::fs::read_dir(ev.path())? {
            let band = band?;
            let p = band.path().join(&name);
            if band.file_type()?.is_dir() && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    Ok(v)
}

fn unit_name(src: &str, f: &SrcFile) -> String {
    format!("{src}_{}", f.name.trim_end_matches(".parquet"))
}

pub fn sha256_file(p: &Path) -> Result<String> {
    let mut f = File::open(p).with_context(|| format!("opening {}", p.display()))?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 8 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(h.finalize().iter().map(|b| format!("{b:02x}")).collect())
}

fn describe(v: &[u32]) -> Value {
    if v.len() == BUCKETS as usize && v.iter().enumerate().all(|(i, &b)| b == i as u32) {
        json!("all")
    } else {
        json!(v)
    }
}

pub struct Plan {
    pub cloud: Vec<SrcFile>,
    pub fishnet: Vec<SrcFile>,
    pub groups: Vec<Vec<u32>>,
    pub lock: Value,
    pub want: Vec<bool>,
}

fn plan(cfg: &Config) -> Result<Plan> {
    if !cfg.test_inputs {
        let root = cfg.cloud.parent().ok_or_else(|| anyhow!("--cloud has no parent"))?;
        if !root.join("_DOWNLOAD.DONE").is_file() {
            return Err(refused(format!("{} has no _DOWNLOAD.DONE", root.display())));
        }
        if !cfg.book.join("_BOOK.DONE").is_file() {
            return Err(refused(format!("{} has no _BOOK.DONE", cfg.book.display())));
        }
    }
    for (name, p) in [("--work", &cfg.work), ("--out", &cfg.out)] {
        if sys::volume_root(p).is_some_and(|v| v.starts_with("f:")) {
            return Err(refused(format!("{name} {} is on F: (never F:)", p.display())));
        }
    }
    let cloud = list_sources(&cfg.cloud, false, !cfg.test_inputs)?;
    let fishnet = list_sources(&cfg.fishnet, true, !cfg.test_inputs)?;
    let groups: Vec<Vec<u32>> = cfg.child_sources.chunks(cfg.child_group()).map(|c| c.to_vec()).collect();
    let mut want = vec![false; BUCKETS as usize];
    for &b in &cfg.buckets {
        want[b as usize] = true;
    }
    let files = |v: &[SrcFile]| v.iter().map(|f| json!([f.name, f.bytes])).collect::<Vec<_>>();
    let lock = json!({
        "schema": SCHEMA,
        "book": cfg.book.display().to_string(),
        "cloud": cfg.cloud.display().to_string(),
        "fishnet": cfg.fishnet.display().to_string(),
        "work": cfg.work.display().to_string(),
        "out": cfg.out.display().to_string(),
        "threads": cfg.threads,
        "mem_gb": cfg.mem_gb,
        "buckets": describe(&cfg.buckets),
        "child_sources": describe(&cfg.child_sources),
        "child_group": cfg.child_group(),
        "cloud_files": files(&cloud),
        "fishnet_files": files(&fishnet),
        "tool": {"version": sys::VERSION, "commit": sys::GIT_COMMIT, "built": sys::BUILD_DATE},
    });
    Ok(Plan { cloud, fishnet, groups, lock, want })
}

fn check_lock(dir: &Path, want: &Value) -> Result<()> {
    let p = dir.join(LOCK_FILE);
    if p.exists() {
        let have = read_json(&p)?;
        if &have != want {
            let mut diff = Vec::new();
            if let (Some(a), Some(b)) = (have.as_object(), want.as_object()) {
                let keys: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
                for k in keys {
                    if a.get(k) != b.get(k) {
                        let s = |v: Option<&Value>| {
                            let t = v.map_or("(absent)".to_string(), |v| v.to_string());
                            if t.chars().count() > 200 { format!("{}...", t.chars().take(200).collect::<String>()) } else { t }
                        };
                        diff.push(format!("{k}: locked {} / this run {}", s(a.get(k)), s(b.get(k))));
                    }
                }
            }
            return Err(refused(format!(
                "{} records different settings; a build is never resumed with other settings or \
                 another build of the tool (delete --work and --out to start over):\n  {}",
                p.display(),
                diff.join("\n  ")
            )));
        }
        return Ok(());
    }
    write_json_atomic(&p, want)
}

// ── parquet plumbing ─────────────────────────────────────────────────────────

fn cloud_shard_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("position_hash", DataType::Int64, false),
        Field::new("pos", DataType::FixedSizeBinary(PACKED_BYTES as i32), false),
        Field::new("variant", DataType::Boolean, false),
        Field::new("depth", DataType::Int16, false),
        Field::new("knodes", DataType::Int64, false),
        Field::new("skey", DataType::Int32, false),
        Field::new("line", DataType::Utf8, false),
        Field::new("file", DataType::Int32, false),
        Field::new("row", DataType::Int64, false),
        Field::new("npv", DataType::Int32, false),
        Field::new("bad", DataType::Boolean, false),
    ]))
}

fn fish_shard_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("position_hash", DataType::Int64, false),
        Field::new("pos", DataType::FixedSizeBinary(PACKED_BYTES as i32), false),
        Field::new("variant", DataType::Boolean, false),
        Field::new("tier", DataType::Int8, false),
        Field::new("skey", DataType::Int32, false),
        Field::new("n", DataType::Int64, false),
    ]))
}

fn child_shard_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("child_hash", DataType::Int64, false)]))
}

/// The output schema (docs/eval-db-spec.md section 6).
pub fn out_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("position_hash", DataType::Int64, false),
        Field::new("epd", DataType::Utf8, false),
        Field::new("in_book", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("cp", DataType::Int32, true),
        Field::new("mate", DataType::Int32, true),
        Field::new("eval_cp", DataType::Int16, false),
        Field::new("cloud_depth", DataType::Int16, true),
        Field::new("cloud_knodes", DataType::Int64, true),
        Field::new("cloud_cp", DataType::Int32, true),
        Field::new("cloud_mate", DataType::Int32, true),
        Field::new("cloud_line", DataType::Utf8, true),
        Field::new("cloud_n_evals", DataType::Int32, true),
        Field::new("fishnet_cp", DataType::Int32, true),
        Field::new("fishnet_mate", DataType::Int32, true),
        Field::new("fishnet_tier", DataType::Utf8, true),
        Field::new("fishnet_n_tier", DataType::Int32, true),
        Field::new("fishnet_n", DataType::Int32, true),
        Field::new("hash_ambiguous", DataType::Boolean, false),
        Field::new("fishnet_disagrees", DataType::Boolean, false),
    ]))
}

/// Shards are transient: zstd 1 for speed.
fn shard_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).expect("zstd level")))
        .set_max_row_group_row_count(Some(OUT_ROW_GROUP))
        .build()
}

pub fn out_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL).expect("zstd level")))
        .set_max_row_group_row_count(Some(OUT_ROW_GROUP))
        .build()
}

fn write_batches(
    path: &Path,
    schema: &SchemaRef,
    props: WriterProperties,
    batches: impl Iterator<Item = Result<RecordBatch>>,
) -> Result<u64> {
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d)?;
    }
    let f = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut w = ArrowWriter::try_new(f, schema.clone(), Some(props))?;
    for b in batches {
        w.write(&b?)?;
    }
    w.close()?;
    Ok(std::fs::metadata(path)?.len())
}

fn fsb(vals: impl Iterator<Item = [u8; PACKED_BYTES]>, n: usize) -> Result<FixedSizeBinaryArray> {
    let mut b = FixedSizeBinaryBuilder::with_capacity(n, PACKED_BYTES as i32);
    for v in vals {
        b.append_value(v)?;
    }
    Ok(b.finish())
}

fn open_reader(path: &Path, cols: &[&str], batch: usize) -> Result<ParquetRecordBatchReader> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let b = ParquetRecordBatchReaderBuilder::try_new(f)
        .with_context(|| format!("reading the footer of {}", path.display()))?;
    let idx = cols
        .iter()
        .map(|c| b.schema().index_of(c).with_context(|| format!("{}: no column {c}", path.display())))
        .collect::<Result<Vec<_>>>()?;
    let mask = ProjectionMask::roots(b.parquet_schema(), idx);
    Ok(b.with_projection(mask).with_batch_size(batch).build()?)
}

fn col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    b.column_by_name(name).ok_or_else(|| anyhow!("no column {name}"))
}

/// Any integer column (or a string of digits) as int64; unparseable -> NULL.
fn i64_col(b: &RecordBatch, name: &str) -> Result<Int64Array> {
    let c = col(b, name)?;
    Ok(if c.data_type() == &DataType::Int64 {
        c.as_primitive::<Int64Type>().clone()
    } else {
        arrow::compute::cast(c, &DataType::Int64)?.as_primitive::<Int64Type>().clone()
    })
}

fn str_col(b: &RecordBatch, name: &str) -> Result<StringArray> {
    let c = col(b, name)?;
    Ok(if c.data_type() == &DataType::Utf8 {
        c.as_string::<i32>().clone()
    } else {
        arrow::compute::cast(c, &DataType::Utf8)?.as_string::<i32>().clone()
    })
}

fn i32_col(b: &RecordBatch, name: &str) -> Result<Int32Array> {
    Ok(col(b, name)?.as_primitive_opt::<Int32Type>().ok_or_else(|| anyhow!("{name} is not int32"))?.clone())
}

// ── Phase E: sources -> bucket shards ────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub struct UnitStats {
    pub rows: u64,
    pub fails: [u64; FAIL_NAMES.len()],
    /// Positions parsed (cloud: contiguous runs; fishnet: distinct FEN4 per
    /// row group) and the ep-variant hashes they gained beyond the canonical one.
    pub parsed: u64,
    pub variants: u64,
    /// Rows whose non-NULL depth is not an integer.
    pub depth_cast: u64,
    /// Shard rows written, and rows routed to buckets outside --buckets.
    pub shard_rows: u64,
    pub dropped_bucket: u64,
    pub files: u64,
    pub bytes: u64,
    /// cloud: contiguous runs, eval blocks, and the row-order check over each
    /// run's chosen block: blocks with >= 2 PVs, and those whose first PV is
    /// not the best score for the side to move.
    pub runs: u64,
    pub blocks: u64,
    pub order_n: u64,
    pub order_bad: u64,
    /// cloud: runs whose FEN printed an ep square that the legal EPD drops.
    pub ep_dropped: u64,
    pub secs: f64,
}

impl UnitStats {
    fn add(&mut self, o: &UnitStats) {
        self.rows += o.rows;
        for i in 0..FAIL_NAMES.len() {
            self.fails[i] += o.fails[i];
        }
        self.parsed += o.parsed;
        self.variants += o.variants;
        self.depth_cast += o.depth_cast;
        self.shard_rows += o.shard_rows;
        self.dropped_bucket += o.dropped_bucket;
        self.files += o.files;
        self.bytes += o.bytes;
        self.runs += o.runs;
        self.blocks += o.blocks;
        self.order_n += o.order_n;
        self.order_bad += o.order_bad;
        self.ep_dropped += o.ep_dropped;
        self.secs += o.secs;
    }

    fn to_json(&self) -> Value {
        let fails: serde_json::Map<String, Value> =
            FAIL_NAMES.iter().zip(self.fails.iter()).map(|(n, v)| (n.to_string(), json!(v))).collect();
        json!({
            "rows": self.rows, "fails": fails, "parsed": self.parsed, "variants": self.variants,
            "depth_cast": self.depth_cast, "shard_rows": self.shard_rows,
            "dropped_bucket": self.dropped_bucket, "files": self.files, "bytes": self.bytes,
            "runs": self.runs, "blocks": self.blocks, "order_n": self.order_n,
            "order_bad": self.order_bad, "ep_dropped": self.ep_dropped, "secs": self.secs,
        })
    }

    fn from_json(v: &Value) -> UnitStats {
        let u = |k: &str| v.get(k).and_then(Value::as_u64).unwrap_or(0);
        let mut s = UnitStats {
            rows: u("rows"),
            parsed: u("parsed"),
            variants: u("variants"),
            depth_cast: u("depth_cast"),
            shard_rows: u("shard_rows"),
            dropped_bucket: u("dropped_bucket"),
            files: u("files"),
            bytes: u("bytes"),
            runs: u("runs"),
            blocks: u("blocks"),
            order_n: u("order_n"),
            order_bad: u("order_bad"),
            ep_dropped: u("ep_dropped"),
            secs: v.get("secs").and_then(Value::as_f64).unwrap_or(0.0),
            ..Default::default()
        };
        for (i, n) in FAIL_NAMES.iter().enumerate() {
            s.fails[i] = v["fails"].get(*n).and_then(Value::as_u64).unwrap_or(0);
        }
        s
    }
}

#[derive(Clone, Debug)]
struct CRow {
    hash: i64,
    pos: [u8; PACKED_BYTES],
    variant: bool,
    depth: i16,
    knodes: i64,
    key: i32,
    line: Box<str>,
    file: u32,
    row: u64,
    npv: u32,
    bad: bool,
}

#[derive(Clone, Copy, Debug)]
struct FRow {
    hash: i64,
    pos: [u8; PACKED_BYTES],
    variant: bool,
    tier: u8,
    key: i32,
    n: u64,
}

trait ShardRow: Send + Sized {
    const MEM: usize;
    fn mem(&self) -> usize {
        Self::MEM
    }
    /// Sort (and for fishnet, reduce) one bucket's rows before they are written.
    fn prepare(v: &mut Vec<Self>);
    fn batch(rows: &[Self]) -> Result<RecordBatch>;
    fn schema() -> SchemaRef;
}

impl ShardRow for CRow {
    const MEM: usize = 104;
    fn mem(&self) -> usize {
        Self::MEM + self.line.len()
    }
    fn prepare(v: &mut Vec<Self>) {
        v.sort_unstable_by(|a, b| (a.hash, a.pos, a.file, a.row).cmp(&(b.hash, b.pos, b.file, b.row)));
    }
    fn batch(rows: &[Self]) -> Result<RecordBatch> {
        let n = rows.len();
        Ok(RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.hash))),
                Arc::new(fsb(rows.iter().map(|r| r.pos), n)?),
                Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.variant)))),
                Arc::new(Int16Array::from_iter_values(rows.iter().map(|r| r.depth))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.knodes))),
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.key))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &*r.line))),
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.file as i32))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.row as i64))),
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.npv as i32))),
                Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.bad)))),
            ],
        )?)
    }
    fn schema() -> SchemaRef {
        cloud_shard_schema()
    }
}

impl ShardRow for FRow {
    const MEM: usize = 64;
    fn prepare(v: &mut Vec<Self>) {
        v.sort_unstable_by(|a, b| (a.hash, a.pos, a.tier, a.key).cmp(&(b.hash, b.pos, b.tier, b.key)));
        let mut w = 0usize;
        for i in 0..v.len() {
            if w > 0 {
                let (p, c) = (&v[w - 1], &v[i]);
                if (p.hash, p.pos, p.tier, p.key) == (c.hash, c.pos, c.tier, c.key) {
                    v[w - 1].n += v[i].n;
                    continue;
                }
            }
            v[w] = v[i];
            w += 1;
        }
        v.truncate(w);
    }
    fn batch(rows: &[Self]) -> Result<RecordBatch> {
        let n = rows.len();
        Ok(RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.hash))),
                Arc::new(fsb(rows.iter().map(|r| r.pos), n)?),
                Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.variant)))),
                Arc::new(Int8Array::from_iter_values(rows.iter().map(|r| r.tier as i8))),
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.key))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.n as i64))),
            ],
        )?)
    }
    fn schema() -> SchemaRef {
        fish_shard_schema()
    }
}

/// Per-bucket buffers flushed as `bkt<iii>.p<kkk>.parquet` parts once they
/// hold `cap` bytes.
struct Sink<'a, R: ShardRow> {
    dir: PathBuf,
    want: &'a [bool],
    bufs: Vec<Vec<R>>,
    parts: Vec<u32>,
    mem: usize,
    cap: usize,
    rows: u64,
    files: u64,
    bytes: u64,
    dropped: u64,
}

impl<'a, R: ShardRow> Sink<'a, R> {
    fn new(dir: PathBuf, want: &'a [bool], cap: usize) -> Self {
        Sink {
            dir,
            want,
            bufs: (0..BUCKETS).map(|_| Vec::new()).collect(),
            parts: vec![0; BUCKETS as usize],
            mem: 0,
            cap,
            rows: 0,
            files: 0,
            bytes: 0,
            dropped: 0,
        }
    }

    fn push(&mut self, hash: i64, r: R) {
        let b = bucket_of(hash, BUCKETS) as usize;
        if !self.want[b] {
            self.dropped += 1;
            return;
        }
        self.mem += r.mem();
        self.bufs[b].push(r);
    }

    fn full(&self) -> bool {
        self.mem >= self.cap
    }

    fn flush(&mut self) -> Result<()> {
        let dir = &self.dir;
        let parts = &self.parts;
        let res: Vec<Result<(usize, u64, u64)>> = self
            .bufs
            .par_iter_mut()
            .enumerate()
            .filter(|(_, v)| !v.is_empty())
            .map(|(b, v)| {
                R::prepare(v);
                let p = dir.join(format!("bkt{b:03}.p{:03}.parquet", parts[b]));
                let n = v.len() as u64;
                let bytes = write_batches(&p, &R::schema(), shard_props(), v.chunks(OUT_BATCH).map(R::batch))?;
                *v = Vec::new();
                Ok((b, n, bytes))
            })
            .collect();
        for r in res {
            let (b, n, bytes) = r?;
            self.parts[b] += 1;
            self.rows += n;
            self.files += 1;
            self.bytes += bytes;
        }
        self.mem = 0;
        Ok(())
    }
}

/// The 4-field prefix of a FEN (everything before the 4th space).
pub fn fen4(s: &[u8]) -> &[u8] {
    let mut sp = 0;
    for (i, &c) in s.iter().enumerate() {
        if c == b' ' {
            sp += 1;
            if sp == 4 {
                return &s[..i];
            }
        }
    }
    s
}

/// Whether a FEN prints an ep square (its 4th field is not "-").
fn fen_has_ep(s: &[u8]) -> bool {
    s.split(|&c| c == b' ').nth(3).is_some_and(|f| f != b"-")
}

struct CloudBlock {
    depth: i16,
    knodes: i64,
    key: i32,
    line: Box<str>,
    row: u64,
    npv: u32,
    /// The best score for the side to move among the block's PVs.
    best: i32,
}

/// Emit one contiguous run's blocks (one shard row per block per hash) and
/// count its row-order check.
fn finish_run(id: Option<Ident>, blocks: &mut Vec<CloudBlock>, file: u32, stats: &mut UnitStats, sink: &mut Sink<CRow>) {
    let Some(id) = id else {
        blocks.clear();
        return;
    };
    if blocks.is_empty() {
        return;
    }
    stats.runs += 1;
    stats.blocks += blocks.len() as u64;
    let mut ch = 0usize;
    for (i, b) in blocks.iter().enumerate() {
        if (b.depth, b.knodes) > (blocks[ch].depth, blocks[ch].knodes) {
            ch = i;
        }
    }
    if blocks[ch].npv >= 2 {
        stats.order_n += 1;
        stats.order_bad += u64::from(blocks[ch].key != blocks[ch].best);
    }
    let canon = id.hashes.canonical();
    let pos = id.packed.to_bytes();
    for b in blocks.drain(..) {
        for &h in id.hashes.as_slice() {
            sink.push(
                h,
                CRow {
                    hash: h,
                    pos,
                    variant: h != canon,
                    depth: b.depth,
                    knodes: b.knodes,
                    key: b.key,
                    line: b.line.clone(),
                    file,
                    row: b.row,
                    npv: b.npv,
                    bad: b.key != b.best,
                },
            );
        }
    }
}

/// One cloud file, in file order on one thread: rows of one position form a
/// contiguous run, a run's rows with one (depth, knodes) a block (one eval's
/// PVs, best first), and each block is kept as its first row.
fn cloud_unit(path: &Path, file_idx: u32, dir: &Path, want: &[bool], cap: usize) -> Result<UnitStats> {
    let t0 = Instant::now();
    let mut stats = UnitStats::default();
    let mut sink: Sink<CRow> = Sink::new(dir.to_path_buf(), want, cap);
    let rdr = open_reader(path, &["fen", "line", "depth", "knodes", "cp", "mate"], READ_BATCH)?;
    let mut cur: Option<Vec<u8>> = None;
    let mut ident: std::result::Result<Ident, Fail> = Err(Fail::Fen);
    let mut blocks: Vec<CloudBlock> = Vec::new();
    let mut in_block = false;
    let mut row: u64 = 0;
    for batch in rdr {
        let batch = batch?;
        let fen = str_col(&batch, "fen")?;
        let line = str_col(&batch, "line")?;
        let depth_raw = col(&batch, "depth")?.clone();
        let depth = i64_col(&batch, "depth")?;
        let knodes = i64_col(&batch, "knodes")?;
        let cp = i64_col(&batch, "cp")?;
        let mate = i64_col(&batch, "mate")?;
        for i in 0..batch.num_rows() {
            let r = row;
            row += 1;
            stats.rows += 1;
            let f = if fen.is_null(i) { &b""[..] } else { fen.value(i).as_bytes() };
            if cur.as_deref() != Some(f) {
                finish_run(ident.ok(), &mut blocks, file_idx, &mut stats, &mut sink);
                if sink.full() {
                    sink.flush()?;
                }
                cur = Some(f.to_vec());
                in_block = false;
                ident = identify(f);
                if let Ok(x) = &ident {
                    stats.parsed += 1;
                    stats.variants += x.hashes.as_slice().len() as u64 - 1;
                    if fen_has_ep(f) && x.packed.render().ends_with(" -") {
                        stats.ep_dropped += 1;
                    }
                }
            }
            let id = match &ident {
                Ok(id) => *id,
                Err(fl) => {
                    stats.fails[fl.index()] += 1;
                    continue;
                }
            };
            let skip = |stats: &mut UnitStats, fl: Fail| stats.fails[fl.index()] += 1;
            if depth.is_null(i) || !(0..=i64::from(i16::MAX)).contains(&depth.value(i)) {
                skip(&mut stats, Fail::Depth);
                stats.depth_cast += u64::from(depth.is_null(i) && !depth_raw.is_null(i));
                in_block = false;
                continue;
            }
            if knodes.is_null(i) {
                skip(&mut stats, Fail::Knodes);
                in_block = false;
                continue;
            }
            if line.is_null(i) {
                skip(&mut stats, Fail::Line);
                in_block = false;
                continue;
            }
            let c = (!cp.is_null(i)).then(|| cp.value(i));
            let m = (!mate.is_null(i)).then(|| mate.value(i));
            let key = match score_key(c, m, id.white()) {
                Ok(k) => k,
                Err(fl) => {
                    skip(&mut stats, fl);
                    in_block = false;
                    continue;
                }
            };
            let (d, kn) = (depth.value(i) as i16, knodes.value(i));
            match blocks.last_mut() {
                Some(b) if in_block && b.depth == d && b.knodes == kn => {
                    b.npv += 1;
                    b.best = if id.white() { b.best.max(key) } else { b.best.min(key) };
                }
                _ => blocks.push(CloudBlock { depth: d, knodes: kn, key, line: line.value(i).into(), row: r, npv: 1, best: key }),
            }
            in_block = true;
        }
    }
    finish_run(ident.ok(), &mut blocks, file_idx, &mut stats, &mut sink);
    sink.flush()?;
    stats.shard_rows = sink.rows;
    stats.files = sink.files;
    stats.bytes = sink.bytes;
    stats.dropped_bucket = sink.dropped;
    stats.secs = t0.elapsed().as_secs_f64();
    Ok(stats)
}

/// One fishnet row group: each distinct FEN4 parsed once, rows reduced to
/// (position, score) counts, then one shard row per hash.
fn fishnet_rg(path: &Path, meta: &ArrowReaderMetadata, rg: usize, tier: u8) -> Result<(Vec<FRow>, UnitStats)> {
    let mut stats = UnitStats::default();
    let f = File::open(path)?;
    let b = ParquetRecordBatchReaderBuilder::new_with_metadata(f, meta.clone());
    let idx = ["fen", "cp", "mate"]
        .iter()
        .map(|c| b.schema().index_of(c).with_context(|| format!("{}: no column {c}", path.display())))
        .collect::<Result<Vec<_>>>()?;
    let rows = meta.metadata().row_group(rg).num_rows() as usize;
    let mask = ProjectionMask::roots(b.parquet_schema(), idx);
    let rdr = b.with_projection(mask).with_row_groups(vec![rg]).with_batch_size(rows.max(1)).build()?;
    let mut out: Vec<FRow> = Vec::new();
    for batch in rdr {
        let batch = batch?;
        let fen = str_col(&batch, "fen")?;
        let cp = i64_col(&batch, "cp")?;
        let mate = i64_col(&batch, "mate")?;
        let n = batch.num_rows();
        stats.rows += n as u64;
        let mut seen: FastMap<&[u8], u32> = FastMap::default();
        let mut ids: Vec<std::result::Result<Ident, Fail>> = Vec::new();
        let mut recs: Vec<(u32, i32)> = Vec::with_capacity(n);
        for i in 0..n {
            let f = if fen.is_null(i) { &b""[..] } else { fen4(fen.value(i).as_bytes()) };
            let k = *seen.entry(f).or_insert_with(|| {
                let id = identify(f);
                if let Ok(x) = &id {
                    stats.parsed += 1;
                    stats.variants += x.hashes.as_slice().len() as u64 - 1;
                }
                ids.push(id);
                (ids.len() - 1) as u32
            });
            let id = match &ids[k as usize] {
                Ok(id) => id,
                Err(fl) => {
                    stats.fails[fl.index()] += 1;
                    continue;
                }
            };
            let c = (!cp.is_null(i)).then(|| cp.value(i));
            let m = (!mate.is_null(i)).then(|| mate.value(i));
            match score_key(c, m, id.white()) {
                Ok(key) => recs.push((k, key)),
                Err(fl) => stats.fails[fl.index()] += 1,
            }
        }
        recs.sort_unstable();
        let mut j = 0;
        while j < recs.len() {
            let mut e = j + 1;
            while e < recs.len() && recs[e] == recs[j] {
                e += 1;
            }
            let (k, key) = recs[j];
            let id = ids[k as usize].as_ref().expect("only parsed rows are recorded");
            let canon = id.hashes.canonical();
            let pos = id.packed.to_bytes();
            for &h in id.hashes.as_slice() {
                out.push(FRow { hash: h, pos, variant: h != canon, tier, key, n: (e - j) as u64 });
            }
            j = e;
        }
    }
    Ok((out, stats))
}

fn fishnet_unit(ctx: &Ctx, f: &SrcFile, dir: &Path) -> Result<UnitStats> {
    let t0 = Instant::now();
    let (y, m) = f.ym.expect("fishnet files carry (year, month)");
    let tier = tier_of(y, m);
    let meta = ArrowReaderMetadata::load(&File::open(&f.path)?, ArrowReaderOptions::new())
        .with_context(|| format!("reading the footer of {}", f.path.display()))?;
    let n_rg = meta.metadata().num_row_groups();
    let mut stats = UnitStats::default();
    // Half the budget for buffered shard rows; the rest covers the row groups
    // in flight and the flush.
    let mut sink: Sink<FRow> = Sink::new(dir.to_path_buf(), &ctx.plan.want, (ctx.cfg.mem_bytes() / 2) as usize);
    let rgs: Vec<usize> = (0..n_rg).collect();
    for chunk in rgs.chunks(ctx.cfg.threads.max(1) * 2) {
        let outs: Vec<Result<(Vec<FRow>, UnitStats)>> =
            chunk.par_iter().map(|&rg| fishnet_rg(&f.path, &meta, rg, tier)).collect();
        for o in outs {
            let (rows, st) = o?;
            stats.add(&st);
            for r in rows {
                sink.push(r.hash, r);
            }
        }
        if sink.full() {
            sink.flush()?;
        }
    }
    sink.flush()?;
    stats.shard_rows = sink.rows;
    stats.files = sink.files;
    stats.bytes = sink.bytes;
    stats.dropped_bucket = sink.dropped;
    stats.secs = t0.elapsed().as_secs_f64();
    Ok(stats)
}

// ── run context ──────────────────────────────────────────────────────────────

pub struct Ctx<'a> {
    pub cfg: &'a Config,
    pub plan: Plan,
    t0: Instant,
}

fn e_dir(cfg: &Config) -> PathBuf {
    cfg.work.join("e")
}

fn c_dir(cfg: &Config) -> PathBuf {
    cfg.work.join("c")
}

fn crash(cfg: &Config, ph: CrashPhase, n: u32) {
    if cfg.crash_at == Some((ph, n)) {
        eprintln!("test: crashing at {ph:?}:{n}");
        std::process::exit(86);
    }
}

fn log(ctx: &Ctx, msg: &str) {
    let (lim, avail) = sys::commit().unwrap_or((0, 0));
    eprintln!(
        "[{} +{:.0}s commit {:.1}/{:.1} GB] {msg}",
        utc_now(),
        ctx.t0.elapsed().as_secs_f64(),
        lim.saturating_sub(avail) as f64 / 1e9,
        lim as f64 / 1e9
    );
}

/// Delete every entry of `root` (but `_done`) that is not a finished unit.
fn clean_units(root: &Path) -> Result<()> {
    let done = root.join("_done");
    std::fs::create_dir_all(&done)?;
    for e in std::fs::read_dir(root)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        if name == "_done" {
            continue;
        }
        let unit = name.strip_prefix("_tmp_").unwrap_or(&name);
        if name.starts_with("_tmp_") || !done.join(format!("{unit}.json")).is_file() {
            if e.file_type()?.is_dir() {
                rmtree(&e.path())?;
            } else {
                rm_file(&e.path())?;
            }
        }
    }
    Ok(())
}

/// A unit's shards are built in `_tmp_<name>` and renamed into place; the
/// sentinel follows.
fn phase_e(ctx: &Ctx) -> Result<()> {
    let cfg = ctx.cfg;
    let root = e_dir(cfg);
    std::fs::create_dir_all(&root)?;
    clean_units(&root)?;
    let done = root.join("_done");
    let units: Vec<(String, &SrcFile, bool, u32)> = ctx
        .plan
        .cloud
        .iter()
        .enumerate()
        .map(|(i, f)| (unit_name("cloud", f), f, true, i as u32))
        .chain(ctx.plan.fishnet.iter().enumerate().map(|(i, f)| (unit_name("fishnet", f), f, false, i as u32)))
        .collect();
    let todo: Vec<usize> = (0..units.len()).filter(|&i| !done.join(format!("{}.json", units[i].0)).is_file()).collect();
    log(ctx, &format!("phase E: {} units ({} cloud, {} fishnet), {} to do", units.len(), ctx.plan.cloud.len(),
                      ctx.plan.fishnet.len(), todo.len()));
    let finish = |i: usize, st: &UnitStats| -> Result<()> {
        let (name, f, _, _) = &units[i];
        crash(cfg, CrashPhase::E, i as u32);
        rename_retry(&root.join(format!("_tmp_{name}")), &root.join(name))?;
        let mut v = st.to_json();
        v["unit"] = json!(name);
        v["source"] = json!(f.name);
        v["finished"] = json!(utc_now());
        write_json_atomic(&done.join(format!("{name}.json")), &v)?;
        log(ctx, &format!(
            "E {name}: {} rows, {} parsed, {} variants, {} failed, {} shard rows in {} files ({:.2} GB), {:.0}s ({:.2} M rows/s){}",
            fmt_n(st.rows), fmt_n(st.parsed), fmt_n(st.variants), fmt_n(st.fails.iter().sum()),
            fmt_n(st.shard_rows), st.files, st.bytes as f64 / 1e9, st.secs,
            st.rows as f64 / st.secs.max(1e-9) / 1e6,
            if st.runs > 0 { format!(", {} runs, order check {}/{}", fmt_n(st.runs), st.order_bad, fmt_n(st.order_n)) } else { String::new() }
        ));
        Ok(())
    };
    // Cloud: a file per task (runs must be read in file order), files in parallel.
    let cloud_todo: Vec<usize> = todo.iter().copied().filter(|&i| units[i].2).collect();
    let cap = (cfg.mem_bytes() / 2 / cfg.threads.max(1) as u64) as usize;
    let first_err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
    cloud_todo.par_iter().for_each(|&i| {
        if first_err.lock().unwrap().is_some() {
            return;
        }
        let (name, f, _, idx) = &units[i];
        let dir = root.join(format!("_tmp_{name}"));
        let r = std::fs::create_dir_all(&dir)
            .map_err(anyhow::Error::from)
            .and_then(|_| cloud_unit(&f.path, *idx, &dir, &ctx.plan.want, cap))
            .with_context(|| format!("cloud {}", f.name))
            .and_then(|st| finish(i, &st));
        if let Err(e) = r {
            first_err.lock().unwrap().get_or_insert(e);
        }
    });
    if let Some(e) = first_err.into_inner().unwrap() {
        return Err(e);
    }
    // Fishnet: a file at a time, its row groups in parallel.
    for &i in todo.iter().filter(|&&i| !units[i].2) {
        let (name, f, _, _) = &units[i];
        let dir = root.join(format!("_tmp_{name}"));
        std::fs::create_dir_all(&dir)?;
        let st = fishnet_unit(ctx, f, &dir).with_context(|| format!("fishnet {}", f.name))?;
        finish(i, &st)?;
    }
    Ok(())
}

// ── Phase C: book children -> bucket shards ─────────────────────────────────

fn phase_c(ctx: &Ctx) -> Result<()> {
    let cfg = ctx.cfg;
    let root = c_dir(cfg);
    std::fs::create_dir_all(&root)?;
    clean_units(&root)?;
    let done = root.join("_done");
    let groups = &ctx.plan.groups;
    let todo: Vec<usize> = (0..groups.len()).filter(|&g| !done.join(format!("g{g:03}.json")).is_file()).collect();
    log(ctx, &format!("phase C: {} book buckets in {} groups of <= {}, {} groups to do",
                      cfg.child_sources.len(), groups.len(), cfg.child_group(), todo.len()));
    for g in todo {
        let t0 = Instant::now();
        let name = format!("g{g:03}");
        let tmp = root.join(format!("_tmp_{name}"));
        std::fs::create_dir_all(&tmp)?;
        let mut per: Vec<Vec<i64>> = (0..BUCKETS).map(|_| Vec::new()).collect();
        let (mut rows_in, mut distinct, mut files_in) = (0u64, 0u64, 0u64);
        for &s in &groups[g] {
            let files = book_bucket_files(&cfg.book, s)?;
            if files.is_empty() && !cfg.test_inputs {
                bail!("book bucket {s} has no ps files");
            }
            files_in += files.len() as u64;
            let parts: Vec<Result<Vec<i64>>> = files
                .par_iter()
                .map(|p| {
                    let mut v = Vec::new();
                    for b in open_reader(p, &["child_hash"], READ_BATCH)? {
                        let c = i64_col(&b?, "child_hash")?;
                        if c.null_count() > 0 {
                            bail!("{}: NULL child_hash", p.display());
                        }
                        v.extend_from_slice(c.values());
                    }
                    Ok(v)
                })
                .collect();
            let mut v: Vec<i64> = Vec::new();
            for p in parts {
                v.extend(p?);
            }
            rows_in += v.len() as u64;
            v.par_sort_unstable();
            v.dedup();
            distinct += v.len() as u64;
            for h in v {
                let d = bucket_of(h, BUCKETS) as usize;
                if ctx.plan.want[d] {
                    per[d].push(h);
                }
            }
        }
        let res: Vec<Result<(u64, u64)>> = per
            .par_iter_mut()
            .enumerate()
            .filter(|(_, v)| !v.is_empty())
            .map(|(d, v)| {
                v.sort_unstable();
                v.dedup();
                let p = tmp.join(format!("bkt{d:03}.parquet"));
                let bytes = write_batches(
                    &p,
                    &child_shard_schema(),
                    shard_props(),
                    v.chunks(OUT_BATCH * 16).map(|c| {
                        Ok(RecordBatch::try_new(child_shard_schema(), vec![Arc::new(Int64Array::from(c.to_vec())) as ArrayRef])?)
                    }),
                )?;
                let n = v.len() as u64;
                *v = Vec::new();
                Ok((n, bytes))
            })
            .collect();
        let (mut out_rows, mut out_bytes, mut out_files) = (0u64, 0u64, 0u64);
        for r in res {
            let (n, b) = r?;
            out_rows += n;
            out_bytes += b;
            out_files += 1;
        }
        crash(cfg, CrashPhase::C, g as u32);
        rename_retry(&tmp, &root.join(&name))?;
        let secs = t0.elapsed().as_secs_f64();
        write_json_atomic(
            &done.join(format!("{name}.json")),
            &json!({"group": g, "sources": groups[g], "files_in": files_in, "rows_in": rows_in,
                    "distinct_per_source": distinct, "rows": out_rows, "files": out_files,
                    "bytes": out_bytes, "secs": secs, "finished": utc_now()}),
        )?;
        log(ctx, &format!(
            "C {name} (book buckets {:?}): {} files, {} child rows, {} distinct per source, {} routed in {} files ({:.2} GB), {:.0}s",
            groups[g], files_in, fmt_n(rows_in), fmt_n(distinct), fmt_n(out_rows), out_files,
            out_bytes as f64 / 1e9, secs
        ));
    }
    Ok(())
}

// ── Phase J: per output bucket ───────────────────────────────────────────────

fn shard_files(dir: &Path, b: u32) -> Result<Vec<PathBuf>> {
    let pre = format!("bkt{b:03}.");
    let mut v = Vec::new();
    if !dir.is_dir() {
        return Ok(v);
    }
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let n = e.file_name().to_string_lossy().into_owned();
        if n.starts_with(&pre) && n.ends_with(".parquet") {
            v.push(e.path());
        }
    }
    v.sort();
    Ok(v)
}

fn fixed_col(b: &RecordBatch, name: &str) -> Result<FixedSizeBinaryArray> {
    Ok(col(b, name)?.as_fixed_size_binary_opt().ok_or_else(|| anyhow!("{name} is not fixed binary"))?.clone())
}

fn load_fish(files: &[PathBuf], rows: u64) -> Result<Vec<FRow>> {
    let mut v = Vec::with_capacity(rows as usize);
    for p in files {
        for b in open_reader(p, &["position_hash", "pos", "variant", "tier", "skey", "n"], READ_BATCH)? {
            let b = b?;
            let h = i64_col(&b, "position_hash")?;
            let pos = fixed_col(&b, "pos")?;
            let var = col(&b, "variant")?.as_boolean().clone();
            let tier = col(&b, "tier")?.as_primitive::<Int8Type>().clone();
            let key = i32_col(&b, "skey")?;
            let n = i64_col(&b, "n")?;
            for i in 0..b.num_rows() {
                let mut p = [0u8; PACKED_BYTES];
                p.copy_from_slice(pos.value(i));
                v.push(FRow { hash: h.value(i), pos: p, variant: var.value(i), tier: tier.value(i) as u8, key: key.value(i), n: n.value(i) as u64 });
            }
        }
    }
    Ok(v)
}

fn load_cloud(files: &[PathBuf]) -> Result<Vec<CRow>> {
    let mut v = Vec::new();
    for p in files {
        for b in open_reader(
            p,
            &["position_hash", "pos", "variant", "depth", "knodes", "skey", "line", "file", "row", "npv", "bad"],
            READ_BATCH,
        )? {
            let b = b?;
            let h = i64_col(&b, "position_hash")?;
            let pos = fixed_col(&b, "pos")?;
            let var = col(&b, "variant")?.as_boolean().clone();
            let depth = col(&b, "depth")?.as_primitive::<Int16Type>().clone();
            let kn = i64_col(&b, "knodes")?;
            let key = i32_col(&b, "skey")?;
            let line = str_col(&b, "line")?;
            let file = i32_col(&b, "file")?;
            let row = i64_col(&b, "row")?;
            let npv = i32_col(&b, "npv")?;
            let bad = col(&b, "bad")?.as_boolean().clone();
            for i in 0..b.num_rows() {
                let mut p = [0u8; PACKED_BYTES];
                p.copy_from_slice(pos.value(i));
                v.push(CRow {
                    hash: h.value(i),
                    pos: p,
                    variant: var.value(i),
                    depth: depth.value(i),
                    knodes: kn.value(i),
                    key: key.value(i),
                    line: line.value(i).into(),
                    file: file.value(i) as u32,
                    row: row.value(i) as u64,
                    npv: npv.value(i) as u32,
                    bad: bad.value(i),
                });
            }
        }
    }
    Ok(v)
}

/// One book file, read by hash group.
struct BookCursor {
    path: PathBuf,
    rdr: ParquetRecordBatchReader,
    hash: Int64Array,
    epd: StringArray,
    ply: Int32Array,
    total: Int64Array,
    i: usize,
    rows: u64,
}

impl BookCursor {
    fn open(path: &Path) -> Result<Option<BookCursor>> {
        let rdr = open_reader(path, &["parent_hash", "parent_epd", "ply", "total"], READ_BATCH)?;
        let mut c = BookCursor {
            path: path.to_path_buf(),
            rdr,
            hash: Int64Array::from(Vec::<i64>::new()),
            epd: StringArray::from(Vec::<&str>::new()),
            ply: Int32Array::from(Vec::<i32>::new()),
            total: Int64Array::from(Vec::<i64>::new()),
            i: 0,
            rows: 0,
        };
        Ok(if c.load()? { Some(c) } else { None })
    }

    /// Load the next non-empty batch; false at the end.
    fn load(&mut self) -> Result<bool> {
        loop {
            let Some(b) = self.rdr.next() else { return Ok(false) };
            let b = b?;
            if b.num_rows() == 0 {
                continue;
            }
            self.hash = i64_col(&b, "parent_hash")?;
            self.epd = str_col(&b, "parent_epd")?;
            self.ply = i32_col(&b, "ply")?;
            self.total = i64_col(&b, "total")?;
            if self.hash.null_count() + self.epd.null_count() + self.ply.null_count() + self.total.null_count() > 0 {
                bail!("{}: NULLs in parent_hash/parent_epd/ply/total", self.path.display());
            }
            self.i = 0;
            return Ok(true);
        }
    }

    fn head(&self) -> i64 {
        self.hash.value(self.i)
    }

    /// Move every row with hash `h` into the group; false at the end of the file.
    fn drain(&mut self, h: i64, g: &mut Group) -> Result<bool> {
        loop {
            while self.i < self.hash.len() && self.hash.value(self.i) == h {
                g.push(self.epd.value(self.i), self.ply.value(self.i), self.total.value(self.i));
                self.i += 1;
                self.rows += 1;
            }
            if self.i < self.hash.len() {
                if self.hash.value(self.i) < h {
                    bail!("{}: parent_hash not sorted ({} after {h})", self.path.display(), self.hash.value(self.i));
                }
                return Ok(true);
            }
            if !self.load()? {
                return Ok(false);
            }
        }
    }
}

/// The rows of one parent_hash across the bucket's files.
#[derive(Default)]
struct Group {
    arena: String,
    rows: Vec<(u32, u32, i32, i64)>,
}

impl Group {
    fn clear(&mut self) {
        self.arena.clear();
        self.rows.clear();
    }

    fn push(&mut self, epd: &str, ply: i32, total: i64) {
        let s = self.arena.len() as u32;
        self.arena.push_str(epd);
        self.rows.push((s, epd.len() as u32, ply, total));
    }

    fn epd(&self, i: usize) -> &str {
        let r = &self.rows[i];
        &self.arena[r.0 as usize..(r.0 + r.1) as usize]
    }
}

const PLIES: usize = 32;

#[derive(Clone, Debug, Default)]
struct Coverage {
    positions: [u64; PLIES],
    with_eval: [u64; PLIES],
    games: [u64; PLIES],
    games_with_eval: [u64; PLIES],
}

impl Coverage {
    fn to_json(&self) -> Value {
        json!({"positions": self.positions.to_vec(), "with_eval": self.with_eval.to_vec(),
               "games": self.games.to_vec(), "games_with_eval": self.games_with_eval.to_vec()})
    }

    fn add_json(&mut self, v: &Value) {
        for (k, a) in [("positions", &mut self.positions), ("with_eval", &mut self.with_eval),
                       ("games", &mut self.games), ("games_with_eval", &mut self.games_with_eval)] {
            if let Some(arr) = v[k].as_array() {
                for (i, x) in arr.iter().enumerate().take(PLIES) {
                    a[i] += x.as_u64().unwrap_or(0);
                }
            }
        }
    }
}

struct OutRow {
    hash: i64,
    epd: String,
    in_book: &'static str,
    chosen: Chosen,
    cloud: Option<CloudPick>,
    fish: Option<FishPick>,
    ambiguous: bool,
    variant: bool,
}

fn sat32(x: u64) -> i32 {
    i32::try_from(x).unwrap_or(i32::MAX)
}

fn out_batch(rows: &[OutRow]) -> Result<RecordBatch> {
    let (cp, mate): (Vec<_>, Vec<_>) = rows.iter().map(|r| key_cp_mate(r.chosen.key)).unzip();
    let (ccp, cmate): (Vec<_>, Vec<_>) =
        rows.iter().map(|r| r.cloud.as_ref().map_or((None, None), |c| key_cp_mate(c.cand.key))).unzip();
    let (fcp, fmate): (Vec<_>, Vec<_>) = rows.iter().map(|r| r.fish.map_or((None, None), |f| key_cp_mate(f.key))).unzip();
    Ok(RecordBatch::try_new(
        out_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.hash))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.epd.as_str()))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.in_book))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.chosen.source))),
            Arc::new(Int32Array::from(cp)),
            Arc::new(Int32Array::from(mate)),
            Arc::new(Int16Array::from_iter_values(rows.iter().map(|r| r.chosen.eval_cp))),
            Arc::new(Int16Array::from(rows.iter().map(|r| r.cloud.as_ref().map(|c| c.cand.depth)).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(rows.iter().map(|r| r.cloud.as_ref().map(|c| c.cand.knodes)).collect::<Vec<_>>())),
            Arc::new(Int32Array::from(ccp)),
            Arc::new(Int32Array::from(cmate)),
            Arc::new(StringArray::from(rows.iter().map(|r| r.cloud.as_ref().map(|c| c.cand.line.as_str())).collect::<Vec<_>>())),
            Arc::new(Int32Array::from(rows.iter().map(|r| r.cloud.as_ref().map(|c| c.n_evals as i32)).collect::<Vec<_>>())),
            Arc::new(Int32Array::from(fcp)),
            Arc::new(Int32Array::from(fmate)),
            Arc::new(StringArray::from(rows.iter().map(|r| r.fish.map(|f| TIERS[f.tier as usize])).collect::<Vec<_>>())),
            Arc::new(Int32Array::from(rows.iter().map(|r| r.fish.map(|f| sat32(f.n_tier))).collect::<Vec<_>>())),
            Arc::new(Int32Array::from(rows.iter().map(|r| r.fish.map(|f| sat32(f.n))).collect::<Vec<_>>())),
            Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.ambiguous)))),
            Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(r.chosen.disagrees)))),
        ],
    )?)
}

/// A per-row digest over every output column, as text, the same on write and
/// on re-read; summed (wrapping) over the file.
fn row_digest(fields: &[Option<String>]) -> u64 {
    let mut buf = Vec::with_capacity(256);
    for f in fields {
        match f {
            Some(s) => buf.extend_from_slice(s.as_bytes()),
            None => buf.extend_from_slice(b"\x00N"),
        }
        buf.push(0xFF);
    }
    xxh3_64_with_seed(&buf, 7)
}

fn s<T: ToString>(x: T) -> Option<String> {
    Some(x.to_string())
}

fn digest_out_row(r: &OutRow) -> u64 {
    let (cp, mate) = key_cp_mate(r.chosen.key);
    let (ccp, cmate) = r.cloud.as_ref().map_or((None, None), |c| key_cp_mate(c.cand.key));
    let (fcp, fmate) = r.fish.map_or((None, None), |f| key_cp_mate(f.key));
    row_digest(&[
        s(r.hash),
        s(&r.epd),
        s(r.in_book),
        s(r.chosen.source),
        cp.map(|x| x.to_string()),
        mate.map(|x| x.to_string()),
        s(r.chosen.eval_cp),
        r.cloud.as_ref().map(|c| c.cand.depth.to_string()),
        r.cloud.as_ref().map(|c| c.cand.knodes.to_string()),
        ccp.map(|x| x.to_string()),
        cmate.map(|x| x.to_string()),
        r.cloud.as_ref().map(|c| c.cand.line.clone()),
        r.cloud.as_ref().map(|c| c.n_evals.to_string()),
        fcp.map(|x| x.to_string()),
        fmate.map(|x| x.to_string()),
        r.fish.map(|f| TIERS[f.tier as usize].to_string()),
        r.fish.map(|f| sat32(f.n_tier).to_string()),
        r.fish.map(|f| sat32(f.n).to_string()),
        s(r.ambiguous),
        s(r.chosen.disagrees),
    ])
}

/// Re-read an output file in full: schema, strict (hash, epd) order, bucket,
/// the exactly-one-of-cp-and-mate rule, the value sets, and the digest.
fn verify_out(path: &Path, b: u32, want_rows: u64, want_digest: u64) -> Result<()> {
    let f = File::open(path)?;
    let rdr = ParquetRecordBatchReaderBuilder::try_new(f)?.with_batch_size(READ_BATCH).build()?;
    if rdr.schema().fields() != out_schema().fields() {
        bail!("{}: schema differs from the output schema", path.display());
    }
    let mut n = 0u64;
    let mut dg = 0u64;
    let mut last: Option<(i64, String)> = None;
    for batch in rdr {
        let batch = batch?;
        let text: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| arrow::compute::cast(c, &DataType::Utf8))
            .collect::<std::result::Result<_, _>>()?;
        let text: Vec<&StringArray> = text.iter().map(|c| c.as_string::<i32>()).collect();
        let h = batch.column(0).as_primitive::<Int64Type>();
        for i in 0..batch.num_rows() {
            let key = (h.value(i), text[1].value(i).to_string());
            if bucket_of(key.0, BUCKETS) != b {
                bail!("{}: row {n} hash {} is not in bucket {b}", path.display(), key.0);
            }
            if last.as_ref().is_some_and(|l| *l >= key) {
                bail!("{}: rows not strictly increasing on (position_hash, epd) at row {n}", path.display());
            }
            if text[4].is_null(i) == text[5].is_null(i) {
                bail!("{}: row {n} has both or neither of cp and mate", path.display());
            }
            if !matches!(text[2].value(i), "parent" | "child") || !matches!(text[3].value(i), "cloud" | "fishnet") {
                bail!("{}: row {n} in_book/source out of range", path.display());
            }
            let fields: Vec<Option<String>> =
                text.iter().map(|c| (!c.is_null(i)).then(|| c.value(i).to_string())).collect();
            dg = dg.wrapping_add(row_digest(&fields));
            last = Some(key);
            n += 1;
        }
    }
    if n != want_rows || dg != want_digest {
        bail!("{}: re-read {n} rows / digest {dg:016x}, wrote {want_rows} / {want_digest:016x}", path.display());
    }
    Ok(())
}

fn out_path(cfg: &Config, b: u32) -> PathBuf {
    cfg.out.join(format!("bkt{b:03}.parquet"))
}

fn j_done(cfg: &Config, b: u32) -> PathBuf {
    cfg.out.join(J_DONE_DIR).join(format!("bkt{b:03}.json"))
}

struct BucketInputs {
    est: u64,
    fish_rows: u64,
    fish: Vec<PathBuf>,
    cloud: Vec<PathBuf>,
    child: Vec<PathBuf>,
}

/// A bucket's shard files and the bytes its J holds at peak, from their footers.
fn j_inputs(ctx: &Ctx, b: u32) -> Result<BucketInputs> {
    let root = e_dir(ctx.cfg);
    let (mut fish, mut cloud, mut child) = (Vec::new(), Vec::new(), Vec::new());
    for f in &ctx.plan.cloud {
        cloud.extend(shard_files(&root.join(unit_name("cloud", f)), b)?);
    }
    for f in &ctx.plan.fishnet {
        fish.extend(shard_files(&root.join(unit_name("fishnet", f)), b)?);
    }
    for g in 0..ctx.plan.groups.len() {
        let p = c_dir(ctx.cfg).join(format!("g{g:03}")).join(format!("bkt{b:03}.parquet"));
        if p.is_file() {
            child.push(p);
        }
    }
    let rows = |v: &[PathBuf]| -> Result<u64> { v.iter().map(|p| footer_rows(p)).sum() };
    let fish_rows = rows(&fish)?;
    // Fishnet rows as loaded (56 B, reserved exactly) and the transient set of
    // eval hashes; cloud rows with their lines; the book cursors and output.
    let est = fish_rows * (56 + 20) + rows(&cloud)? * 300 + 512_000_000;
    Ok(BucketInputs { est, fish_rows, fish, cloud, child })
}

/// A counting semaphore over bytes.
struct MemGate {
    free: Mutex<u64>,
    cv: Condvar,
    cap: u64,
}

impl MemGate {
    fn acquire(&self, want: u64) -> u64 {
        let want = want.min(self.cap);
        let mut f = self.free.lock().unwrap();
        while *f < want {
            f = self.cv.wait(f).unwrap();
        }
        *f -= want;
        want
    }

    fn release(&self, n: u64) {
        *self.free.lock().unwrap() += n;
        self.cv.notify_all();
    }
}

/// One eval position's rows within the sorted fishnet and cloud vectors.
struct PosRows {
    pos: [u8; PACKED_BYTES],
    variant: bool,
    fish: std::ops::Range<usize>,
    cloud: std::ops::Range<usize>,
}

/// The eval positions of hash `h`: the runs of `fish` from `*fi` and of
/// `cloud` from `*ci` with that hash, grouped by position (both vectors are
/// sorted by (hash, pos, ..)). Advances both cursors past `h`.
fn eval_group(h: i64, fish: &[FRow], cloud: &[CRow], fi: &mut usize, ci: &mut usize, out: &mut Vec<PosRows>) {
    out.clear();
    let (fe, ce) = {
        let mut fe = *fi;
        while fe < fish.len() && fish[fe].hash == h {
            fe += 1;
        }
        let mut ce = *ci;
        while ce < cloud.len() && cloud[ce].hash == h {
            ce += 1;
        }
        (fe, ce)
    };
    let (mut i, mut j) = (*fi, *ci);
    while i < fe || j < ce {
        let p = match (fish.get(i).filter(|_| i < fe), cloud.get(j).filter(|_| j < ce)) {
            (Some(f), Some(c)) => f.pos.min(c.pos),
            (Some(f), None) => f.pos,
            (None, Some(c)) => c.pos,
            (None, None) => unreachable!(),
        };
        let (i0, j0) = (i, j);
        let mut variant = false;
        while i < fe && fish[i].pos == p {
            variant = fish[i].variant;
            i += 1;
        }
        while j < ce && cloud[j].pos == p {
            variant = cloud[j].variant;
            j += 1;
        }
        out.push(PosRows { pos: p, variant, fish: i0..i, cloud: j0..j });
    }
    *fi = fe;
    *ci = ce;
}

fn fish_of(pr: &PosRows, fish: &[FRow]) -> Option<FishPick> {
    let trip: Vec<(u8, i32, u64)> = fish[pr.fish.clone()].iter().map(|r| (r.tier, r.key, r.n)).collect();
    fish_pick(&trip)
}

fn cloud_of(pr: &PosRows, cloud: &[CRow]) -> Option<CloudPick> {
    if pr.cloud.is_empty() {
        return None;
    }
    let cands: Vec<CloudCand> = cloud[pr.cloud.clone()]
        .iter()
        .map(|r| CloudCand {
            depth: r.depth,
            knodes: r.knodes,
            key: r.key,
            line: r.line.to_string(),
            file: r.file,
            row: r.row,
            npv: r.npv,
            bad: r.bad,
        })
        .collect();
    cloud_pick(&cands)
}

/// One bucket: a three-way merge by hash of the book's parent groups, the
/// sorted eval rows and the child hashes. Only matched positions are reduced
/// and rendered, so memory is the shard rows themselves.
fn do_bucket(ctx: &Ctx, b: u32, inp: &BucketInputs) -> Result<Value> {
    let cfg = ctx.cfg;
    let t0 = Instant::now();
    let mut fish = load_fish(&inp.fish, inp.fish_rows)?;
    fish.par_sort_unstable_by(|a, b| (a.hash, a.pos, a.tier, a.key).cmp(&(b.hash, b.pos, b.tier, b.key)));
    let mut cloud = load_cloud(&inp.cloud)?;
    cloud.par_sort_unstable_by(|a, b| (a.hash, a.pos, a.file, a.row).cmp(&(b.hash, b.pos, b.file, b.row)));
    let (n_fish_rows, n_cloud_rows) = (fish.len(), cloud.len());
    // Child hashes that some eval carries.
    let mut children: Vec<i64> = Vec::new();
    let mut child_rows = 0u64;
    {
        let mut eval_hashes: FastSet<i64> = FastSet::default();
        eval_hashes.extend(fish.iter().map(|r| r.hash).chain(cloud.iter().map(|r| r.hash)));
        for p in &inp.child {
            for bt in open_reader(p, &["child_hash"], READ_BATCH * 16)? {
                let c = i64_col(&bt?, "child_hash")?;
                child_rows += c.len() as u64;
                children.extend(c.values().iter().copied().filter(|h| eval_hashes.contains(h)));
            }
        }
    }
    children.sort_unstable();
    children.dedup();
    let t_load = t0.elapsed().as_secs_f64();
    // The book: a k-way merge of the bucket's files by hash.
    let files = book_bucket_files(&cfg.book, b)?;
    if files.is_empty() && !cfg.test_inputs {
        bail!("book bucket {b} has no ps files");
    }
    let mut curs: Vec<BookCursor> = Vec::new();
    for p in &files {
        if let Some(c) = BookCursor::open(p)? {
            curs.push(c);
        }
    }
    let mut heap: BinaryHeap<Reverse<(i64, usize)>> = curs.iter().enumerate().map(|(i, c)| Reverse((c.head(), i))).collect();
    let mut cov = Coverage::default();
    let (mut book_rows, mut book_parents, mut matched_parents) = (0u64, 0u64, 0u64);
    let mut g = Group::default();
    let mut idx: Vec<usize> = Vec::new();
    let mut last_book: Option<i64> = None;
    // Book positions of the current hash: (epd, ply mask, games per ply, has an eval).
    let mut bpos: Vec<(String, u32, [u64; PLIES], bool)> = Vec::new();
    let mut prs: Vec<PosRows> = Vec::new();
    let (mut fi, mut ci) = (0usize, 0usize);
    let mut rows: Vec<OutRow> = Vec::new();
    let mut amb_list: Vec<Value> = Vec::new();
    let (mut amb_hashes, mut n_evals, mut n_fish_pos) = (0u64, 0u64, 0u64);
    let (mut order_n, mut order_bad, mut order_n_out, mut order_bad_out) = (0u64, 0u64, 0u64, 0u64);
    loop {
        let hb = heap.peek().map(|r| r.0 .0);
        let he = match (fish.get(fi).map(|r| r.hash), cloud.get(ci).map(|r| r.hash)) {
            (Some(a), Some(c)) => Some(a.min(c)),
            (a, c) => a.or(c),
        };
        let h = match (hb, he) {
            (Some(x), Some(y)) => x.min(y),
            (x, y) => match x.or(y) {
                Some(v) => v,
                None => break,
            },
        };
        // The book's positions with this hash.
        bpos.clear();
        if hb == Some(h) {
            if last_book.is_some_and(|l| h <= l) {
                bail!("bucket {b}: book hash groups out of order at {h}");
            }
            last_book = Some(h);
            if bucket_of(h, BUCKETS) != b {
                bail!("bucket {b}: book parent_hash {h} is in bucket {}", bucket_of(h, BUCKETS));
            }
            g.clear();
            while let Some(&Reverse((hh, c))) = heap.peek() {
                if hh != h {
                    break;
                }
                heap.pop();
                if curs[c].drain(h, &mut g)? {
                    heap.push(Reverse((curs[c].head(), c)));
                }
            }
            book_rows += g.rows.len() as u64;
            idx.clear();
            idx.extend(0..g.rows.len());
            idx.sort_by(|&x, &y| g.epd(x).cmp(g.epd(y)));
            let mut k = 0;
            while k < idx.len() {
                let mut mask = 0u32;
                let mut totals = [0u64; PLIES];
                let mut e = k;
                while e < idx.len() && g.epd(idx[e]) == g.epd(idx[k]) {
                    let r = g.rows[idx[e]];
                    if !(1..PLIES as i32).contains(&r.2) || r.3 < 1 {
                        bail!("bucket {b}: ply {} / total {} out of range at hash {h}", r.2, r.3);
                    }
                    mask |= 1 << r.2;
                    totals[r.2 as usize] += r.3 as u64;
                    e += 1;
                }
                bpos.push((g.epd(idx[k]).to_string(), mask, totals, false));
                k = e;
            }
        }
        // The evals with this hash.
        eval_group(h, &fish, &cloud, &mut fi, &mut ci, &mut prs);
        let is_child = bpos.is_empty() && !prs.is_empty() && children.binary_search(&h).is_ok();
        let mut grp: Vec<OutRow> = Vec::new();
        for pr in &prs {
            n_evals += 1;
            n_fish_pos += u64::from(!pr.fish.is_empty());
            let c = cloud_of(pr, &cloud);
            if let Some(c) = c.as_ref().filter(|c| c.cand.npv >= 2) {
                order_n += 1;
                order_bad += u64::from(c.cand.bad);
            }
            let matched = if bpos.is_empty() {
                None
            } else {
                let epd = Packed::from_bytes(&pr.pos).render();
                bpos.iter().position(|x| x.0 == epd).map(|i| (i, epd))
            };
            if matched.is_none() && !is_child {
                continue;
            }
            let f = fish_of(pr, &fish);
            if let Some(c) = c.as_ref().filter(|c| c.cand.npv >= 2) {
                order_n_out += 1;
                order_bad_out += u64::from(c.cand.bad);
            }
            let chosen = choose(c.as_ref(), f.as_ref()).expect("an eval position has a source");
            let (epd, in_book) = match matched {
                Some((i, epd)) => {
                    bpos[i].3 = true;
                    (epd, "parent")
                }
                None => (Packed::from_bytes(&pr.pos).render(), "child"),
            };
            grp.push(OutRow { hash: h, epd, in_book, chosen, cloud: c, fish: f, ambiguous: false, variant: pr.variant });
        }
        if is_child && grp.len() >= 2 {
            amb_hashes += 1;
            let n = grp.len();
            for r in &mut grp {
                r.ambiguous = true;
                amb_list.push(json!({"position_hash": h, "epd": r.epd, "n_epds": n, "source": r.chosen.source}));
            }
        }
        grp.sort_by(|x, y| x.epd.cmp(&y.epd));
        rows.extend(grp);
        for (_, mask, totals, has) in &bpos {
            book_parents += 1;
            matched_parents += u64::from(*has);
            for p in 1..PLIES {
                if mask & (1 << p) != 0 {
                    cov.positions[p] += 1;
                    cov.games[p] += totals[p];
                    if *has {
                        cov.with_eval[p] += 1;
                        cov.games_with_eval[p] += totals[p];
                    }
                }
            }
        }
    }
    let footer: u64 = files.iter().map(|p| footer_rows(p)).sum::<Result<u64>>()?;
    let read: u64 = curs.iter().map(|c| c.rows).sum();
    if read != footer || read != book_rows {
        bail!("bucket {b}: read {read} book rows, grouped {book_rows}, footers say {footer}");
    }
    drop(curs);
    drop(fish);
    drop(cloud);
    let t_book = t0.elapsed().as_secs_f64() - t_load;
    let cnt = |f: &dyn Fn(&OutRow) -> bool| rows.iter().filter(|r| f(r)).count() as u64;
    let n_parent = cnt(&|r| r.in_book == "parent");
    let n_cloud = cnt(&|r| r.chosen.source == "cloud");
    let n_amb = cnt(&|r| r.ambiguous);
    let n_var = cnt(&|r| r.variant);
    let n_dis = cnt(&|r| r.chosen.disagrees);
    let n_fish_any = cnt(&|r| r.fish.is_some());
    // Write, re-read, publish.
    let path = out_path(cfg, b);
    let tmp = tmp_of(&path);
    let digest = rows.iter().fold(0u64, |a, r| a.wrapping_add(digest_out_row(r)));
    let bytes = write_batches(&tmp, &out_schema(), out_props(), rows.chunks(OUT_BATCH).map(out_batch))?;
    verify_out(&tmp, b, rows.len() as u64, digest)?;
    let sha = sha256_file(&tmp)?;
    crash(cfg, CrashPhase::J, b);
    rename_retry(&tmp, &path)?;
    crash(cfg, CrashPhase::JPublish, b);
    let n = rows.len() as u64;
    let man = json!({
        "bucket": b, "rows": n, "parents": n_parent, "children": n - n_parent,
        "cloud": n_cloud, "fishnet": n - n_cloud, "with_cloud": n_cloud, "with_fishnet": n_fish_any,
        "ambiguous_rows": n_amb, "ambiguous_hashes": amb_hashes,
        "ep_variant_rows": n_var, "fishnet_disagrees": n_dis,
        "book_rows": book_rows, "book_parents": book_parents, "book_parents_with_eval": matched_parents,
        "book_games": cov.games.iter().sum::<u64>(), "book_games_with_eval": cov.games_with_eval.iter().sum::<u64>(),
        "eval_positions": n_evals, "fishnet_positions": n_fish_pos,
        "fishnet_shard_rows": n_fish_rows, "cloud_shard_rows": n_cloud_rows,
        "child_hashes_read": child_rows, "child_hashes_with_eval": children.len(),
        "cloud_order_n": order_n, "cloud_order_bad": order_bad,
        "cloud_order_n_out": order_n_out, "cloud_order_bad_out": order_bad_out,
        "bytes": bytes, "sha256": sha,
    });
    Ok(json!({"manifest": man, "coverage": cov.to_json(), "ambiguous": amb_list,
              "secs": {"load": t_load, "book": t_book, "total": t0.elapsed().as_secs_f64()},
              "digest": format!("{digest:016x}"), "finished": utc_now(),
              "peak_commit_gb": sys::peak_commit().map(|x| x as f64 / 1e9)}))
}

fn phase_j(ctx: &Ctx) -> Result<()> {
    let cfg = ctx.cfg;
    let e_done = e_dir(cfg).join("_done");
    let missing_e = ctx
        .plan
        .cloud
        .iter()
        .map(|f| unit_name("cloud", f))
        .chain(ctx.plan.fishnet.iter().map(|f| unit_name("fishnet", f)))
        .filter(|u| !e_done.join(format!("{u}.json")).is_file())
        .count();
    let missing_c = (0..ctx.plan.groups.len())
        .filter(|g| !c_dir(cfg).join("_done").join(format!("g{g:03}.json")).is_file())
        .count();
    if missing_e + missing_c > 0 {
        return Err(refused(format!(
            "phase J needs phases E and C complete: {missing_e} E units and {missing_c} C groups are not done"
        )));
    }
    std::fs::create_dir_all(cfg.out.join(J_DONE_DIR))?;
    let todo: Vec<u32> = cfg.buckets.iter().copied().filter(|&b| !j_done(cfg, b).is_file()).collect();
    for &b in &todo {
        rm_file(&out_path(cfg, b))?;
        rm_file(&tmp_of(&out_path(cfg, b)))?;
    }
    log(ctx, &format!("phase J: {} buckets, {} to do", cfg.buckets.len(), todo.len()));
    let cap = (cfg.mem_bytes() as f64 * 0.8) as u64;
    let gate = MemGate { free: Mutex::new(cap), cv: Condvar::new(), cap };
    let done_n = AtomicUsize::new(0);
    let first_err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
    let t0 = Instant::now();
    todo.par_iter().for_each(|&b| {
        if first_err.lock().unwrap().is_some() {
            return;
        }
        let r = (|| -> Result<()> {
            let inp = j_inputs(ctx, b)?;
            let held = gate.acquire(inp.est);
            let r = do_bucket(ctx, b, &inp);
            gate.release(held);
            let v = r?;
            write_json_atomic(&j_done(cfg, b), &v)?;
            let m = &v["manifest"];
            let u = |k: &str| m[k].as_u64().unwrap_or(0);
            let n = done_n.fetch_add(1, AtOrd::SeqCst) + 1;
            let el = t0.elapsed().as_secs_f64();
            log(ctx, &format!(
                "J bkt{b:03}: {} rows ({} parent, {} child; {} cloud, {} fishnet; {} ambiguous, {} ep-variant, {} disagree) of {} eval positions; book {} rows / {} parents, {} with eval; {:.2} GB; load {:.0}s book {:.0}s total {:.0}s (est {:.1} GB) | {n}/{} done, ETA {:.0} min",
                fmt_n(u("rows")), fmt_n(u("parents")), fmt_n(u("children")), fmt_n(u("cloud")), fmt_n(u("fishnet")),
                u("ambiguous_rows"), u("ep_variant_rows"), u("fishnet_disagrees"), fmt_n(u("eval_positions")),
                fmt_n(u("book_rows")), fmt_n(u("book_parents")), fmt_n(u("book_parents_with_eval")),
                u("bytes") as f64 / 1e9, v["secs"]["load"].as_f64().unwrap_or(0.0),
                v["secs"]["book"].as_f64().unwrap_or(0.0), v["secs"]["total"].as_f64().unwrap_or(0.0),
                inp.est as f64 / 1e9, todo.len(), el / n as f64 * (todo.len() - n) as f64 / 60.0
            ));
            Ok(())
        })()
        .with_context(|| format!("bucket {b}"));
        if let Err(e) = r {
            first_err.lock().unwrap().get_or_insert(e);
        }
    });
    if let Some(e) = first_err.into_inner().unwrap() {
        return Err(e);
    }
    Ok(())
}

// ── finalize ─────────────────────────────────────────────────────────────────

/// Summed into meta "totals"; with bucket, bytes and the timings, the
/// _manifest columns.
const COUNT_COLS: [&str; 27] = [
    "rows", "parents", "children", "cloud", "fishnet", "with_cloud", "with_fishnet", "ambiguous_rows",
    "ambiguous_hashes", "ep_variant_rows", "fishnet_disagrees", "book_rows", "book_parents",
    "book_parents_with_eval", "book_games", "book_games_with_eval", "eval_positions", "fishnet_positions",
    "fishnet_shard_rows", "cloud_shard_rows", "child_hashes_read", "child_hashes_with_eval",
    "cloud_order_n", "cloud_order_bad", "cloud_order_n_out", "cloud_order_bad_out", "bytes",
];

fn write_small(path: &Path, batch: &RecordBatch) -> Result<u64> {
    let tmp = tmp_of(path);
    let n = write_batches(&tmp, &batch.schema(), out_props(), std::iter::once(Ok(batch.clone())))?;
    rename_retry(&tmp, path)?;
    Ok(n)
}

pub const README: &str = include_str!("evals_readme.md");

fn finalize(ctx: &Ctx) -> Result<()> {
    let cfg = ctx.cfg;
    let missing = cfg.buckets.iter().filter(|&&b| !j_done(cfg, b).is_file()).count();
    if missing > 0 {
        log(ctx, &format!("finalize: {missing} buckets not done; _DONE not written"));
        return Ok(());
    }
    let t0 = Instant::now();
    let mut sents = Vec::new();
    for &b in &cfg.buckets {
        sents.push(read_json(&j_done(cfg, b))?);
    }
    // Recount the tree: every output file exists with its sentinel's bytes and rows.
    for v in &sents {
        let m = &v["manifest"];
        let b = m["bucket"].as_u64().unwrap_or(u64::MAX) as u32;
        let p = out_path(cfg, b);
        let len = std::fs::metadata(&p).with_context(|| format!("{} is missing", p.display()))?.len();
        if len != m["bytes"].as_u64().unwrap_or(0) || footer_rows(&p)? != m["rows"].as_u64().unwrap_or(0) {
            bail!("{}: size or footer rows disagree with its sentinel", p.display());
        }
    }
    let man_cols: Vec<&str> = ["bucket"].into_iter().chain(COUNT_COLS.iter().copied()).collect();
    let mut fields = Vec::new();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for c in &man_cols {
        fields.push(Field::new(*c, DataType::Int64, false));
        cols.push(Arc::new(Int64Array::from_iter_values(sents.iter().map(|v| v["manifest"][*c].as_i64().unwrap_or(0)))));
    }
    for (c, k) in [("load_secs", "load"), ("book_secs", "book"), ("total_secs", "total")] {
        fields.push(Field::new(c, DataType::Float64, false));
        cols.push(Arc::new(arrow::array::Float64Array::from_iter_values(
            sents.iter().map(|v| v["secs"][k].as_f64().unwrap_or(0.0)),
        )));
    }
    fields.push(Field::new("sha256", DataType::Utf8, false));
    cols.push(Arc::new(StringArray::from_iter_values(
        sents.iter().map(|v| v["manifest"]["sha256"].as_str().unwrap_or("").to_string()),
    )));
    write_small(&cfg.out.join("_manifest.parquet"), &RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)?;
    let amb: Vec<&Value> = sents.iter().flat_map(|v| v["ambiguous"].as_array().into_iter().flatten()).collect();
    write_small(
        &cfg.out.join("_ambiguous.parquet"),
        &RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("position_hash", DataType::Int64, false),
                Field::new("epd", DataType::Utf8, false),
                Field::new("n_epds", DataType::Int32, false),
                Field::new("source", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(amb.iter().map(|v| v["position_hash"].as_i64().unwrap_or(0)))),
                Arc::new(StringArray::from_iter_values(amb.iter().map(|v| v["epd"].as_str().unwrap_or("").to_string()))),
                Arc::new(Int32Array::from_iter_values(amb.iter().map(|v| v["n_epds"].as_i64().unwrap_or(0) as i32))),
                Arc::new(StringArray::from_iter_values(amb.iter().map(|v| v["source"].as_str().unwrap_or("").to_string()))),
            ],
        )?,
    )?;
    let mut cov = Coverage::default();
    for v in &sents {
        cov.add_json(&v["coverage"]);
    }
    let plies: Vec<usize> = (1..PLIES).filter(|&p| cov.positions[p] > 0).collect();
    write_small(
        &cfg.out.join("_coverage.parquet"),
        &RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ply", DataType::Int32, false),
                Field::new("positions", DataType::Int64, false),
                Field::new("positions_with_eval", DataType::Int64, false),
                Field::new("games", DataType::Int64, false),
                Field::new("games_with_eval", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int32Array::from_iter_values(plies.iter().map(|&p| p as i32))),
                Arc::new(Int64Array::from_iter_values(plies.iter().map(|&p| cov.positions[p] as i64))),
                Arc::new(Int64Array::from_iter_values(plies.iter().map(|&p| cov.with_eval[p] as i64))),
                Arc::new(Int64Array::from_iter_values(plies.iter().map(|&p| cov.games[p] as i64))),
                Arc::new(Int64Array::from_iter_values(plies.iter().map(|&p| cov.games_with_eval[p] as i64))),
            ],
        )?,
    )?;
    let (mut e_cloud, mut e_fish) = (UnitStats::default(), UnitStats::default());
    let e_done = e_dir(cfg).join("_done");
    for f in &ctx.plan.cloud {
        e_cloud.add(&UnitStats::from_json(&read_json(&e_done.join(format!("{}.json", unit_name("cloud", f))))?));
    }
    for f in &ctx.plan.fishnet {
        e_fish.add(&UnitStats::from_json(&read_json(&e_done.join(format!("{}.json", unit_name("fishnet", f))))?));
    }
    let mut c_tot = json!({"files_in": 0, "rows_in": 0, "distinct_per_source": 0, "rows": 0, "files": 0, "bytes": 0, "secs": 0.0});
    for g in 0..ctx.plan.groups.len() {
        let v = read_json(&c_dir(cfg).join("_done").join(format!("g{g:03}.json")))?;
        for k in ["files_in", "rows_in", "distinct_per_source", "rows", "files", "bytes"] {
            c_tot[k] = json!(c_tot[k].as_u64().unwrap_or(0) + v[k].as_u64().unwrap_or(0));
        }
        c_tot["secs"] = json!(c_tot["secs"].as_f64().unwrap_or(0.0) + v["secs"].as_f64().unwrap_or(0.0));
    }
    let sum = |k: &str| sents.iter().map(|v| v["manifest"][k].as_u64().unwrap_or(0)).sum::<u64>();
    let totals: serde_json::Map<String, Value> = COUNT_COLS.iter().map(|k| (k.to_string(), json!(sum(k)))).collect();
    let jsecs: f64 = sents.iter().map(|v| v["secs"]["total"].as_f64().unwrap_or(0.0)).sum();
    let book_meta = cfg.book.join("_book.meta.json");
    let book_meta_sha = if book_meta.is_file() { Some(sha256_file(&book_meta)?) } else { None };
    let mut shas = serde_json::Map::new();
    if let Some(m) = cfg.cloud.parent().map(|p| p.join("manifest.tsv")).filter(|p| p.is_file()) {
        for line in std::fs::read_to_string(m)?.lines() {
            let f: Vec<&str> = line.split('\t').collect();
            if f.len() >= 3 {
                shas.insert(f[0].to_string(), json!({"bytes": f[1].parse::<u64>().ok(), "sha256": f[2], "url": f.get(3)}));
            }
        }
    }
    let exe = std::env::current_exe().ok();
    let exe_sha = exe.as_ref().and_then(|p| sha256_file(p).ok());
    let meta = json!({
        "schema": SCHEMA,
        "tool": {"version": sys::VERSION, "commit": sys::GIT_COMMIT, "built": sys::BUILD_DATE,
                 "exe": exe.map(|p| p.display().to_string()), "exe_sha256": exe_sha,
                 "version_line": sys::version_line()},
        "inputs": {
            "book": cfg.book.display().to_string(), "book_meta_sha256": book_meta_sha,
            "cloud": cfg.cloud.display().to_string(), "fishnet": cfg.fishnet.display().to_string(),
            "cloud_files": ctx.plan.cloud.iter().map(|f| f.name.clone()).collect::<Vec<_>>(),
            "fishnet_files": ctx.plan.fishnet.iter().map(|f| f.name.clone()).collect::<Vec<_>>(),
            "datasets": {"cloud": "Lichess/chess-position-evaluations@ae8ede377fc5e267ddb60f56987cf9cd963ceacb",
                         "fishnet": "Lichess/fishnet-evals@1b6d7c91ddef44ec89efe64e47a2e313b7648ece"},
            "download_manifest": shas,
        },
        "params": ctx.plan.lock,
        "rules": {
            "cloud": "max depth, then max knodes, then the first row in (file name, row index) order; cloud_n_evals = distinct (depth, knodes)",
            "fishnet": "best tier present (nnue 2021-01+, classical 2016-01..2020-12, early 2013-2015); lower median of White-POV-ordered scores",
            "eval_cp": "cp clamped to +-2000; mate -> sign * 2000; mate 0 by the mated side",
            "fishnet_disagrees": "source cloud AND |fishnet eval_cp| = 2000 AND fishnet_n_tier >= 5 AND sign(fishnet eval_cp) != sign(eval_cp); not applied",
            "match": "parent: (hash, epd) = a book (parent_hash, parent_epd); child: hash is a book child_hash and not a book parent_hash",
        },
        "phase_e": {"cloud": e_cloud.to_json(), "fishnet": e_fish.to_json()},
        "phase_c": c_tot,
        "phase_j_secs_summed": jsecs,
        "totals": totals,
        "buckets": describe(&cfg.buckets),
        "peak_commit_gb_this_run": sys::peak_commit().map(|b| b as f64 / 1e9),
        "finalize_secs": t0.elapsed().as_secs_f64(),
        "finished": utc_now(),
    });
    write_json_atomic(&cfg.out.join("_build.meta.json"), &meta)?;
    let readme = cfg.out.join("README.md");
    std::fs::write(tmp_of(&readme), README)?;
    rename_retry(&tmp_of(&readme), &readme)?;
    let done = cfg.out.join("_DONE");
    std::fs::write(tmp_of(&done), format!("{}\n", utc_now()))?;
    rename_retry(&tmp_of(&done), &done)?;
    log(ctx, &format!(
        "finalize: {} rows in {} buckets ({} parent, {} child; {} cloud, {} fishnet; {} ambiguous, {} ep-variant, {} disagree); \
         cloud row-order check (phase E, every position) {} of {} chosen blocks; {:.2} GB; _DONE written",
        fmt_n(sum("rows")), cfg.buckets.len(), fmt_n(sum("parents")), fmt_n(sum("children")),
        fmt_n(sum("cloud")), fmt_n(sum("fishnet")), sum("ambiguous_rows"), sum("ep_variant_rows"),
        sum("fishnet_disagrees"), e_cloud.order_bad, fmt_n(e_cloud.order_n), sum("bytes") as f64 / 1e9
    ));
    Ok(())
}

// ── entry ────────────────────────────────────────────────────────────────────

pub fn run(cfg: &Config) -> Result<()> {
    if cfg.buckets.is_empty() || cfg.child_sources.is_empty() {
        return Err(refused("--buckets and --child-sources must not be empty"));
    }
    let plan = plan(cfg)?;
    if let Some((lim, avail)) = sys::commit() {
        let need = cfg.mem_bytes() + 4_000_000_000;
        if avail < need && !cfg.test_inputs {
            return Err(Exit {
                code: EXIT_COMMIT,
                msg: format!(
                    "free commit {:.1} GB (of {:.1} GB) is below --mem-gb {} + 4 GB; nothing was done, retry later",
                    avail as f64 / 1e9, lim as f64 / 1e9, cfg.mem_gb
                ),
            }
            .into());
        }
    }
    std::fs::create_dir_all(&cfg.work)?;
    std::fs::create_dir_all(&cfg.out)?;
    check_lock(&cfg.work, &plan.lock)?;
    check_lock(&cfg.out, &plan.lock)?;
    let ctx = Ctx { cfg, plan, t0: Instant::now() };
    log(&ctx, &format!(
        "{} | evals: {} cloud + {} fishnet files, {} output buckets, {} child-source buckets, {} threads, {} GB",
        sys::version_line(), ctx.plan.cloud.len(), ctx.plan.fishnet.len(), cfg.buckets.len(),
        cfg.child_sources.len(), cfg.threads, cfg.mem_gb
    ));
    if cfg.out.join("_DONE").is_file() {
        log(&ctx, "_DONE exists: nothing to do");
        return Ok(());
    }
    if cfg.phases.0 {
        phase_e(&ctx)?;
    }
    if cfg.phases.1 {
        phase_c(&ctx)?;
    }
    if cfg.phases.2 {
        phase_j(&ctx)?;
        finalize(&ctx)?;
    }
    log(&ctx, &format!("done; peak commit {:.1} GB", sys::peak_commit().unwrap_or(0) as f64 / 1e9));
    Ok(())
}
