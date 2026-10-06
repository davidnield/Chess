//! `merge`: the Rust months -> the all-time banded explorer book (the blog
//! repo's docs/explorer-merge-spec.md is the contract; the crate README sums
//! it up).
//!
//! INPUT  a months root written by `month --ply-key` (30 plies, 512 buckets):
//!        month=Y_M/bkt=i/part-0000.parquet, strictly increasing on PsKey,
//!        plus each month's manifest, provenance, term monthly and sentinel.
//! OUTPUT <out>/ps/event=E/elo_band=B/bkt<iii>.parquet, one file per (slice,
//!        bucket) with a row, strictly increasing on (parent_hash, parent_epd,
//!        move_san, ply) and unique on the 6-column key; term/bkt<iii>.parquet;
//!        _collisions, _slices, _manifest, _done, the settings lock, the meta,
//!        README.md, and _BOOK.DONE written LAST.
//!
//! PER BUCKET (one worker, one thread): a streaming k-way merge of the
//! bucket's month files by parent_hash. A hash's rows form a group, sorted by
//! (parent_epd, move_san, event, elo_band, ply) and reduced: counts summed,
//! child_hash required identical. Two EPDs under one hash are 64-bit collision
//! twins: separate rows, listed in _collisions. Every input row is validated as
//! it streams, every output file is re-read in full, and a linear 128-bit
//! digest (xxh3, seeds 1 and 2, of the key and child, times each count) must
//! agree across the two. Only then is the bucket published (renames on the
//! book's volume), with its sentinel LAST.
//!
//! TERM (one task beside the buckets): a streaming merge of the term monthlies
//! on TermKey, routed to 512 bucket files, verified the same way.
//!
//! FINALIZE (all 512 bucket sentinels and term's): the tree recounted against
//! the sentinels, per-month conservation against every manifest and
//! provenance, the months' own collision records as a positive control,
//! _slices, the meta, README.md, then _BOOK.DONE.
//!
//! I/O: only the stager thread reads the months root, and only whole files
//! (stage.rs). Workers read the stage.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Int64Type, Schema, SchemaRef};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use serde_json::{json, Value};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::chesspos::START_HASH;
use crate::keys::{bucket_of, ps_schema, rename_retry, san_of, term_schema_with, tmp_of, San, BANDS};
use crate::stage::{self, bucket_dir_name, CopyJob, Queue, Staged};
use crate::sys;

pub const BUCKETS: u32 = 512;
pub const MAX_PLY: i32 = 30;
pub const BOOK_SCHEMA: &str = "book-v1";
pub const ZSTD_LEVEL: i32 = 3;
/// The six speeds, alphabetical: the months' event index is this order.
pub const EVENTS: [&str; 6] = ["Blitz", "Bullet", "Classical", "Correspondence", "Rapid", "UltraBullet"];
pub const SLICES: usize = EVENTS.len() * BANDS.len();
/// Columns whose dictionary encoding `--no-dictionary` may turn off.
pub const DICT_TUNABLE: [&str; 3] = ["parent_epd", "parent_hash", "child_hash"];

pub const EXIT_FAIL: u8 = 1;
pub const EXIT_SPACE: u8 = 4;
pub const EXIT_REFUSED: u8 = 5;

const READ_BATCH: usize = 16_384;
const OUT_BATCH: usize = 16_384;
const TERM_OUT_BATCH: usize = 8_192;
const VERIFY_BATCH: usize = 65_536;
/// Buckets done before the space projection is trusted.
const PROJECT_AFTER: usize = 16;
const COLLISIONS_FATAL: usize = 1_000;
const COLLISIONS_LOW: usize = 50;
const COLLISIONS_HIGH: usize = 170;
/// Commit charge a worker and the term task are budgeted at, and the margin.
const WORKER_COMMIT: u64 = 1_000_000_000;
const TERM_COMMIT: u64 = 2_000_000_000;
const BASE_COMMIT: u64 = 500_000_000;
const COMMIT_MARGIN: u64 = 8_000_000_000;

// ── exit statuses ────────────────────────────────────────────────────────────

/// An error with the exit status the spec gives it (4 out of space, 5 refused).
#[derive(Debug)]
pub struct Exit {
    pub code: u8,
    pub msg: String,
}

impl std::fmt::Display for Exit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for Exit {}

fn refused(msg: impl Into<String>) -> anyhow::Error {
    Exit { code: EXIT_REFUSED, msg: msg.into() }.into()
}

fn out_of_space(msg: impl Into<String>) -> anyhow::Error {
    Exit { code: EXIT_SPACE, msg: msg.into() }.into()
}

/// The exit status for an error from `run`: 4 and 5 where the spec says so, else 1.
pub fn exit_code(e: &anyhow::Error) -> u8 {
    e.chain().find_map(|c| c.downcast_ref::<Exit>()).map_or(EXIT_FAIL, |x| x.code)
}

// ── configuration ────────────────────────────────────────────────────────────

/// Test hooks: `--test-crash-at PHASE:BUCKET` exits as if killed there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Stage,
    Merge,
    Verify,
    Publish,
    Term,
    TermPublish,
}

pub fn parse_crash(s: &str) -> Result<(Phase, u32)> {
    let (p, b) = s.split_once(':').ok_or_else(|| anyhow!("--test-crash-at wants PHASE:BUCKET"))?;
    let phase = match p {
        "stage" => Phase::Stage,
        "merge" => Phase::Merge,
        "verify" => Phase::Verify,
        "publish" => Phase::Publish,
        "term" => Phase::Term,
        "term-publish" => Phase::TermPublish,
        _ => bail!("--test-crash-at: unknown phase {p:?}"),
    };
    Ok((phase, b.parse()?))
}

pub struct Config {
    pub months_root: PathBuf,
    pub months: Vec<(i32, u32)>,
    pub out: PathBuf,
    pub stage_dir: PathBuf,
    pub stage_bytes: u64,
    /// The requested buckets, sorted and unique.
    pub buckets: Vec<u32>,
    pub threads: usize,
    pub row_group_rows: usize,
    pub min_free_bytes: u64,
    pub no_dictionary: Vec<String>,
    /// Test only: the stage may share the input volume.
    pub allow_one_volume: bool,
    pub crash_at: Option<(Phase, u32)>,
}

pub fn tag(y: i32, m: u32) -> String {
    format!("{y}_{m}")
}

fn parse_ym(s: &str) -> Result<(i32, u32)> {
    let (y, m) = s.split_once('_').ok_or_else(|| anyhow!("month {s:?} is not Y_M"))?;
    let (y, m): (i32, u32) = (y.trim().parse()?, m.trim().parse()?);
    if !(1..=12).contains(&m) {
        bail!("month {s:?}: {m} is not a month");
    }
    Ok((y, m))
}

/// `--months`: Y_M values and Y_M..Y_M ranges (inclusive), space- or
/// comma-separated. Sorted, unique.
pub fn parse_months(args: &[String]) -> Result<Vec<(i32, u32)>> {
    let mut set = BTreeSet::new();
    for tok in args.iter().flat_map(|a| a.split(',')).map(str::trim).filter(|t| !t.is_empty()) {
        if let Some((a, b)) = tok.split_once("..") {
            let (mut y, mut m) = parse_ym(a)?;
            let end = parse_ym(b)?;
            if end < (y, m) {
                bail!("--months range {tok:?} runs backwards");
            }
            while (y, m) <= end {
                set.insert((y, m));
                m += 1;
                if m > 12 {
                    m = 1;
                    y += 1;
                }
            }
        } else {
            set.insert(parse_ym(tok)?);
        }
    }
    if set.is_empty() {
        bail!("--months is empty");
    }
    Ok(set.into_iter().collect())
}

/// `--buckets`: numbers and a-b ranges, space- or comma-separated.
pub fn parse_buckets(args: &[String]) -> Result<Vec<u32>> {
    let mut set = BTreeSet::new();
    for tok in args.iter().flat_map(|a| a.split(',')).map(str::trim).filter(|t| !t.is_empty()) {
        let (a, b) = match tok.split_once('-') {
            Some((a, b)) => (a.trim().parse::<u32>()?, b.trim().parse::<u32>()?),
            None => {
                let v = tok.parse::<u32>()?;
                (v, v)
            }
        };
        if a > b || b >= BUCKETS {
            bail!("--buckets {tok:?} is not within 0-{}", BUCKETS - 1);
        }
        set.extend(a..=b);
    }
    if set.is_empty() {
        bail!("--buckets is empty");
    }
    Ok(set.into_iter().collect())
}

// ── the book's format ────────────────────────────────────────────────────────

/// The book's ps schema: every column REQUIRED, `child_eval` dropped,
/// `white_score_avg` added.
pub fn book_ps_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("parent_hash", DataType::Int64, false),
        Field::new("move_san", DataType::Utf8, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("elo_band", DataType::Int64, false),
        Field::new("parent_epd", DataType::Utf8, false),
        Field::new("child_hash", DataType::Int64, false),
        Field::new("ply", DataType::Int32, false),
        Field::new("white_wins", DataType::Int64, false),
        Field::new("draws", DataType::Int64, false),
        Field::new("black_wins", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
        Field::new("white_score_avg", DataType::Float64, false),
    ]))
}

/// The merge's own writer settings (month mode keeps `writer_props`): zstd 3,
/// page statistics and page indexes (the arrow-rs defaults), and
/// `row_group_rows`-row groups.
pub fn book_writer_props(row_group_rows: usize, no_dictionary: &[String]) -> WriterProperties {
    let mut b = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL).expect("zstd level")))
        .set_max_row_group_row_count(Some(row_group_rows));
    for c in no_dictionary {
        b = b.set_column_dictionary_enabled(ColumnPath::from(c.as_str()), false);
    }
    b.build()
}

/// The writer settings as the lock records them.
pub fn writer_settings(props: &WriterProperties) -> Value {
    let mut dict = serde_json::Map::new();
    for f in book_ps_schema().fields() {
        dict.insert(f.name().clone(), json!(props.dictionary_enabled(&ColumnPath::from(f.name().as_str()))));
    }
    json!({
        "compression": "zstd",
        "zstd_level": ZSTD_LEVEL,
        "max_row_group_rows": props.max_row_group_row_count(),
        "data_page_size_limit": props.data_page_size_limit(),
        "data_page_row_count_limit": props.data_page_row_count_limit(),
        "dictionary_page_size_limit": props.dictionary_page_size_limit(),
        "write_batch_size": props.write_batch_size(),
        "statistics": format!("{:?}", props.statistics_enabled(&ColumnPath::from("parent_hash"))),
        "offset_index_disabled": props.offset_index_disabled(),
        "writer_version": format!("{:?}", props.writer_version()),
        "created_by": props.created_by(),
        "dictionary": dict,
        "out_batch_rows": OUT_BATCH,
        "term_out_batch_rows": TERM_OUT_BATCH,
    })
}

fn collisions_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("parent_hash", DataType::Int64, false),
        Field::new("parent_epd", DataType::Utf8, false),
        Field::new("n_epds", DataType::Int32, false),
        Field::new("rows", DataType::Int64, false),
        Field::new("white_wins", DataType::Int64, false),
        Field::new("draws", DataType::Int64, false),
        Field::new("black_wins", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
        Field::new("months", DataType::Int32, false),
        Field::new("first_month", DataType::Utf8, false),
        Field::new("last_month", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
    ]))
}

fn bucket_manifest_schema() -> SchemaRef {
    let i64f = |n: &str| Field::new(n, DataType::Int64, false);
    Arc::new(Schema::new(vec![
        Field::new("bucket", DataType::Int32, false),
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        i64f("rows"),
        i64f("total"),
        i64f("white_wins"),
        i64f("draws"),
        i64f("black_wins"),
        i64f("ply1_games"),
        i64f("bytes"),
    ]))
}

fn slices_schema() -> SchemaRef {
    let i64f = |n: &str| Field::new(n, DataType::Int64, false);
    Arc::new(Schema::new(vec![
        Field::new("event", DataType::Utf8, false),
        i64f("elo_band"),
        i64f("files"),
        i64f("rows"),
        i64f("bytes"),
        i64f("total"),
        i64f("white_wins"),
        i64f("draws"),
        i64f("black_wins"),
        i64f("ply1_games"),
    ]))
}

/// `ps/event=E/elo_band=B/bkt<iii>.parquet`, relative to the book.
pub fn ps_rel_path(ev: u8, band: u8, b: u32) -> String {
    format!("ps/event={}/elo_band={}/{}.parquet", EVENTS[ev as usize], BANDS[band as usize], bucket_dir_name(b))
}

pub fn term_rel_path(b: u32) -> String {
    format!("term/{}.parquet", bucket_dir_name(b))
}

fn rel_to_path(root: &Path, rel: &str) -> PathBuf {
    rel.split('/').fold(root.to_path_buf(), |p, s| p.join(s))
}

// ── the digest ───────────────────────────────────────────────────────────────

/// Sum over rows of k * c, wrapping, for k in (xxh3 seed 1, seed 2) of the
/// key and child bytes and c in (white_wins, draws, black_wins, total). Linear,
/// so a merge that sums counts keeps it; merging two different keys changes it.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct Digest(pub [u64; 8]);

impl Digest {
    #[inline]
    pub fn add(&mut self, k1: u64, k2: u64, c: [u64; 4]) {
        for j in 0..4 {
            self.0[j] = self.0[j].wrapping_add(k1.wrapping_mul(c[j]));
            self.0[4 + j] = self.0[4 + j].wrapping_add(k2.wrapping_mul(c[j]));
        }
    }

    pub fn merge(&mut self, o: &Digest) {
        for j in 0..8 {
            self.0[j] = self.0[j].wrapping_add(o.0[j]);
        }
    }

    pub fn to_json(&self) -> Value {
        let h = |s: &[u64]| s.iter().map(|x| format!("{x:016x}")).collect::<Vec<_>>();
        json!({"counts": ["white_wins", "draws", "black_wins", "total"],
               "seed1": h(&self.0[..4]), "seed2": h(&self.0[4..])})
    }
}

/// (seed 1, seed 2) hashes of parent_hash LE, parent_epd, 0xFF, move_san,
/// 0xFF, event, 0xFF, elo_band LE, ply LE, child_hash LE.
#[allow(clippy::too_many_arguments)]
#[inline]
pub fn ps_key_hashes(
    buf: &mut Vec<u8>,
    hash: i64,
    epd: &[u8],
    san: &[u8],
    event: &[u8],
    band: i64,
    ply: i32,
    child: i64,
) -> (u64, u64) {
    buf.clear();
    buf.extend_from_slice(&hash.to_le_bytes());
    buf.extend_from_slice(epd);
    buf.push(0xFF);
    buf.extend_from_slice(san);
    buf.push(0xFF);
    buf.extend_from_slice(event);
    buf.push(0xFF);
    buf.extend_from_slice(&band.to_le_bytes());
    buf.extend_from_slice(&ply.to_le_bytes());
    buf.extend_from_slice(&child.to_le_bytes());
    (xxh3_64_with_seed(buf, 1), xxh3_64_with_seed(buf, 2))
}

/// The same over (position_hash, kind, reason, end_ply), all LE.
#[inline]
pub fn term_key_hashes(key: (i64, i32, i32, i32)) -> (u64, u64) {
    let mut b = [0u8; 20];
    b[..8].copy_from_slice(&key.0.to_le_bytes());
    b[8..12].copy_from_slice(&key.1.to_le_bytes());
    b[12..16].copy_from_slice(&key.2.to_le_bytes());
    b[16..].copy_from_slice(&key.3.to_le_bytes());
    (xxh3_64_with_seed(&b, 1), xxh3_64_with_seed(&b, 2))
}

/// The EPD's side to move: Some(true) for " w ", Some(false) for " b ".
#[inline]
pub fn epd_white(epd: &str) -> Option<bool> {
    let b = epd.as_bytes();
    let sp = b.iter().position(|&c| c == b' ')?;
    let side = *b.get(sp + 1)?;
    if b.get(sp + 2).is_some_and(|&c| c != b' ') {
        return None;
    }
    match side {
        b'w' => Some(true),
        b'b' => Some(false),
        _ => None,
    }
}

#[inline]
pub fn white_score_avg(w: i64, d: i64, t: i64) -> f64 {
    (w as f64 + 0.5 * d as f64) / t as f64
}

// ── small helpers ────────────────────────────────────────────────────────────

pub(crate) fn fmt_n(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn gb(b: u64) -> f64 {
    b as f64 / 1e9
}

/// Seconds since the epoch as an ISO-8601 UTC time.
pub fn utc_now() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let t = secs % 86_400;
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", t / 3600, t / 60 % 60, t % 60)
}

pub(crate) fn rmtree(p: &Path) -> Result<()> {
    match std::fs::remove_dir_all(p) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", p.display())),
    }
}

pub(crate) fn rm_file(p: &Path) -> Result<()> {
    match std::fs::remove_file(p) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", p.display())),
    }
}

pub(crate) fn write_json_atomic(path: &Path, v: &Value) -> Result<()> {
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d)?;
    }
    let tmp = tmp_of(path);
    std::fs::write(&tmp, serde_json::to_string_pretty(v)? + "\n")
        .with_context(|| format!("writing {}", tmp.display()))?;
    rename_retry(&tmp, path)
}

pub(crate) fn read_json(path: &Path) -> Result<Value> {
    let s = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&s).with_context(|| format!("parsing {}", path.display()))
}

/// One small parquet file, via .tmp and a rename.
fn write_parquet_atomic(path: &Path, batch: &RecordBatch, props: &WriterProperties) -> Result<u64> {
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d)?;
    }
    let tmp = tmp_of(path);
    let f = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    let mut w = ArrowWriter::try_new(f, batch.schema(), Some(props.clone()))?;
    w.write(batch)?;
    w.close()?;
    let n = std::fs::metadata(&tmp)?.len();
    rename_retry(&tmp, path)?;
    Ok(n)
}

fn read_all(path: &Path) -> Result<Vec<RecordBatch>> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let r = ParquetRecordBatchReaderBuilder::try_new(f)
        .with_context(|| format!("reading the footer of {}", path.display()))?
        .build()?;
    r.map(|b| b.map_err(anyhow::Error::from)).collect()
}

pub(crate) fn footer_rows(path: &Path) -> Result<u64> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let b = ParquetRecordBatchReaderBuilder::try_new(f)
        .with_context(|| format!("reading the footer of {}", path.display()))?;
    Ok(b.metadata().file_metadata().num_rows() as u64)
}

fn col_i64(b: &RecordBatch, name: &str) -> Result<Int64Array> {
    let i = b.schema().index_of(name)?;
    b.column(i)
        .as_primitive_opt::<Int64Type>()
        .cloned()
        .ok_or_else(|| anyhow!("column {name} is not int64"))
}

fn col_str(b: &RecordBatch, name: &str) -> Result<StringArray> {
    let i = b.schema().index_of(name)?;
    b.column(i).as_string_opt::<i32>().cloned().ok_or_else(|| anyhow!("column {name} is not utf8"))
}

// ── pre-flight ───────────────────────────────────────────────────────────────

/// A month manifest's numbers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ManNums {
    pub files: u64,
    pub rows: u64,
    pub total: u64,
    pub w: u64,
    pub d: u64,
    pub b: u64,
    pub ply1: u64,
}

pub struct MonthIn {
    pub y: i32,
    pub m: u32,
    pub tag: String,
    bkts: [u64; 8],
    pub man: ManNums,
    pub term_path: PathBuf,
    pub term_bytes: u64,
    pub term_rows: u64,
    /// counters.kept - counters.failed: the month's term SUM(total).
    pub term_total: u64,
    pub commit: String,
    pub source: Option<String>,
}

impl MonthIn {
    pub fn has(&self, b: u32) -> bool {
        (self.bkts[(b / 64) as usize] >> (b % 64)) & 1 == 1
    }
}

/// A month's collision record: a hash with two EPDs, both of which the book
/// must list in _collisions.
#[derive(Clone, Debug)]
pub struct Control {
    pub month: String,
    pub hash: i64,
    pub a: String,
    pub b: String,
}

pub struct Plan {
    pub months_root: PathBuf,
    pub months: Vec<MonthIn>,
    pub params: Value,
    pub producer: String,
    pub controls: Vec<Control>,
    /// Month conflict records of other kinds (edge-child): recorded, not controls.
    pub other_conflicts: Vec<Value>,
    pub term_bytes: u64,
}

fn read_manifest(p: &Path) -> Result<ManNums> {
    let batches = read_all(p)?;
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    if n != 1 {
        bail!("{}: {n} rows, want 1", p.display());
    }
    let b = batches.iter().find(|b| b.num_rows() == 1).unwrap();
    let g = |name: &str| -> Result<u64> {
        let v = col_i64(b, name).with_context(|| p.display().to_string())?.value(0);
        u64::try_from(v).map_err(|_| anyhow!("{}: {name} = {v}", p.display()))
    };
    Ok(ManNums {
        files: g("files")?,
        rows: g("rows")?,
        total: g("total")?,
        w: g("white_wins")?,
        d: g("draws")?,
        b: g("black_wins")?,
        ply1: g("ply1_games")?,
    })
}

fn preflight(cfg: &Config) -> Result<Plan> {
    let root = std::path::absolute(&cfg.months_root)?;
    let mut bad: Vec<String> = Vec::new();

    // 1. Settings.
    let pp = root.join("_extract_params.json");
    let params = read_json(&pp).map_err(|e| refused(format!("{e:#}")))?;
    let want_events: Vec<Value> = EVENTS.iter().map(|e| json!(e)).collect();
    if params["producer"] != json!("rust-month") {
        bad.push(format!("producer is {}, not \"rust-month\"", params["producer"]));
    }
    if params["ply_key"] != json!(true) {
        bad.push(format!(
            "ply_key is {}: this merge is defined for ply-keyed months only (no MIN-ply merging)",
            params["ply_key"]
        ));
    }
    if params["max_ply"] != json!(MAX_PLY) {
        bad.push(format!("max_ply is {}, not {MAX_PLY}", params["max_ply"]));
    }
    if params["buckets"] != json!(BUCKETS) {
        bad.push(format!("buckets is {}, not {BUCKETS}", params["buckets"]));
    }
    if params["events"] != Value::Array(want_events) {
        bad.push(format!("events is {}, not the six speeds {EVENTS:?}", params["events"]));
    }
    if !bad.is_empty() {
        return Err(refused(format!("{}: {}", pp.display(), bad.join("; "))));
    }

    // 2. Months: sentinels, no _tmp_month, and each month's files.
    let mut sentinels = BTreeSet::new();
    let mut tmp_months = Vec::new();
    for e in std::fs::read_dir(&root).with_context(|| format!("listing {}", root.display()))? {
        let name = e?.file_name().to_string_lossy().into_owned();
        if let Some(t) = name.strip_prefix("_month=").and_then(|s| s.strip_suffix(".DONE")) {
            if let Ok(ym) = parse_ym(t) {
                sentinels.insert(ym);
            }
        } else if name.starts_with("_tmp_month=") {
            tmp_months.push(name);
        }
    }
    if !tmp_months.is_empty() {
        return Err(refused(format!(
            "{} holds unfinished month dirs {:?}: a month is still being written",
            root.display(),
            tmp_months
        )));
    }
    let missing: Vec<String> =
        cfg.months.iter().filter(|ym| !sentinels.contains(ym)).map(|&(y, m)| tag(y, m)).collect();
    if !missing.is_empty() {
        return Err(refused(format!(
            "{} of the {} expected months have no _month=Y_M.DONE sentinel: {:?}",
            missing.len(),
            cfg.months.len(),
            missing
        )));
    }
    let mut term_sizes = BTreeMap::new();
    let tdir = root.join("_term");
    for e in std::fs::read_dir(&tdir).with_context(|| format!("listing {}", tdir.display()))? {
        let e = e?;
        term_sizes.insert(e.file_name().to_string_lossy().into_owned(), e.metadata()?.len());
    }
    let mut months = Vec::with_capacity(cfg.months.len());
    for &(y, m) in &cfg.months {
        let t = tag(y, m);
        let mp = root.join("_manifest").join(format!("month={t}.parquet"));
        let pv = root.join("_provenance").join(format!("month={t}.json"));
        let tname = format!("year={y}_month={m}.term.parquet");
        if !mp.exists() || !pv.exists() || !term_sizes.contains_key(&tname) {
            bad.push(format!(
                "{t}: missing{}{}{}",
                if mp.exists() { "" } else { " manifest" },
                if pv.exists() { "" } else { " provenance" },
                if term_sizes.contains_key(&tname) { "" } else { " term file" }
            ));
            continue;
        }
        let man = read_manifest(&mp).map_err(|e| refused(format!("{e:#}")))?;
        let prov = read_json(&pv).map_err(|e| refused(format!("{e:#}")))?;
        let commit = prov["commit"].as_str().unwrap_or("").to_string();
        let (Some(term_rows), Some(kept), Some(failed)) = (
            prov["term_rows"].as_u64(),
            prov["counters"]["kept"].as_u64(),
            prov["counters"]["failed"].as_u64(),
        ) else {
            bad.push(format!("{t}: provenance lacks term_rows / counters.kept / counters.failed"));
            continue;
        };
        if prov["max_ply"] != json!(MAX_PLY) || prov["ply_key"] != json!(true) {
            bad.push(format!(
                "{t}: provenance says max_ply {} ply_key {}",
                prov["max_ply"], prov["ply_key"]
            ));
        }
        // 3. Directory recount: one listing per month.
        let mdir = root.join(format!("month={t}"));
        let mut bkts = [0u64; 8];
        let mut n = 0u64;
        let mut junk = Vec::new();
        for e in std::fs::read_dir(&mdir).with_context(|| format!("listing {}", mdir.display()))? {
            let e = e?;
            let name = e.file_name().to_string_lossy().into_owned();
            match name.strip_prefix("bkt=").and_then(|s| s.parse::<u32>().ok()) {
                Some(b) if b < BUCKETS && e.file_type()?.is_dir() && format!("bkt={b}") == name => {
                    bkts[(b / 64) as usize] |= 1 << (b % 64);
                    n += 1;
                }
                _ => junk.push(name),
            }
        }
        if !junk.is_empty() {
            bad.push(format!("{t}: month dir holds {:?}", &junk[..junk.len().min(5)]));
        }
        if n != man.files || man.files > u64::from(BUCKETS) {
            bad.push(format!("{t}: {n} bkt=N dirs on disk, the manifest says files {}", man.files));
        }
        months.push(MonthIn {
            y,
            m,
            tag: t,
            bkts,
            man,
            term_path: tdir.join(&tname),
            term_bytes: term_sizes[&tname],
            term_rows,
            term_total: kept.saturating_sub(failed),
            commit,
            source: prov["flags"]["source"].as_str().map(str::to_string),
        });
    }
    if !bad.is_empty() {
        return Err(refused(format!("pre-flight: {}", bad.join("; "))));
    }
    let commits: BTreeSet<&str> = months.iter().map(|m| m.commit.as_str()).collect();
    if commits.len() != 1 || commits.contains("") {
        let by: BTreeMap<&str, Vec<&str>> = months.iter().fold(BTreeMap::new(), |mut acc, m| {
            acc.entry(m.commit.as_str()).or_insert_with(Vec::new).push(m.tag.as_str());
            acc
        });
        let desc: Vec<String> = by
            .iter()
            .map(|(c, ts)| format!("{c:?}: {} months ({}{})", ts.len(), ts[..ts.len().min(3)].join(", "),
                                   if ts.len() > 3 { ", ..." } else { "" }))
            .collect();
        return Err(refused(format!(
            "the months were built by more than one producer commit (one producer per book): {}",
            desc.join("; ")
        )));
    }
    let producer = commits.into_iter().next().unwrap().to_string();

    // 4. The positive-control list: every month collision record.
    let mut controls = Vec::new();
    let mut other_conflicts = Vec::new();
    for mi in &months {
        let cdir = root.join("_conflicts").join(format!("month={}", mi.tag));
        if !cdir.is_dir() {
            continue;
        }
        let mut files: Vec<PathBuf> = std::fs::read_dir(&cdir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
            .collect();
        files.sort();
        for f in files {
            for b in read_all(&f).map_err(|e| refused(format!("{e:#}")))? {
                let (h, a, bb, k) =
                    (col_i64(&b, "hash")?, col_str(&b, "epd_a")?, col_str(&b, "epd_b")?, col_str(&b, "kind")?);
                for i in 0..b.num_rows() {
                    if k.value(i) == "parent-epd" {
                        controls.push(Control {
                            month: mi.tag.clone(),
                            hash: h.value(i),
                            a: a.value(i).to_string(),
                            b: bb.value(i).to_string(),
                        });
                    } else {
                        other_conflicts.push(json!({"month": mi.tag, "hash": h.value(i),
                            "a": a.value(i), "b": bb.value(i), "kind": k.value(i)}));
                    }
                }
            }
        }
    }
    let term_bytes = months.iter().map(|m| m.term_bytes).sum();
    Ok(Plan { months_root: root, months, params, producer, controls, other_conflicts, term_bytes })
}

/// The settings lock's content.
fn lock_value(cfg: &Config, plan: &Plan, props: &WriterProperties) -> Value {
    json!({
        "schema": BOOK_SCHEMA,
        "months_root": plan.months_root.display().to_string(),
        "months": plan.months.iter().map(|m| m.tag.clone()).collect::<Vec<_>>(),
        "input_params": plan.params,
        "producer_commit": plan.producer,
        "tool": {"version": sys::VERSION, "commit": sys::GIT_COMMIT, "built": sys::BUILD_DATE},
        "buckets": BUCKETS,
        "row_group_rows": cfg.row_group_rows,
        "zstd_level": ZSTD_LEVEL,
        "writer": writer_settings(props),
    })
}

pub const LOCK_FILE: &str = "_merge_params.json";

fn check_lock(out: &Path, want: &Value) -> Result<()> {
    let p = out.join(LOCK_FILE);
    if p.exists() {
        let have = read_json(&p)?;
        if &have != want {
            let mut diff = Vec::new();
            if let (Some(a), Some(b)) = (have.as_object(), want.as_object()) {
                for k in a.keys().chain(b.keys()).collect::<BTreeSet<_>>() {
                    if a.get(k) != b.get(k) {
                        let s = |v: Option<&Value>| {
                            let t = v.map_or("(absent)".to_string(), |v| v.to_string());
                            if t.chars().count() > 300 { format!("{}...", t.chars().take(300).collect::<String>()) } else { t }
                        };
                        diff.push(format!("{k}: locked {} / this run {}", s(a.get(k)), s(b.get(k))));
                    }
                }
            }
            return Err(refused(format!(
                "{} records different settings; a book is never continued by another build or \
                 other settings (delete the book dir after a code change):\n  {}",
                p.display(),
                diff.join("\n  ")
            )));
        }
        return Ok(());
    }
    write_json_atomic(&p, want)
}

// ── resume ───────────────────────────────────────────────────────────────────

fn done_path(out: &Path, b: u32) -> PathBuf {
    out.join("_done").join(format!("{}.DONE", bucket_dir_name(b)))
}

fn term_done_path(out: &Path) -> PathBuf {
    out.join("_done").join("term.DONE")
}

/// The slice dirs under <out>/ps that exist.
fn slice_dirs(out: &Path) -> Result<Vec<PathBuf>> {
    let mut v = Vec::new();
    let ps = out.join("ps");
    if !ps.is_dir() {
        return Ok(v);
    }
    for e in std::fs::read_dir(&ps)? {
        let e = e?;
        if e.file_type()?.is_dir() {
            for f in std::fs::read_dir(e.path())? {
                let f = f?;
                if f.file_type()?.is_dir() {
                    v.push(f.path());
                }
            }
        }
    }
    v.sort();
    Ok(v)
}

/// Remove every trace of the unfinished buckets (and of term, if unfinished):
/// their staged outputs, published slice files, manifests and collisions.
fn clean_unfinished(out: &Path, todo: &[u32], term: bool) -> Result<usize> {
    let set: BTreeSet<u32> = todo.iter().copied().collect();
    let mut n = 0;
    for &b in todo {
        let name = bucket_dir_name(b);
        let sd = out.join("_stage").join(&name);
        if sd.exists() {
            rmtree(&sd)?;
            n += 1;
        }
        for p in [
            out.join("_manifest").join(format!("{name}.parquet")),
            out.join("_collisions").join(format!("{name}.parquet")),
        ] {
            for q in [tmp_of(&p), p] {
                if q.exists() {
                    rm_file(&q)?;
                    n += 1;
                }
            }
        }
        rm_file(&tmp_of(&done_path(out, b)))?;
    }
    for d in slice_dirs(out)? {
        for e in std::fs::read_dir(&d)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            let stem = name.strip_suffix(".parquet.tmp").or_else(|| name.strip_suffix(".parquet"));
            if let Some(b) = stem.and_then(|s| s.strip_prefix("bkt")).and_then(|s| s.parse::<u32>().ok()) {
                if set.contains(&b) {
                    rm_file(&d.join(&name))?;
                    n += 1;
                }
            }
        }
    }
    if term {
        for p in [out.join("term"), out.join("_stage").join("term")] {
            if p.exists() {
                rmtree(&p)?;
                n += 1;
            }
        }
        rm_file(&tmp_of(&term_done_path(out)))?;
    }
    Ok(n)
}

// ── the run ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct Stats {
    rows: u64,
    /// white_wins, draws, black_wins, total.
    sums: [u64; 4],
    ply1: u64,
}

impl Stats {
    #[inline]
    fn add(&mut self, c: [u64; 4], ply: i32) {
        self.rows += 1;
        for j in 0..4 {
            self.sums[j] += c[j];
        }
        if ply == 1 {
            self.ply1 += c[3];
        }
    }

    fn merge(&mut self, o: &Stats) {
        self.rows += o.rows;
        for j in 0..4 {
            self.sums[j] += o.sums[j];
        }
        self.ply1 += o.ply1;
    }

    fn json(&self) -> Value {
        json!({"rows": self.rows, "white_wins": self.sums[0], "draws": self.sums[1],
               "black_wins": self.sums[2], "total": self.sums[3], "ply1_games": self.ply1})
    }
}

/// A finished bucket, for the log and the progress line.
struct BucketSummary {
    b: u32,
    rows_in: u64,
    months: usize,
    bytes_in: u64,
    stage_secs: f64,
    rows_out: u64,
    files: usize,
    bytes_out: u64,
    positions: u64,
    collision_hashes: usize,
    merge_secs: f64,
    verify_secs: f64,
}

#[derive(Default)]
struct RunState {
    done_before: usize,
    bytes_before: u64,
    done: Vec<BucketSummary>,
    term_secs: Option<f64>,
}

struct Ctx<'a> {
    cfg: &'a Config,
    plan: &'a Plan,
    q: Queue,
    props: WriterProperties,
    schema: SchemaRef,
    term_schema: SchemaRef,
    t0: Instant,
    todo: usize,
    st: Mutex<RunState>,
}

impl Ctx<'_> {
    fn crash_point(&self, p: Phase, b: u32) {
        if self.cfg.crash_at == Some((p, b)) {
            eprintln!("explorer-extract merge: --test-crash-at {p:?}:{b} reached; exiting as if killed");
            std::process::exit(86);
        }
    }

    fn check_stop(&self) -> Result<()> {
        match self.q.stopped() {
            Some(s) => Err(anyhow!("abandoned: the run is stopping ({})", s.msg)),
            None => Ok(()),
        }
    }
}

/// Stops the run if its thread unwinds, so no other thread waits forever.
struct PanicGuard<'a> {
    q: &'a Queue,
    what: &'static str,
    stager: bool,
}

impl Drop for PanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.q.stop(EXIT_FAIL, format!("{} panicked", self.what));
        }
        if self.stager {
            self.q.stager_finished();
        }
    }
}

pub fn run(cfg: &Config) -> Result<()> {
    let t0 = Instant::now();
    eprintln!(
        "{} | merge: {} months, buckets {}, {} threads",
        sys::version_line(),
        cfg.months.len(),
        describe_buckets(&cfg.buckets),
        cfg.threads
    );
    for c in &cfg.no_dictionary {
        if !DICT_TUNABLE.contains(&c.as_str()) {
            return Err(refused(format!("--no-dictionary {c}: only {DICT_TUNABLE:?} may be tuned")));
        }
    }
    // Every pre-flight failure is a refusal (5): nothing has been written yet.
    let plan = preflight(cfg).map_err(|e| if exit_code(&e) == EXIT_FAIL { refused(format!("{e:#}")) } else { e })?;
    eprintln!(
        "  pre-flight: {} months ({} .. {}), producer {}, term {:.1} GB, {} collision control(s)",
        plan.months.len(),
        plan.months.first().map_or("", |m| m.tag.as_str()),
        plan.months.last().map_or("", |m| m.tag.as_str()),
        plan.producer,
        gb(plan.term_bytes),
        plan.controls.len()
    );

    // 6. Placement.
    let in_vol = sys::volume_root(&plan.months_root);
    let stage_vol = sys::volume_root(&cfg.stage_dir);
    if in_vol.is_some() && in_vol == stage_vol {
        if cfg.allow_one_volume {
            eprintln!("  WARNING: --stage-dir is on the input volume (allowed by a test flag)");
        } else {
            return Err(refused(format!(
                "--stage-dir {} is on the input volume {}: staging exists to keep the input \
                 disk's reads whole-file and sequential",
                cfg.stage_dir.display(),
                in_vol.unwrap()
            )));
        }
    }
    if in_vol.is_some() && in_vol == sys::volume_root(&cfg.out) {
        eprintln!(
            "  WARNING: --out {} is on the input volume: reads and writes will share one disk",
            cfg.out.display()
        );
    }
    let workers = cfg.threads.saturating_sub(1).max(1);
    let estimate = BASE_COMMIT + workers as u64 * WORKER_COMMIT + TERM_COMMIT;
    if let Some((limit, avail)) = sys::commit() {
        if avail < estimate + COMMIT_MARGIN {
            return Err(refused(format!(
                "free commit {:.1} GB (of {:.1} GB) is below this run's estimate {:.1} GB + 8 GB",
                gb(avail),
                gb(limit),
                gb(estimate)
            )));
        }
    }
    if cfg.stage_bytes < plan.term_bytes {
        return Err(refused(format!(
            "--stage-gb {:.1} is below the term files' {:.1} GB, which are staged together",
            gb(cfg.stage_bytes),
            gb(plan.term_bytes)
        )));
    }
    let abs = |p: &Path| std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
    let (sd, od, rd) = (abs(&cfg.stage_dir), abs(&cfg.out), abs(&plan.months_root));
    if sd == od || sd.starts_with(&od) || od.starts_with(&sd) || sd.starts_with(&rd) || rd.starts_with(&sd) {
        return Err(refused("--stage-dir must be separate from --out and --months-root"));
    }
    match stage::inspect_stage_dir(&cfg.stage_dir)? {
        stage::StageCheck::Foreign(why) => return Err(refused(format!("--stage-dir: {why}"))),
        _ => {}
    }

    // 5. The settings lock.
    let props = book_writer_props(cfg.row_group_rows, &cfg.no_dictionary);
    std::fs::create_dir_all(&cfg.out).with_context(|| format!("creating {}", cfg.out.display()))?;
    check_lock(&cfg.out, &lock_value(cfg, &plan, &props))?;

    // Resume.
    if let Err(why) = stage::prepare_stage_dir(&cfg.stage_dir)? {
        return Err(refused(format!("--stage-dir: {why}")));
    }
    let todo: Vec<u32> = cfg.buckets.iter().copied().filter(|&b| !done_path(&cfg.out, b).exists()).collect();
    let term_todo = !term_done_path(&cfg.out).exists();
    if !todo.is_empty() || term_todo {
        // A book with work left is not finished, whatever an earlier finalize said.
        rm_file(&cfg.out.join("_BOOK.DONE"))?;
    }
    let cleaned = clean_unfinished(&cfg.out, &todo, term_todo)?;
    let (mut done_before, mut bytes_before) = (0usize, 0u64);
    for b in 0..BUCKETS {
        let p = done_path(&cfg.out, b);
        if p.exists() {
            done_before += 1;
            bytes_before += read_json(&p)?["bytes_out"].as_u64().unwrap_or(0);
        }
    }
    eprintln!(
        "  resume: {} of {} requested buckets to do ({} of {BUCKETS} done in this book), term {}; \
         {cleaned} stale item(s) removed",
        todo.len(),
        cfg.buckets.len(),
        done_before,
        if term_todo { "to do" } else { "done" }
    );
    space_check(cfg, done_before, bytes_before)?;

    let ctx = Ctx {
        cfg,
        plan: &plan,
        q: Queue::new(cfg.stage_bytes),
        props,
        schema: book_ps_schema(),
        term_schema: term_schema_with(true),
        t0,
        todo: todo.len(),
        st: Mutex::new(RunState { done_before, bytes_before, ..Default::default() }),
    };
    if !todo.is_empty() || term_todo {
        run_pipeline(&ctx, &todo, term_todo, workers);
        let _ = stage::prepare_stage_dir(&cfg.stage_dir);
        if let Some(stop) = ctx.q.stopped() {
            // Nothing unverified was published. Free what the unfinished
            // buckets staged; a rerun redoes them.
            let left: Vec<u32> = todo.iter().copied().filter(|&b| !done_path(&cfg.out, b).exists()).collect();
            let _ = clean_unfinished(&cfg.out, &left, !term_done_path(&cfg.out).exists());
            return Err(Exit { code: stop.code, msg: stop.msg }.into());
        }
        let st = ctx.st.lock().unwrap();
        let (rin, rout, bout) = st.done.iter().fold((0u64, 0u64, 0u64), |a, s| (a.0 + s.rows_in, a.1 + s.rows_out, a.2 + s.bytes_out));
        eprintln!(
            "  this run: {} buckets, {} rows in -> {} rows out, {:.2} GB, {:.1} min",
            st.done.len(),
            fmt_n(rin),
            fmt_n(rout),
            gb(bout),
            t0.elapsed().as_secs_f64() / 60.0
        );
    }

    let n_done = (0..BUCKETS).filter(|&b| done_path(&cfg.out, b).exists()).count();
    let term_done = term_done_path(&cfg.out).exists();
    if n_done < BUCKETS as usize || !term_done {
        eprintln!(
            "  {n_done}/{BUCKETS} buckets and {} done: finalize waits for all of them",
            if term_done { "term" } else { "not term" }
        );
        return Ok(());
    }
    if cfg.out.join("_BOOK.DONE").exists() {
        eprintln!("  the book is complete (_BOOK.DONE exists)");
        return Ok(());
    }
    finalize(&ctx)
}

fn describe_buckets(v: &[u32]) -> String {
    let mut parts = Vec::new();
    let mut i = 0;
    while i < v.len() {
        let mut j = i;
        while j + 1 < v.len() && v[j + 1] == v[j] + 1 {
            j += 1;
        }
        parts.push(if i == j { v[i].to_string() } else { format!("{}-{}", v[i], v[j]) });
        i = j + 1;
    }
    parts.join(",")
}

/// Exit 4 when the book's volume is below --min-free-gb, or (after
/// PROJECT_AFTER buckets) when the projected remainder will not fit.
fn space_check(cfg: &Config, done: usize, bytes_done: u64) -> Result<()> {
    let Some(free) = sys::free_bytes(&cfg.out) else { return Ok(()) };
    if free < cfg.min_free_bytes {
        return Err(out_of_space(format!(
            "{} has {:.1} GB free, below --min-free-gb {:.1}",
            cfg.out.display(),
            gb(free),
            gb(cfg.min_free_bytes)
        )));
    }
    if done >= PROJECT_AFTER {
        let remaining = (BUCKETS as usize).saturating_sub(done) as u64;
        let need = bytes_done / done as u64 * remaining;
        if need > free - cfg.min_free_bytes {
            return Err(out_of_space(format!(
                "projected remainder {:.1} GB ({remaining} buckets at {:.2} GB) exceeds the free \
                 {:.1} GB less --min-free-gb {:.1}",
                gb(need),
                gb(bytes_done / done as u64),
                gb(free),
                gb(cfg.min_free_bytes)
            )));
        }
    }
    Ok(())
}

fn run_pipeline(ctx: &Ctx, todo: &[u32], term_todo: bool, workers: usize) {
    std::thread::scope(|s| {
        let mut handles = Vec::new();
        handles.push(s.spawn(|| stager(ctx, todo, term_todo)));
        if term_todo {
            handles.push(s.spawn(|| {
                let _g = PanicGuard { q: &ctx.q, what: "the term task", stager: false };
                if let Some(st) = ctx.q.take_term() {
                    if let Err(e) = do_term(ctx, st) {
                        ctx.q.stop(exit_code(&e), format!("term: {e:#}"));
                    }
                }
            }));
        }
        for _ in 0..workers.min(todo.len().max(1)) {
            handles.push(s.spawn(|| {
                let _g = PanicGuard { q: &ctx.q, what: "a bucket worker", stager: false };
                while let Some(st) = ctx.q.next_bucket() {
                    let b = st.bucket.unwrap();
                    if let Err(e) = do_bucket(ctx, st) {
                        ctx.q.stop(exit_code(&e), format!("bkt {b:03}: {e:#}"));
                        break;
                    }
                }
            }));
        }
        let mut last = Instant::now();
        while !handles.iter().all(|h| h.is_finished()) {
            ctx.q.wait_a_while(Duration::from_secs(1));
            if last.elapsed() >= Duration::from_secs(60) {
                last = Instant::now();
                progress(ctx);
            }
        }
        for h in handles {
            if h.join().is_err() {
                ctx.q.stop(EXIT_FAIL, "a merge thread panicked".into());
            }
        }
    });
}

fn progress(ctx: &Ctx) {
    let (held, waiting, copied, csecs) = ctx.q.snapshot();
    let st = ctx.st.lock().unwrap();
    let n = st.done.len();
    let el = ctx.t0.elapsed().as_secs_f64();
    let eta = if n > 0 { format!("{:.2} h", (ctx.todo - n) as f64 * el / n as f64 / 3600.0) } else { "-".into() };
    let commit = sys::commit().map_or("-".into(), |(l, a)| format!("{:.1}/{:.1} GB", gb(l - a), gb(l)));
    eprintln!(
        "  [{}] buckets {}/{} this run ({}/{BUCKETS} in the book), {} staged ahead ({:.1} GB held); \
         ETA {eta}; staging {:.0} MB/s; term {}; commit {commit}, this process peak {:.1} GB",
        utc_now(),
        n,
        ctx.todo,
        st.done_before + n,
        waiting,
        gb(held),
        if csecs > 0.0 { copied as f64 / 1e6 / csecs } else { 0.0 },
        st.term_secs.map_or("running or done".into(), |s| format!("done in {:.0} s", s)),
        gb(sys::peak_commit().unwrap_or(0))
    );
}

// ── the stager ───────────────────────────────────────────────────────────────

fn stager(ctx: &Ctx, todo: &[u32], term_todo: bool) {
    let _g = PanicGuard { q: &ctx.q, what: "the stager", stager: true };
    let r = (|| -> Result<()> {
        let mut buf = vec![0u8; stage::COPY_BUF];
        if term_todo {
            let jobs: Vec<CopyJob> = ctx
                .plan
                .months
                .iter()
                .enumerate()
                .map(|(i, m)| CopyJob {
                    month: i,
                    src: m.term_path.clone(),
                    name: m.term_path.file_name().unwrap().to_string_lossy().into_owned(),
                })
                .collect();
            let dir = ctx.cfg.stage_dir.join("term");
            match stage::stage_group(&ctx.q, None, &dir, &jobs, &mut buf, |_| {})? {
                Some(st) => {
                    eprintln!(
                        "  staged term: {} files, {:.2} GB in {:.0} s ({:.0} MB/s)",
                        st.files.len(),
                        gb(st.bytes),
                        st.secs,
                        st.bytes as f64 / 1e6 / st.secs.max(1e-9)
                    );
                    ctx.q.push(st);
                }
                None => return Ok(()),
            }
        }
        let root = &ctx.plan.months_root;
        for &b in todo {
            let jobs: Vec<CopyJob> = ctx
                .plan
                .months
                .iter()
                .enumerate()
                .filter(|(_, m)| m.has(b))
                .map(|(i, m)| CopyJob {
                    month: i,
                    src: root.join(format!("month={}", m.tag)).join(format!("bkt={b}")).join("part-0000.parquet"),
                    name: format!("month={}.parquet", m.tag),
                })
                .collect();
            let dir = ctx.cfg.stage_dir.join(bucket_dir_name(b));
            let staged = stage::stage_group(&ctx.q, Some(b), &dir, &jobs, &mut buf, |k| {
                if k == 0 {
                    ctx.crash_point(Phase::Stage, b)
                }
            })
            .with_context(|| format!("staging bkt {b:03}"))?;
            match staged {
                Some(st) => ctx.q.push(st),
                None => return Ok(()),
            }
        }
        Ok(())
    })();
    if let Err(e) = r {
        ctx.q.stop(EXIT_FAIL, format!("{e:#}"));
    }
}

// ── one bucket: the inputs ───────────────────────────────────────────────────

struct PsBatch {
    hash: Int64Array,
    san: StringArray,
    event: StringArray,
    band: Int64Array,
    epd: StringArray,
    child: Int64Array,
    ply: Int32Array,
    w: Int64Array,
    d: Int64Array,
    b: Int64Array,
    t: Int64Array,
    n: usize,
}

const PS_NONNULL: [(usize, &str); 10] = [
    (0, "parent_hash"),
    (1, "move_san"),
    (2, "event"),
    (3, "elo_band"),
    (5, "child_hash"),
    (7, "ply"),
    (8, "white_wins"),
    (9, "draws"),
    (10, "black_wins"),
    (11, "total"),
];

impl PsBatch {
    fn new(b: &RecordBatch, path: &Path, first_row: u64) -> Result<PsBatch> {
        let n = b.num_rows();
        for (i, name) in PS_NONNULL {
            if b.column(i).null_count() > 0 {
                let k = (0..n).find(|&k| b.column(i).is_null(k)).unwrap_or(0);
                bail!("{} row {}: NULL {name}", path.display(), first_row + k as u64);
            }
        }
        let epd = b.column(4);
        if epd.null_count() > 0 {
            let k = (0..n).find(|&k| epd.is_null(k)).unwrap_or(0);
            bail!("{} row {}: NULL parent_epd", path.display(), first_row + k as u64);
        }
        if b.column(6).null_count() != n {
            bail!("{} rows {first_row}..: child_eval is not NULL on every row", path.display());
        }
        let i64c = |i: usize| b.column(i).as_primitive::<Int64Type>().clone();
        Ok(PsBatch {
            hash: i64c(0),
            san: b.column(1).as_string::<i32>().clone(),
            event: b.column(2).as_string::<i32>().clone(),
            band: i64c(3),
            epd: b.column(4).as_string::<i32>().clone(),
            child: i64c(5),
            ply: b.column(7).as_primitive::<Int32Type>().clone(),
            w: i64c(8),
            d: i64c(9),
            b: i64c(10),
            t: i64c(11),
            n,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PsKeyV {
    h: i64,
    san: San,
    ev: u8,
    band: u8,
    ply: u8,
}

struct PsIn {
    mi: usize,
    path: PathBuf,
    reader: ParquetRecordBatchReader,
    cur: Option<PsBatch>,
    i: usize,
    footer: u64,
    read: u64,
    prev: Option<PsKeyV>,
    acc: Stats,
    bytes: u64,
}

impl PsIn {
    fn open(mi: usize, path: &Path, bytes: u64) -> Result<PsIn> {
        let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let bld = ParquetRecordBatchReaderBuilder::try_new(f)
            .with_context(|| format!("reading the footer of {}", path.display()))?;
        if bld.schema().fields() != ps_schema(false).fields() {
            bail!("{}: the schema is not the months' ps schema: {:?}", path.display(), bld.schema());
        }
        let footer = bld.metadata().file_metadata().num_rows() as u64;
        let reader = bld.with_batch_size(READ_BATCH).build()?;
        let mut s = PsIn {
            mi,
            path: path.to_path_buf(),
            reader,
            cur: None,
            i: 0,
            footer,
            read: 0,
            prev: None,
            acc: Stats::default(),
            bytes,
        };
        s.load()?;
        Ok(s)
    }

    fn load(&mut self) -> Result<()> {
        self.cur = None;
        self.i = 0;
        for b in self.reader.by_ref() {
            let b = b.with_context(|| format!("decoding {}", self.path.display()))?;
            if b.num_rows() > 0 {
                self.cur = Some(PsBatch::new(&b, &self.path, self.read)?);
                return Ok(());
            }
        }
        if self.read != self.footer {
            bail!("{}: read {} rows, the footer says {}", self.path.display(), self.read, self.footer);
        }
        Ok(())
    }

    #[inline]
    fn hash(&self) -> Option<i64> {
        self.cur.as_ref().map(|c| c.hash.value(self.i))
    }

    #[inline]
    fn advance(&mut self) -> Result<()> {
        self.i += 1;
        self.read += 1;
        if self.i >= self.cur.as_ref().map_or(0, |c| c.n) {
            self.load()?;
        }
        Ok(())
    }
}

// ── one bucket: the groups ───────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct GRow {
    epd: u32,
    san: San,
    ev: u8,
    band: u8,
    ply: u8,
    month: u16,
    child: i64,
    /// white_wins, draws, black_wins, total.
    c: [u64; 4],
}

impl GRow {
    #[inline]
    fn key(&self) -> (u32, San, u8, u8, u8) {
        (self.epd, self.san, self.ev, self.band, self.ply)
    }
}

/// One parent_hash's rows from every month, and the EPDs they carry.
#[derive(Default)]
struct Group {
    rows: Vec<GRow>,
    epd_buf: Vec<u8>,
    epds: Vec<(u32, u32)>,
}

impl Group {
    fn clear(&mut self) {
        self.rows.clear();
        self.epd_buf.clear();
        self.epds.clear();
    }

    #[inline]
    fn epd(&self, k: u32) -> &[u8] {
        let (s, l) = self.epds[k as usize];
        &self.epd_buf[s as usize..(s + l) as usize]
    }

    #[inline]
    fn epd_index(&mut self, e: &[u8]) -> u32 {
        for k in 0..self.epds.len() as u32 {
            if self.epd(k) == e {
                return k;
            }
        }
        let s = self.epd_buf.len() as u32;
        self.epd_buf.extend_from_slice(e);
        self.epds.push((s, e.len() as u32));
        self.epds.len() as u32 - 1
    }
}

struct CollisionRow {
    hash: i64,
    epd: String,
    n_epds: u32,
    rows: u64,
    sums: [u64; 4],
    months: u32,
    first: String,
    last: String,
}

// ── one bucket: the outputs ──────────────────────────────────────────────────

struct SliceOut {
    ev: u8,
    band: u8,
    path: PathBuf,
    w: ArrowWriter<File>,
    schema: SchemaRef,
    hash: Vec<i64>,
    san: StringBuilder,
    epd: StringBuilder,
    child: Vec<i64>,
    ply: Vec<i32>,
    c: [Vec<i64>; 4],
    wsa: Vec<f64>,
    st: Stats,
}

struct SliceFile {
    ev: u8,
    band: u8,
    path: PathBuf,
    bytes: u64,
    st: Stats,
}

impl SliceOut {
    fn create(dir: &Path, b: u32, ev: u8, band: u8, schema: &SchemaRef, props: &WriterProperties) -> Result<SliceOut> {
        let d = dir.join(format!("event={}", EVENTS[ev as usize])).join(format!("elo_band={}", BANDS[band as usize]));
        std::fs::create_dir_all(&d).with_context(|| format!("creating {}", d.display()))?;
        let path = d.join(format!("{}.parquet", bucket_dir_name(b)));
        let f = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        Ok(SliceOut {
            ev,
            band,
            w: ArrowWriter::try_new(f, schema.clone(), Some(props.clone()))?,
            path,
            schema: schema.clone(),
            hash: Vec::new(),
            san: StringBuilder::new(),
            epd: StringBuilder::new(),
            child: Vec::new(),
            ply: Vec::new(),
            c: Default::default(),
            wsa: Vec::new(),
            st: Stats::default(),
        })
    }

    #[inline]
    fn push(&mut self, h: i64, epd: &[u8], r: &GRow) -> Result<()> {
        self.hash.push(h);
        self.san.append_value(crate::keys::san_str(&r.san));
        // The EPD bytes came from a StringArray, so they are UTF-8.
        self.epd.append_value(std::str::from_utf8(epd)?);
        self.child.push(r.child);
        self.ply.push(i32::from(r.ply));
        for j in 0..4 {
            self.c[j].push(r.c[j] as i64);
        }
        self.wsa.push(white_score_avg(r.c[0] as i64, r.c[1] as i64, r.c[3] as i64));
        self.st.add(r.c, i32::from(r.ply));
        if self.hash.len() >= OUT_BATCH {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let n = self.hash.len();
        if n == 0 {
            return Ok(());
        }
        let ev = EVENTS[self.ev as usize];
        let band = BANDS[self.band as usize];
        let take = |v: &mut Vec<i64>| -> ArrayRef { Arc::new(Int64Array::from(std::mem::take(v))) };
        let [c0, c1, c2, c3] = &mut self.c;
        let cols: Vec<ArrayRef> = vec![
            take(&mut self.hash),
            Arc::new(self.san.finish()),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(ev, n))),
            Arc::new(Int64Array::from(vec![band; n])),
            Arc::new(self.epd.finish()),
            take(&mut self.child),
            Arc::new(Int32Array::from(std::mem::take(&mut self.ply))),
            take(c0),
            take(c1),
            take(c2),
            take(c3),
            Arc::new(Float64Array::from(std::mem::take(&mut self.wsa))),
        ];
        self.w.write(&RecordBatch::try_new(self.schema.clone(), cols)?)?;
        Ok(())
    }

    fn close(mut self) -> Result<SliceFile> {
        self.flush()?;
        self.w.close()?;
        let bytes = std::fs::metadata(&self.path)?.len();
        Ok(SliceFile { ev: self.ev, band: self.band, path: self.path, bytes, st: self.st })
    }
}

// ── one bucket: merge, verify, publish ───────────────────────────────────────

struct BucketMerge<'a> {
    ctx: &'a Ctx<'a>,
    b: u32,
    out_stage: PathBuf,
    ins: Vec<PsIn>,
    heap: BinaryHeap<Reverse<(i64, usize)>>,
    g: Group,
    slices: Vec<Option<SliceOut>>,
    digest: Digest,
    keybuf: Vec<u8>,
    positions: u64,
    rows_in: u64,
    collisions: Vec<CollisionRow>,
    groups: u64,
}

impl<'a> BucketMerge<'a> {
    fn take(&mut self, ci: usize) -> Result<()> {
        let b = self.b;
        let inp = &mut self.ins[ci];
        let bt = inp.cur.as_ref().expect("a current row");
        let i = inp.i;
        let tag = &self.ctx.plan.months[inp.mi].tag;
        let at = || format!("bkt {b:03}, month {tag}, row {}", inp.read);
        let h = bt.hash.value(i);
        if bucket_of(h, BUCKETS) != b {
            bail!("{}: parent_hash {h} is in bucket {}, not {b}", at(), bucket_of(h, BUCKETS));
        }
        let san_s = bt.san.value(i);
        let san = san_of(san_s).with_context(at)?;
        let ev_s = bt.event.value(i);
        let Some(ev) = EVENTS.iter().position(|e| *e == ev_s) else {
            bail!("{}: event {ev_s:?} is not one of {EVENTS:?}", at());
        };
        let band_v = bt.band.value(i);
        let Some(band) = BANDS.iter().position(|&x| x == band_v) else {
            bail!("{}: elo_band {band_v} is not one of {BANDS:?}", at());
        };
        let ply = bt.ply.value(i);
        if !(1..=MAX_PLY).contains(&ply) {
            bail!("{}: ply {ply} is outside 1..={MAX_PLY}", at());
        }
        let key = PsKeyV { h, san, ev: ev as u8, band: band as u8, ply: ply as u8 };
        if let Some(p) = inp.prev {
            if key <= p {
                bail!(
                    "{}: rows are not strictly increasing on (parent_hash, move_san, event, elo_band, \
                     ply): ({h}, {san_s}, {ev_s}, {band_v}, {ply}) follows ({}, {}, {}, {}, {})",
                    at(),
                    p.h,
                    crate::keys::san_str(&p.san),
                    EVENTS[p.ev as usize],
                    BANDS[p.band as usize],
                    p.ply
                );
            }
        }
        inp.prev = Some(key);
        let epd = bt.epd.value(i);
        if epd.is_empty() {
            bail!("{}: empty parent_epd", at());
        }
        match epd_white(epd) {
            Some(white) if white == (ply % 2 == 1) => {}
            Some(_) => bail!("{}: parity: {epd:?} at ply {ply} (ply 1 is White to move)", at()),
            None => bail!("{}: parent_epd {epd:?} has no side to move", at()),
        }
        let (w, d, bl, t) = (bt.w.value(i), bt.d.value(i), bt.b.value(i), bt.t.value(i));
        if w < 0 || d < 0 || bl < 0 || t < 1 || t != w + d + bl {
            bail!("{}: counts W {w} D {d} B {bl} total {t} (want total >= 1 and total = W + D + B)", at());
        }
        let child = bt.child.value(i);
        let c = [w as u64, d as u64, bl as u64, t as u64];
        inp.acc.add(c, ply);
        let (k1, k2) = ps_key_hashes(&mut self.keybuf, h, epd.as_bytes(), san_s.as_bytes(), ev_s.as_bytes(), band_v, ply, child);
        self.digest.add(k1, k2, c);
        let e = self.g.epd_index(epd.as_bytes());
        self.g.rows.push(GRow {
            epd: e,
            san,
            ev: ev as u8,
            band: band as u8,
            ply: ply as u8,
            month: inp.mi as u16,
            child,
            c,
        });
        self.rows_in += 1;
        Ok(())
    }

    /// Take cursor `ci`'s rows with parent_hash `h` (contiguous in a month).
    fn drain(&mut self, ci: usize, h: i64) -> Result<()> {
        loop {
            self.take(ci)?;
            self.ins[ci].advance()?;
            match self.ins[ci].hash() {
                Some(nh) if nh == h => {}
                Some(nh) => {
                    self.heap.push(Reverse((nh, ci)));
                    return Ok(());
                }
                None => return Ok(()),
            }
        }
    }

    fn emit(&mut self, h: i64, epd: &[u8], r: &GRow) -> Result<()> {
        let si = r.ev as usize * BANDS.len() + r.band as usize;
        if self.slices[si].is_none() {
            self.slices[si] = Some(SliceOut::create(&self.out_stage, self.b, r.ev, r.band, &self.ctx.schema, &self.ctx.props)?);
        }
        self.slices[si].as_mut().unwrap().push(h, epd, r)
    }

    fn reduce(&mut self, h: i64) -> Result<()> {
        let mut g = std::mem::take(&mut self.g);
        let n_epd = g.epds.len();
        if n_epd > 1 {
            // Rank the EPDs as strings, so twins come out in EPD order.
            let mut order: Vec<u32> = (0..n_epd as u32).collect();
            order.sort_by(|&x, &y| g.epd(x).cmp(g.epd(y)));
            let mut rank = vec![0u32; n_epd];
            for (r, &e) in order.iter().enumerate() {
                rank[e as usize] = r as u32;
            }
            for row in g.rows.iter_mut() {
                row.epd = rank[row.epd as usize];
            }
            g.epds = order.iter().map(|&e| g.epds[e as usize]).collect();
        }
        if g.rows.len() > 1 {
            g.rows.sort_unstable_by_key(GRow::key);
        }
        self.positions += n_epd as u64;
        let mut per: Vec<(u64, [u64; 4], BTreeSet<u16>)> = if n_epd > 1 {
            vec![(0, [0; 4], BTreeSet::new()); n_epd]
        } else {
            Vec::new()
        };
        let mut i = 0;
        while i < g.rows.len() {
            let mut acc = g.rows[i];
            let mut j = i + 1;
            while j < g.rows.len() && g.rows[j].key() == acc.key() {
                if g.rows[j].child != acc.child {
                    let months: Vec<String> = g.rows[i..]
                        .iter()
                        .take_while(|r| r.key() == acc.key())
                        .map(|r| format!("{} (child {})", self.ctx.plan.months[r.month as usize].tag, r.child))
                        .collect();
                    bail!(
                        "child disagreement: parent_hash {h}, EPD {:?}, SAN {}, {} {}, ply {}: {}",
                        String::from_utf8_lossy(g.epd(acc.epd)),
                        crate::keys::san_str(&acc.san),
                        EVENTS[acc.ev as usize],
                        BANDS[acc.band as usize],
                        acc.ply,
                        months.join(", ")
                    );
                }
                for k in 0..4 {
                    acc.c[k] += g.rows[j].c[k];
                }
                j += 1;
            }
            if n_epd > 1 {
                let p = &mut per[acc.epd as usize];
                p.0 += 1;
                for k in 0..4 {
                    p.1[k] += acc.c[k];
                }
                for r in &g.rows[i..j] {
                    p.2.insert(r.month);
                }
            }
            let (s, l) = g.epds[acc.epd as usize];
            let epd = &g.epd_buf[s as usize..(s + l) as usize];
            self.emit(h, epd, &acc)?;
            i = j;
        }
        if n_epd > 1 {
            for (k, (rows, sums, months)) in per.into_iter().enumerate() {
                let tagm = |m: Option<&u16>| m.map_or(String::new(), |&m| self.ctx.plan.months[m as usize].tag.clone());
                self.collisions.push(CollisionRow {
                    hash: h,
                    epd: String::from_utf8_lossy(g.epd(k as u32)).into_owned(),
                    n_epds: n_epd as u32,
                    rows,
                    sums,
                    months: months.len() as u32,
                    first: tagm(months.first()),
                    last: tagm(months.last()),
                });
            }
        }
        g.clear();
        self.g = g;
        Ok(())
    }

    fn run(&mut self) -> Result<()> {
        for (ci, inp) in self.ins.iter().enumerate() {
            if let Some(h) = inp.hash() {
                self.heap.push(Reverse((h, ci)));
            }
        }
        while let Some(&Reverse((h, _))) = self.heap.peek() {
            while let Some(&Reverse((hh, ci))) = self.heap.peek() {
                if hh != h {
                    break;
                }
                self.heap.pop();
                self.drain(ci, h)?;
            }
            self.reduce(h)?;
            self.groups += 1;
            if self.groups == 1 {
                self.ctx.crash_point(Phase::Merge, self.b);
            }
            if self.groups % 65_536 == 0 {
                self.ctx.check_stop()?;
            }
        }
        Ok(())
    }
}

/// Re-read a staged slice file in full; its stats, and its rows into `dg`.
fn verify_slice(b: u32, sf: &SliceFile, schema: &SchemaRef, dg: &mut Digest, buf: &mut Vec<u8>) -> Result<Stats> {
    let p = sf.path.display();
    let f = File::open(&sf.path)?;
    let bld = ParquetRecordBatchReaderBuilder::try_new(f)?;
    if bld.schema().fields() != schema.fields() {
        bail!("{p}: the schema is not the book's: {:?}", bld.schema());
    }
    let footer = bld.metadata().file_metadata().num_rows() as u64;
    let rd = bld.with_batch_size(VERIFY_BATCH).build()?;
    let (ev_s, band_v) = (EVENTS[sf.ev as usize], BANDS[sf.band as usize]);
    let mut st = Stats::default();
    let (mut ph, mut pepd, mut psan, mut pply, mut first) = (0i64, Vec::<u8>::new(), Vec::<u8>::new(), 0i32, true);
    for batch in rd {
        let batch = batch?;
        for (k, f) in schema.fields().iter().enumerate() {
            if batch.column(k).null_count() > 0 {
                bail!("{p}: NULL {}", f.name());
            }
        }
        let i64c = |k: usize| batch.column(k).as_primitive::<Int64Type>();
        let (h, band, child, w, d, bl, t) = (i64c(0), i64c(3), i64c(5), i64c(7), i64c(8), i64c(9), i64c(10));
        let (san, ev, epd) =
            (batch.column(1).as_string::<i32>(), batch.column(2).as_string::<i32>(), batch.column(4).as_string::<i32>());
        let ply = batch.column(6).as_primitive::<Int32Type>();
        let wsa = batch.column(11).as_primitive::<Float64Type>();
        for i in 0..batch.num_rows() {
            let at = || format!("{p} row {}", st.rows);
            let hv = h.value(i);
            if bucket_of(hv, BUCKETS) != b {
                bail!("{}: parent_hash {hv} is not in bucket {b}", at());
            }
            if ev.value(i) != ev_s || band.value(i) != band_v {
                bail!("{}: event/elo_band {}/{} disagree with the path", at(), ev.value(i), band.value(i));
            }
            let (e, s, pl) = (epd.value(i).as_bytes(), san.value(i).as_bytes(), ply.value(i));
            if !first {
                let o = hv.cmp(&ph).then_with(|| e.cmp(&pepd)).then_with(|| s.cmp(&psan)).then_with(|| pl.cmp(&pply));
                if o != Ordering::Greater {
                    bail!("{}: not strictly increasing on (parent_hash, parent_epd, move_san, ply)", at());
                }
            }
            first = false;
            ph = hv;
            pepd.clear();
            pepd.extend_from_slice(e);
            psan.clear();
            psan.extend_from_slice(s);
            pply = pl;
            if !(1..=MAX_PLY).contains(&pl) {
                bail!("{}: ply {pl}", at());
            }
            if e.is_empty() || epd_white(epd.value(i)) != Some(pl % 2 == 1) {
                bail!("{}: parity: {:?} at ply {pl}", at(), epd.value(i));
            }
            let (wv, dv, bv, tv) = (w.value(i), d.value(i), bl.value(i), t.value(i));
            if wv < 0 || dv < 0 || bv < 0 || tv < 1 || tv != wv + dv + bv {
                bail!("{}: counts {wv} {dv} {bv} {tv}", at());
            }
            if wsa.value(i).to_bits() != white_score_avg(wv, dv, tv).to_bits() {
                bail!("{}: white_score_avg {} != the formula's {}", at(), wsa.value(i), white_score_avg(wv, dv, tv));
            }
            let c = [wv as u64, dv as u64, bv as u64, tv as u64];
            st.add(c, pl);
            let (k1, k2) = ps_key_hashes(buf, hv, e, s, ev_s.as_bytes(), band_v, pl, child.value(i));
            dg.add(k1, k2, c);
        }
    }
    if st.rows != footer {
        bail!("{p}: re-read {} rows, the footer says {footer}", st.rows);
    }
    Ok(st)
}

fn manifest_batch(b: u32, ins: &[(usize, Stats, u64)], plan: &Plan) -> Result<RecordBatch> {
    let i64s = |f: &dyn Fn(&(usize, Stats, u64)) -> i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(ins.iter().map(f)))
    };
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![b as i32; ins.len()])),
        Arc::new(Int32Array::from_iter_values(ins.iter().map(|x| plan.months[x.0].y))),
        Arc::new(Int32Array::from_iter_values(ins.iter().map(|x| plan.months[x.0].m as i32))),
        i64s(&|x| x.1.rows as i64),
        i64s(&|x| x.1.sums[3] as i64),
        i64s(&|x| x.1.sums[0] as i64),
        i64s(&|x| x.1.sums[1] as i64),
        i64s(&|x| x.1.sums[2] as i64),
        i64s(&|x| x.1.ply1 as i64),
        i64s(&|x| x.2 as i64),
    ];
    Ok(RecordBatch::try_new(bucket_manifest_schema(), cols)?)
}

fn collisions_batch(rows: &[CollisionRow]) -> Result<RecordBatch> {
    let i64s = |f: &dyn Fn(&CollisionRow) -> i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(rows.iter().map(f)))
    };
    let cols: Vec<ArrayRef> = vec![
        i64s(&|r| r.hash),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.epd.as_str()))),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.n_epds as i32))),
        i64s(&|r| r.rows as i64),
        i64s(&|r| r.sums[0] as i64),
        i64s(&|r| r.sums[1] as i64),
        i64s(&|r| r.sums[2] as i64),
        i64s(&|r| r.sums[3] as i64),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.months as i32))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.first.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.last.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|_| "parent"))),
    ];
    Ok(RecordBatch::try_new(collisions_schema(), cols)?)
}

fn do_bucket(ctx: &Ctx, staged: Staged) -> Result<()> {
    let cfg = ctx.cfg;
    let b = staged.bucket.expect("a bucket");
    let name = bucket_dir_name(b);
    let out_stage = cfg.out.join("_stage").join(&name);
    rmtree(&out_stage)?;
    let t_merge = Instant::now();
    let mut m = BucketMerge {
        ctx,
        b,
        out_stage: out_stage.clone(),
        ins: Vec::with_capacity(staged.files.len()),
        heap: BinaryHeap::new(),
        g: Group::default(),
        slices: (0..SLICES).map(|_| None).collect(),
        digest: Digest::default(),
        keybuf: Vec::with_capacity(128),
        positions: 0,
        rows_in: 0,
        collisions: Vec::new(),
        groups: 0,
    };
    for (mi, path, bytes) in &staged.files {
        m.ins.push(PsIn::open(*mi, path, *bytes)?);
    }
    m.run()?;
    let ins: Vec<(usize, Stats, u64)> = m.ins.iter().map(|x| (x.mi, x.acc, x.bytes)).collect();
    let mut total_in = Stats::default();
    for (mi, s, _) in &ins {
        if s.rows != footer_of(&m.ins, *mi) {
            bail!("month {}: consumed {} rows of {}", ctx.plan.months[*mi].tag, s.rows, footer_of(&m.ins, *mi));
        }
        total_in.merge(s);
    }
    drop(std::mem::take(&mut m.ins));
    let mut files = Vec::new();
    for s in m.slices.drain(..).flatten() {
        files.push(s.close()?);
    }
    files.sort_by_key(|f| (f.ev, f.band));
    let merge_secs = t_merge.elapsed().as_secs_f64();

    // Verify: every staged output file re-read in full.
    let t_verify = Instant::now();
    let mut dg = Digest::default();
    let mut buf = Vec::with_capacity(128);
    let mut total_out = Stats::default();
    for (k, f) in files.iter().enumerate() {
        let back = verify_slice(b, f, &ctx.schema, &mut dg, &mut buf)?;
        if back != f.st {
            bail!("{}: re-read {:?}, wrote {:?}", f.path.display(), back, f.st);
        }
        total_out.merge(&back);
        if k == 0 {
            ctx.crash_point(Phase::Verify, b);
        }
    }
    if dg != m.digest {
        bail!("the output digest {:?} != the input digest {:?}", dg.0, m.digest.0);
    }
    if total_out.sums != total_in.sums || total_out.ply1 != total_in.ply1 {
        bail!("output sums {:?} / ply-1 {} != input {:?} / {}", total_out.sums, total_out.ply1, total_in.sums, total_in.ply1);
    }
    let verify_secs = t_verify.elapsed().as_secs_f64();
    let bytes_out: u64 = files.iter().map(|f| f.bytes).sum();

    // Space, then publish.
    ctx.check_stop()?;
    {
        let st = ctx.st.lock().unwrap();
        let done = st.done_before + st.done.len();
        let bytes = st.bytes_before + st.done.iter().map(|s| s.bytes_out).sum::<u64>();
        drop(st);
        space_check(cfg, done, bytes)?;
    }
    let t_pub = Instant::now();
    let mut flist = Vec::new();
    for (k, f) in files.iter().enumerate() {
        let rel = ps_rel_path(f.ev, f.band, b);
        let dst = rel_to_path(&cfg.out, &rel);
        std::fs::create_dir_all(dst.parent().unwrap())?;
        rename_retry(&f.path, &dst)?;
        if k == 0 {
            ctx.crash_point(Phase::Publish, b);
        }
        flist.push(json!({"path": rel, "event": EVENTS[f.ev as usize], "elo_band": BANDS[f.band as usize],
                          "bytes": f.bytes, "rows": f.st.rows, "white_wins": f.st.sums[0], "draws": f.st.sums[1],
                          "black_wins": f.st.sums[2], "total": f.st.sums[3], "ply1_games": f.st.ply1}));
    }
    write_parquet_atomic(&cfg.out.join("_manifest").join(format!("{name}.parquet")), &manifest_batch(b, &ins, ctx.plan)?, &ctx.props)?;
    if !m.collisions.is_empty() {
        write_parquet_atomic(&cfg.out.join("_collisions").join(format!("{name}.parquet")), &collisions_batch(&m.collisions)?, &ctx.props)?;
    }
    let hashes: BTreeSet<i64> = m.collisions.iter().map(|c| c.hash).collect();
    let months_json: Vec<Value> = ins
        .iter()
        .map(|(mi, s, bytes)| {
            let mut v = s.json();
            v["month"] = json!(ctx.plan.months[*mi].tag);
            v["bytes"] = json!(bytes);
            v
        })
        .collect();
    let publish_secs = t_pub.elapsed().as_secs_f64();
    let done = json!({
        "bucket": b,
        "rows_in": total_in.rows,
        "rows_out": total_out.rows,
        "white_wins": total_out.sums[0],
        "draws": total_out.sums[1],
        "black_wins": total_out.sums[2],
        "total": total_out.sums[3],
        "ply1_total": total_out.ply1,
        "positions": m.positions,
        "collision_hashes": hashes.len(),
        "collision_rows": m.collisions.len(),
        "collisions": hashes.iter().collect::<Vec<_>>(),
        "bytes_in": staged.bytes,
        "bytes_out": bytes_out,
        "files": flist,
        "months": months_json,
        "digest": dg.to_json(),
        "seconds": {"stage": staged.secs, "merge": merge_secs, "verify": verify_secs, "publish": publish_secs},
        "stage_mb_s": staged.bytes as f64 / 1e6 / staged.secs.max(1e-9),
        "tool": sys::version_line(),
        "finished": utc_now(),
    });
    write_json_atomic(&done_path(&cfg.out, b), &done)?;

    // After the sentinel: the stages go.
    rmtree(&out_stage)?;
    rmtree(&staged.dir)?;
    ctx.q.release(staged.bytes);
    let s = BucketSummary {
        b,
        rows_in: total_in.rows,
        months: ins.len(),
        bytes_in: staged.bytes,
        stage_secs: staged.secs,
        rows_out: total_out.rows,
        files: files.len(),
        bytes_out,
        positions: m.positions,
        collision_hashes: hashes.len(),
        merge_secs,
        verify_secs,
    };
    eprintln!(
        "bkt {:03}: in {} rows / {} months / {:.2} GB (staged {:.0} s, {:.0} MB/s) -> {} rows, {} files, \
         {:.2} GB ({:.1} B/row); positions {}; {} collision{}; merge {:.0} s, verify {:.0} s",
        s.b,
        fmt_n(s.rows_in),
        s.months,
        gb(s.bytes_in),
        s.stage_secs,
        s.bytes_in as f64 / 1e6 / s.stage_secs.max(1e-9),
        fmt_n(s.rows_out),
        s.files,
        gb(s.bytes_out),
        s.bytes_out as f64 / s.rows_out.max(1) as f64,
        fmt_n(s.positions),
        s.collision_hashes,
        if s.collision_hashes == 1 { "" } else { "s" },
        s.merge_secs,
        s.verify_secs
    );
    ctx.st.lock().unwrap().done.push(s);
    Ok(())
}

fn footer_of(ins: &[PsIn], mi: usize) -> u64 {
    ins.iter().find(|x| x.mi == mi).map_or(0, |x| x.footer)
}

// ── term ─────────────────────────────────────────────────────────────────────

type TKey = (i64, i32, i32, i32);

/// Phase T's rule for a term key, from game.rs walk(): a kept game with no
/// moves writes term(START_HASH, 0, 0); every other game writes end_ply =
/// min(30, its moves), kind 1 (horizon) only if it had more moves than that.
pub fn term_key_rule(key: (i64, i32, i32, i32)) -> std::result::Result<(), String> {
    let (hash, kind, end) = (key.0, key.1, key.3);
    match kind {
        0 if !(0..=MAX_PLY).contains(&end) => Err(format!("kind 0 with end_ply {end}, outside 0..={MAX_PLY}")),
        0 if end == 0 && hash != START_HASH => Err(format!(
            "end_ply 0 at position_hash {hash}: only the start position {START_HASH} ends at ply 0"
        )),
        1 if end != MAX_PLY => Err(format!("kind 1 (horizon) with end_ply {end}, not {MAX_PLY}")),
        0 | 1 => Ok(()),
        _ => Err(format!("kind {kind} is not 0 or 1")),
    }
}

struct TermBatch {
    hash: Int64Array,
    kind: Int32Array,
    reason: Int32Array,
    end: Int32Array,
    c: [Int64Array; 4],
    n: usize,
}

struct TermIn {
    mi: usize,
    path: PathBuf,
    reader: ParquetRecordBatchReader,
    cur: Option<TermBatch>,
    i: usize,
    footer: u64,
    read: u64,
    prev: Option<TKey>,
    acc: Stats,
    bytes: u64,
}

impl TermIn {
    fn open(mi: usize, path: &Path, bytes: u64, schema: &SchemaRef) -> Result<TermIn> {
        let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let bld = ParquetRecordBatchReaderBuilder::try_new(f)?;
        if bld.schema().fields() != schema.fields() {
            bail!("{}: the schema is not the ply-keyed term schema: {:?}", path.display(), bld.schema());
        }
        let footer = bld.metadata().file_metadata().num_rows() as u64;
        let reader = bld.with_batch_size(READ_BATCH).build()?;
        let mut s = TermIn { mi, path: path.to_path_buf(), reader, cur: None, i: 0, footer, read: 0, prev: None, acc: Stats::default(), bytes };
        s.load()?;
        Ok(s)
    }

    fn load(&mut self) -> Result<()> {
        self.cur = None;
        self.i = 0;
        for b in self.reader.by_ref() {
            let b = b?;
            let n = b.num_rows();
            if n == 0 {
                continue;
            }
            for k in 0..b.num_columns() {
                if b.column(k).null_count() > 0 {
                    bail!("{} rows {}..: NULL {}", self.path.display(), self.read, b.schema().field(k).name());
                }
            }
            let i32c = |k: usize| b.column(k).as_primitive::<Int32Type>().clone();
            let i64c = |k: usize| b.column(k).as_primitive::<Int64Type>().clone();
            self.cur = Some(TermBatch { hash: i64c(0), kind: i32c(1), reason: i32c(2), end: i32c(3), c: [i64c(4), i64c(5), i64c(6), i64c(7)], n });
            return Ok(());
        }
        if self.read != self.footer {
            bail!("{}: read {} rows, the footer says {}", self.path.display(), self.read, self.footer);
        }
        Ok(())
    }

    #[inline]
    fn key(&self) -> Option<TKey> {
        self.cur.as_ref().map(|c| (c.hash.value(self.i), c.kind.value(self.i), c.reason.value(self.i), c.end.value(self.i)))
    }

    /// Validate and take the current row into `sum` and `dg`, then advance.
    fn take(&mut self, sum: &mut [u64; 4], dg: &mut Digest, tag: &str) -> Result<()> {
        let bt = self.cur.as_ref().expect("a current row");
        let i = self.i;
        let key = (bt.hash.value(i), bt.kind.value(i), bt.reason.value(i), bt.end.value(i));
        let at = || format!("term, month {tag}, row {}", self.read);
        if let Some(p) = self.prev {
            if key <= p {
                bail!("{}: rows are not strictly increasing on TermKey: {key:?} follows {p:?}", at());
            }
        }
        if let Err(why) = term_key_rule(key) {
            bail!("{}: {why}", at());
        }
        let c = [bt.c[0].value(i), bt.c[1].value(i), bt.c[2].value(i), bt.c[3].value(i)];
        if c.iter().any(|&x| x < 0) || c[3] < 1 {
            bail!("{}: counts {c:?} (want total >= 1)", at());
        }
        let c = c.map(|x| x as u64);
        self.prev = Some(key);
        self.acc.add(c, 0);
        let (k1, k2) = term_key_hashes(key);
        dg.add(k1, k2, c);
        for j in 0..4 {
            sum[j] += c[j];
        }
        self.i += 1;
        self.read += 1;
        if self.i >= bt.n {
            self.load()?;
        }
        Ok(())
    }
}

struct TermOut {
    path: PathBuf,
    w: ArrowWriter<File>,
    rows: Vec<(TKey, [u64; 4])>,
    st: Stats,
}

impl TermOut {
    fn flush(&mut self, schema: &SchemaRef) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let r = &self.rows;
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(r.iter().map(|x| x.0 .0))),
            Arc::new(Int32Array::from_iter_values(r.iter().map(|x| x.0 .1))),
            Arc::new(Int32Array::from_iter_values(r.iter().map(|x| x.0 .2))),
            Arc::new(Int32Array::from_iter_values(r.iter().map(|x| x.0 .3))),
            Arc::new(Int64Array::from_iter_values(r.iter().map(|x| x.1[0] as i64))),
            Arc::new(Int64Array::from_iter_values(r.iter().map(|x| x.1[1] as i64))),
            Arc::new(Int64Array::from_iter_values(r.iter().map(|x| x.1[2] as i64))),
            Arc::new(Int64Array::from_iter_values(r.iter().map(|x| x.1[3] as i64))),
        ];
        self.w.write(&RecordBatch::try_new(schema.clone(), cols)?)?;
        self.rows.clear();
        Ok(())
    }
}

fn verify_term_file(b: u32, path: &Path, schema: &SchemaRef, dg: &mut Digest) -> Result<Stats> {
    let p = path.display();
    let bld = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    if bld.schema().fields() != schema.fields() {
        bail!("{p}: the schema is not the term schema: {:?}", bld.schema());
    }
    let footer = bld.metadata().file_metadata().num_rows() as u64;
    let mut st = Stats::default();
    let mut prev: Option<TKey> = None;
    for batch in bld.with_batch_size(VERIFY_BATCH).build()? {
        let batch = batch?;
        for k in 0..batch.num_columns() {
            if batch.column(k).null_count() > 0 {
                bail!("{p}: NULL {}", schema.field(k).name());
            }
        }
        let i32c = |k: usize| batch.column(k).as_primitive::<Int32Type>();
        let i64c = |k: usize| batch.column(k).as_primitive::<Int64Type>();
        let (h, kind, reason, end) = (i64c(0), i32c(1), i32c(2), i32c(3));
        let cs = [i64c(4), i64c(5), i64c(6), i64c(7)];
        for i in 0..batch.num_rows() {
            let key = (h.value(i), kind.value(i), reason.value(i), end.value(i));
            if bucket_of(key.0, BUCKETS) != b {
                bail!("{p} row {}: position_hash {} is not in bucket {b}", st.rows, key.0);
            }
            if prev.is_some_and(|q| key <= q) {
                bail!("{p} row {}: not strictly increasing on TermKey", st.rows);
            }
            prev = Some(key);
            if let Err(why) = term_key_rule(key) {
                bail!("{p} row {}: {why}", st.rows);
            }
            let c = [cs[0].value(i), cs[1].value(i), cs[2].value(i), cs[3].value(i)];
            if c.iter().any(|&x| x < 0) || c[3] < 1 {
                bail!("{p} row {}: counts {c:?}", st.rows);
            }
            let c = c.map(|x| x as u64);
            st.add(c, 0);
            let (k1, k2) = term_key_hashes(key);
            dg.add(k1, k2, c);
        }
    }
    if st.rows != footer {
        bail!("{p}: re-read {} rows, the footer says {footer}", st.rows);
    }
    Ok(st)
}

fn do_term(ctx: &Ctx, staged: Staged) -> Result<()> {
    let cfg = ctx.cfg;
    let t0 = Instant::now();
    let out_stage = cfg.out.join("_stage").join("term");
    rmtree(&out_stage)?;
    std::fs::create_dir_all(&out_stage)?;
    let schema = &ctx.term_schema;
    let mut ins = Vec::with_capacity(staged.files.len());
    for (mi, path, bytes) in &staged.files {
        ins.push(TermIn::open(*mi, path, *bytes, schema)?);
    }
    let tags: Vec<String> = ins.iter().map(|x| ctx.plan.months[x.mi].tag.clone()).collect();
    let mut outs: Vec<Option<TermOut>> = (0..BUCKETS).map(|_| None).collect();
    let open = |b: u32| -> Result<TermOut> {
        let path = out_stage.join(format!("{}.parquet", bucket_dir_name(b)));
        let f = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        Ok(TermOut { w: ArrowWriter::try_new(f, schema.clone(), Some(ctx.props.clone()))?, path, rows: Vec::new(), st: Stats::default() })
    };
    let mut heap: BinaryHeap<Reverse<(TKey, usize)>> =
        ins.iter().enumerate().filter_map(|(ci, x)| x.key().map(|k| Reverse((k, ci)))).collect();
    let mut dg_in = Digest::default();
    let mut emitted = 0u64;
    while let Some(Reverse((key, ci))) = heap.pop() {
        let mut sum = [0u64; 4];
        ins[ci].take(&mut sum, &mut dg_in, &tags[ci])?;
        if let Some(k) = ins[ci].key() {
            heap.push(Reverse((k, ci)));
        }
        while let Some(&Reverse((k2, cj))) = heap.peek() {
            if k2 != key {
                break;
            }
            heap.pop();
            ins[cj].take(&mut sum, &mut dg_in, &tags[cj])?;
            if let Some(k) = ins[cj].key() {
                heap.push(Reverse((k, cj)));
            }
        }
        let b = bucket_of(key.0, BUCKETS);
        if outs[b as usize].is_none() {
            outs[b as usize] = Some(open(b)?);
        }
        let o = outs[b as usize].as_mut().unwrap();
        o.rows.push((key, sum));
        o.st.add(sum, 0);
        if o.rows.len() >= TERM_OUT_BATCH {
            o.flush(schema)?;
        }
        emitted += 1;
        if emitted == 1 {
            ctx.crash_point(Phase::Term, 0);
        }
        if emitted % 1_048_576 == 0 {
            ctx.check_stop()?;
        }
    }
    let mut month_stats = Vec::new();
    let mut total_in = Stats::default();
    for x in &ins {
        if x.read != x.footer || x.cur.is_some() {
            bail!("{}: consumed {} of {} rows", x.path.display(), x.read, x.footer);
        }
        total_in.merge(&x.acc);
        month_stats.push((x.mi, x.acc, x.bytes));
    }
    drop(ins);
    let mut files = Vec::new();
    for b in 0..BUCKETS {
        let mut o = match outs[b as usize].take() {
            Some(o) => o,
            None => open(b)?,
        };
        o.flush(schema)?;
        o.w.close()?;
        let bytes = std::fs::metadata(&o.path)?.len();
        files.push((b, o.path, bytes, o.st));
    }
    let merge_secs = t0.elapsed().as_secs_f64();

    let tv = Instant::now();
    let mut dg_out = Digest::default();
    let mut total_out = Stats::default();
    for (b, path, _, st) in &files {
        let back = verify_term_file(*b, path, schema, &mut dg_out)?;
        if back != *st {
            bail!("{}: re-read {:?}, wrote {:?}", path.display(), back, st);
        }
        total_out.merge(&back);
    }
    if dg_out != dg_in {
        bail!("term: the output digest {:?} != the input digest {:?}", dg_out.0, dg_in.0);
    }
    if total_out.sums != total_in.sums {
        bail!("term: output sums {:?} != input {:?}", total_out.sums, total_in.sums);
    }
    let verify_secs = tv.elapsed().as_secs_f64();

    ctx.check_stop()?;
    let tp = Instant::now();
    std::fs::create_dir_all(cfg.out.join("term"))?;
    let mut flist = Vec::new();
    for (k, (b, path, bytes, st)) in files.iter().enumerate() {
        let rel = term_rel_path(*b);
        rename_retry(path, &rel_to_path(&cfg.out, &rel))?;
        if k == 0 {
            ctx.crash_point(Phase::TermPublish, 0);
        }
        let mut v = st.json();
        v["path"] = json!(rel);
        v["bucket"] = json!(b);
        v["bytes"] = json!(bytes);
        flist.push(v);
    }
    let months_json: Vec<Value> = month_stats
        .iter()
        .map(|(mi, s, bytes)| {
            let mut v = s.json();
            v["month"] = json!(ctx.plan.months[*mi].tag);
            v["bytes"] = json!(bytes);
            v
        })
        .collect();
    let bytes_out: u64 = files.iter().map(|f| f.2).sum();
    let done = json!({
        "rows_in": total_in.rows,
        "rows_out": total_out.rows,
        "white_wins": total_out.sums[0],
        "draws": total_out.sums[1],
        "black_wins": total_out.sums[2],
        "total": total_out.sums[3],
        "bytes_in": staged.bytes,
        "bytes_out": bytes_out,
        "files": flist,
        "months": months_json,
        "digest": dg_out.to_json(),
        "seconds": {"stage": staged.secs, "merge": merge_secs, "verify": verify_secs, "publish": tp.elapsed().as_secs_f64()},
        "tool": sys::version_line(),
        "finished": utc_now(),
    });
    write_json_atomic(&term_done_path(&cfg.out), &done)?;
    rmtree(&out_stage)?;
    rmtree(&staged.dir)?;
    ctx.q.release(staged.bytes);
    let secs = t0.elapsed().as_secs_f64();
    ctx.st.lock().unwrap().term_secs = Some(secs);
    eprintln!(
        "term: in {} rows / {} months / {:.2} GB (staged {:.0} s) -> {} rows, 512 files, {:.2} GB; \
         merge {:.0} s, verify {:.0} s",
        fmt_n(total_in.rows),
        month_stats.len(),
        gb(staged.bytes),
        staged.secs,
        fmt_n(total_out.rows),
        gb(bytes_out),
        merge_secs,
        verify_secs
    );
    Ok(())
}

// ── finalize ─────────────────────────────────────────────────────────────────

fn u(v: &Value, k: &str) -> Result<u64> {
    v[k].as_u64().ok_or_else(|| anyhow!("sentinel field {k:?} is missing"))
}

/// The files under `dir` (recursively), as book-relative paths.
fn list_files(root: &Path, dir: &Path, out: &mut BTreeSet<String>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let p = e.path();
        if e.file_type()?.is_dir() {
            list_files(root, &p, out)?;
        } else {
            let rel = p.strip_prefix(root)?.components().map(|c| c.as_os_str().to_string_lossy().into_owned()).collect::<Vec<_>>().join("/");
            out.insert(rel);
        }
    }
    Ok(())
}

pub const READ_CONTRACT: [&str; 10] = [
    "Whole book: read_parquet('<book>/ps/event=*/elo_band=*/*.parquet'). One slice: glob ps/event=Blitz/elo_band=1800/*.parquet.",
    "By position: h = zobrist_int64(board), epd = board.epd(), i = ((h % 512) + 512) % 512; read ps/event=*/elo_band=*/bkt{i:03d}.parquet; filter parent_hash = h AND parent_epd = epd. The EPD test is the collision contract: both sides use python-chess's legal-en-passant EPD.",
    "Lichess-explorer counts sum over ply. A lower coverage cap C uses ply <= C. For endings at cap C, use ply_cap.derived_term: it is linear, so it holds for the summed book.",
    "Walking to a child: push the move and look up the child board's hash and EPD. Never follow a bare child_hash: a twin that only ever appears as a child cannot be detected here.",
    "Hash-only consumers (stage 3's dicts, eval_arrays) must read _collisions.parquet and exclude or handle those hashes. They must never take the first row.",
    "Term is pooled over event and elo_band, and keyed by position_hash alone (term/bkt<iii>.parquet holds the positions of ps bucket iii). So the endings of collision twins are summed together, and per-slice 'games that ended here' is not available.",
    "Key: (parent_hash, parent_epd, move_san, event, elo_band, ply), unique across the book. Within a slice file rows are strictly increasing on (parent_hash, parent_epd, move_san, ply); collision twins sit side by side, grouped by EPD.",
    "event and elo_band are stored in every file as well as in the path, so every hive mode of DuckDB and Polars returns them.",
    "pyarrow's default hive partitioning clashes with those in-file columns (pyarrow 24: 'Unable to merge: Field event has incompatible types'), for a file or the tree alike: read with pq.read_table(path, partitioning=None) or pq.ParquetFile(path).read().",
    "white_score_avg = (white_wins + 0.5 * draws) / total, in IEEE doubles.",
];

fn population(plan: &Plan) -> String {
    let p = &plan.params;
    format!(
        "Population: rated, non-tournament, standard games{}, months {} to {} ({} months), events {}; min_elo {}, \
         exclude_bots {}, excluded terminations {}. The months stay at {} as the per-month (time-series) artifact.",
        plan.months.first().and_then(|m| m.source.as_ref()).map_or(String::new(), |s| format!(" from {s}")),
        plan.months.first().map_or("", |m| m.tag.as_str()),
        plan.months.last().map_or("", |m| m.tag.as_str()),
        plan.months.len(),
        p["events"],
        p["min_elo"],
        p["exclude_bots"],
        p["excluded_terminations"],
        plan.months_root.display()
    )
}

fn finalize(ctx: &Ctx) -> Result<()> {
    let (cfg, plan) = (ctx.cfg, ctx.plan);
    let out = &cfg.out;
    let t0 = Instant::now();
    eprintln!("finalize: all {BUCKETS} buckets and term are done");
    let mut dones = Vec::with_capacity(BUCKETS as usize);
    for b in 0..BUCKETS {
        dones.push(read_json(&done_path(out, b))?);
    }
    let tdone = read_json(&term_done_path(out))?;

    // 1. The tree against the sentinels, and every footer.
    let mut want: BTreeMap<String, u64> = BTreeMap::new();
    for d in dones.iter().chain(std::iter::once(&tdone)) {
        for f in d["files"].as_array().ok_or_else(|| anyhow!("a sentinel has no file list"))? {
            let rel = f["path"].as_str().ok_or_else(|| anyhow!("a file entry has no path"))?.to_string();
            if want.insert(rel.clone(), u(f, "rows")?).is_some() {
                bail!("{rel} is listed by two sentinels");
            }
        }
    }
    let mut have = BTreeSet::new();
    list_files(out, &out.join("ps"), &mut have)?;
    list_files(out, &out.join("term"), &mut have)?;
    let want_set: BTreeSet<String> = want.keys().cloned().collect();
    if have != want_set {
        let extra: Vec<_> = have.difference(&want_set).take(5).collect();
        let missing: Vec<_> = want_set.difference(&have).take(5).collect();
        bail!("the book's files differ from the sentinels': on disk only {extra:?}, listed only {missing:?}");
    }
    for (rel, rows) in &want {
        let n = footer_rows(&rel_to_path(out, rel))?;
        if n != *rows {
            bail!("{rel}: the footer says {n} rows, the sentinel {rows}");
        }
    }
    let mut left = BTreeSet::new();
    list_files(out, &out.join("_stage"), &mut left)?;
    if !left.is_empty() {
        bail!("_stage still holds {:?}", left.iter().take(5).collect::<Vec<_>>());
    }
    rmtree(&out.join("_stage"))?;
    let n_term = want.keys().filter(|k| k.starts_with("term/")).count();
    if n_term != BUCKETS as usize {
        bail!("term has {n_term} files, want {BUCKETS}");
    }

    // 2. Per-month conservation, exact.
    let mut per: BTreeMap<String, Stats> = BTreeMap::new();
    for d in &dones {
        for m in d["months"].as_array().ok_or_else(|| anyhow!("a sentinel has no months"))? {
            let s = per.entry(m["month"].as_str().unwrap_or("").to_string()).or_default();
            s.merge(&Stats { rows: u(m, "rows")?, sums: [u(m, "white_wins")?, u(m, "draws")?, u(m, "black_wins")?, u(m, "total")?], ply1: u(m, "ply1_games")? });
        }
    }
    let mut tper: BTreeMap<String, Stats> = BTreeMap::new();
    for m in tdone["months"].as_array().ok_or_else(|| anyhow!("term.DONE has no months"))? {
        tper.insert(m["month"].as_str().unwrap_or("").to_string(),
                    Stats { rows: u(m, "rows")?, sums: [u(m, "white_wins")?, u(m, "draws")?, u(m, "black_wins")?, u(m, "total")?], ply1: 0 });
    }
    let mut bad = Vec::new();
    let mut man_tot = Stats::default();
    for mi in &plan.months {
        let got = per.remove(&mi.tag).unwrap_or_default();
        let man = &mi.man;
        let want = Stats { rows: man.rows, sums: [man.w, man.d, man.b, man.total], ply1: man.ply1 };
        man_tot.merge(&want);
        if got != want {
            bad.push(format!("{}: consumed {got:?}, the manifest says {want:?}", mi.tag));
        }
        let t = tper.remove(&mi.tag).unwrap_or_default();
        if t.rows != mi.term_rows || t.sums[3] != mi.term_total {
            bad.push(format!(
                "{} term: consumed {} rows / total {}, provenance term_rows {} / kept - failed {}",
                mi.tag, t.rows, t.sums[3], mi.term_rows, mi.term_total
            ));
        }
    }
    if !per.is_empty() || !tper.is_empty() {
        bad.push(format!("sentinels name months outside the plan: {:?} {:?}", per.keys().collect::<Vec<_>>(), tper.keys().collect::<Vec<_>>()));
    }
    if !bad.is_empty() {
        bail!("per-month conservation failed:\n  {}", bad.join("\n  "));
    }

    // 3. Book totals against the manifests.
    let mut book = Stats::default();
    let (mut rows_in, mut bytes, mut positions) = (0u64, 0u64, 0u64);
    let mut secs = [0f64; 4];
    let mut staged = (0u64, 0f64);
    for d in &dones {
        book.merge(&Stats { rows: u(d, "rows_out")?, sums: [u(d, "white_wins")?, u(d, "draws")?, u(d, "black_wins")?, u(d, "total")?], ply1: u(d, "ply1_total")? });
        rows_in += u(d, "rows_in")?;
        bytes += u(d, "bytes_out")?;
        positions += u(d, "positions")?;
        for (k, n) in ["stage", "merge", "verify", "publish"].iter().enumerate() {
            secs[k] += d["seconds"][n].as_f64().unwrap_or(0.0);
        }
        staged.0 += u(d, "bytes_in")?;
        staged.1 += d["seconds"]["stage"].as_f64().unwrap_or(0.0);
    }
    if book.sums != man_tot.sums || book.ply1 != man_tot.ply1 || rows_in != man_tot.rows {
        bail!(
            "book totals {:?}, ply-1 {}, rows in {} != the manifests' {:?}, ply-1 {}, rows {}",
            book.sums, book.ply1, rows_in, man_tot.sums, man_tot.ply1, man_tot.rows
        );
    }

    // 4. Collisions, and the positive control.
    let mut coll: Vec<RecordBatch> = Vec::new();
    for b in 0..BUCKETS {
        let p = out.join("_collisions").join(format!("{}.parquet", bucket_dir_name(b)));
        if p.exists() {
            coll.extend(read_all(&p)?);
        }
    }
    let mut pairs: BTreeSet<(i64, String)> = BTreeSet::new();
    for b in &coll {
        let (h, e) = (col_i64(b, "parent_hash")?, col_str(b, "parent_epd")?);
        for i in 0..b.num_rows() {
            pairs.insert((h.value(i), e.value(i).to_string()));
        }
    }
    let hashes: BTreeSet<i64> = pairs.iter().map(|p| p.0).collect();
    let coll_batch = if coll.is_empty() {
        collisions_batch(&[])?
    } else {
        arrow::compute::concat_batches(&collisions_schema(), &coll)?
    };
    write_parquet_atomic(&out.join("_collisions.parquet"), &coll_batch, &ctx.props)?;
    let mut controls = Vec::new();
    let mut failed = Vec::new();
    for c in &plan.controls {
        let ok = pairs.contains(&(c.hash, c.a.clone())) && pairs.contains(&(c.hash, c.b.clone()));
        controls.push(json!({"month": c.month, "hash": c.hash, "epd_a": c.a, "epd_b": c.b, "present": ok}));
        if !ok {
            failed.push(format!("{} hash {}", c.month, c.hash));
        }
    }
    if !failed.is_empty() {
        bail!("positive control failed: these month collision records lack one or both EPDs in _collisions: {failed:?}");
    }
    if hashes.len() > COLLISIONS_FATAL {
        bail!("{} collision hashes (> {COLLISIONS_FATAL}): a hash or EPD bug; no _BOOK.DONE", hashes.len());
    }
    let coll_status = if hashes.len() < COLLISIONS_LOW || hashes.len() > COLLISIONS_HIGH {
        eprintln!(
            "  WARNING: {} collision hashes, outside the expected {COLLISIONS_LOW}-{COLLISIONS_HIGH} for the full book",
            hashes.len()
        );
        "warn"
    } else {
        "ok"
    };

    // 5. _slices.parquet.
    let mut sl: BTreeMap<(usize, usize), (u64, u64, Stats)> = BTreeMap::new();
    for d in &dones {
        for f in d["files"].as_array().unwrap() {
            let ev = EVENTS.iter().position(|e| Some(*e) == f["event"].as_str()).ok_or_else(|| anyhow!("bad event in a sentinel"))?;
            let band = BANDS.iter().position(|&x| Some(x) == f["elo_band"].as_i64()).ok_or_else(|| anyhow!("bad band in a sentinel"))?;
            let e = sl.entry((ev, band)).or_default();
            e.0 += 1;
            e.1 += u(f, "bytes")?;
            e.2.merge(&Stats { rows: u(f, "rows")?, sums: [u(f, "white_wins")?, u(f, "draws")?, u(f, "black_wins")?, u(f, "total")?], ply1: u(f, "ply1_games")? });
        }
    }
    let keys: Vec<(usize, usize)> = (0..EVENTS.len()).flat_map(|e| (0..BANDS.len()).map(move |b| (e, b))).collect();
    let get = |k: &(usize, usize)| sl.get(k).copied().unwrap_or_default();
    let i64s = |f: &dyn Fn(&(u64, u64, Stats)) -> u64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(keys.iter().map(|k| f(&get(k)) as i64)))
    };
    let slices = RecordBatch::try_new(slices_schema(), vec![
        Arc::new(StringArray::from_iter_values(keys.iter().map(|k| EVENTS[k.0]))),
        Arc::new(Int64Array::from_iter_values(keys.iter().map(|k| BANDS[k.1]))),
        i64s(&|x| x.0),
        i64s(&|x| x.2.rows),
        i64s(&|x| x.1),
        i64s(&|x| x.2.sums[3]),
        i64s(&|x| x.2.sums[0]),
        i64s(&|x| x.2.sums[1]),
        i64s(&|x| x.2.sums[2]),
        i64s(&|x| x.2.ply1),
    ])?;
    write_parquet_atomic(&out.join("_slices.parquet"), &slices, &ctx.props)?;

    // 6. The meta and README.
    let files = want.len() - n_term;
    let meta = json!({
        "schema": BOOK_SCHEMA,
        "tool": {"line": sys::version_line(), "version": sys::VERSION, "commit": sys::GIT_COMMIT, "built": sys::BUILD_DATE},
        "input": {
            "months_root": plan.months_root.display().to_string(),
            "months": plan.months.iter().map(|m| m.tag.clone()).collect::<Vec<_>>(),
            "params": plan.params,
            "producer_commit": plan.producer,
        },
        "writer": writer_settings(&ctx.props),
        "totals": {
            "rows_in": rows_in, "rows": book.rows, "compaction": book.rows as f64 / rows_in.max(1) as f64,
            "white_wins": book.sums[0], "draws": book.sums[1], "black_wins": book.sums[2], "total": book.sums[3],
            "ply1_games": book.ply1, "files": files, "bytes": bytes,
            "bytes_per_row": bytes as f64 / book.rows.max(1) as f64, "positions": positions,
        },
        "term": {"rows_in": tdone["rows_in"], "rows": tdone["rows_out"], "total": tdone["total"], "bytes": tdone["bytes_out"], "files": n_term},
        "collisions": {"hashes": hashes.len(), "rows": pairs.len(), "list": hashes.iter().collect::<Vec<_>>(),
                       "status": coll_status, "positive_controls": controls, "other_month_conflicts": plan.other_conflicts},
        "timings": {"stage_secs": secs[0], "merge_secs": secs[1], "verify_secs": secs[2], "publish_secs": secs[3],
                    "term": tdone["seconds"], "staging_mb_s": staged.0 as f64 / 1e6 / staged.1.max(1e-9),
                    "finalize_secs": t0.elapsed().as_secs_f64(), "this_run_wall_secs": ctx.t0.elapsed().as_secs_f64()},
        "peak_commit_bytes_this_run": sys::peak_commit(),
        "ps_schema": ctx.schema.fields().iter().map(|f| format!("{}: {} (required)", f.name(), f.data_type())).collect::<Vec<_>>(),
        "term_schema": ctx.term_schema.fields().iter().map(|f| format!("{}: {}", f.name(), f.data_type())).collect::<Vec<_>>(),
        "read_contract": READ_CONTRACT,
        "population": population(plan),
        "finished": utc_now(),
    });
    write_json_atomic(&out.join("_book.meta.json"), &meta)?;
    let mut readme = format!(
        "# The banded explorer book ({BOOK_SCHEMA})\n\nBuilt by `explorer-extract merge` ({}) from {} Rust months ({} to {}) \
         under `{}`, producer commit {}.\n\n{} book rows ({} input rows), {} term rows, {} files, {:.1} GB; {} positions; \
         {} collision hashes.\n\n## Read contract\n\n",
        sys::version_line(),
        plan.months.len(),
        plan.months.first().map_or("", |m| m.tag.as_str()),
        plan.months.last().map_or("", |m| m.tag.as_str()),
        plan.months_root.display(),
        plan.producer,
        fmt_n(book.rows),
        fmt_n(rows_in),
        fmt_n(tdone["rows_out"].as_u64().unwrap_or(0)),
        files,
        gb(bytes),
        fmt_n(positions),
        hashes.len()
    );
    for line in READ_CONTRACT {
        readme.push_str(&format!("- {line}\n"));
    }
    readme.push_str(&format!("- {}\n\n## Layout\n\n", population(plan)));
    readme.push_str(
        "- `ps/event=<E>/elo_band=<B>/bkt<iii>.parquet`: one file per (slice, bucket) with a row.\n\
         - `term/bkt<iii>.parquet`: 512 files, term rows whose position_hash is in bucket iii.\n\
         - `_collisions.parquet`: every hash with two or more EPDs, one row per (hash, EPD).\n\
         - `_slices.parquet`: per slice, files, rows, bytes, the four sums and ply-1 games.\n\
         - `_manifest/bkt<iii>.parquet`: per bucket and input month, rows in and the sums.\n\
         - `_done/*.DONE`: per-bucket sentinels; `_merge_params.json`: the settings lock; \
         `_book.meta.json`: provenance; `_BOOK.DONE` is written last.\n",
    );
    std::fs::write(out.join("README.md"), readme)?;

    // 7. Last.
    write_json_atomic(&out.join("_BOOK.DONE"), &json!({
        "rows": book.rows, "bytes": bytes, "files": files, "term_rows": tdone["rows_out"],
        "collision_hashes": hashes.len(), "finished": utc_now(), "tool": sys::version_line(),
    }))?;
    eprintln!(
        "finalize: {} rows ({} in, compaction {:.3}), {} files, {:.2} GB ({:.1} B/row), {} positions, \
         {} collision hashes ({}), positive controls {}/{}; _BOOK.DONE written ({:.0} s)",
        fmt_n(book.rows),
        fmt_n(rows_in),
        book.rows as f64 / rows_in.max(1) as f64,
        files,
        gb(bytes),
        bytes as f64 / book.rows.max(1) as f64,
        fmt_n(positions),
        hashes.len(),
        coll_status,
        plan.controls.len(),
        plan.controls.len(),
        t0.elapsed().as_secs_f64()
    );
    Ok(())
}
