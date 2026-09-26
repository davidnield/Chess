//! `month`: a month's games -> a finished 512-bucket month (the layout
//! backfill_epd.py and bucket_month.py write) plus its term monthly, in memory,
//! with no spill files. It replaces extract + consolidation + EPD backfill.
//!
//! PASSES. `--passes P` (a power of two dividing the bucket count) splits the
//! buckets into P contiguous ranges. Every pass walks EVERY game -- the walk is
//! what reaches the later plies -- but records only plies whose parent bucket
//! is in its range, and term rows likewise by position_hash, so each row lands
//! in exactly one pass.
//!
//! PER CHUNK (the partials' chunk: one task, one thread, row order) a local map
//! per bucket takes each key's first occurrence: its ply, its position (the
//! EPD source) and its child, plus the counts. The local maps become sorted
//! runs, appended to their bucket. At the end of the pass each bucket k-way
//! merges its runs:
//!   ply        MIN over the chunks' first plies (exactly derivable from the
//!              partials, and never above any value consolidation's any_value
//!              could pick)
//!   parent_epd MIN; child_hash MIN; either one differing is a conflict
//!   counts     summed.
//! The merged values are commutative, so the output does not depend on thread
//! count, pass count or scheduling -- which _test_rust_extract.py checks.
//!
//! THE BUCKET FILE is written, then re-read in full: rows, the four sums, no NULL
//! EPD, no parity violation (the EPD's side to move against ply % 2). At the end
//! of the month, in this order: the term monthly, the promote of _tmp_month, the
//! conflicts / provenance / manifest, and the sentinel LAST. Month gates: the
//! buckets' SUM(total) equals the plies walked (and W/D/B likewise), the term
//! SUM(total) equals kept - failed games, and every pass saw identical counters.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use rayon::prelude::*;
use serde::Serialize;
use serde_json::{json, Value};
use shakmaty::Chess;

use crate::chesspos::{pack, Packed};
use crate::fasthash::FastMap;
use crate::game::{count_ply, drive_game, Counts, Filters, GameSink, Outcome, Walker};
use crate::keys::{
    band_index, bucket_of, ps_schema, rename_retry, san_of, san_str, term_schema, tmp_of,
    write_parquet, writer_props, PsKey, TermKey, BANDS,
};
use crate::partials::term_batch;
use crate::pyre::Tokens;
use crate::source::{chunks, FileMeta, SourceFile};
use crate::stats::Counters;
use crate::sys;

pub const BUCKETS: u32 = 512;
/// Tracked bytes per DISTINCT month key, for choosing P: a run entry is
/// ENTRY_BYTES, and a key appears in ~1.3 chunks of a full-size month.
/// Measured values are reported in the provenance; see the crate README.
pub const DEFAULT_BYTES_PER_KEY: f64 = 125.0;
const OUT_BATCH: usize = 262_144;

#[derive(Clone, Copy)]
pub struct Local {
    child: i64,
    counts: Counts,
    pos: Packed,
    ply: u16,
}

pub const ENTRY_BYTES: usize = std::mem::size_of::<(PsKey, Local)>();

type Run = Vec<(PsKey, Local)>;

pub struct Config {
    pub out: PathBuf,
    pub term_dir: PathBuf,
    pub passes: Option<u32>,
    pub mem_gb: f64,
    pub keys_per_game: f64,
    pub bytes_per_key: f64,
    pub max_ply: usize,
    pub chunk: u64,
    pub filters: Filters,
    pub events: Vec<String>,
    pub buckets: u32,
    pub threads: usize,
    /// Every flag, for the provenance JSON.
    pub flags: Value,
}

// ── one chunk ────────────────────────────────────────────────────────────────

struct ChunkSink {
    lo: u32,
    hi: u32,
    nb: u32,
    maps: Vec<FastMap<PsKey, Local>>,
    term: FastMap<TermKey, Counts>,
    event: u8,
    band: u8,
    outcome: Outcome,
    reason: u8,
    c: Counters,
    err: Option<anyhow::Error>,
}

impl Walker for ChunkSink {
    #[inline]
    fn row(&mut self, ply: u32, pos: &Chess, ph: i64, san: &str, ch: i64) {
        count_ply(&mut self.c, self.outcome, san);
        let s = match san_of(san) {
            Ok(s) => s,
            Err(e) => {
                self.err.get_or_insert(e);
                return;
            }
        };
        let b = bucket_of(ph, self.nb);
        if b < self.lo || b >= self.hi {
            return;
        }
        let key = PsKey { hash: ph, san: s, event: self.event, band: self.band };
        let o = self.outcome;
        self.maps[(b - self.lo) as usize]
            .entry(key)
            .or_insert_with(|| Local { child: ch, counts: Counts::default(), pos: pack(pos), ply: ply as u16 })
            .counts
            .add(o);
    }

    #[inline]
    fn term(&mut self, hash: i64, kind: i32) {
        let b = bucket_of(hash, self.nb);
        if b < self.lo || b >= self.hi {
            return;
        }
        let key = TermKey { hash, kind: kind as u8, reason: self.reason };
        self.term.entry(key).or_default().add(self.outcome);
    }
}

impl GameSink for ChunkSink {
    fn start_game(&mut self, band: i64, ws: f64, reason: i32) {
        self.band = band_index(band);
        self.outcome = Outcome::of(ws);
        self.reason = reason as u8;
    }

    fn counters(&mut self) -> &mut Counters {
        &mut self.c
    }

    fn take_err(&mut self) -> Option<anyhow::Error> {
        self.err.take()
    }
}

// ── one pass ─────────────────────────────────────────────────────────────────

struct Pass {
    lo: u32,
    hi: u32,
    runs: Vec<Mutex<Vec<Run>>>,
    term: Mutex<FastMap<TermKey, Counts>>,
    term_bytes: AtomicU64,
    run_bytes: AtomicU64,
    counters: Mutex<Counters>,
}

fn map_bytes<K, V>(m: &FastMap<K, V>) -> u64 {
    // hashbrown holds capacity * 8/7 slots of (K, V) plus one control byte each.
    let slots = (m.capacity() as u64 * 8).div_ceil(7);
    slots * (std::mem::size_of::<(K, V)>() as u64 + 1)
}

/// A conflict row in backfill_epd's schema: (hash, epd_a, epd_b, kind).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Conflict {
    pub hash: i64,
    pub a: String,
    pub b: String,
    pub kind: &'static str,
}

#[derive(Default, Clone, Copy)]
struct BucketStats {
    rows: u64,
    sums: [u64; 4],
    ply1: u64,
    bytes: u64,
    parity: u64,
}

/// K-way merge of one bucket's sorted runs, streamed to its parquet file.
fn write_bucket(
    runs: Vec<Run>,
    path: &Path,
    events: &[String],
) -> Result<(BucketStats, Vec<Conflict>)> {
    let schema = ps_schema(false);
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d)?;
    }
    let f = std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut w = ArrowWriter::try_new(f, schema.clone(), Some(writer_props()))?;
    let mut st = BucketStats::default();
    let mut conflicts = Vec::new();
    let mut out = OutBatch::default();
    let mut idx = vec![0usize; runs.len()];
    let mut heap: BinaryHeap<Reverse<(PsKey, usize)>> = runs
        .iter()
        .enumerate()
        .filter(|(_, r)| !r.is_empty())
        .map(|(i, r)| Reverse((r[0].0, i)))
        .collect();
    let mut cur: Option<(PsKey, Merged)> = None;
    let mut last_hash_epd: Option<(i64, String)> = None;
    let mut flush = |k: &PsKey, m: Merged, out: &mut OutBatch, st: &mut BucketStats,
                     conflicts: &mut Vec<Conflict>, w: &mut ArrowWriter<std::fs::File>|
     -> Result<()> {
        let epd = m.pos.render();
        if m.epd_conflict.is_some() || m.child_conflict {
            if let Some(other) = &m.epd_conflict {
                let (a, b) = if epd <= *other { (epd.clone(), other.clone()) } else { (other.clone(), epd.clone()) };
                conflicts.push(Conflict { hash: k.hash, a, b, kind: "parent-epd" });
            }
            if m.child_conflict {
                conflicts.push(Conflict {
                    hash: k.hash,
                    a: epd.clone(),
                    b: san_str(&k.san).to_string(),
                    kind: "edge-child",
                });
            }
        }
        match &last_hash_epd {
            Some((h, e)) if *h == k.hash && *e != epd => {
                let (a, b) = if *e < epd { (e.clone(), epd.clone()) } else { (epd.clone(), e.clone()) };
                conflicts.push(Conflict { hash: k.hash, a, b, kind: "parent-epd" });
            }
            _ => {}
        }
        // Parity: ply 1 is White to move, so an odd ply wants " w ".
        if (m.ply % 2 == 1) != m.pos.white_to_move() {
            st.parity += 1;
        }
        st.rows += 1;
        st.sums[0] += m.counts[3];
        st.sums[1] += m.counts[0];
        st.sums[2] += m.counts[1];
        st.sums[3] += m.counts[2];
        if m.ply == 1 {
            st.ply1 += m.counts[3];
        }
        out.push(k, &m, &epd, events);
        last_hash_epd = Some((k.hash, epd));
        if out.len() >= OUT_BATCH {
            w.write(&out.finish()?)?;
        }
        Ok(())
    };
    while let Some(Reverse((k, i))) = heap.pop() {
        let v = runs[i][idx[i]].1;
        idx[i] += 1;
        if idx[i] < runs[i].len() {
            heap.push(Reverse((runs[i][idx[i]].0, i)));
        }
        if matches!(&cur, Some((ck, _)) if *ck == k) {
            cur.as_mut().unwrap().1.absorb(&v);
        } else {
            if let Some((ck, m)) = cur.take() {
                flush(&ck, m, &mut out, &mut st, &mut conflicts, &mut w)?;
            }
            cur = Some((k, Merged::from(&v)));
        }
    }
    if let Some((ck, m)) = cur.take() {
        flush(&ck, m, &mut out, &mut st, &mut conflicts, &mut w)?;
    }
    drop(runs);
    if out.len() > 0 {
        w.write(&out.finish()?)?;
    }
    w.close()?;
    st.bytes = std::fs::metadata(path)?.len();
    Ok((st, conflicts))
}

struct Merged {
    ply: u16,
    pos: Packed,
    child: i64,
    /// w, d, b, total.
    counts: [u64; 4],
    epd_conflict: Option<String>,
    child_conflict: bool,
}

impl Merged {
    fn from(v: &Local) -> Merged {
        Merged {
            ply: v.ply,
            pos: v.pos,
            child: v.child,
            counts: [v.counts.w, v.counts.d, v.counts.b, v.counts.t].map(u64::from),
            epd_conflict: None,
            child_conflict: false,
        }
    }

    fn absorb(&mut self, v: &Local) {
        self.ply = self.ply.min(v.ply);
        if v.pos != self.pos {
            // Collision twins under one key: keep the smaller EPD, report both.
            let (a, b) = (self.pos.render(), v.pos.render());
            if b < a {
                self.pos = v.pos;
                self.epd_conflict = Some(a);
            } else {
                self.epd_conflict = Some(b);
            }
        }
        if v.child != self.child {
            self.child_conflict = true;
            self.child = self.child.min(v.child);
        }
        for (x, y) in self.counts.iter_mut().zip([v.counts.w, v.counts.d, v.counts.b, v.counts.t]) {
            *x += u64::from(y);
        }
    }
}

#[derive(Default)]
struct OutBatch {
    hash: Vec<i64>,
    san: StringBuilder,
    event: StringBuilder,
    band: Vec<i64>,
    epd: StringBuilder,
    child: Vec<i64>,
    ply: Vec<i32>,
    w: Vec<i64>,
    d: Vec<i64>,
    b: Vec<i64>,
    t: Vec<i64>,
}

impl OutBatch {
    fn len(&self) -> usize {
        self.hash.len()
    }

    fn push(&mut self, k: &PsKey, m: &Merged, epd: &str, events: &[String]) {
        self.hash.push(k.hash);
        self.san.append_value(san_str(&k.san));
        self.event.append_value(&events[k.event as usize]);
        self.band.push(BANDS[k.band as usize]);
        self.epd.append_value(epd);
        self.child.push(m.child);
        self.ply.push(i32::from(m.ply));
        self.w.push(m.counts[0] as i64);
        self.d.push(m.counts[1] as i64);
        self.b.push(m.counts[2] as i64);
        self.t.push(m.counts[3] as i64);
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let n = self.len();
        let take = |v: &mut Vec<i64>| -> ArrayRef { Arc::new(Int64Array::from(std::mem::take(v))) };
        let cols: Vec<ArrayRef> = vec![
            take(&mut self.hash),
            Arc::new(self.san.finish()),
            Arc::new(self.event.finish()),
            take(&mut self.band),
            Arc::new(self.epd.finish()),
            take(&mut self.child),
            Arc::new(Int32Array::new_null(n)),
            Arc::new(Int32Array::from(std::mem::take(&mut self.ply))),
            take(&mut self.w),
            take(&mut self.d),
            take(&mut self.b),
            take(&mut self.t),
        ];
        Ok(RecordBatch::try_new(ps_schema(false), cols)?)
    }
}

/// Read a bucket file back in full: rows, sums, NULL EPDs, parity violations.
fn verify_bucket(path: &Path) -> Result<BucketStats> {
    let f = std::fs::File::open(path)?;
    let r = ParquetRecordBatchReaderBuilder::try_new(f)?.with_batch_size(OUT_BATCH).build()?;
    let mut st = BucketStats::default();
    for b in r {
        let b = b?;
        let epd = b.column(4).as_string::<i32>();
        let ply = b.column(7).as_primitive::<Int32Type>();
        let sums = [11usize, 8, 9, 10].map(|c| b.column(c).as_primitive::<Int64Type>().clone());
        for i in 0..b.num_rows() {
            st.rows += 1;
            if epd.is_null(i) {
                st.parity += 1;
                continue;
            }
            let white = epd.value(i).split(' ').nth(1) == Some("w");
            if (ply.value(i) % 2 == 1) != white {
                st.parity += 1;
            }
            for (k, a) in sums.iter().enumerate() {
                st.sums[k] += a.value(i) as u64;
            }
            if ply.value(i) == 1 {
                st.ply1 += sums[0].value(i) as u64;
            }
        }
    }
    st.bytes = std::fs::metadata(path)?.len();
    Ok(st)
}

// ── the month ────────────────────────────────────────────────────────────────

/// The manifest every producer writes (backfill_epd, bucket_month, this).
#[derive(Serialize, Clone, Debug)]
pub struct Manifest {
    pub year: i64,
    pub month: i64,
    pub files: i64,
    pub bytes: i64,
    pub rows: i64,
    pub ply1_games: i64,
    pub total: i64,
    pub white_wins: i64,
    pub draws: i64,
    pub black_wins: i64,
    pub buckets: i64,
    pub edges_replayed: i64,
    pub positions_resolved: i64,
    pub replays_per_sec: f64,
    pub mismatches: i64,
    pub conflicts: i64,
    pub unresolved: i64,
    pub quarantine_edges: i64,
    pub quarantine_rows: i64,
    pub quarantine_total: i64,
    pub quarantine_white_wins: i64,
    pub quarantine_draws: i64,
    pub quarantine_black_wins: i64,
    pub quarantine_by_reason: String,
    pub unreachable_positions: i64,
    pub seconds: f64,
}

/// The 26 manifest fields in their contract order, with their types: int64
/// unless listed here (backfill_epd infers these types from Python values).
pub const MANIFEST_FIELDS: [&str; 26] = [
    "year", "month", "files", "bytes", "rows", "ply1_games", "total", "white_wins", "draws",
    "black_wins", "buckets", "edges_replayed", "positions_resolved", "replays_per_sec",
    "mismatches", "conflicts", "unresolved", "quarantine_edges", "quarantine_rows",
    "quarantine_total", "quarantine_white_wins", "quarantine_draws", "quarantine_black_wins",
    "quarantine_by_reason", "unreachable_positions", "seconds",
];

fn manifest_batch(m: &Manifest) -> Result<RecordBatch> {
    // Built field by field in MANIFEST_FIELDS order: a serde_json object would
    // sort its keys, and the order is part of the contract.
    let v = serde_json::to_value(m)?;
    let mut fields = Vec::new();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for k in MANIFEST_FIELDS {
        let x = &v[k];
        match k {
            "replays_per_sec" | "seconds" => {
                fields.push(Field::new(k, DataType::Float64, true));
                cols.push(Arc::new(Float64Array::from(vec![x.as_f64().unwrap()])));
            }
            "quarantine_by_reason" => {
                fields.push(Field::new(k, DataType::Utf8, true));
                cols.push(Arc::new(StringArray::from(vec![x.as_str().unwrap().to_string()])));
            }
            _ => {
                fields.push(Field::new(k, DataType::Int64, true));
                cols.push(Arc::new(Int64Array::from(vec![x.as_i64().unwrap()])));
            }
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

fn conflict_batch(c: &[Conflict]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("hash", DataType::Int64, true),
        Field::new("epd_a", DataType::Utf8, true),
        Field::new("epd_b", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(c.iter().map(|x| x.hash))),
        Arc::new(StringArray::from_iter_values(c.iter().map(|x| x.a.as_str()))),
        Arc::new(StringArray::from_iter_values(c.iter().map(|x| x.b.as_str()))),
        Arc::new(StringArray::from_iter_values(c.iter().map(|x| x.kind))),
    ];
    Ok(RecordBatch::try_new(schema, cols)?)
}

fn rmtree(p: &Path) -> Result<()> {
    if p.exists() {
        std::fs::remove_dir_all(p).with_context(|| format!("removing {}", p.display()))?;
    }
    Ok(())
}

/// P for a month of `games` games: the smallest power of two whose per-pass
/// share of the estimated keys, plus the chunks in flight, fits the budget.
pub fn choose_passes(games: u64, cfg: &Config) -> u32 {
    let budget = cfg.mem_gb * 1e9;
    let keys = games as f64 * cfg.keys_per_game;
    let mut p = 1u32;
    while p < cfg.buckets {
        let in_flight = cfg.threads as f64 * (0.2e9 + 21.0 * cfg.chunk as f64 * 150.0 / f64::from(p));
        if keys * cfg.bytes_per_key / f64::from(p) + in_flight <= budget {
            break;
        }
        p *= 2;
    }
    p
}

pub fn run_month(cfg: &Config, year: i32, month: u32, files: &[SourceFile]) -> Result<Option<Manifest>> {
    let tag = format!("{year}_{month}");
    let sentinel = cfg.out.join(format!("_month={tag}.DONE"));
    if sentinel.exists() {
        eprintln!("  {year}/{month}: already done");
        return Ok(None);
    }
    let t_month = Instant::now();
    let nb = cfg.buckets;
    let out_tmp = cfg.out.join(format!("_tmp_month={tag}"));
    rmtree(&out_tmp)?;
    let metas: Vec<FileMeta> = files.iter().map(|f| FileMeta::open(&f.path)).collect::<Result<_>>()?;
    let games: u64 = metas.iter().map(|m| m.rows).sum();
    let p = match cfg.passes {
        Some(p) => {
            if p == 0 || !p.is_power_of_two() || nb % p != 0 {
                bail!("--passes {p} must be a power of two dividing {nb}");
            }
            p
        }
        None => choose_passes(games, cfg),
    };
    let budget = (cfg.mem_gb * 1e9) as u64;
    if let Some((limit, avail)) = sys::commit() {
        let need = budget + 8_000_000_000;
        if avail < need {
            bail!(
                "free commit {:.1} GB (of {:.1} GB) is below the --mem-gb budget + 8 GB = {:.1} GB",
                avail as f64 / 1e9,
                limit as f64 / 1e9,
                need as f64 / 1e9
            );
        }
    }
    let mut jobs = Vec::new();
    for (fi, m) in metas.iter().enumerate() {
        for (_, s, e, _) in chunks(m.rows, cfg.chunk) {
            if e > s {
                jobs.push((fi, s, e));
            }
        }
    }
    eprintln!(
        "  {year}/{month}: {} files, {games} games, {} chunks, {p} pass(es) of {} buckets, \
         budget {:.1} GB",
        files.len(),
        jobs.len(),
        nb / p,
        cfg.mem_gb
    );

    let mut pass_counters: Vec<Counters> = Vec::new();
    let mut pass_secs = Vec::new();
    let mut stats_all = BucketStats::default();
    let mut n_files = 0u64;
    let mut conflicts: Vec<Conflict> = Vec::new();
    let mut term_rows: Vec<(TermKey, Counts)> = Vec::new();
    let mut peak_tracked = 0u64;
    let mut keys_per_pass = Vec::new();
    let per = nb / p;
    for pi in 0..p {
        let t_pass = Instant::now();
        let pass = Pass {
            lo: pi * per,
            hi: (pi + 1) * per,
            runs: (0..per).map(|_| Mutex::new(Vec::new())).collect(),
            term: Mutex::new(FastMap::default()),
            term_bytes: AtomicU64::new(0),
            run_bytes: AtomicU64::new(0),
            counters: Mutex::new(Counters::default()),
        };
        let over = AtomicBool::new(false);
        let done = AtomicUsize::new(0);
        let last_log = Mutex::new(Instant::now());
        jobs.par_iter().try_for_each(|&(fi, s, e)| -> Result<()> {
            if over.load(Ordering::Relaxed) {
                return Ok(());
            }
            let f = &files[fi];
            let batches = metas[fi].read_rows(&f.path, s, e)?;
            let mut sink = ChunkSink {
                lo: pass.lo,
                hi: pass.hi,
                nb,
                maps: (0..per).map(|_| FastMap::default()).collect(),
                term: FastMap::default(),
                event: f.event_idx,
                band: 0,
                outcome: Outcome::Other,
                reason: 3,
                c: Counters::default(),
                err: None,
            };
            let mut toks = Tokens::default();
            let start = Chess::default();
            let mut row = s;
            for b in &batches {
                let cols = metas[fi].columns(b)?;
                for i in 0..b.num_rows() {
                    drive_game(&mut sink, row, &cols.row(i), &cfg.filters, cfg.max_ply, &mut toks, &start)
                        .with_context(|| f.path.display().to_string())?;
                    row += 1;
                }
            }
            drop(batches);
            let mut added = 0u64;
            for (i, m) in sink.maps.into_iter().enumerate() {
                if m.is_empty() {
                    continue;
                }
                let mut run: Run = m.into_iter().collect();
                run.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                run.shrink_to_fit();
                added += (run.capacity() * ENTRY_BYTES) as u64;
                pass.runs[i].lock().unwrap().push(run);
            }
            let tb = {
                let mut t = pass.term.lock().unwrap();
                for (k, c) in sink.term {
                    t.entry(k).or_default().merge(&c);
                }
                map_bytes(&t)
            };
            pass.term_bytes.store(tb, Ordering::Relaxed);
            let tracked = pass.run_bytes.fetch_add(added, Ordering::Relaxed) + added + tb;
            pass.counters.lock().unwrap().add(&sink.c);
            if tracked > budget {
                over.store(true, Ordering::Relaxed);
                bail!(
                    "pass {}/{p}: tracked map bytes {:.1} GB exceed the --mem-gb budget {:.1} GB; \
                     rerun with --passes {}",
                    pi + 1,
                    tracked as f64 / 1e9,
                    cfg.mem_gb,
                    (p * 2).min(nb)
                );
            }
            let d = done.fetch_add(1, Ordering::Relaxed) + 1;
            let mut ll = last_log.lock().unwrap();
            if ll.elapsed().as_secs() >= 60 {
                *ll = Instant::now();
                eprintln!(
                    "    pass {}/{p}: {d}/{} chunks, {:.1} GB tracked, {:.1} min",
                    pi + 1,
                    jobs.len(),
                    tracked as f64 / 1e9,
                    t_pass.elapsed().as_secs_f64() / 60.0
                );
            }
            Ok(())
        })?;
        let walk_secs = t_pass.elapsed().as_secs_f64();
        let tracked = pass.run_bytes.load(Ordering::Relaxed) + pass.term_bytes.load(Ordering::Relaxed);
        peak_tracked = peak_tracked.max(tracked);
        let c = *pass.counters.lock().unwrap();
        if let Some(c0) = pass_counters.first() {
            if *c0 != c {
                bail!("pass {} saw different counters than pass 1: {c:?} vs {c0:?}", pi + 1);
            }
        }
        pass_counters.push(c);

        // Each bucket: merge its runs, write, re-read.
        let results: Vec<(u32, BucketStats, Vec<Conflict>)> = pass
            .runs
            .into_par_iter()
            .enumerate()
            .map(|(i, runs)| -> Result<(u32, BucketStats, Vec<Conflict>)> {
                let b = pass.lo + i as u32;
                let runs = runs.into_inner().unwrap();
                if runs.iter().all(|r| r.is_empty()) {
                    return Ok((b, BucketStats::default(), Vec::new()));
                }
                let path = out_tmp.join(format!("bkt={b}")).join("part-0000.parquet");
                let (st, cf) = write_bucket(runs, &path, &cfg.events)?;
                let back = verify_bucket(&path)?;
                if back.rows != st.rows || back.sums != st.sums || back.parity != 0 || st.parity != 0 {
                    bail!(
                        "bucket {b}: re-read {} rows, sums {:?}, {} NULL-EPD/parity rows; wrote \
                         {} rows, sums {:?}, {} parity",
                        back.rows, back.sums, back.parity, st.rows, st.sums, st.parity
                    );
                }
                Ok((b, st, cf))
            })
            .collect::<Result<_>>()?;
        let mut keys = 0u64;
        for (_, st, cf) in results {
            if st.rows > 0 {
                n_files += 1;
            }
            keys += st.rows;
            stats_all.rows += st.rows;
            for k in 0..4 {
                stats_all.sums[k] += st.sums[k];
            }
            stats_all.ply1 += st.ply1;
            stats_all.bytes += st.bytes;
            conflicts.extend(cf);
        }
        keys_per_pass.push(keys);
        let mut t: Vec<(TermKey, Counts)> = pass.term.into_inner().unwrap().into_iter().collect();
        term_rows.append(&mut t);
        let secs = t_pass.elapsed().as_secs_f64();
        pass_secs.push(json!({"walk": walk_secs, "total": secs, "keys": keys, "tracked_bytes": tracked}));
        eprintln!(
            "    pass {}/{p}: {} keys, {:.2} GB tracked ({:.0} B/key), walk {:.0}s, write {:.0}s",
            pi + 1,
            keys,
            tracked as f64 / 1e9,
            tracked as f64 / keys.max(1) as f64,
            walk_secs,
            secs - walk_secs
        );
    }

    // ── month gates ───────────────────────────────────────────────────────────
    let c = pass_counters[0];
    let want = [c.plies, c.plies_white, c.plies_draw, c.plies_black];
    if stats_all.sums != want {
        bail!("{year}/{month}: bucket sums (total, W, D, B) {:?} != plies walked {want:?}", stats_all.sums);
    }
    term_rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let term_total: u64 = term_rows.iter().map(|(_, c)| u64::from(c.t)).sum();
    if term_total != c.kept - c.failed {
        bail!(
            "{year}/{month}: term SUM(total) {term_total} != kept {} - failed {}",
            c.kept,
            c.failed
        );
    }

    // ── outputs, sentinel last ────────────────────────────────────────────────
    std::fs::create_dir_all(&cfg.term_dir)?;
    let term_path = cfg.term_dir.join(format!("year={year}_month={month}.term.parquet"));
    let term_tmp = tmp_of(&term_path);
    write_parquet(&term_tmp, &term_schema(), &[term_batch(&term_rows)?])?;
    let back = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&term_tmp)?)?.build()?;
    let (mut n, mut tot) = (0u64, 0u64);
    for b in back {
        let b = b?;
        n += b.num_rows() as u64;
        tot += b.column(6).as_primitive::<Int64Type>().values().iter().map(|&x| x as u64).sum::<u64>();
    }
    if n != term_rows.len() as u64 || tot != term_total {
        bail!("{}: re-read {n} rows / total {tot}, wrote {} / {term_total}", term_tmp.display(), term_rows.len());
    }
    rename_retry(&term_tmp, &term_path)?;

    let final_dir = cfg.out.join(format!("month={tag}"));
    rmtree(&final_dir)?;
    if out_tmp.exists() {
        rename_retry(&out_tmp, &final_dir)?;
    }
    conflicts.sort();
    conflicts.dedup();
    let cpath = cfg.out.join("_conflicts").join(format!("month={tag}")).join("rust.parquet");
    if conflicts.is_empty() {
        let _ = std::fs::remove_file(&cpath);
    } else {
        write_parquet(&cpath, &conflict_batch(&conflicts)?.schema(), &[conflict_batch(&conflicts)?])?;
    }
    let secs = t_month.elapsed().as_secs_f64();
    let man = Manifest {
        year: i64::from(year),
        month: i64::from(month),
        files: n_files as i64,
        bytes: stats_all.bytes as i64,
        rows: stats_all.rows as i64,
        ply1_games: stats_all.ply1 as i64,
        total: stats_all.sums[0] as i64,
        white_wins: stats_all.sums[1] as i64,
        draws: stats_all.sums[2] as i64,
        black_wins: stats_all.sums[3] as i64,
        buckets: i64::from(nb),
        edges_replayed: 0,
        positions_resolved: 0,
        replays_per_sec: 0.0,
        mismatches: 0,
        conflicts: conflicts.len() as i64,
        unresolved: 0,
        quarantine_edges: 0,
        quarantine_rows: 0,
        quarantine_total: 0,
        quarantine_white_wins: 0,
        quarantine_draws: 0,
        quarantine_black_wins: 0,
        quarantine_by_reason: "{}".into(),
        unreachable_positions: 0,
        seconds: secs,
    };
    let prov = json!({
        "tool": sys::version_line(),
        "version": sys::VERSION,
        "commit": sys::GIT_COMMIT,
        "built": sys::BUILD_DATE,
        "flags": cfg.flags,
        "month": tag,
        "files": files.iter().map(|f| f.path.display().to_string()).collect::<Vec<_>>(),
        "games_in_footers": games,
        "passes": p,
        "pass_times": pass_secs,
        "counters": c,
        "peak_tracked_bytes": peak_tracked,
        "peak_tracked_bytes_per_key": peak_tracked as f64 / keys_per_pass.iter().copied().max().unwrap_or(1).max(1) as f64,
        "entry_bytes": ENTRY_BYTES,
        "peak_commit_bytes": sys::peak_commit(),
        "rows": stats_all.rows,
        "output_bytes": stats_all.bytes,
        "term_rows": term_rows.len(),
        "term_path": term_path.display().to_string(),
        "conflicts": conflicts.len(),
        "seconds": secs,
    });
    let pdir = cfg.out.join("_provenance");
    std::fs::create_dir_all(&pdir)?;
    std::fs::write(pdir.join(format!("month={tag}.json")), serde_json::to_string_pretty(&prov)? + "\n")?;
    let mpath = cfg.out.join("_manifest").join(format!("month={tag}.parquet"));
    let mb = manifest_batch(&man)?;
    let mtmp = tmp_of(&mpath);
    write_parquet(&mtmp, &mb.schema(), &[mb])?;
    rename_retry(&mtmp, &mpath)?;
    std::fs::write(&sentinel, serde_json::to_string_pretty(&man)?)
        .with_context(|| format!("writing {}", sentinel.display()))?;
    eprintln!(
        "  {year}/{month}: {} buckets, {} rows, {:.1} GB, {} term rows, {} conflicts; {p} pass(es), \
         {:.1} min",
        n_files,
        stats_all.rows,
        stats_all.bytes as f64 / 1e9,
        term_rows.len(),
        conflicts.len(),
        secs / 60.0
    );
    Ok(Some(man))
}
