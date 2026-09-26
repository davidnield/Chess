//! `partials`: a drop-in for `build_pooled_stats.py --phase extract` under the
//! explorer contract, exact against it file for file -- including `ply` and,
//! through the per-batch EPD memo, including parent_epd under a 64-bit
//! collision inside one 50,000-row batch.
//!
//! Per chunk (`extract_file` :587-812), in (game, ply) order on one thread:
//!   ply        the key's FIRST occurrence in the chunk
//!   parent_epd the first non-NULL EPD; a row's EPD is memo[parent_hash], where
//!              the memo holds the first position seen with that hash in the
//!              current 50,000-row batch at a ply <= --epd-max-ply
//!   child_hash first; child_eval NULL; W/D/B/total counts.
//! Files: `year=Y_month=M_event=E_{stem}_c{k:03d}.{ps,term}.parquet`, written as
//! .tmp then renamed with ps LAST; full chunks always, the tail only if it has a
//! row (or is c000); then the zero-byte `_{stem}.DONE`. Resume skips a file with
//! its sentinel and otherwise deletes its chunks and redoes it.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use arrow::array::{
    ArrayRef, Int32Array, Int64Array, LargeStringBuilder, RecordBatch,
};
use rayon::prelude::*;
use serde_json::{json, Value};
use shakmaty::Chess;

use crate::chesspos::{pack, Packed};
use crate::fasthash::FastMap;
use crate::game::{count_ply, drive_game, Counts, Filters, GameRow, GameSink, Outcome, Walker};
use crate::keys::{
    band_index, rename_retry, san_of, san_str, term_schema, tmp_of, write_parquet, PsKey, TermKey,
    BANDS,
};
use crate::pyre::Tokens;
use crate::source::{chunks, FileMeta, SourceFile, READ_BATCH_GAMES};
use crate::stats::{Counters, RunStats};

pub struct Config {
    pub partial_dir: PathBuf,
    pub epd_max_ply: u32,
    pub max_ply: usize,
    pub chunk: u64,
    pub filters: Filters,
    pub events: Vec<String>,
    /// The CLI's --chunk-games, as recorded in the lock.
    pub chunk_games_arg: u64,
}

pub const PARAMS_FILE: &str = "_extract_params.json";

/// `build_pooled_stats.extract_params`, with producer "rust".
pub fn params(cfg: &Config) -> Value {
    let mut ex = cfg.filters.excluded_terminations.clone();
    ex.sort();
    json!({
        "epd_max_ply": cfg.epd_max_ply,
        "max_ply": cfg.max_ply,
        "chunk_games": cfg.chunk_games_arg,
        "events": cfg.events,
        "min_elo": cfg.filters.min_elo,
        "exclude_bots": cfg.filters.exclude_bots,
        "excluded_terminations": ex,
        "producer": "rust",
    })
}

/// Write the lock on first use; refuse a dir locked with different settings.
pub fn lock(dir: &Path, want: &Value) -> Result<()> {
    let p = dir.join(PARAMS_FILE);
    if p.exists() {
        let have: Value = serde_json::from_str(&std::fs::read_to_string(&p)?)
            .with_context(|| format!("parsing {}", p.display()))?;
        if &have != want {
            bail!(
                "{} records different extract parameters:\n  locked:   {have}\n  this run: {want}\n\
                 Partials built under different settings must not share a directory.",
                p.display()
            );
        }
        return Ok(());
    }
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!("{PARAMS_FILE}.tmp"));
    std::fs::write(&tmp, serde_json::to_string_pretty(want)? + "\n")?;
    rename_retry(&tmp, &p)
}

#[derive(Clone, Copy)]
pub struct PsVal {
    pub child: i64,
    pub epd: Option<Packed>,
    pub counts: Counts,
    pub ply: u16,
}

/// One chunk's accumulators, and the walk context of the game being walked.
pub struct ChunkAgg {
    pub ps: FastMap<PsKey, PsVal>,
    pub term: FastMap<TermKey, Counts>,
    memo: FastMap<i64, Packed>,
    epd_max_ply: u32,
    // per game
    event: u8,
    band: u8,
    outcome: Outcome,
    reason: u8,
    pub c: Counters,
    err: Option<anyhow::Error>,
}

impl ChunkAgg {
    pub fn new(epd_max_ply: u32) -> ChunkAgg {
        ChunkAgg {
            ps: FastMap::default(),
            term: FastMap::default(),
            memo: FastMap::default(),
            epd_max_ply,
            event: 0,
            band: 0,
            outcome: Outcome::Other,
            reason: 3,
            c: Counters::default(),
            err: None,
        }
    }

    pub fn clear_memo(&mut self) {
        self.memo.clear();
    }


    pub fn is_empty(&self) -> bool {
        self.ps.is_empty() && self.term.is_empty()
    }
}

impl Walker for ChunkAgg {
    #[inline]
    fn before_parse(&mut self, ply: u32, pos: &Chess, ph: i64) {
        if ply <= self.epd_max_ply {
            self.memo.entry(ph).or_insert_with(|| pack(pos));
        }
    }

    #[inline]
    fn row(&mut self, ply: u32, _pos: &Chess, ph: i64, san: &str, ch: i64) {
        let s = match san_of(san) {
            Ok(s) => s,
            Err(e) => {
                self.err.get_or_insert(e);
                return;
            }
        };
        count_ply(&mut self.c, self.outcome, san);
        let key = PsKey { hash: ph, san: s, event: self.event, band: self.band };
        let want_epd = ply <= self.epd_max_ply;
        let v = self.ps.entry(key).or_insert_with(|| PsVal {
            child: ch,
            epd: None,
            counts: Counts::default(),
            ply: ply as u16,
        });
        if want_epd && v.epd.is_none() {
            v.epd = self.memo.get(&ph).copied();
        }
        v.counts.add(self.outcome);
    }

    #[inline]
    fn term(&mut self, hash: i64, kind: i32) {
        let key = TermKey { hash, kind: kind as u8, reason: self.reason };
        self.term.entry(key).or_default().add(self.outcome);
    }
}

impl ChunkAgg {
    /// One source row: the EPD memo is reset on every 50,000-row batch
    /// boundary, exactly where extract_file clears it, then the shared driver.
    #[allow(clippy::too_many_arguments)]
    pub fn game(
        &mut self,
        row: u64,
        g: &GameRow,
        filters: &Filters,
        max_ply: usize,
        event: u8,
        toks: &mut Tokens,
        start_pos: &Chess,
    ) -> Result<()> {
        if row % READ_BATCH_GAMES == 0 {
            self.clear_memo();
        }
        self.event = event;
        drive_game(self, row, g, filters, max_ply, toks, start_pos)
    }
}

impl GameSink for ChunkAgg {
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

/// Walk rows [start, end) of `file` into a fresh ChunkAgg.
#[allow(clippy::too_many_arguments)]
pub fn aggregate_chunk(
    file: &SourceFile,
    meta: &FileMeta,
    start: u64,
    end: u64,
    filters: &Filters,
    max_ply: usize,
    epd_max_ply: u32,
    stats: &RunStats,
) -> Result<ChunkAgg> {
    let batches = meta.read_rows(&file.path, start, end)?;
    let mut agg = ChunkAgg::new(epd_max_ply);
    let mut toks = Tokens::default();
    let start_pos = Chess::default();
    let mut row = start;
    for b in &batches {
        let cols = meta.columns(b)?;
        for i in 0..b.num_rows() {
            agg.game(row, &cols.row(i), filters, max_ply, 0, &mut toks, &start_pos)
                .with_context(|| file.path.display().to_string())?;
            row += 1;
        }
    }
    stats.add(&agg.c);
    Ok(agg)
}

pub fn ps_batch(agg: &ChunkAgg, event: &str) -> Result<RecordBatch> {
    let mut rows: Vec<(&PsKey, &PsVal)> = agg.ps.iter().collect();
    rows.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let n = rows.len();
    let mut san = LargeStringBuilder::with_capacity(n, n * 4);
    let mut ev = LargeStringBuilder::with_capacity(n, n * event.len());
    let mut epd = LargeStringBuilder::with_capacity(n, n * 60);
    let mut buf = String::with_capacity(80);
    for (k, v) in &rows {
        san.append_value(san_str(&k.san));
        ev.append_value(event);
        match &v.epd {
            Some(p) => {
                buf.clear();
                p.render_into(&mut buf);
                epd.append_value(&buf);
            }
            None => epd.append_null(),
        }
    }
    let i64s = |f: &dyn Fn(&PsKey, &PsVal) -> i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|(k, v)| f(k, v))))
    };
    let cols: Vec<ArrayRef> = vec![
        i64s(&|k, _| k.hash),
        Arc::new(san.finish()),
        Arc::new(ev.finish()),
        i64s(&|k, _| BANDS[k.band as usize]),
        Arc::new(epd.finish()),
        i64s(&|_, v| v.child),
        Arc::new(Int32Array::new_null(n)),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|(_, v)| i32::from(v.ply)))),
        i64s(&|_, v| i64::from(v.counts.w)),
        i64s(&|_, v| i64::from(v.counts.d)),
        i64s(&|_, v| i64::from(v.counts.b)),
        i64s(&|_, v| i64::from(v.counts.t)),
    ];
    Ok(RecordBatch::try_new(crate::keys::ps_schema(true), cols)?)
}

pub fn term_batch(rows: &[(TermKey, Counts)]) -> Result<RecordBatch> {
    let i64s = |f: &dyn Fn(&TermKey, &Counts) -> i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|(k, c)| f(k, c))))
    };
    let cols: Vec<ArrayRef> = vec![
        i64s(&|k, _| k.hash),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|(k, _)| i32::from(k.kind)))),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|(k, _)| i32::from(k.reason)))),
        i64s(&|_, c| i64::from(c.w)),
        i64s(&|_, c| i64::from(c.d)),
        i64s(&|_, c| i64::from(c.b)),
        i64s(&|_, c| i64::from(c.t)),
    ];
    Ok(RecordBatch::try_new(term_schema(), cols)?)
}

/// The partial's path for chunk k: `{stem}_c{k:03d}.{kind}.parquet`.
pub fn chunk_path(dir: &Path, stem: &str, k: u32, kind: &str) -> PathBuf {
    dir.join(format!("{stem}_c{k:03}.{kind}.parquet"))
}

pub fn sentinel_path(dir: &Path, stem: &str) -> PathBuf {
    dir.join(format!("_{stem}.DONE"))
}

fn write_chunk(dir: &Path, file: &SourceFile, k: u32, agg: &ChunkAgg) -> Result<(u64, u64)> {
    let stem = file.partial_stem();
    let ps_p = chunk_path(dir, &stem, k, "ps");
    let tm_p = chunk_path(dir, &stem, k, "term");
    let (ps_t, tm_t) = (tmp_of(&ps_p), tmp_of(&tm_p));
    let psb = ps_batch(agg, &file.event)?;
    let mut trows: Vec<(TermKey, Counts)> = agg.term.iter().map(|(k, c)| (*k, *c)).collect();
    trows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let tmb = term_batch(&trows)?;
    let b1 = write_parquet(&ps_t, &crate::keys::ps_schema(true), &[psb])?;
    let b2 = write_parquet(&tm_t, &term_schema(), &[tmb])?;
    // ps LAST: a half-written chunk never shows a complete-looking ps partial.
    rename_retry(&tm_t, &tm_p)?;
    rename_retry(&ps_t, &ps_p)?;
    Ok((agg.ps.len() as u64, b1 + b2))
}

/// Remove a file's chunks (`{stem}_c[0-9][0-9][0-9].*`) before redoing it.
fn clear_stale(dir: &Path, stem: &str) -> Result<usize> {
    let prefix = format!("{stem}_c");
    let mut n = 0;
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name().to_string_lossy().into_owned();
        if let Some(rest) = name.strip_prefix(&prefix) {
            let b = rest.as_bytes();
            if b.len() > 4 && b[..3].iter().all(u8::is_ascii_digit) && b[3] == b'.' {
                std::fs::remove_file(dir.join(&name))?;
                n += 1;
            }
        }
    }
    Ok(n)
}

pub struct Summary {
    pub files: usize,
    pub skipped: usize,
    pub chunks_written: u64,
    pub ps_rows: u64,
    pub bytes: u64,
    pub secs: f64,
}

pub fn run(cfg: &Config, files: &[SourceFile], stats: &RunStats) -> Result<Summary> {
    let t0 = Instant::now();
    std::fs::create_dir_all(&cfg.partial_dir)?;
    lock(&cfg.partial_dir, &params(cfg))?;
    let mut todo = Vec::new();
    let mut skipped = 0;
    for f in files {
        let stem = f.partial_stem();
        if sentinel_path(&cfg.partial_dir, &stem).exists() {
            skipped += 1;
            continue;
        }
        let n = clear_stale(&cfg.partial_dir, &stem)?;
        if n > 0 {
            eprintln!("  {}: cleared {n} stale chunk file(s) of an interrupted run", stem);
        }
        todo.push(f);
    }
    eprintln!(
        "partials: {} source files, {} done already, {} to extract",
        files.len(),
        skipped,
        todo.len()
    );
    let metas: Vec<FileMeta> = todo
        .iter()
        .map(|f| FileMeta::open(&f.path))
        .collect::<Result<_>>()?;
    let mut jobs = Vec::new();
    let remaining: Vec<AtomicUsize> = metas
        .iter()
        .map(|m| AtomicUsize::new(chunks(m.rows, cfg.chunk).len()))
        .collect();
    for (fi, m) in metas.iter().enumerate() {
        for (k, s, e, tail) in chunks(m.rows, cfg.chunk) {
            jobs.push((fi, k, s, e, tail));
        }
    }
    let total_games: u64 = metas.iter().map(|m| m.rows).sum();
    eprintln!("  {} chunks over {} games", jobs.len(), total_games);
    let written = AtomicU64::new(0);
    let ps_rows = AtomicU64::new(0);
    let bytes = AtomicU64::new(0);
    let done_files = AtomicUsize::new(0);
    let progress = Mutex::new(Instant::now());
    jobs.par_iter().try_for_each(|&(fi, k, s, e, tail)| -> Result<()> {
        let f = todo[fi];
        let agg = aggregate_chunk(
            f,
            &metas[fi],
            s,
            e,
            &cfg.filters,
            cfg.max_ply,
            cfg.epd_max_ply,
            stats,
        )
        .with_context(|| format!("{} chunk {k}", f.path.display()))?;
        if !tail || k == 0 || !agg.is_empty() {
            let (rows, b) = write_chunk(&cfg.partial_dir, f, k, &agg)?;
            written.fetch_add(1, Ordering::Relaxed);
            ps_rows.fetch_add(rows, Ordering::Relaxed);
            bytes.fetch_add(b, Ordering::Relaxed);
        }
        drop(agg);
        if remaining[fi].fetch_sub(1, Ordering::AcqRel) == 1 {
            let p = sentinel_path(&cfg.partial_dir, &f.partial_stem());
            std::fs::write(&p, b"").with_context(|| format!("writing {}", p.display()))?;
            let d = done_files.fetch_add(1, Ordering::Relaxed) + 1;
            let mut last = progress.lock().unwrap();
            if last.elapsed().as_secs() >= 30 || d == todo.len() {
                *last = Instant::now();
                let el = t0.elapsed().as_secs_f64();
                let g = stats.get().games;
                eprintln!(
                    "  [{d}/{}] {} | {:.0} games/s | {:.1} min",
                    todo.len(),
                    f.partial_stem(),
                    g as f64 / el,
                    el / 60.0
                );
            }
        }
        Ok(())
    })?;
    Ok(Summary {
        files: files.len(),
        skipped,
        chunks_written: written.into_inner(),
        ps_rows: ps_rows.into_inner(),
        bytes: bytes.into_inner(),
        secs: t0.elapsed().as_secs_f64(),
    })
}
