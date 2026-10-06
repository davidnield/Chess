//! Source discovery and chunked reads, matching build_pooled_stats (d79a0c7).
//!
//! * Discovery (`discover_source_files` :895): per requested month, each event
//!   in CLI order whose `event=E` directory exists -- by EXACT name, since a
//!   case-insensitive filesystem would otherwise label Blitz rows "blitz" --
//!   then every `*.parquet` in sorted order.
//! * Chunk k is source rows [chunk*k, chunk*(k+1)), counting rows READ. Python
//!   only closes a chunk at the end of a 50,000-row read batch, which lands on
//!   those boundaries only if every non-final row group is a multiple of 50,000
//!   rows; files that are not are refused (every D: file has 1,000,000-row
//!   groups).
//! * One chunk is one task: its rows are decoded on their own (a row selection
//!   inside the covering row groups) and walked on one thread, in row order.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Int32Type};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use parquet::arrow::ProjectionMask;

use crate::game::GameRow;

pub const READ_BATCH_GAMES: u64 = 50_000;
pub const COLUMNS: [&str; 6] = [
    "movetext",
    "white_score",
    "termination",
    "mean_elo",
    "white_title",
    "black_title",
];

#[derive(Clone, Debug)]
pub struct SourceFile {
    pub path: PathBuf,
    pub year: i32,
    pub month: u32,
    /// Index into the CLI event list, and the label itself.
    pub event_idx: u8,
    pub event: String,
    /// `src_file.stem`, e.g. "part-0".
    pub stem: String,
}

impl SourceFile {
    /// `year=Y_month=M_event=E_{stem}`: the partials' name stem.
    pub fn partial_stem(&self) -> String {
        format!("year={}_month={}_event={}_{}", self.year, self.month, self.event, self.stem)
    }
}

/// `Y_M` strings from the CLI.
pub fn parse_month(s: &str) -> Result<(i32, u32)> {
    let (y, m) = s
        .split_once('_')
        .ok_or_else(|| anyhow!("month {s:?} is not Y_M (e.g. 2024_6)"))?;
    let (y, m): (i32, u32) = (y.parse()?, m.parse()?);
    if !(1..=12).contains(&m) {
        bail!("month {s:?}: {m} is not a month");
    }
    Ok((y, m))
}

fn entries(dir: &Path) -> Result<Vec<String>> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        v.push(e?.file_name().to_string_lossy().into_owned());
    }
    Ok(v)
}

pub fn discover(source: &Path, months: &[(i32, u32)], events: &[String]) -> Result<Vec<SourceFile>> {
    let mut out = Vec::new();
    for &(year, month) in months {
        let ydir = source.join(format!("year={year}"));
        if !ydir.is_dir() {
            continue;
        }
        let mut mdirs: Vec<String> = entries(&ydir)?
            .into_iter()
            .filter(|n| {
                n.strip_prefix("month=")
                    .and_then(|m| m.parse::<u32>().ok())
                    .is_some_and(|m| m == month)
            })
            .collect();
        mdirs.sort();
        for mname in mdirs {
            let mdir = ydir.join(&mname);
            let names = entries(&mdir)?;
            for (ei, ev) in events.iter().enumerate() {
                let want = format!("event={ev}");
                if !names.iter().any(|n| *n == want) {
                    if let Some(n) = names.iter().find(|n| n.eq_ignore_ascii_case(&want)) {
                        bail!(
                            "{}: the event label {ev:?} does not match the directory {n:?} \
                             exactly; rows would be labelled with the wrong case",
                            mdir.display()
                        );
                    }
                    continue;
                }
                let edir = mdir.join(&want);
                let mut files: Vec<String> = entries(&edir)?
                    .into_iter()
                    .filter(|n| n.ends_with(".parquet"))
                    .collect();
                files.sort();
                for f in files {
                    let path = edir.join(&f);
                    if !path.is_file() {
                        continue;
                    }
                    out.push(SourceFile {
                        stem: f.trim_end_matches(".parquet").to_string(),
                        path,
                        year,
                        month,
                        event_idx: u8::try_from(ei)?,
                        event: ev.clone(),
                    });
                }
            }
        }
    }
    Ok(out)
}

/// A source file's footer, parsed once and shared by its chunk tasks.
pub struct FileMeta {
    pub meta: ArrowReaderMetadata,
    pub rg_rows: Vec<u64>,
    pub rows: u64,
    projection: ProjectionMask,
    col_index: [usize; 6],
}

impl FileMeta {
    pub fn open(path: &Path) -> Result<FileMeta> {
        let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let meta = ArrowReaderMetadata::load(&f, ArrowReaderOptions::new())
            .with_context(|| format!("reading the footer of {}", path.display()))?;
        let md = meta.metadata();
        let rg_rows: Vec<u64> = md.row_groups().iter().map(|rg| rg.num_rows() as u64).collect();
        let rows = rg_rows.iter().sum();
        if let Some((i, n)) = rg_rows[..rg_rows.len().saturating_sub(1)]
            .iter()
            .enumerate()
            .find(|(_, &n)| n % READ_BATCH_GAMES != 0)
        {
            bail!(
                "{}: row group {i} has {n} rows, not a multiple of {READ_BATCH_GAMES}; the \
                 Python extract's chunk boundaries would not fall on {READ_BATCH_GAMES}-row \
                 batches, so this tool refuses the file",
                path.display()
            );
        }
        let schema = meta.schema();
        let mut roots = Vec::new();
        let mut col_index = [0usize; 6];
        for (k, name) in COLUMNS.iter().enumerate() {
            let i = schema
                .index_of(name)
                .map_err(|_| anyhow!("{}: no column {name:?}", path.display()))?;
            roots.push(i);
            col_index[k] = i;
        }
        let projection = ProjectionMask::roots(meta.parquet_schema(), roots.iter().copied());
        // The projected batch keeps the file's column order; map names to it.
        let mut sorted = roots.clone();
        sorted.sort_unstable();
        for c in col_index.iter_mut() {
            *c = sorted.iter().position(|x| x == c).unwrap();
        }
        Ok(FileMeta { meta, rg_rows, rows, projection, col_index })
    }

    /// Rows [start, end) as record batches, decoded with a row selection inside
    /// the row groups that cover them.
    pub fn read_rows(&self, path: &Path, start: u64, end: u64) -> Result<Vec<RecordBatch>> {
        let mut rgs = Vec::new();
        let (mut rg_start, mut first_start) = (0u64, None);
        for (i, &n) in self.rg_rows.iter().enumerate() {
            let rg_end = rg_start + n;
            if rg_end > start && rg_start < end {
                if first_start.is_none() {
                    first_start = Some(rg_start);
                }
                rgs.push(i);
            }
            rg_start = rg_end;
        }
        let Some(first) = first_start else { return Ok(Vec::new()) };
        let sel = RowSelection::from(vec![
            RowSelector::skip((start - first) as usize),
            RowSelector::select((end - start) as usize),
        ]);
        let f = File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(f, self.meta.clone())
            .with_row_groups(rgs)
            .with_projection(self.projection.clone())
            .with_row_selection(sel)
            .with_batch_size((end - start) as usize)
            .build()?;
        let mut out = Vec::new();
        let mut got = 0u64;
        for b in reader {
            let b = b?;
            got += b.num_rows() as u64;
            out.push(b);
        }
        if got != end - start {
            bail!("{}: read {got} rows of [{start}, {end})", path.display());
        }
        Ok(out)
    }

    pub fn columns(&self, batch: &RecordBatch) -> Result<Columns> {
        let c = |k: usize| batch.column(self.col_index[k]).clone();
        Ok(Columns {
            movetext: StrCol::new(c(0), "movetext")?,
            white_score: f64_col(c(1))?,
            termination: StrCol::new(c(2), "termination")?,
            mean_elo: i32_col(c(3))?,
            white_title: StrCol::new(c(4), "white_title")?,
            black_title: StrCol::new(c(5), "black_title")?,
        })
    }
}

/// A string column, Utf8 or LargeUtf8 (anything else is cast to Utf8).
pub enum StrCol {
    Small(arrow::array::StringArray),
    Large(arrow::array::LargeStringArray),
}

impl StrCol {
    fn new(a: ArrayRef, name: &str) -> Result<StrCol> {
        Ok(match a.data_type() {
            DataType::Utf8 => StrCol::Small(a.as_string::<i32>().clone()),
            DataType::LargeUtf8 => StrCol::Large(a.as_string::<i64>().clone()),
            _ => {
                let c = arrow::compute::cast(&a, &DataType::Utf8)
                    .with_context(|| format!("column {name} is not a string column"))?;
                StrCol::Small(c.as_string::<i32>().clone())
            }
        })
    }

    #[inline]
    pub fn get(&self, i: usize) -> Option<&str> {
        match self {
            StrCol::Small(a) => a.is_valid(i).then(|| a.value(i)),
            StrCol::Large(a) => a.is_valid(i).then(|| a.value(i)),
        }
    }
}

fn f64_col(a: ArrayRef) -> Result<Arc<arrow::array::Float64Array>> {
    let a = if a.data_type() == &DataType::Float64 {
        a
    } else {
        arrow::compute::cast(&a, &DataType::Float64).context("white_score is not numeric")?
    };
    Ok(Arc::new(a.as_primitive::<Float64Type>().clone()))
}

fn i32_col(a: ArrayRef) -> Result<Arc<arrow::array::Int32Array>> {
    let a = arrow::compute::cast(&a, &DataType::Int32).context("mean_elo is not an integer")?;
    Ok(Arc::new(a.as_primitive::<Int32Type>().clone()))
}

pub struct Columns {
    pub movetext: StrCol,
    pub white_score: Arc<arrow::array::Float64Array>,
    pub termination: StrCol,
    pub mean_elo: Arc<arrow::array::Int32Array>,
    pub white_title: StrCol,
    pub black_title: StrCol,
}

impl Columns {
    #[inline]
    pub fn row(&self, i: usize) -> GameRow<'_> {
        GameRow {
            movetext: self.movetext.get(i),
            white_score: self.white_score.is_valid(i).then(|| self.white_score.value(i)),
            termination: self.termination.get(i),
            mean_elo: self.mean_elo.is_valid(i).then(|| self.mean_elo.value(i)),
            white_title: self.white_title.get(i),
            black_title: self.black_title.get(i),
        }
    }
}

/// Python's effective chunk size: a chunk only closes at a 50,000-row batch
/// end, so a smaller request collapses to one batch and a non-multiple rounds up.
pub fn effective_chunk(chunk_games: u64) -> u64 {
    chunk_games.max(READ_BATCH_GAMES).div_ceil(READ_BATCH_GAMES) * READ_BATCH_GAMES
}

/// The chunks of a file: (k, start, end, is_tail). A final partial chunk is the
/// tail; a file whose row count is an exact multiple of the chunk has none, and
/// an empty file has only an (empty) tail c000.
pub fn chunks(rows: u64, chunk: u64) -> Vec<(u32, u64, u64, bool)> {
    let mut v = Vec::new();
    let mut k = 0u32;
    let mut s = 0u64;
    while s + chunk <= rows {
        v.push((k, s, s + chunk, false));
        k += 1;
        s += chunk;
    }
    if s < rows || v.is_empty() {
        v.push((k, s, rows, true));
    }
    v
}
