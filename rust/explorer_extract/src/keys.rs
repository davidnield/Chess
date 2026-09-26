//! Aggregation keys and the parquet schemas every output shares.

use std::fs::File;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// The rating groups `rating_band` returns, in order; keys store the index.
pub const BANDS: [i64; 9] = [0, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2500];

#[inline]
pub fn band_index(band: i64) -> u8 {
    BANDS.iter().position(|&b| b == band).expect("band from rating_band") as u8
}

/// Longest token parse_san can accept: piece, file, rank, separator, square,
/// "=Q", check -- nine ASCII bytes. Castling and null tokens are shorter.
pub const SAN_BYTES: usize = 9;

/// move_san, zero-padded. Byte order is string order, so sorting on it sorts
/// the column as text.
pub type San = [u8; SAN_BYTES];

pub fn san_of(tok: &str) -> Result<San> {
    let b = tok.as_bytes();
    if b.len() > SAN_BYTES || b.contains(&0) {
        bail!("parsed move token {tok:?} does not fit the {SAN_BYTES}-byte key");
    }
    let mut s = [0u8; SAN_BYTES];
    s[..b.len()].copy_from_slice(b);
    Ok(s)
}

pub fn san_str(s: &San) -> &str {
    let n = s.iter().position(|&b| b == 0).unwrap_or(SAN_BYTES);
    std::str::from_utf8(&s[..n]).expect("ASCII")
}

/// (parent_hash, move_san, event, elo_band). Ord is the output sort order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct PsKey {
    pub hash: i64,
    pub san: San,
    pub event: u8,
    pub band: u8,
}

impl Hash for PsKey {
    #[inline]
    fn hash<H: Hasher>(&self, st: &mut H) {
        let lo = u64::from_le_bytes(self.san[..8].try_into().unwrap());
        st.write_u64(self.hash as u64);
        st.write_u64(
            lo ^ (u64::from(self.san[8]) << 7)
                ^ (u64::from(self.event) << 59)
                ^ (u64::from(self.band) << 52),
        );
    }
}

/// (position_hash, kind, reason).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TermKey {
    pub hash: i64,
    pub kind: u8,
    pub reason: u8,
}

impl Hash for TermKey {
    #[inline]
    fn hash<H: Hasher>(&self, st: &mut H) {
        st.write_u64(self.hash as u64 ^ (u64::from(self.kind) << 3) ^ (u64::from(self.reason) << 5));
    }
}

/// `((h % n) + n) % n`: `_bucket_expr` in DuckDB, rem_euclid here.
#[inline]
pub fn bucket_of(hash: i64, n: u32) -> u32 {
    hash.rem_euclid(i64::from(n)) as u32
}

// ── schemas ──────────────────────────────────────────────────────────────────

/// PS_COLS. The partials are Polars-written, so their strings are large_string;
/// the bucketed months and DuckDB monthlies use plain string.
pub fn ps_schema(large_strings: bool) -> SchemaRef {
    let s = if large_strings { DataType::LargeUtf8 } else { DataType::Utf8 };
    Arc::new(Schema::new(vec![
        Field::new("parent_hash", DataType::Int64, true),
        Field::new("move_san", s.clone(), true),
        Field::new("event", s.clone(), true),
        Field::new("elo_band", DataType::Int64, true),
        Field::new("parent_epd", s, true),
        Field::new("child_hash", DataType::Int64, true),
        Field::new("child_eval", DataType::Int32, true),
        Field::new("ply", DataType::Int32, true),
        Field::new("white_wins", DataType::Int64, true),
        Field::new("draws", DataType::Int64, true),
        Field::new("black_wins", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]))
}

pub fn term_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("position_hash", DataType::Int64, true),
        Field::new("kind", DataType::Int32, true),
        Field::new("reason", DataType::Int32, true),
        Field::new("white_wins", DataType::Int64, true),
        Field::new("draws", DataType::Int64, true),
        Field::new("black_wins", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]))
}

pub fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).expect("zstd level")))
        .build()
}

/// Write `batches` (possibly none) to `path` as one parquet file.
pub fn write_parquet(path: &Path, schema: &SchemaRef, batches: &[RecordBatch]) -> Result<u64> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let f = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut w = ArrowWriter::try_new(f, schema.clone(), Some(writer_props()))?;
    for b in batches {
        w.write(b)?;
    }
    w.close()?;
    Ok(std::fs::metadata(path)?.len())
}

/// Rename, retrying for a few seconds: Defender and indexers briefly lock
/// freshly written files on Windows.
pub fn rename_retry(from: &Path, to: &Path) -> Result<()> {
    let mut last = None;
    for i in 0..50 {
        match std::fs::rename(from, to) {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = Some(e);
                std::thread::sleep(Duration::from_millis(100 + 20 * i));
            }
        }
    }
    Err(last.unwrap()).with_context(|| format!("renaming {} -> {}", from.display(), to.display()))
}

/// `<name>.parquet` -> `<name>.parquet.tmp`, as `Path.with_suffix(".parquet.tmp")`.
pub fn tmp_of(path: &Path) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    s.into()
}
