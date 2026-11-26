pub mod key;
use anyhow::{ensure, Context, Result};
use arrow::array::{Array, BinaryArray, Int32Array, ListArray, StringArray, StructArray};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::file::statistics::Statistics;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    time::{Instant, UNIX_EPOCH},
};

const SCHEMA: &str = "chessdb-archive-query-index-v1";
fn s<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str().with_context(|| format!("missing string {k}"))
}
fn u(v: &Value, k: &str) -> Result<u64> {
    v[k].as_u64()
        .with_context(|| format!("missing unsigned {k}"))
}
fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn load(p: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(
        &fs::read(p).with_context(|| format!("read {}", p.display()))?,
    )?)
}
fn stamp(p: &Path) -> Result<(u64, u64)> {
    let m = fs::metadata(p)?;
    ensure!(m.is_file(), "not a regular archive file");
    Ok((
        m.len(),
        m.modified()?
            .duration_since(UNIX_EPOCH)?
            .as_nanos()
            .try_into()?,
    ))
}
fn inside(root: &Path, name: &str) -> Result<PathBuf> {
    let p = Path::new(name);
    ensure!(
        !p.is_absolute()
            && p.components().all(|c| matches!(c, Component::Normal(_)))
            && !name.contains(':'),
        "unsafe archive path"
    );
    Ok(root.join(p))
}
pub fn write_new(p: &Path, v: &Value) -> Result<()> {
    ensure!(!p.exists(), "output already exists: {}", p.display());
    let tmp = p.with_extension("query.tmp");
    let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    serde_json::to_writer(&mut f, v)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    drop(f);
    fs::rename(tmp, p)?;
    Ok(())
}

pub fn index(root: &Path, audit_path: &Path, out: &Path) -> Result<()> {
    let root = fs::canonicalize(root)?;
    let marker_bytes = fs::read(root.join("ARCHIVE.COMPLETE.json"))?;
    let marker: Value = serde_json::from_slice(&marker_bytes)?;
    let marker_sha = sha(&marker_bytes);
    let audit = load(audit_path)?;
    ensure!(
        audit["verified"] == true
            && audit["deep"] == true
            && audit["bounded"] == false
            && audit["original_source_required"] == false,
        "requires accepted source-independent full deep audit"
    );
    ensure!(
        s(&audit, "marker_sha256")? == marker_sha && marker["bounded"] == false,
        "archive/audit generation differs"
    );
    let receipts = marker["receipts"]
        .as_object()
        .context("missing receipt inventory")?;
    let mut files = Vec::new();
    let mut seen = BTreeSet::new();
    let (mut bytes, mut rows) = (0u64, 0u64);
    for (name, expected) in receipts {
        let data = fs::read(inside(&root, name)?)?;
        ensure!(
            Some(sha(&data).as_str()) == expected.as_str(),
            "receipt hash differs: {name}"
        );
        let r: Value = serde_json::from_slice(&data)?;
        let file = s(&r, "file")?;
        ensure!(
            r["identity"] == marker["identity"] && seen.insert(file.to_string()),
            "duplicate file or changed identity"
        );
        for field in [
            "records",
            "first_key_hex",
            "last_key_hex",
            "source_logical_sha256",
            "stream_sha256",
        ] {
            ensure!(
                r["source"][field] == r["audit"][field] && !r["source"][field].is_null(),
                "receipt audit mismatch"
            );
        }
        let (size, mtime) = stamp(&inside(&root, file)?)?;
        ensure!(
            size == u(&r["audit"], "bytes")?,
            "archive shard size changed: {file}"
        );
        let n = u(&r["source"], "records")?;
        ensure!(n > 0, "empty shard");
        let first = s(&r["source"], "first_key_hex")?;
        let last = s(&r["source"], "last_key_hex")?;
        ensure!(key::unhex(first)? <= key::unhex(last)?, "reversed shard");
        rows = rows.checked_add(n).context("row overflow")?;
        bytes = bytes.checked_add(size).context("size overflow")?;
        files.push(
            json!({"file":file,"bytes":size,"mtime_ns":mtime,"rows":n,"first":first,"last":last,
            "receipt":name,"receipt_sha256":expected,"file_sha256":r["audit"]["file_sha256"]}),
        );
    }
    files.sort_by(|a, b| a["first"].as_str().cmp(&b["first"].as_str()));
    for pair in files.windows(2) {
        ensure!(
            key::unhex(s(&pair[0], "last")?)? < key::unhex(s(&pair[1], "first")?)?,
            "overlapping shards"
        );
    }
    ensure!(
        rows == u(&marker, "records")?
            && bytes == u(&marker, "parquet_bytes")?
            && files.len() as u64 == u(&marker, "shards")?,
        "incomplete archive"
    );
    ensure!(
        audit["records"] == marker["records"]
            && audit["bytes"] == marker["parquet_bytes"]
            && audit["shards"] == marker["shards"],
        "audit totals differ"
    );
    let mut v = json!({"schema":SCHEMA,"archive":root,"marker_sha256":marker_sha,"audit":audit,
        "records":rows,"bytes":bytes,"files":files,"integrity":"accepted full audit plus current manifest/size/mtime; not a new full-byte hash audit"});
    let digest = sha(&serde_json::to_vec(&v)?);
    v["index_sha256"] = digest.into();
    write_new(out, &v)?;
    eprintln!("indexed {} shards / {} records", receipts.len(), rows);
    Ok(())
}

struct Shard {
    path: PathBuf,
    bytes: u64,
    mtime: u64,
    rows: u64,
    first: Vec<u8>,
    last: Vec<u8>,
}
pub struct Archive {
    root: PathBuf,
    marker_sha: String,
    shards: Vec<Shard>,
    metadata: BTreeMap<usize, ArrowReaderMetadata>,
}
impl Archive {
    pub fn open(index: &Path) -> Result<Self> {
        let mut v = load(index)?;
        let expected = s(&v, "index_sha256")?.to_owned();
        v.as_object_mut().unwrap().remove("index_sha256");
        ensure!(
            sha(&serde_json::to_vec(&v)?) == expected && v["schema"] == SCHEMA,
            "query index changed"
        );
        let root = PathBuf::from(s(&v, "archive")?);
        let marker_sha = s(&v, "marker_sha256")?.to_owned();
        ensure!(
            sha(&fs::read(root.join("ARCHIVE.COMPLETE.json"))?) == marker_sha,
            "archive generation changed"
        );
        let mut shards = Vec::new();
        let mut rows = 0;
        let mut bytes = 0;
        for x in v["files"].as_array().context("missing files")? {
            let path = inside(&root, s(x, "file")?)?;
            let b = u(x, "bytes")?;
            let m = u(x, "mtime_ns")?;
            ensure!(
                stamp(&path)? == (b, m),
                "missing or changed archive shard: {}",
                path.display()
            );
            let r = u(x, "rows")?;
            rows += r;
            bytes += b;
            shards.push(Shard {
                path,
                bytes: b,
                mtime: m,
                rows: r,
                first: key::unhex(s(x, "first")?)?,
                last: key::unhex(s(x, "last")?)?,
            });
        }
        ensure!(
            rows == u(&v, "records")? && bytes == u(&v, "bytes")?,
            "index totals differ"
        );
        for pair in shards.windows(2) {
            ensure!(pair[0].last < pair[1].first, "overlapping index ranges");
        }
        Ok(Self {
            root,
            marker_sha,
            shards,
            metadata: BTreeMap::new(),
        })
    }
    fn locate(&self, key: &[u8]) -> Option<usize> {
        let i = self.shards.partition_point(|s| s.last.as_slice() < key);
        if i < self.shards.len() && self.shards[i].first.as_slice() <= key {
            Some(i)
        } else {
            None
        }
    }
    pub fn query(&mut self, requests: &[Value]) -> Result<Value> {
        let started = Instant::now();
        ensure!(
            sha(&fs::read(self.root.join("ARCHIVE.COMPLETE.json"))?) == self.marker_sha,
            "archive generation changed"
        );
        let mut result = Vec::new();
        let mut groups: BTreeMap<usize, BTreeMap<Vec<u8>, Vec<usize>>> = BTreeMap::new();
        for (i, r) in requests.iter().enumerate() {
            let (fen, key, mirrored) = key::key(s(r, "fen")?)?;
            result.push(json!({"id":r.get("id").cloned().unwrap_or(json!(i)),"fen":fen,"source_key":key::hex(&key),
                "mirrored":mirrored,"record_present":false,"found":false,"ply":-2,"moves":[]}));
            if let Some(shard) = self.locate(&key) {
                groups
                    .entry(shard)
                    .or_default()
                    .entry(key)
                    .or_default()
                    .push(i);
            }
        }
        let (mut scanned, mut selected, mut compressed) = (0u64, 0usize, 0u64);
        let files = groups.len();
        for (sid, keys) in groups {
            let shard = &self.shards[sid];
            ensure!(
                stamp(&shard.path)? == (shard.bytes, shard.mtime),
                "archive shard changed"
            );
            if !self.metadata.contains_key(&sid) {
                // A bounded metadata cache supports a long-lived process without retaining every footer.
                if self.metadata.len() >= 16 {
                    let first = *self.metadata.keys().next().unwrap();
                    self.metadata.remove(&first);
                }
                let meta = ArrowReaderMetadata::load(
                    &File::open(&shard.path)?,
                    ArrowReaderOptions::default(),
                )?;
                ensure!(
                    meta.metadata().file_metadata().num_rows() as u64 == shard.rows,
                    "row count differs"
                );
                self.metadata.insert(sid, meta);
            }
            let meta = self.metadata[&sid].clone();
            let mut keep = Vec::new();
            for (i, rg) in meta.metadata().row_groups().iter().enumerate() {
                let retain = match rg.column(0).statistics() {
                    Some(Statistics::ByteArray(st)) if st.min_is_exact() && st.max_is_exact() => {
                        match (st.min_opt(), st.max_opt()) {
                            (Some(lo), Some(hi)) => keys
                                .range(lo.data().to_vec()..=hi.data().to_vec())
                                .next()
                                .is_some(),
                            _ => true,
                        }
                    }
                    _ => true,
                };
                if retain {
                    keep.push(i);
                    compressed += rg.compressed_size() as u64;
                }
            }
            selected += keep.len();
            if keep.is_empty() {
                continue;
            }
            let builder =
                ParquetRecordBatchReaderBuilder::new_with_metadata(File::open(&shard.path)?, meta);
            ensure!(
                builder
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    == [
                        "source_key",
                        "source_value",
                        "fen",
                        "moves",
                        "cdb_ply",
                        "cdb_mirror_ply"
                    ],
                "archive schema differs"
            );
            let mask = ProjectionMask::roots(builder.parquet_schema(), [0, 2, 3, 4, 5]);
            let reader = builder
                .with_projection(mask)
                .with_row_groups(keep)
                .with_batch_size(32768)
                .build()?;
            for b in reader {
                let b = b?;
                scanned += b.num_rows() as u64;
                let k = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .context("source key type")?;
                let f = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("fen type")?;
                let m = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .context("moves type")?;
                let p = b
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .context("ply type")?;
                let mp = b
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .context("mirror ply type")?;
                for row in 0..b.num_rows() {
                    ensure!(!k.is_null(row), "null source key");
                    if let Some(ids) = keys.get(k.value(row)) {
                        ensure!(
                            !f.is_null(row)
                                && !m.is_null(row)
                                && !p.is_null(row)
                                && !mp.is_null(row),
                            "position record lacks decoded fields"
                        );
                        let (_, canonical, mirrored) = key::key(f.value(row))?;
                        ensure!(
                            canonical == k.value(row) && !mirrored,
                            "archive FEN/key orientation differs"
                        );
                        let moves = m.value(row);
                        let moves = moves
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .context("move struct")?;
                        let uci = moves
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .context("move type")?;
                        let score = moves
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .context("score type")?;
                        for &id in ids {
                            ensure!(
                                result[id]["record_present"] == false,
                                "duplicate archive key"
                            );
                            let mirror = result[id]["mirrored"] == true;
                            let ply = if mirror { mp.value(row) } else { p.value(row) };
                            let mut out = Vec::new();
                            for j in 0..moves.len() {
                                ensure!(
                                    !moves.is_null(j) && !uci.is_null(j) && !score.is_null(j),
                                    "null move/score"
                                );
                                let u = if mirror {
                                    key::mirror_move(uci.value(j))?
                                } else {
                                    uci.value(j).to_owned()
                                };
                                out.push(json!({"uci":u,"score":score.value(j)}));
                            }
                            ensure!(
                                ply >= -2 && (ply != -2 || out.is_empty()),
                                "invalid missing-value sentinel"
                            );
                            result[id]["record_present"] = true.into();
                            result[id]["found"] = (ply != -2).into();
                            result[id]["ply"] = ply.into();
                            result[id]["moves"] = out.into();
                        }
                    }
                }
            }
            ensure!(
                stamp(&shard.path)? == (shard.bytes, shard.mtime),
                "archive changed during lookup"
            );
        }
        Ok(
            json!({"schema":"chessdb-archive-query-result-v1","marker_sha256":self.marker_sha,"results":result,
            "metrics":{"seconds":started.elapsed().as_secs_f64(),"shards":files,"row_groups":selected,"rows_decoded":scanned,
                "selected_compressed_bytes_all_columns":compressed}}),
        )
    }
}

#[cfg(test)]
mod tests;
