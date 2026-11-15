//! `cargo test --test merge`: `explorer-extract merge` end to end, on synthetic
//! month roots written with the months' own schema and writer (the cases of
//! explorer-merge-spec.md "Tests"). EPDs are stand-ins: the merge reads only
//! their side to move, so twins are two EPD strings under one hash.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Int64Type, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::{json, Value};

use explorer_extract::chesspos::{START_HASH, START_HASH_HI};
use explorer_extract::keys::{
    bucket_of, ps_schema, ps_schema_with, term_schema_full, term_schema_with, writer_props, BANDS,
};
use explorer_extract::merge::{self, Digest, EVENTS};
use explorer_extract::month::MANIFEST_FIELDS;

const EXE: &str = env!("CARGO_BIN_EXE_explorer-extract");
const COMMIT: &str = "c0ffee000001";

// ── synthetic months ─────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Row {
    h: i64,
    san: String,
    ev: usize,
    band: usize,
    epd: String,
    child: i64,
    ply: i32,
    w: i64,
    d: i64,
    b: i64,
    /// parent_hash_hi and child_hash_hi (128-bit months; 0 otherwise).
    hi: i64,
    chi: i64,
}

/// A stand-in EPD for position `pos`, with the side to move `ply` implies.
fn epd(pos: u32, ply: i32) -> String {
    format!("{pos}p6/8/8/8/8/8/8/8 {} - -", if ply % 2 == 1 { "w" } else { "b" })
}

/// A hash in bucket `bkt` (k may be negative).
fn hb(bkt: u32, k: i64) -> i64 {
    k * 512 + i64::from(bkt)
}

#[allow(clippy::too_many_arguments)]
fn row(h: i64, san: &str, ev: usize, band: usize, pos: u32, ply: i32, child: i64, wdb: [i64; 3]) -> Row {
    Row { h, san: san.into(), ev, band, epd: epd(pos, ply), child, ply, w: wdb[0], d: wdb[1], b: wdb[2], hi: 0, chi: 0 }
}

/// A row of a 128-bit month: `row` with the two keys' high halves.
#[allow(clippy::too_many_arguments)]
fn wrow(h: i64, hi: i64, san: &str, ev: usize, band: usize, pos: u32, ply: i32, child: i64, chi: i64, wdb: [i64; 3]) -> Row {
    Row { hi, chi, ..row(h, san, ev, band, pos, ply, child, wdb) }
}

type TermRow = (i64, i32, i32, i32, [i64; 4]);
/// (position_hash, position_hash_hi, kind, reason, end_ply, counts).
type WTermRow = (i64, i64, i32, i32, i32, [i64; 4]);

#[derive(Clone, Default)]
struct Month {
    y: i32,
    m: u32,
    rows: Vec<Row>,
    term: Vec<TermRow>,
    conflicts: Vec<(i64, String, String)>,
    commit: Option<String>,
    /// Write rows in the given order instead of sorting them.
    unsorted: bool,
    null_epd_at: Option<usize>,
    /// (row index, bucket): write that row into another bucket's file.
    misplace: Option<(usize, u32)>,
    man_rows_delta: i64,
    man_ww_delta: i64,
    man_files_delta: i64,
    term_rows_delta: i64,
    /// A `month --key128` month: the wide schemas, `wterm` for the term rows.
    wide: bool,
    wterm: Vec<WTermRow>,
    /// The provenance's max_ply (0 = 30).
    max_ply: i32,
}

fn wmonth(y: i32, m: u32, rows: Vec<Row>, wterm: Vec<WTermRow>) -> Month {
    Month { y, m, rows, wterm, wide: true, max_ply: 50, ..Default::default() }
}

fn month(y: i32, m: u32, rows: Vec<Row>, term: Vec<TermRow>) -> Month {
    Month { y, m, rows, term, ..Default::default() }
}

fn params() -> Value {
    json!({"producer": "rust-month", "max_ply": 30, "ply_key": true, "chunk_games": 250000,
           "events": EVENTS, "min_elo": 0, "exclude_bots": true,
           "excluded_terminations": ["Abandoned", "Rules infraction"], "buckets": 512})
}

fn write_params(root: &Path, v: &Value) {
    std::fs::create_dir_all(root).unwrap();
    std::fs::write(root.join("_extract_params.json"), serde_json::to_string_pretty(v).unwrap()).unwrap();
}

fn write_batch(path: &Path, batch: &RecordBatch) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), Some(writer_props())).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

fn ps_batch(rows: &[Row], null_epd_at: Option<usize>) -> RecordBatch {
    ps_batch_w(rows, null_epd_at, false)
}

fn ps_batch_w(rows: &[Row], null_epd_at: Option<usize>, wide: bool) -> RecordBatch {
    let i64s = |f: &dyn Fn(&Row) -> i64| -> ArrayRef { Arc::new(Int64Array::from_iter_values(rows.iter().map(f))) };
    let epds: Vec<Option<&str>> =
        rows.iter().enumerate().map(|(i, r)| if Some(i) == null_epd_at { None } else { Some(r.epd.as_str()) }).collect();
    let mut cols = vec![
        i64s(&|r| r.h),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.san.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| EVENTS[r.ev]))),
        i64s(&|r| BANDS[r.band]),
        Arc::new(StringArray::from(epds)),
        i64s(&|r| r.child),
        Arc::new(Int32Array::new_null(rows.len())),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.ply))),
        i64s(&|r| r.w),
        i64s(&|r| r.d),
        i64s(&|r| r.b),
        i64s(&|r| r.w + r.d + r.b),
    ];
    if wide {
        cols.push(i64s(&|r| r.hi));
        cols.push(i64s(&|r| r.chi));
    }
    RecordBatch::try_new(ps_schema_with(false, wide), cols).unwrap()
}

fn sort_key(r: &Row) -> (i64, i64, Vec<u8>, usize, usize, i32) {
    (r.h, r.hi, r.san.as_bytes().to_vec(), r.ev, r.band, r.ply)
}

fn manifest_batch(v: &BTreeMap<&str, Value>) -> RecordBatch {
    let mut fields = Vec::new();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for k in MANIFEST_FIELDS {
        match k {
            "replays_per_sec" | "seconds" => {
                fields.push(Field::new(k, DataType::Float64, true));
                cols.push(Arc::new(Float64Array::from(vec![0.0])));
            }
            "quarantine_by_reason" => {
                fields.push(Field::new(k, DataType::Utf8, true));
                cols.push(Arc::new(StringArray::from(vec!["{}"])));
            }
            _ => {
                fields.push(Field::new(k, DataType::Int64, true));
                cols.push(Arc::new(Int64Array::from(vec![v.get(k).and_then(Value::as_i64).unwrap_or(0)])));
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn write_month(root: &Path, s: &Month) {
    let tag = format!("{}_{}", s.y, s.m);
    let mut by: BTreeMap<u32, Vec<(usize, Row)>> = BTreeMap::new();
    for (i, r) in s.rows.iter().enumerate() {
        let b = match s.misplace {
            Some((j, b)) if j == i => b,
            _ => bucket_of(r.h, 512),
        };
        by.entry(b).or_default().push((i, r.clone()));
    }
    let (mut rows, mut sums, mut ply1) = (0i64, [0i64; 4], 0i64);
    for (b, v) in &mut by {
        if !s.unsorted {
            v.sort_by_key(|(_, r)| sort_key(r));
        }
        let null_at = s.null_epd_at.and_then(|j| v.iter().position(|(i, _)| *i == j));
        let rs: Vec<Row> = v.iter().map(|(_, r)| r.clone()).collect();
        write_batch(&root.join(format!("month={tag}")).join(format!("bkt={b}")).join("part-0000.parquet"), &ps_batch_w(&rs, null_at, s.wide));
        for r in &rs {
            rows += 1;
            let t = r.w + r.d + r.b;
            for (k, x) in [r.w, r.d, r.b, t].into_iter().enumerate() {
                sums[k] += x;
            }
            if r.ply == 1 {
                ply1 += t;
            }
        }
    }
    let man: BTreeMap<&str, Value> = [
        ("year", json!(s.y)),
        ("month", json!(s.m)),
        ("files", json!(by.len() as i64 + s.man_files_delta)),
        ("bytes", json!(0)),
        ("rows", json!(rows + s.man_rows_delta)),
        ("ply1_games", json!(ply1)),
        ("total", json!(sums[3])),
        ("white_wins", json!(sums[0] + s.man_ww_delta)),
        ("draws", json!(sums[1])),
        ("black_wins", json!(sums[2])),
        ("buckets", json!(512)),
    ]
    .into_iter()
    .collect();
    write_batch(&root.join("_manifest").join(format!("month={tag}.parquet")), &manifest_batch(&man));
    let mut term = s.term.clone();
    term.sort_by_key(|t| (t.0, t.1, t.2, t.3));
    let tb = RecordBatch::try_new(term_schema_with(true), vec![
        Arc::new(Int64Array::from_iter_values(term.iter().map(|t| t.0))) as ArrayRef,
        Arc::new(Int32Array::from_iter_values(term.iter().map(|t| t.1))),
        Arc::new(Int32Array::from_iter_values(term.iter().map(|t| t.2))),
        Arc::new(Int32Array::from_iter_values(term.iter().map(|t| t.3))),
        Arc::new(Int64Array::from_iter_values(term.iter().map(|t| t.4[0]))),
        Arc::new(Int64Array::from_iter_values(term.iter().map(|t| t.4[1]))),
        Arc::new(Int64Array::from_iter_values(term.iter().map(|t| t.4[2]))),
        Arc::new(Int64Array::from_iter_values(term.iter().map(|t| t.4[3]))),
    ])
    .unwrap();
    let (tb, n_term, term_total) = if s.wide {
        let mut wt = s.wterm.clone();
        wt.sort_by_key(|t| (t.0, t.1, t.2, t.3, t.4));
        let i32s = |f: &dyn Fn(&WTermRow) -> i32| -> ArrayRef { Arc::new(Int32Array::from_iter_values(wt.iter().map(f))) };
        let i64s = |f: &dyn Fn(&WTermRow) -> i64| -> ArrayRef { Arc::new(Int64Array::from_iter_values(wt.iter().map(f))) };
        let b = RecordBatch::try_new(term_schema_full(true, true), vec![
            i64s(&|t| t.0), i32s(&|t| t.2), i32s(&|t| t.3), i32s(&|t| t.4),
            i64s(&|t| t.5[0]), i64s(&|t| t.5[1]), i64s(&|t| t.5[2]), i64s(&|t| t.5[3]), i64s(&|t| t.1),
        ])
        .unwrap();
        (b, wt.len() as i64, wt.iter().map(|t| t.5[3]).sum::<i64>())
    } else {
        (tb, term.len() as i64, term.iter().map(|t| t.4[3]).sum::<i64>())
    };
    write_batch(&root.join("_term").join(format!("year={}_month={}.term.parquet", s.y, s.m)), &tb);
    let mut prov = json!({"commit": s.commit.clone().unwrap_or_else(|| COMMIT.into()),
                          "max_ply": if s.max_ply == 0 { 30 } else { s.max_ply }, "ply_key": true,
                          "term_rows": n_term + s.term_rows_delta,
                          "counters": {"kept": term_total + 5, "failed": 5}, "flags": {"source": "synthetic"}});
    if s.wide {
        prov["key_bits"] = json!(128);
    }
    std::fs::create_dir_all(root.join("_provenance")).unwrap();
    std::fs::write(root.join("_provenance").join(format!("month={tag}.json")), prov.to_string()).unwrap();
    if !s.conflicts.is_empty() {
        let c = &s.conflicts;
        let cb = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("hash", DataType::Int64, true),
                Field::new("epd_a", DataType::Utf8, true),
                Field::new("epd_b", DataType::Utf8, true),
                Field::new("kind", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(c.iter().map(|x| x.0))) as ArrayRef,
                Arc::new(StringArray::from_iter_values(c.iter().map(|x| x.1.as_str()))),
                Arc::new(StringArray::from_iter_values(c.iter().map(|x| x.2.as_str()))),
                Arc::new(StringArray::from_iter_values(c.iter().map(|_| "parent-epd"))),
            ],
        )
        .unwrap();
        write_batch(&root.join("_conflicts").join(format!("month={tag}")).join("rust.parquet"), &cb);
    }
    std::fs::write(root.join(format!("_month={tag}.DONE")), "{}").unwrap();
}

fn build_root(root: &Path, months: &[Month]) {
    let _ = std::fs::remove_dir_all(root);
    write_params(root, &params());
    for m in months {
        write_month(root, m);
    }
}

/// The lock of a `month --key128 --max-ply 50` months root.
fn wide_params() -> Value {
    let mut p = params();
    p["max_ply"] = json!(50);
    p["key_bits"] = json!(128);
    p
}

fn build_root_wide(root: &Path, months: &[Month]) {
    let _ = std::fs::remove_dir_all(root);
    write_params(root, &wide_params());
    for m in months {
        write_month(root, m);
    }
}

// ── the expected book ────────────────────────────────────────────────────────

type Key = (i64, String, String, usize, usize, i32);

fn expected(months: &[Month]) -> BTreeMap<Key, (i64, [i64; 4])> {
    let mut e: BTreeMap<Key, (i64, [i64; 4])> = BTreeMap::new();
    for m in months {
        for r in &m.rows {
            let v = e.entry((r.h, r.epd.clone(), r.san.clone(), r.ev, r.band, r.ply)).or_insert((r.child, [0; 4]));
            assert_eq!(v.0, r.child, "a test case gave one key two children");
            for (k, x) in [r.w, r.d, r.b, r.w + r.d + r.b].into_iter().enumerate() {
                v.1[k] += x;
            }
        }
    }
    e
}

fn expected_term(months: &[Month]) -> BTreeMap<(i64, i32, i32, i32), [i64; 4]> {
    let mut e = BTreeMap::new();
    for m in months {
        for t in &m.term {
            let v = e.entry((t.0, t.1, t.2, t.3)).or_insert([0i64; 4]);
            for k in 0..4 {
                v[k] += t.4[k];
            }
        }
    }
    e
}

// ── running and reading ──────────────────────────────────────────────────────

struct Case {
    dir: PathBuf,
}

impl Case {
    fn new(name: &str) -> Case {
        let dir = std::env::temp_dir().join(format!("ee_merge_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Case { dir }
    }

    fn root(&self) -> PathBuf {
        self.dir.join("months")
    }

    fn book(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn stage(&self) -> PathBuf {
        self.dir.join("stage")
    }

    fn merge(&self, book: &str, months: &str, extra: &[&str]) -> Output {
        self.merge_stage(book, months, &self.stage(), extra)
    }

    fn merge_stage(&self, book: &str, months: &str, stage: &Path, extra: &[&str]) -> Output {
        let (root, out) = (self.root(), self.book(book));
        let mut args: Vec<String> = vec![
            "merge".into(), "--months-root".into(), root.display().to_string(), "--months".into(), months.into(),
            "--out".into(), out.display().to_string(), "--stage-dir".into(), stage.display().to_string(),
            "--test-one-volume".into(),
        ];
        if !extra.contains(&"--threads") {
            args.extend(["--threads".into(), "3".into()]);
        }
        args.extend(extra.iter().map(|s| s.to_string()));
        Command::new(EXE).args(&args).output().expect("running explorer-extract")
    }
}

impl Drop for Case {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn log(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn code(o: &Output) -> i32 {
    o.status.code().unwrap_or(-1)
}

fn batches(p: &Path) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(File::open(p).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap())
        .collect()
}

/// Every parquet file under `dir`, relative path -> bytes.
fn parquet_files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, d: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for e in std::fs::read_dir(d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(root, &p, out);
            } else if p.extension().is_some_and(|x| x == "parquet") {
                let rel = p.strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/");
                out.insert(rel, std::fs::read(&p).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    if dir.exists() {
        walk(dir, dir, &mut out);
    }
    out
}

/// The book's ps rows, checking every file's path, bucket and order.
fn read_book(out: &Path) -> BTreeMap<Key, (i64, [i64; 4], f64)> {
    let mut rows = BTreeMap::new();
    for (rel, _) in parquet_files(&out.join("ps")) {
        let parts: Vec<&str> = rel.split('/').collect();
        assert_eq!(parts.len(), 3, "{rel}");
        let ev = parts[0].strip_prefix("event=").unwrap();
        let band: i64 = parts[1].strip_prefix("elo_band=").unwrap().parse().unwrap();
        let bkt: u32 = parts[2].strip_prefix("bkt").unwrap().strip_suffix(".parquet").unwrap().parse().unwrap();
        let p = out.join("ps").join(parts[0]).join(parts[1]).join(parts[2]);
        let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap()).unwrap().schema().clone();
        assert_eq!(schema.fields(), merge::book_ps_schema().fields(), "{rel}: schema");
        let mut prev: Option<(i64, String, String, i32)> = None;
        for b in batches(&p) {
            let h = b.column(0).as_primitive::<Int64Type>();
            let san = b.column(1).as_string::<i32>();
            let e = b.column(2).as_string::<i32>();
            let bd = b.column(3).as_primitive::<Int64Type>();
            let pe = b.column(4).as_string::<i32>();
            let ch = b.column(5).as_primitive::<Int64Type>();
            let ply = b.column(6).as_primitive::<Int32Type>();
            let c: Vec<_> = (7..11).map(|k| b.column(k).as_primitive::<Int64Type>().clone()).collect();
            let wsa = b.column(11).as_primitive::<Float64Type>();
            for i in 0..b.num_rows() {
                assert_eq!(e.value(i), ev, "{rel}: event column vs path");
                assert_eq!(bd.value(i), band, "{rel}: elo_band column vs path");
                assert_eq!(bucket_of(h.value(i), 512), bkt, "{rel}: bucket");
                let k = (h.value(i), pe.value(i).to_string(), san.value(i).to_string(), ply.value(i));
                if let Some(p) = &prev {
                    assert!(
                        (k.0, k.1.as_bytes(), k.2.as_bytes(), k.3) > (p.0, p.1.as_bytes(), p.2.as_bytes(), p.3),
                        "{rel}: not strictly increasing at {k:?}"
                    );
                }
                prev = Some(k.clone());
                let cs = [c[0].value(i), c[1].value(i), c[2].value(i), c[3].value(i)];
                let evi = EVENTS.iter().position(|x| *x == ev).unwrap();
                let bi = BANDS.iter().position(|&x| x == band).unwrap();
                let key = (k.0, k.1, k.2, evi, bi, k.3);
                assert!(rows.insert(key.clone(), (ch.value(i), cs, wsa.value(i))).is_none(), "duplicate key {key:?}");
            }
        }
    }
    rows
}

fn assert_book(out: &Path, months: &[Month]) {
    let got = read_book(out);
    let want = expected(months);
    let g: Vec<_> = got.iter().map(|(k, v)| (k.clone(), v.0, v.1)).collect();
    let w: Vec<_> = want.iter().map(|(k, v)| (k.clone(), v.0, v.1)).collect();
    assert_eq!(g, w, "the book's rows");
    for (k, v) in &got {
        let f = (v.1[0] as f64 + 0.5 * v.1[1] as f64) / v.1[3] as f64;
        assert_eq!(v.2.to_bits(), f.to_bits(), "white_score_avg at {k:?}");
    }
}

fn collisions(out: &Path) -> BTreeSet<(i64, String)> {
    let mut s = BTreeSet::new();
    for b in batches(&out.join("_collisions.parquet")) {
        let (h, e) = (b.column(0).as_primitive::<Int64Type>(), b.column(1).as_string::<i32>());
        for i in 0..b.num_rows() {
            s.insert((h.value(i), e.value(i).to_string()));
        }
    }
    s
}

fn ps_files_of_bucket(out: &Path, bkt: u32) -> Vec<String> {
    parquet_files(&out.join("ps")).into_keys().filter(|k| k.ends_with(&format!("bkt{bkt:03}.parquet"))).collect()
}

/// A base case: three months over several slices and buckets (0, 1, 7, 511),
/// with overlapping and disjoint keys, negative hashes, and term rows.
fn base_months() -> Vec<Month> {
    let (a, b0, n) = (hb(0, 3), hb(0, -2), hb(511, -1));
    let (c, d) = (hb(7, 11), hb(1, 5));
    let m1 = vec![
        row(a, "e4", 0, 5, 1, 1, 900, [3, 1, 2]),
        row(a, "e4", 0, 6, 1, 1, 900, [1, 0, 0]),
        row(a, "d4", 0, 5, 1, 1, 901, [0, 1, 0]),
        row(a, "e4", 4, 5, 1, 1, 900, [2, 0, 1]),
        row(b0, "Nf3", 1, 0, 2, 2, 902, [1, 0, 0]),
        row(b0, "Nf3", 1, 0, 2, 4, 902, [0, 0, 1]),
        row(n, "c5", 2, 8, 3, 2, 903, [1, 1, 1]),
        row(c, "O-O", 3, 3, 4, 9, 904, [0, 2, 0]),
        row(d, "exd5", 5, 1, 5, 3, 905, [4, 0, 0]),
    ];
    let m2 = vec![
        row(a, "e4", 0, 5, 1, 1, 900, [10, 5, 7]),
        row(a, "c4", 0, 5, 1, 1, 906, [1, 0, 0]),
        row(b0, "Nf3", 1, 0, 2, 2, 902, [2, 2, 2]),
        row(n, "c5", 2, 8, 3, 2, 903, [0, 0, 1]),
        row(n, "e6", 2, 8, 3, 2, 907, [1, 0, 0]),
        row(c, "O-O", 3, 3, 4, 9, 904, [1, 0, 0]),
        row(c, "O-O", 3, 4, 4, 9, 904, [0, 0, 3]),
    ];
    let m3 = vec![
        row(a, "e4", 0, 5, 1, 1, 900, [1, 0, 0]),
        row(d, "exd5", 5, 1, 5, 3, 905, [0, 0, 1]),
        row(d, "Qxd5", 5, 1, 5, 3, 908, [0, 1, 0]),
    ];
    // A kept game with no moves ends at the start position at ply 0 (walk()).
    let s = START_HASH;
    let t1 = vec![(a, 0, 0, 3, [1, 0, 0, 1]), (c, 1, 3, 30, [0, 1, 0, 1]), (n, 0, 1, 12, [2, 0, 1, 3]),
                  (s, 0, 0, 0, [1, 0, 0, 1])];
    let t2 = vec![(a, 0, 0, 3, [0, 0, 2, 2]), (c, 1, 3, 30, [1, 0, 0, 1]), (d, 0, 0, 5, [1, 0, 0, 1]),
                  (s, 0, 0, 0, [0, 1, 0, 1]), (s, 0, 1, 4, [0, 0, 1, 1])];
    let t3 = vec![(c, 1, 2, 30, [0, 0, 1, 1]), (b0, 1, 0, 30, [5, 0, 0, 5]), (s, 0, 1, 0, [0, 0, 1, 1])];
    vec![month(2099, 1, m1, t1), month(2099, 2, m2, t2), month(2099, 3, m3, t3)]
}

const BASE: &str = "2099_1..2099_3";

// ── 1. overlap and routing (and the digest and _slices) ──────────────────────

#[test]
fn overlap_and_routing() {
    let t = Case::new("overlap");
    let months = base_months();
    build_root(&t.root(), &months);
    let o = t.merge("book", BASE, &[]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    let out = t.book("book");
    assert!(out.join("_BOOK.DONE").exists());
    assert_book(&out, &months);
    // Slice routing: exactly the slices with rows have files, per bucket.
    let want_files: BTreeSet<String> = expected(&months)
        .keys()
        .map(|k| format!("event={}/elo_band={}/bkt{:03}.parquet", EVENTS[k.3], BANDS[k.4], bucket_of(k.0, 512)))
        .collect();
    let have: BTreeSet<String> = parquet_files(&out.join("ps")).into_keys().collect();
    assert_eq!(have, want_files);
    assert!(collisions(&out).is_empty());
    // The digest over the expected rows equals the sum of the sentinels'.
    let mut want = Digest::default();
    let mut buf = Vec::new();
    for (k, (child, c)) in expected(&months) {
        let (k1, k2) = merge::ps_key_hashes(&mut buf, k.0, k.1.as_bytes(), k.2.as_bytes(), EVENTS[k.3].as_bytes(), BANDS[k.4], k.5, child);
        want.add(k1, k2, c.map(|x| x as u64));
    }
    let mut got = Digest::default();
    for b in 0..512u32 {
        let d: Value = serde_json::from_str(&std::fs::read_to_string(out.join("_done").join(format!("bkt{b:03}.DONE"))).unwrap()).unwrap();
        let hex = |s: &str| d["digest"][s].as_array().unwrap().iter().map(|x| u64::from_str_radix(x.as_str().unwrap(), 16).unwrap()).collect::<Vec<_>>();
        let (s1, s2) = (hex("seed1"), hex("seed2"));
        let mut one = Digest::default();
        one.0[..4].copy_from_slice(&s1);
        one.0[4..].copy_from_slice(&s2);
        got.merge(&one);
    }
    assert_eq!(got, want, "the sentinels' digests");
    // _slices: 54 rows whose sums add up to the book's.
    let sl = batches(&out.join("_slices.parquet"));
    let n: usize = sl.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 54);
    let tot: i64 = sl.iter().map(|b| b.column(5).as_primitive::<Int64Type>().values().iter().sum::<i64>()).sum();
    let want_tot: i64 = expected(&months).values().map(|v| v.1[3]).sum();
    assert_eq!(tot, want_tot);
    let meta: Value = serde_json::from_str(&std::fs::read_to_string(out.join("_book.meta.json")).unwrap()).unwrap();
    assert_eq!(meta["totals"]["rows"].as_u64().unwrap(), expected(&months).len() as u64);
    assert!(out.join("README.md").exists() && out.join("_merge_params.json").exists());
    assert!(!out.join("_stage").exists(), "_stage left behind");
    // A rerun of a finished book does nothing and succeeds.
    let o = t.merge("book", BASE, &[]);
    assert_eq!(code(&o), 0, "{}", log(&o));
}

// ── 2. cross-month twins; 3. within-month twins and the positive control ─────

fn twin_months(shared_key: bool) -> Vec<Month> {
    let h = hb(3, 42);
    let (a, b) = (10u32, 20u32);
    let m1 = vec![row(h, "Nf3", 0, 5, a, 3, 1000, [2, 1, 0]), row(hb(3, 1), "e4", 0, 5, 30, 1, 5, [1, 0, 0])];
    let m2 = if shared_key {
        vec![row(h, "Nf3", 0, 5, b, 3, 2000, [0, 0, 4])]
    } else {
        vec![row(h, "Bb5", 1, 2, b, 5, 2001, [1, 1, 1]), row(h, "a6", 1, 2, b, 5, 2002, [0, 1, 0])]
    };
    vec![month(2099, 1, m1, vec![]), month(2099, 2, m2, vec![(h, 0, 0, 7, [1, 0, 0, 1])])]
}

#[test]
fn cross_month_twins() {
    for shared in [true, false] {
        let t = Case::new(if shared { "twins_shared" } else { "twins_disjoint" });
        let months = twin_months(shared);
        build_root(&t.root(), &months);
        let o = t.merge("book", "2099_1..2099_2", &[]);
        assert_eq!(code(&o), 0, "{}", log(&o));
        let out = t.book("book");
        assert_book(&out, &months);
        let h = hb(3, 42);
        // Twin B is at ply 3 when it shares A's key, at ply 5 when its moves are disjoint.
        let want: BTreeSet<(i64, String)> = [(h, epd(10, 3)), (h, epd(20, if shared { 3 } else { 5 }))].into_iter().collect();
        assert_eq!(collisions(&out), want);
        // Each twin's rows carry exactly its own month's sums.
        let book = read_book(&out);
        for (k, v) in &book {
            if k.0 == h && k.1.starts_with("10p") {
                assert_eq!(v.1, [2, 1, 0, 3]);
            }
        }
    }
}

fn within_months(drop_twin: bool) -> Vec<Month> {
    let h = hb(9, -77);
    let (ea, eb) = (epd(1, 2), epd(2, 4));
    let mut rows = vec![row(h, "Nc6", 2, 4, 1, 2, 3000, [1, 0, 0]), row(h, "Nc6", 2, 4, 2, 4, 3001, [0, 0, 1])];
    if drop_twin {
        rows.pop();
    }
    let mut m = month(2099, 1, rows, vec![(h, 1, 0, 30, [1, 0, 0, 1])]);
    // As month mode records it: (hash, the smaller EPD, the larger).
    assert!(ea < eb);
    m.conflicts = vec![(h, ea, eb)];
    vec![m]
}

#[test]
fn within_month_twins_and_positive_control() {
    let t = Case::new("within");
    let months = within_months(false);
    build_root(&t.root(), &months);
    let o = t.merge("book", "2099_1", &[]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    let out = t.book("book");
    assert_book(&out, &months);
    assert_eq!(collisions(&out).len(), 2);
    let meta: Value = serde_json::from_str(&std::fs::read_to_string(out.join("_book.meta.json")).unwrap()).unwrap();
    assert_eq!(meta["collisions"]["positive_controls"][0]["present"], json!(true));

    // The month's record stays, one twin goes: the control fails, no _BOOK.DONE.
    let t = Case::new("within_drop");
    build_root(&t.root(), &within_months(true));
    let o = t.merge("book", "2099_1", &[]);
    assert_eq!(code(&o), 1, "{}", log(&o));
    assert!(log(&o).contains("positive control"), "{}", log(&o));
    assert!(!t.book("book").join("_BOOK.DONE").exists());
}

// ── 4. every input violation fails its bucket, and nothing of it is published ─

#[test]
fn input_violations_fail_the_bucket() {
    let bad_bkt = 5u32;
    let h = hb(bad_bkt, 1);
    let good = || vec![row(hb(0, 1), "e4", 0, 0, 1, 1, 1, [1, 0, 0]), row(h, "d4", 0, 0, 2, 1, 2, [1, 0, 0])];
    let mut cases: Vec<(&str, Vec<Month>, &str)> = Vec::new();
    // child disagreement: one (hash, EPD, SAN, slice, ply) with two children
    let mut m2 = good();
    m2[1].child = 99;
    cases.push(("child", vec![month(2099, 1, good(), vec![]), month(2099, 2, m2, vec![])], "child disagreement"));
    // parity: ply 2 with White to move
    let mut p = good();
    p[1].ply = 2;
    cases.push(("parity", vec![month(2099, 1, p, vec![])], "parity"));
    // total != W + D + B (written through a bad total)
    cases.push(("total", vec![], "counts"));
    // unsorted
    let mut u = month(2099, 1, vec![row(h, "e4", 0, 0, 2, 1, 2, [1, 0, 0]), row(h, "d4", 0, 0, 2, 1, 3, [1, 0, 0])], vec![]);
    u.unsorted = true;
    cases.push(("unsorted", vec![u], "strictly increasing"));
    // a row in the wrong bucket
    let mut w = month(2099, 1, vec![row(h, "e4", 0, 0, 2, 1, 2, [1, 0, 0]), row(hb(6, 1), "e4", 0, 0, 3, 1, 4, [1, 0, 0])], vec![]);
    w.misplace = Some((1, bad_bkt));
    cases.push(("bucket", vec![w], "is in bucket"));
    // a NULL EPD
    let mut n = month(2099, 1, good(), vec![]);
    n.null_epd_at = Some(1);
    cases.push(("null_epd", vec![n], "NULL parent_epd"));
    // ply 0 and 31
    for (name, ply) in [("ply0", 0), ("ply31", 31)] {
        let mut r = good();
        r[1].ply = ply;
        r[1].epd = epd(2, if ply == 0 { 2 } else { 31 });
        cases.push((name, vec![month(2099, 1, r, vec![])], "outside 1..=30"));
    }
    for (name, months, needle) in cases {
        let t = Case::new(&format!("viol_{name}"));
        if name == "total" {
            // The months writer derives total; write a good month, then rewrite
            // bucket 5's file with total one too high.
            let ms = vec![month(2099, 1, good(), vec![])];
            build_root(&t.root(), &ms);
            let f = t.root().join("month=2099_1").join(format!("bkt={bad_bkt}")).join("part-0000.parquet");
            let mut b = ps_batch(&[good()[1].clone()], None);
            let mut cols: Vec<ArrayRef> = b.columns().to_vec();
            cols[11] = Arc::new(Int64Array::from(vec![2]));
            b = RecordBatch::try_new(ps_schema(false), cols).unwrap();
            write_batch(&f, &b);
        } else {
            build_root(&t.root(), &months);
        }
        let months_arg = if months.len() == 2 { "2099_1..2099_2" } else { "2099_1" };
        let o = t.merge("book", months_arg, &["--threads", "2"]);
        assert_eq!(code(&o), 1, "{name}: {}", log(&o));
        assert!(log(&o).contains(needle), "{name}: want {needle:?} in:\n{}", log(&o));
        let out = t.book("book");
        assert!(!out.join("_done").join(format!("bkt{bad_bkt:03}.DONE")).exists(), "{name}: bucket published");
        assert!(ps_files_of_bucket(&out, bad_bkt).is_empty(), "{name}: slice files published");
        assert!(!out.join("_BOOK.DONE").exists());
        assert!(!out.join("_stage").join(format!("bkt{bad_bkt:03}")).exists(), "{name}: staged output left");
    }
}

// ── 5. pre-flight refusals (exit 5) ──────────────────────────────────────────

#[test]
fn preflight_refusals() {
    let months = base_months();
    let run = |name: &str, prep: &dyn Fn(&Case), extra: &[&str], needle: &str| {
        let t = Case::new(&format!("refuse_{name}"));
        build_root(&t.root(), &months);
        prep(&t);
        let o = t.merge("book", BASE, extra);
        assert_eq!(code(&o), 5, "{name}: {}", log(&o));
        assert!(log(&o).contains(needle), "{name}: want {needle:?} in:\n{}", log(&o));
        assert!(!t.book("book").join("_done").exists(), "{name}: wrote buckets");
    };
    run("sentinel", &|t| std::fs::remove_file(t.root().join("_month=2099_2.DONE")).unwrap(), &[], "sentinel");
    run("tmp_month", &|t| std::fs::create_dir_all(t.root().join("_tmp_month=2099_4")).unwrap(), &[], "_tmp_month");
    run("commits", &|t| {
        let p = t.root().join("_provenance").join("month=2099_3.json");
        let mut v: Value = serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        v["commit"] = json!("deadbeef0000");
        std::fs::write(&p, v.to_string()).unwrap();
    }, &[], "more than one producer commit");
    run("ply_key", &|t| {
        let mut p = params();
        p["ply_key"] = json!(false);
        write_params(&t.root(), &p);
    }, &[], "ply_key");
    run("key_bits", &|t| {
        let mut p = params();
        p["key_bits"] = json!(128);
        write_params(&t.root(), &p);
    }, &[], "key_bits");
    // events: the six speeds or a subset of them in EVENTS order, nothing else
    for (name, events) in [("events_order", json!(["Blitz", "Rapid", "Classical"])),
                           ("events_unknown", json!(["Blitz", "Chess960"])),
                           ("events_twice", json!(["Blitz", "Blitz"])),
                           ("events_empty", json!([])), ("events_string", json!("Blitz"))] {
        run(name, &|t| {
            let mut p = params();
            p["events"] = events.clone();
            write_params(&t.root(), &p);
        }, &[], "not a non-empty subset of the six speeds in their order");
    }
    run("file_count", &|t| {
        let mut m = base_months().remove(1);
        m.man_files_delta = 1;
        write_month(&t.root(), &m);
    }, &[], "bkt=N dirs");
    // Every other case passes --test-one-volume; without it the one-volume
    // test tree is refused.
    let t = Case::new("refuse_volume");
    build_root(&t.root(), &months);
    let o = Command::new(EXE)
        .args(["merge", "--months-root", &t.root().display().to_string(), "--months", BASE, "--out",
               &t.book("book").display().to_string(), "--stage-dir", &t.stage().display().to_string()])
        .output()
        .unwrap();
    assert_eq!(code(&o), 5, "{}", log(&o));
    assert!(log(&o).contains("input volume"), "{}", log(&o));
    // A changed settings lock.
    let t = Case::new("refuse_lock");
    build_root(&t.root(), &months);
    let o = t.merge("book", BASE, &["--buckets", "0-3"]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    let o = t.merge("book", BASE, &["--row-group-rows", "1000"]);
    assert_eq!(code(&o), 5, "{}", log(&o));
    assert!(log(&o).contains("_merge_params.json"), "{}", log(&o));
    let o = t.merge("book", BASE, &["--no-dictionary", "child_hash"]);
    assert_eq!(code(&o), 5, "{}", log(&o));
    let o = t.merge("book", "2099_1..2099_2", &[]);
    assert_eq!(code(&o), 5, "a different month list: {}", log(&o));
    // The same settings continue it.
    let o = t.merge("book", BASE, &[]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    assert_book(&t.book("book"), &months);
}

// ── 5b. months that hold a subset of the six speeds ──────────────────────────

/// The base case with only its Blitz, Classical and Rapid rows, as `month --events Blitz
/// Classical Rapid` would write it.
fn three_speed_months() -> Vec<Month> {
    let mut months = base_months();
    for m in &mut months {
        m.rows.retain(|r| [0, 2, 4].contains(&r.ev));
    }
    months
}

fn three_speed_root(root: &Path, months: &[Month]) {
    build_root(root, months);
    let mut p = params();
    p["events"] = json!(["Blitz", "Classical", "Rapid"]);
    write_params(root, &p);
}

#[test]
fn months_of_three_speeds() {
    let t = Case::new("three_speeds");
    let months = three_speed_months();
    assert!(months.iter().all(|m| !m.rows.is_empty()));
    three_speed_root(&t.root(), &months);
    let o = t.merge("book", BASE, &[]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    let out = t.book("book");
    assert!(out.join("_BOOK.DONE").exists());
    assert_book(&out, &months);
    // Only the three speeds have slice files; _slices still lists all 54, the others at zero.
    let have: BTreeSet<String> = parquet_files(&out.join("ps")).into_keys().collect();
    let held = ["event=Blitz/", "event=Classical/", "event=Rapid/"];
    assert!(!have.is_empty() && have.iter().all(|f| held.iter().any(|e| f.starts_with(e))), "{have:?}");
    let sl = batches(&out.join("_slices.parquet"));
    assert_eq!(sl.iter().map(|b| b.num_rows()).sum::<usize>(), 54);
    let mut absent_rows = 0i64;
    for b in &sl {
        let (ev, rows) = (b.column(0).as_string::<i32>(), b.column(3).as_primitive::<Int64Type>());
        for i in 0..b.num_rows() {
            if !["Blitz", "Classical", "Rapid"].contains(&ev.value(i)) {
                absent_rows += rows.value(i);
            }
        }
    }
    assert_eq!(absent_rows, 0);
    // The lock and the meta record the months' events, so six-speed months cannot continue it.
    for f in ["_merge_params.json", "_book.meta.json"] {
        let v: Value = serde_json::from_str(&std::fs::read_to_string(out.join(f)).unwrap()).unwrap();
        let p = if f == "_merge_params.json" { &v["input_params"] } else { &v["input"]["params"] };
        assert_eq!(p["events"], json!(["Blitz", "Classical", "Rapid"]), "{f}");
    }
    write_params(&t.root(), &params());
    let o = t.merge("book", BASE, &[]);
    assert_eq!(code(&o), 5, "six-speed params against a three-speed book: {}", log(&o));
    assert!(log(&o).contains("_merge_params.json"), "{}", log(&o));

    // A row of a speed the months do not declare fails its bucket.
    let t = Case::new("three_speeds_stray");
    let mut months = three_speed_months();
    let bad_bkt = 5u32;
    months[0].rows.push(row(hb(bad_bkt, 1), "d4", 1, 0, 2, 1, 2, [1, 0, 0]));
    three_speed_root(&t.root(), &months);
    let o = t.merge("book", BASE, &["--threads", "2"]);
    assert_eq!(code(&o), 1, "{}", log(&o));
    assert!(log(&o).contains("is not one of the months' events"), "{}", log(&o));
    assert!(!t.book("book").join("_done").join(format!("bkt{bad_bkt:03}.DONE")).exists());
    assert!(!t.book("book").join("_BOOK.DONE").exists());
}

// ── 6. month conservation ────────────────────────────────────────────────────

#[test]
fn month_conservation_fails_finalize() {
    for (name, rows, ww, term) in [("rows", 1, 0, 0), ("sums", 0, 1, 0), ("term_rows", 0, 0, 1)] {
        let t = Case::new(&format!("conserve_{name}"));
        let mut months = base_months();
        months[1].man_rows_delta = rows;
        months[1].man_ww_delta = ww;
        months[1].term_rows_delta = term;
        build_root(&t.root(), &months);
        let o = t.merge("book", BASE, &[]);
        assert_eq!(code(&o), 1, "{name}: {}", log(&o));
        assert!(log(&o).contains("conservation"), "{name}: {}", log(&o));
        let out = t.book("book");
        assert!(!out.join("_BOOK.DONE").exists(), "{name}");
        assert!(out.join("_done").join("bkt511.DONE").exists(), "{name}: the buckets themselves pass");
    }
}

// ── 7. resume after a kill in each phase; 8. determinism ─────────────────────

#[test]
fn resume_after_kills_is_byte_identical() {
    let t = Case::new("resume");
    let months = base_months();
    build_root(&t.root(), &months);
    let o = t.merge("ref", BASE, &[]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    let reference = parquet_files(&t.book("ref"));
    assert!(reference.len() > 10);
    // Bucket 0 has files in 3 slices, so a publish kill lands between renames.
    for at in ["stage:0", "merge:0", "verify:0", "publish:0", "stage:511", "term:0", "term-publish:0"] {
        let name = format!("k_{}", at.replace(':', "_"));
        let o = t.merge(&name, BASE, &["--test-crash-at", at]);
        assert_eq!(code(&o), 86, "{at}: {}", log(&o));
        let o = t.merge(&name, BASE, &[]);
        assert_eq!(code(&o), 0, "{at} resume: {}", log(&o));
        let out = t.book(&name);
        assert!(out.join("_BOOK.DONE").exists(), "{at}");
        assert_eq!(parquet_files(&out), reference, "{at}: the resumed book differs");
        assert!(!out.join("_stage").exists(), "{at}: _stage left behind");
        assert_book(&out, &months);
    }
}

#[test]
fn threads_1_and_8_are_byte_identical() {
    let t = Case::new("determinism");
    let months = base_months();
    build_root(&t.root(), &months);
    let o1 = t.merge("t1", BASE, &["--threads", "1"]);
    let o8 = t.merge_stage("t8", BASE, &t.dir.join("stage8"), &["--threads", "8"]);
    assert_eq!((code(&o1), code(&o8)), (0, 0), "{}\n{}", log(&o1), log(&o8));
    let (a, b) = (parquet_files(&t.book("t1")), parquet_files(&t.book("t8")));
    assert!(a.len() > 10);
    assert_eq!(a, b);
}

// ── 9. term ──────────────────────────────────────────────────────────────────

#[test]
fn term_merge_routing_and_digest() {
    let t = Case::new("term");
    let months = base_months();
    build_root(&t.root(), &months);
    let o = t.merge("book", BASE, &[]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    let out = t.book("book");
    let files = parquet_files(&out.join("term"));
    assert_eq!(files.len(), 512);
    let mut got = BTreeMap::new();
    for rel in files.keys() {
        let b: u32 = rel.strip_prefix("bkt").unwrap().strip_suffix(".parquet").unwrap().parse().unwrap();
        let p = out.join("term").join(rel);
        let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap()).unwrap().schema().clone();
        assert_eq!(schema.fields(), term_schema_with(true).fields());
        let mut prev = None;
        for bt in batches(&p) {
            let col = |k: usize| bt.column(k).clone();
            for i in 0..bt.num_rows() {
                let key = (col(0).as_primitive::<Int64Type>().value(i), col(1).as_primitive::<Int32Type>().value(i),
                           col(2).as_primitive::<Int32Type>().value(i), col(3).as_primitive::<Int32Type>().value(i));
                assert_eq!(bucket_of(key.0, 512), b, "routing");
                assert!(prev.is_none_or(|p| key > p), "order");
                prev = Some(key);
                let c: [i64; 4] = std::array::from_fn(|k| col(4 + k).as_primitive::<Int64Type>().value(i));
                assert!(got.insert(key, c).is_none());
            }
        }
    }
    let want = expected_term(&months);
    assert_eq!(got, want, "term rows (end_ply kept, counts summed)");
    assert_eq!(got.get(&(START_HASH, 0, 0, 0)), Some(&[1, 1, 0, 2]), "the no-move games at the start position");
    let mut dg = Digest::default();
    for (k, c) in &want {
        let (k1, k2) = merge::term_key_hashes(*k);
        dg.add(k1, k2, c.map(|x| x as u64));
    }
    let d: Value = serde_json::from_str(&std::fs::read_to_string(out.join("_done").join("term.DONE")).unwrap()).unwrap();
    let hex: Vec<u64> = ["seed1", "seed2"].iter().flat_map(|s| d["digest"][*s].as_array().unwrap().iter()
        .map(|x| u64::from_str_radix(x.as_str().unwrap(), 16).unwrap()).collect::<Vec<_>>()).collect();
    assert_eq!(hex, dg.0.to_vec(), "term digest");
}

/// Phase T's end_ply rule (walk()): kind 0 has 0 <= end_ply <= 30 with 0 only
/// at the start position; kind 1 has end_ply exactly 30.
#[test]
fn term_end_ply_rules() {
    let h = hb(3, 9);
    for (key, ok) in [
        ((START_HASH, 0, 0, 0), true),
        ((START_HASH, 0, 2, 4), true),
        ((h, 0, 3, 17), true),
        ((h, 0, 0, 30), true),
        ((h, 1, 0, 30), true),
        ((h, 0, 0, 0), false),
        ((START_HASH, 1, 0, 0), false),
        ((h, 1, 0, 29), false),
        ((h, 0, 0, 31), false),
        ((h, 0, 0, -1), false),
        ((h, 2, 0, 5), false),
    ] {
        assert_eq!(merge::term_key_rule(key).is_ok(), ok, "{key:?}");
    }
    // End to end: each bad row stops the run at term, and term is not published.
    for (name, row, needle) in [
        ("zero_elsewhere", (h, 0, 0, 0, [1, 0, 0, 1]), "end_ply 0 at position_hash"),
        ("horizon_29", (h, 1, 0, 29, [1, 0, 0, 1]), "kind 1 (horizon) with end_ply 29"),
        ("ended_31", (h, 0, 0, 31, [1, 0, 0, 1]), "outside 0..=30"),
    ] {
        let t = Case::new(&format!("term_rule_{name}"));
        let mut months = base_months();
        months[1].term.push(row);
        build_root(&t.root(), &months);
        let o = t.merge("book", BASE, &[]);
        assert_eq!(code(&o), 1, "{name}: {}", log(&o));
        assert!(log(&o).contains(needle), "{name}: want {needle:?} in:\n{}", log(&o));
        let out = t.book("book");
        assert!(!out.join("_done").join("term.DONE").exists() && !out.join("term").exists(), "{name}: term published");
        assert!(!out.join("_BOOK.DONE").exists(), "{name}");
    }
}

// ── 10. out of space; 11. stage-dir safety ───────────────────────────────────

#[test]
fn out_of_space_exits_4_and_resumes() {
    let t = Case::new("space");
    let months = base_months();
    build_root(&t.root(), &months);
    let o = t.merge("book", BASE, &["--min-free-gb", "100000000"]);
    assert_eq!(code(&o), 4, "{}", log(&o));
    let out = t.book("book");
    assert!(!out.join("ps").exists() && !out.join("_done").exists(), "published something");
    let o = t.merge("book", BASE, &["--min-free-gb", "0"]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    assert_book(&out, &months);
}

#[test]
fn stage_dir_safety() {
    let t = Case::new("stage_safety");
    build_root(&t.root(), &base_months());
    // Non-empty without the marker.
    let s1 = t.dir.join("s_nomarker");
    std::fs::create_dir_all(s1.join("bkt000")).unwrap();
    std::fs::write(s1.join("important.txt"), "keep").unwrap();
    let o = t.merge_stage("b1", BASE, &s1, &[]);
    assert_eq!(code(&o), 5, "{}", log(&o));
    assert!(s1.join("important.txt").exists() && s1.join("bkt000").exists());
    // The marker, plus a foreign file.
    let s2 = t.dir.join("s_foreign");
    std::fs::create_dir_all(s2.join("bkt001")).unwrap();
    std::fs::write(s2.join("_merge_stage"), "").unwrap();
    std::fs::write(s2.join("notes.txt"), "keep").unwrap();
    let o = t.merge_stage("b2", BASE, &s2, &[]);
    assert_eq!(code(&o), 5, "{}", log(&o));
    assert!(s2.join("notes.txt").exists() && s2.join("bkt001").exists());
    // Ours: stale bkt*/ and term/ are deleted, the marker kept.
    let s3 = t.dir.join("s_ours");
    std::fs::create_dir_all(s3.join("bkt002")).unwrap();
    std::fs::create_dir_all(s3.join("term")).unwrap();
    std::fs::write(s3.join("bkt002").join("month=2099_1.parquet"), "stale").unwrap();
    std::fs::write(s3.join("_merge_stage"), "").unwrap();
    let o = t.merge_stage("b3", BASE, &s3, &[]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    let left: Vec<_> = std::fs::read_dir(&s3).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(left, vec![std::ffi::OsString::from("_merge_stage")]);
}

// ── unit tests of the parsers and helpers ────────────────────────────────────

#[test]
fn parsers_and_helpers() {
    let m = merge::parse_months(&["2013_11..2014_2".into(), "2020_5".into()]).unwrap();
    assert_eq!(m, vec![(2013, 11), (2013, 12), (2014, 1), (2014, 2), (2020, 5)]);
    assert_eq!(merge::parse_months(&["2013_1..2026_7".into()]).unwrap().len(), 163);
    assert!(merge::parse_months(&["2014_2..2013_1".into()]).is_err());
    assert_eq!(merge::parse_buckets(&["0-3,9".into(), "511".into()]).unwrap(), vec![0, 1, 2, 3, 9, 511]);
    assert!(merge::parse_buckets(&["510-512".into()]).is_err());
    assert_eq!(merge::epd_white("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"), Some(true));
    assert_eq!(merge::epd_white("8/8/8/8/8/8/8/8 b - e3"), Some(false));
    assert_eq!(merge::epd_white("8/8/8/8/8/8/8/8 x - -"), None);
    assert_eq!(merge::epd_white("8/8/8/8/8/8/8/8 wb - -"), None);
    // The digest is linear: summing a key's counts keeps it.
    let mut buf = Vec::new();
    let (k1, k2) = merge::ps_key_hashes(&mut buf, -5, b"e", b"e4", b"Blitz", 1600, 1, 7);
    let (mut a, mut b) = (Digest::default(), Digest::default());
    a.add(k1, k2, [1, 2, 3, 6]);
    a.add(k1, k2, [4, 0, 1, 5]);
    b.add(k1, k2, [5, 2, 4, 11]);
    assert_eq!(a, b);
    let (j1, _) = merge::ps_key_hashes(&mut buf, -5, b"e", b"e4", b"Blitz", 1600, 1, 8);
    assert_ne!(j1, k1, "the child is part of the digest key");
    assert_eq!(merge::white_score_avg(3, 1, 6).to_bits(), ((3.0 + 0.5) / 6.0f64).to_bits());
}

// ── book-v2: months with 128-bit keys ────────────────────────────────────────

type WKey = (i64, i64, String, usize, usize, i32);
type WVal = (String, i64, i64, [i64; 4]);

/// A book-v2's ps rows by (parent_hash, parent_hash_hi, move_san, event,
/// elo_band, ply), checking every file's schema, bucket and order.
fn read_book_wide(out: &Path) -> BTreeMap<WKey, WVal> {
    let mut rows = BTreeMap::new();
    for (rel, _) in parquet_files(&out.join("ps")) {
        let parts: Vec<&str> = rel.split('/').collect();
        let ev = parts[0].strip_prefix("event=").unwrap();
        let band: i64 = parts[1].strip_prefix("elo_band=").unwrap().parse().unwrap();
        let bkt: u32 = parts[2].strip_prefix("bkt").unwrap().strip_suffix(".parquet").unwrap().parse().unwrap();
        let p = out.join("ps").join(parts[0]).join(parts[1]).join(parts[2]);
        let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap()).unwrap().schema().clone();
        assert_eq!(schema.fields(), merge::book_ps_schema_with(true).fields(), "{rel}: schema");
        let mut prev: Option<(i64, i64, String, i32)> = None;
        for b in batches(&p) {
            let i64c = |k: usize| b.column(k).as_primitive::<Int64Type>().clone();
            let (h, ch, hi, chi) = (i64c(0), i64c(5), i64c(12), i64c(13));
            let (san, pe) = (b.column(1).as_string::<i32>(), b.column(4).as_string::<i32>());
            let ply = b.column(6).as_primitive::<Int32Type>();
            for i in 0..b.num_rows() {
                assert_eq!(bucket_of(h.value(i), 512), bkt, "{rel}: bucket");
                let k = (h.value(i), hi.value(i), san.value(i).to_string(), ply.value(i));
                assert!(prev.as_ref().is_none_or(|p| k > *p), "{rel}: not strictly increasing at {k:?}");
                prev = Some(k.clone());
                let cs: [i64; 4] = std::array::from_fn(|j| i64c(7 + j).value(i));
                let evi = EVENTS.iter().position(|x| *x == ev).unwrap();
                let bi = BANDS.iter().position(|&x| x == band).unwrap();
                let key = (k.0, k.1, k.2, evi, bi, k.3);
                let v = (pe.value(i).to_string(), ch.value(i), chi.value(i), cs);
                assert!(rows.insert(key.clone(), v).is_none(), "duplicate key {key:?}");
            }
        }
    }
    rows
}

fn expected_wide(months: &[Month]) -> BTreeMap<WKey, WVal> {
    let mut e: BTreeMap<WKey, WVal> = BTreeMap::new();
    for m in months {
        for r in &m.rows {
            let v = e
                .entry((r.h, r.hi, r.san.clone(), r.ev, r.band, r.ply))
                .or_insert((r.epd.clone(), r.child, r.chi, [0; 4]));
            assert_eq!((&v.0, v.1, v.2), (&r.epd, r.child, r.chi), "a test case gave one key two EPDs or children");
            for (k, x) in [r.w, r.d, r.b, r.w + r.d + r.b].into_iter().enumerate() {
                v.3[k] += x;
            }
        }
    }
    e
}

/// Two 128-bit months walked to ply 50. Hash `c` is shared by two positions
/// (EPD 4 under high half 8, EPD 6 under high half -5: the key order is the
/// reverse of the EPD order), which play the same move in the same slice.
fn wide_months() -> Vec<Month> {
    let (a, n, c) = (hb(0, 3), hb(511, -1), hb(7, 11));
    let m1 = vec![
        wrow(a, 70, "e4", 0, 5, 1, 1, 900, 71, [3, 1, 2]),
        wrow(a, 70, "e4", 0, 5, 1, 45, 900, 71, [0, 1, 0]),
        wrow(c, 8, "O-O", 3, 3, 4, 9, 904, 72, [0, 2, 0]),
        wrow(c, -5, "O-O", 3, 3, 6, 9, 907, 76, [1, 0, 0]),
        wrow(c, -5, "Nf3", 3, 3, 6, 9, 905, 73, [1, 0, 0]),
        wrow(n, 1, "c5", 2, 8, 3, 2, 903, 74, [1, 1, 1]),
    ];
    let m2 = vec![
        wrow(a, 70, "e4", 0, 5, 1, 1, 900, 71, [10, 5, 7]),
        wrow(c, 8, "O-O", 3, 3, 4, 9, 904, 72, [1, 0, 0]),
        wrow(c, -5, "Nf3", 3, 3, 6, 9, 905, 73, [0, 0, 2]),
        wrow(c, -5, "a3", 3, 4, 6, 9, 906, 75, [1, 0, 0]),
        wrow(n, 1, "c5", 2, 8, 3, 50, 903, 74, [0, 0, 1]),
    ];
    let s = (START_HASH, START_HASH_HI);
    let t1 = vec![(a, 70, 0, 0, 3, [1, 0, 0, 1]), (c, 8, 1, 3, 50, [0, 1, 0, 1]), (c, -5, 1, 3, 50, [2, 0, 0, 2]),
                  (s.0, s.1, 0, 0, 0, [1, 0, 0, 1]), (n, 1, 0, 1, 44, [0, 0, 1, 1])];
    let t2 = vec![(a, 70, 0, 0, 3, [0, 0, 2, 2]), (c, 8, 1, 3, 50, [1, 0, 0, 1]), (s.0, s.1, 0, 1, 0, [0, 0, 1, 1])];
    vec![wmonth(2099, 1, m1, t1), wmonth(2099, 2, m2, t2)]
}

const WBASE: &str = "2099_1..2099_2";

#[test]
fn book_v2_end_to_end() {
    let t = Case::new("v2");
    let months = wide_months();
    build_root_wide(&t.root(), &months);
    let o = t.merge("book", WBASE, &[]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    let out = t.book("book");
    assert!(out.join("_BOOK.DONE").exists());
    let (got, want) = (read_book_wide(&out), expected_wide(&months));
    assert_eq!(got, want, "the book's rows: counts summed per 128-bit key, plies past 30 kept");
    // The two positions under hash c stay apart, also where they share a move.
    let c = hb(7, 11);
    assert_eq!(got[&(c, 8, "O-O".to_string(), 3, 3, 9)], (epd(4, 9), 904, 72, [1, 2, 0, 3]));
    assert_eq!(got[&(c, -5, "O-O".to_string(), 3, 3, 9)], (epd(6, 9), 907, 76, [1, 0, 0, 1]));
    // ... and are listed as a 64-bit collision, for the record.
    let want_coll: BTreeSet<(i64, String)> = [(c, epd(4, 9)), (c, epd(6, 9))].into_iter().collect();
    assert_eq!(collisions(&out), want_coll);
    let meta: Value = serde_json::from_str(&std::fs::read_to_string(out.join("_book.meta.json")).unwrap()).unwrap();
    assert_eq!(meta["schema"], json!("book-v2"));
    assert_eq!(meta["collisions"]["status"], json!("resolved by the 128-bit key"));
    assert_eq!(meta["totals"]["rows"].as_u64().unwrap(), want.len() as u64);
    let lock: Value = serde_json::from_str(&std::fs::read_to_string(out.join("_merge_params.json")).unwrap()).unwrap();
    assert_eq!(lock["schema"], json!("book-v2"));
    assert_eq!(lock["input_params"]["key_bits"], json!(128));
    assert!(std::fs::read_to_string(out.join("README.md")).unwrap().contains("parent_hash_hi"));
    // Term: keyed by the whole key, the high half last.
    let mut term = BTreeMap::new();
    for rel in parquet_files(&out.join("term")).keys() {
        let b: u32 = rel.strip_prefix("bkt").unwrap().strip_suffix(".parquet").unwrap().parse().unwrap();
        let p = out.join("term").join(rel);
        let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap()).unwrap().schema().clone();
        assert_eq!(schema.fields(), term_schema_full(true, true).fields());
        let mut prev = None;
        for bt in batches(&p) {
            let i64c = |k: usize| bt.column(k).as_primitive::<Int64Type>().clone();
            let i32c = |k: usize| bt.column(k).as_primitive::<Int32Type>().clone();
            for i in 0..bt.num_rows() {
                let key = (i64c(0).value(i), i64c(8).value(i), i32c(1).value(i), i32c(2).value(i), i32c(3).value(i));
                assert_eq!(bucket_of(key.0, 512), b, "routing");
                assert!(prev.is_none_or(|p| key > p), "order");
                prev = Some(key);
                let cs: [i64; 4] = std::array::from_fn(|k| i64c(4 + k).value(i));
                assert!(term.insert(key, cs).is_none());
            }
        }
    }
    let mut want_term = BTreeMap::new();
    for m in &months {
        for w in &m.wterm {
            let v = want_term.entry((w.0, w.1, w.2, w.3, w.4)).or_insert([0i64; 4]);
            for k in 0..4 {
                v[k] += w.5[k];
            }
        }
    }
    assert_eq!(term, want_term, "term rows by (position_hash, position_hash_hi, kind, reason, end_ply)");
    // Threads cannot change a byte.
    let o = t.merge("book1", WBASE, &["--threads", "1"]);
    assert_eq!(code(&o), 0, "{}", log(&o));
    assert_eq!(parquet_files(&out.join("ps")), parquet_files(&t.book("book1").join("ps")));
    assert_eq!(parquet_files(&out.join("term")), parquet_files(&t.book("book1").join("term")));
}

#[test]
fn book_v2_violations_fail() {
    let a = hb(0, 3);
    let mut cases: Vec<(&str, Vec<Month>, &str)> = Vec::new();
    let with = |extra: Row| {
        let mut ms = wide_months();
        ms[1].rows.push(extra);
        ms
    };
    // One key, two positions.
    cases.push(("two_epds", with(wrow(a, 70, "d4", 0, 5, 2, 1, 950, 77, [1, 0, 0])), "two EPDs under one 128-bit key"));
    // One position, two keys.
    cases.push(("two_keys", with(wrow(a, 71, "d4", 0, 5, 1, 1, 950, 77, [1, 0, 0])), "one EPD under two 128-bit keys"));
    // One edge, two child keys (the low halves agree).
    let mut ms = wide_months();
    ms[1].rows[0].chi = 99;
    cases.push(("child_hi", ms, "child disagreement"));
    // A ply past the months' cap.
    cases.push(("ply51", with(wrow(a, 70, "d4", 0, 5, 1, 51, 950, 77, [1, 0, 0])), "outside 1..=50"));
    // A no-move game somewhere other than the start position's whole key.
    let mut ms = wide_months();
    ms[1].wterm.push((START_HASH, START_HASH_HI + 1, 0, 0, 0, [1, 0, 0, 1]));
    cases.push(("term_start", ms, "only the start position"));
    for (name, months, needle) in cases {
        let t = Case::new(&format!("v2_viol_{name}"));
        build_root_wide(&t.root(), &months);
        let o = t.merge("book", WBASE, &["--threads", "2"]);
        assert_eq!(code(&o), 1, "{name}: {}", log(&o));
        assert!(log(&o).contains(needle), "{name}: want {needle:?} in:\n{}", log(&o));
        assert!(!t.book("book").join("_BOOK.DONE").exists(), "{name}: the book was finished");
    }
}

#[test]
fn book_v2_preflight_refusals() {
    let run = |name: &str, months: Vec<Month>, prep: &dyn Fn(&Case), needle: &str| {
        let t = Case::new(&format!("v2_refuse_{name}"));
        build_root_wide(&t.root(), &months);
        prep(&t);
        let o = t.merge("book", WBASE, &[]);
        assert_eq!(code(&o), 5, "{name}: {}", log(&o));
        assert!(log(&o).contains(needle), "{name}: want {needle:?} in:\n{}", log(&o));
        assert!(!t.book("book").join("_done").exists(), "{name}: wrote buckets");
    };
    let set = |k: &'static str, v: Value| {
        move |t: &Case| {
            let mut p = wide_params();
            p[k] = v.clone();
            write_params(&t.root(), &p);
        }
    };
    run("bits", wide_months(), &set("key_bits", json!(256)), "only 64-bit months");
    run("cap", wide_months(), &set("max_ply", json!(201)), "outside 1..=200");
    // The lock says 64-bit, the months are 128-bit (and the other way round in preflight_refusals).
    run("lock64", wide_months(), &|t| {
        let mut p = wide_params();
        p.as_object_mut().unwrap().remove("key_bits");
        write_params(&t.root(), &p);
    }, "max_ply is 50, not 30");
    // A --key128 month fails on a conflict, so a finished one has no record of any.
    let mut ms = wide_months();
    ms[0].conflicts = vec![(hb(7, 11), epd(4, 9), epd(6, 9))];
    run("conflicts", ms, &|_| {}, "conflict record");
}
