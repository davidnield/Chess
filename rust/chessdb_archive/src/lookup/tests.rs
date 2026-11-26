use super::*;
use arrow::{
    array::{ArrayRef, BinaryBuilder, Int32Builder, ListBuilder, StringBuilder, StructBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::sync::Arc;

struct Fixture {
    root: PathBuf,
    index: PathBuf,
    file: PathBuf,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let (Ok(p), Ok(t)) = (
            self.root.canonicalize(),
            std::env::temp_dir().canonicalize(),
        ) {
            if p.parent() == Some(t.as_path())
                && p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("chessdb-query-test-")
            {
                let _ = fs::remove_dir_all(p);
            }
        }
    }
}
fn fixture(statistics: bool) -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "chessdb-query-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    fs::create_dir(root.join("parquet")).unwrap();
    let file = root.join("parquet/data.parquet");
    let mf = vec![
        Field::new("uci", DataType::Utf8, true),
        Field::new("score", DataType::Int32, true),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("source_key", DataType::Binary, true),
        Field::new("source_value", DataType::Binary, true),
        Field::new("fen", DataType::Utf8, true),
        Field::new(
            "moves",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(mf.clone().into()),
                true,
            ))),
            true,
        ),
        Field::new("cdb_ply", DataType::Int32, true),
        Field::new("cdb_mirror_ply", DataType::Int32, true),
    ]));
    let positions = [
        "4k3/8/8/3pP3/8/8/8/4K3 w - d6",
        "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -",
        "8/8/8/8/8/8/K7/7k b - -",
    ];
    let mut rows = positions
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let (_, k, m) = key::key(f).unwrap();
            (
                k,
                if m {
                    key::mirror(f).unwrap()
                } else {
                    f.to_string()
                },
                i,
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    let (mut kb, mut vb, mut fb, mut pb, mut mbp) = (
        BinaryBuilder::new(),
        BinaryBuilder::new(),
        StringBuilder::new(),
        Int32Builder::new(),
        Int32Builder::new(),
    );
    let mut mb = ListBuilder::new(StructBuilder::from_fields(mf, 3));
    for (k, f, id) in &rows {
        kb.append_value(k);
        vb.append_value(if *id == 2 { &[][..] } else { &[1, 2, 3, 4][..] });
        fb.append_value(f);
        if *id != 2 {
            for (u, s) in [("e2e4", 29999), ("g1f3", 0), ("b7a8n", -14999)] {
                let v = mb.values();
                v.field_builder::<StringBuilder>(0).unwrap().append_value(u);
                v.field_builder::<Int32Builder>(1).unwrap().append_value(s);
                v.append(true);
            }
        }
        mb.append(true);
        pb.append_value(if *id == 2 { -2 } else { 7 });
        mbp.append_value(if *id == 2 { -2 } else { -1 });
    }
    let a: Vec<ArrayRef> = vec![
        Arc::new(kb.finish()),
        Arc::new(vb.finish()),
        Arc::new(fb.finish()),
        Arc::new(mb.finish()),
        Arc::new(pb.finish()),
        Arc::new(mbp.finish()),
    ];
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .set_statistics_enabled(if statistics {
            EnabledStatistics::Chunk
        } else {
            EnabledStatistics::None
        })
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&file).unwrap(), schema.clone(), Some(props)).unwrap();
    writer
        .write(&RecordBatch::try_new(schema, a).unwrap())
        .unwrap();
    writer.close().unwrap();
    let data = fs::read(&file).unwrap();
    let facts = json!({"records":3,"first_key_hex":key::hex(&rows[0].0),"last_key_hex":key::hex(&rows[2].0),"source_logical_sha256":"fixture","stream_sha256":"fixture"});
    let mut audit = facts.clone();
    audit["bytes"] = data.len().into();
    audit["file_sha256"] = sha(&data).into();
    write_new(
        &root.join("receipt.json"),
        &json!({"identity":"fixture","file":"parquet/data.parquet","source":facts,"audit":audit}),
    )
    .unwrap();
    write_new(&root.join("ARCHIVE.COMPLETE.json"),&json!({"identity":"fixture","bounded":false,"shards":1,"records":3,"parquet_bytes":data.len(),"receipts":{"receipt.json":sha(&fs::read(root.join("receipt.json")).unwrap())}})).unwrap();
    let marker_sha = sha(&fs::read(root.join("ARCHIVE.COMPLETE.json")).unwrap());
    write_new(&root.join("audit.json"),&json!({"verified":true,"deep":true,"bounded":false,"original_source_required":false,"marker_sha256":marker_sha,"shards":1,"records":3,"bytes":data.len()})).unwrap();
    let idx = root.join("index.json");
    index(&root, &root.join("audit.json"), &idx).unwrap();
    Fixture {
        root,
        index: idx,
        file,
    }
}

#[test]
fn queries_mirrors_scores_and_missing_with_and_without_stats() {
    for stats in [true, false] {
        let f = fixture(stats);
        let mut db = Archive::open(&f.index).unwrap();
        let start = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -";
        let r = db
            .query(&[
                json!({"fen":start}),
                json!({"fen":key::mirror(start).unwrap()}),
                json!({"fen":"8/8/8/8/8/8/K7/7k b - -"}),
                json!({"fen":"8/8/8/8/8/8/K7/6k1 b - -"}),
            ])
            .unwrap();
        assert_eq!(
            r["results"][0]["moves"][0],
            json!({"uci":"e2e4","score":29999})
        );
        assert_eq!(
            r["results"][1]["moves"][0],
            json!({"uci":"e7e5","score":29999})
        );
        assert_eq!(r["results"][1]["moves"][2]["uci"], "b2a1n");
        assert_eq!(r["results"][0]["ply"], 7);
        assert_eq!(r["results"][1]["ply"], -1);
        assert_eq!(r["results"][2]["record_present"], true);
        assert_eq!(r["results"][2]["found"], false);
        assert_eq!(r["results"][3]["record_present"], false);
        assert_eq!(r["results"][3]["ply"], -2);
        assert_eq!(db.query(&[]).unwrap()["results"], json!([]));
        assert!(db.query(&[json!({"fen":"not a FEN"})]).is_err());
    }
}
#[test]
fn changed_missing_shards_and_stale_generations_are_errors() {
    let f = fixture(true);
    let mut db = Archive::open(&f.index).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&f.file)
        .unwrap()
        .write_all(b"bad")
        .unwrap();
    assert!(Archive::open(&f.index).is_err());
    assert!(db
        .query(&[json!({"fen":"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"})])
        .is_err());
    fs::remove_file(&f.file).unwrap();
    assert!(Archive::open(&f.index).is_err());
    let f = fixture(true);
    fs::write(f.root.join("ARCHIVE.COMPLETE.json"), b"{}").unwrap();
    assert!(Archive::open(&f.index).is_err());
}
#[test]
fn altered_index_and_receipts_cannot_publish() {
    let f = fixture(true);
    let mut index_value = load(&f.index).unwrap();
    index_value["records"] = 100.into();
    fs::write(&f.index, serde_json::to_vec(&index_value).unwrap()).unwrap();
    assert!(Archive::open(&f.index).is_err());
    fs::write(f.root.join("receipt.json"), b"{}").unwrap();
    assert!(index(
        &f.root,
        &f.root.join("audit.json"),
        &f.root.join("bad-index.json")
    )
    .is_err());
}
