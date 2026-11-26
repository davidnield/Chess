#[path = "../lookup/mod.rs"]
mod lookup;
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::Path,
    time::Instant,
};

fn requests(path: &str) -> Result<Vec<Value>> {
    BufReader::new(File::open(path)?)
        .lines()
        .filter_map(|l| match l {
            Ok(s) if s.trim().is_empty() => None,
            x => Some(x),
        })
        .map(|l| Ok(serde_json::from_str(&l?)?))
        .collect()
}
fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    ensure!(a.len()>=3,
        "index ARCHIVE AUDIT OUTPUT | query INDEX INPUT.jsonl OUTPUT.json | serve INDEX | encode INPUT.jsonl OUTPUT.json | lookup-tsv INDEX INPUT OUTPUT");
    match a[1].as_str() {
        "index" => {
            ensure!(a.len() == 5, "index arguments");
            lookup::index(Path::new(&a[2]), Path::new(&a[3]), Path::new(&a[4]))
        }
        "encode" => {
            ensure!(a.len() == 4, "encode arguments");
            let mut out = Vec::new();
            for r in requests(&a[2])? {
                let (fen, key, mirrored) = lookup::key::key(
                    r["fen"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing FEN"))?,
                )?;
                out.push(json!({"id":r["id"],"fen":fen,"source_key":lookup::key::hex(&key),"mirrored":mirrored}));
            }
            lookup::write_new(Path::new(&a[3]), &json!(out))
        }
        "query" => {
            ensure!(a.len() == 5, "query arguments");
            let t = Instant::now();
            let mut db = lookup::Archive::open(Path::new(&a[2]))?;
            let mut result = db.query(&requests(&a[3])?)?;
            result["total_seconds"] = t.elapsed().as_secs_f64().into();
            lookup::write_new(Path::new(&a[4]), &result)
        }
        "serve" => {
            ensure!(a.len() == 3, "serve arguments");
            let mut db = lookup::Archive::open(Path::new(&a[2]))?;
            for line in std::io::stdin().lock().lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let r: Vec<Value> = serde_json::from_str(&line)?;
                println!("{}", db.query(&r)?);
                std::io::stdout().flush()?;
            }
            Ok(())
        }
        "lookup-tsv" => {
            ensure!(a.len() == 5, "lookup-tsv arguments");
            let mut rs = Vec::new();
            let mut ids = Vec::new();
            for line in BufReader::new(File::open(&a[3])?).lines() {
                let l = line?;
                let f: Vec<_> = l.splitn(3, '\t').collect();
                ensure!(f.len() == 3, "expected lo, hi, FEN");
                f[0].parse::<i64>()?;
                f[1].parse::<i64>()?;
                ids.push(format!("{}\t{}", f[0], f[1]));
                rs.push(json!({"fen":f[2]}));
            }
            let mut db = lookup::Archive::open(Path::new(&a[2]))?;
            let answers = db.query(&rs)?;
            let dest = Path::new(&a[4]);
            ensure!(!dest.exists(), "output exists");
            let tmp = dest.with_extension("query.tmp");
            let mut out = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)?;
            for (id, r) in ids.iter().zip(answers["results"].as_array().unwrap()) {
                let moves = r["moves"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|m| format!("{}:{}", m["uci"].as_str().unwrap(), m["score"]))
                    .collect::<Vec<_>>()
                    .join(";");
                writeln!(out, "{}\t{}\t{}", id, r["ply"], moves)?;
            }
            out.sync_all()?;
            drop(out);
            std::fs::rename(tmp, dest)?;
            eprintln!("{}", answers["metrics"]);
            Ok(())
        }
        _ => anyhow::bail!("unknown command"),
    }
}
