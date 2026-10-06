//! `dump-plies`: per-ply rows for a differential test against python-chess.
//!
//! Every selected game is walked -- dropped ones too, with the reason recorded,
//! so the filters are compared as well -- and two files are written:
//!   <out>.plies.parquet  file, row, ply, token, parent_hash, child_hash, epd
//!                        (the EPD of the position BEFORE the move, always)
//!   <out>.games.parquet  file, row, drop, n_tokens (capped at max_ply + 1),
//!                        outcome (done / failed / null_in_check), failed_ply,
//!                        failed_token, term_hash, term_kind, reason
//! Rows: the first --games-per-file of each file, plus with --special every row
//! whose movetext contains `{`, `(`, `$` or a non-ASCII character.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow::array::{ArrayRef, Int32Builder, Int64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use rayon::prelude::*;
use shakmaty::Chess;

use crate::chesspos::pack;
use crate::game::{term_reason, walk, Drop, Filters, NullInCheck, WalkEnd, Walker};
use crate::keys::write_parquet;
use crate::pyre::Tokens;
use crate::source::{FileMeta, SourceFile};

pub struct Config {
    pub out: PathBuf,
    pub games_per_file: u64,
    pub special: bool,
    pub max_ply: usize,
    pub filters: Filters,
}

#[derive(Default)]
struct Plies {
    file: StringBuilder,
    row: Int64Builder,
    ply: Int32Builder,
    token: StringBuilder,
    parent: Int64Builder,
    child: Int64Builder,
    epd: StringBuilder,
}

#[derive(Default)]
struct Games {
    file: StringBuilder,
    row: Int64Builder,
    drop: StringBuilder,
    n_tokens: Int32Builder,
    outcome: StringBuilder,
    failed_ply: Int32Builder,
    failed_token: StringBuilder,
    term_hash: Int64Builder,
    term_kind: Int32Builder,
    reason: Int32Builder,
}

struct DumpWalker<'a> {
    p: &'a mut Plies,
    file: &'a str,
    row: i64,
    term: Option<(i64, i32)>,
    buf: String,
}

impl Walker for DumpWalker<'_> {
    fn row(&mut self, ply: u32, pos: &Chess, ph: i64, san: &str, ch: i64) {
        self.buf.clear();
        pack(pos).render_into(&mut self.buf);
        self.p.file.append_value(self.file);
        self.p.row.append_value(self.row);
        self.p.ply.append_value(ply as i32);
        self.p.token.append_value(san);
        self.p.parent.append_value(ph);
        self.p.child.append_value(ch);
        self.p.epd.append_value(&self.buf);
    }

    fn term(&mut self, hash: i64, kind: i32, _end_ply: u32) {
        self.term = Some((hash, kind));
    }
}

fn is_special(mt: &str) -> bool {
    mt.bytes().any(|b| b == b'{' || b == b'(' || b == b'$' || b >= 0x80)
}

fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields.iter().map(|(n, t)| Field::new(*n, t.clone(), true)).collect::<Vec<_>>(),
    ))
}

fn dump_file(cfg: &Config, root: &Path, f: &SourceFile) -> Result<(Plies, Games)> {
    let meta = FileMeta::open(&f.path)?;
    let rel = f.path.strip_prefix(root).unwrap_or(&f.path).to_string_lossy().replace('\\', "/");
    let (mut p, mut g) = (Plies::default(), Games::default());
    let mut toks = Tokens::default();
    let start = Chess::default();
    let want_all = cfg.special;
    let head = cfg.games_per_file.min(meta.rows);
    let end = if want_all { meta.rows } else { head };
    // Decode in 250k-row slices to bound memory on a full file.
    let mut s = 0u64;
    while s < end {
        let e = (s + 250_000).min(end);
        let mut row = s;
        for b in meta.read_rows(&f.path, s, e)? {
            let cols = meta.columns(&b)?;
            for i in 0..b.num_rows() {
                let gr = cols.row(i);
                let pick = row < head || (want_all && gr.movetext.is_some_and(is_special));
                if pick {
                    let drop = cfg.filters.drop_reason(&gr);
                    toks.fill(gr.movetext, cfg.max_ply + 1);
                    let mut w = DumpWalker { p: &mut p, file: &rel, row: row as i64, term: None, buf: String::new() };
                    let res = walk(&mut w, &toks, cfg.max_ply, &start);
                    let term = w.term;
                    g.file.append_value(&rel);
                    g.row.append_value(row as i64);
                    g.drop.append_option(drop.map(|d| match d {
                        Drop::Elo => "elo",
                        Drop::NoScore => "no_score",
                        Drop::Termination => "termination",
                        Drop::Bot => "bot",
                    }));
                    g.n_tokens.append_value(toks.len() as i32);
                    match res {
                        Ok(WalkEnd::Done) => {
                            g.outcome.append_value("done");
                            g.failed_ply.append_null();
                            g.failed_token.append_null();
                        }
                        Ok(WalkEnd::Failed { ply }) | Err(NullInCheck { ply }) => {
                            g.outcome.append_value(if res.is_err() { "null_in_check" } else { "failed" });
                            g.failed_ply.append_value(ply as i32);
                            g.failed_token.append_value(toks.get(ply as usize - 1));
                        }
                    }
                    g.term_hash.append_option(term.map(|t| t.0));
                    g.term_kind.append_option(term.map(|t| t.1));
                    g.reason.append_value(term_reason(gr.termination));
                }
                row += 1;
            }
        }
        s = e;
    }
    Ok((p, g))
}

pub fn run(cfg: &Config, root: &Path, files: &[SourceFile]) -> Result<(u64, u64)> {
    let parts: Mutex<Vec<(usize, Plies, Games)>> = Mutex::new(Vec::new());
    files.par_iter().enumerate().try_for_each(|(i, f)| -> Result<()> {
        let (p, g) = dump_file(cfg, root, f)?;
        parts.lock().unwrap().push((i, p, g));
        Ok(())
    })?;
    let mut parts = parts.into_inner().unwrap();
    parts.sort_by_key(|x| x.0);
    let ps = schema(&[
        ("file", DataType::Utf8), ("row", DataType::Int64), ("ply", DataType::Int32),
        ("token", DataType::Utf8), ("parent_hash", DataType::Int64),
        ("child_hash", DataType::Int64), ("epd", DataType::Utf8),
    ]);
    let gs = schema(&[
        ("file", DataType::Utf8), ("row", DataType::Int64), ("drop", DataType::Utf8),
        ("n_tokens", DataType::Int32), ("outcome", DataType::Utf8),
        ("failed_ply", DataType::Int32), ("failed_token", DataType::Utf8),
        ("term_hash", DataType::Int64), ("term_kind", DataType::Int32), ("reason", DataType::Int32),
    ]);
    let (mut pb, mut gb) = (Vec::new(), Vec::new());
    let (mut np, mut ng) = (0u64, 0u64);
    for (_, mut p, mut g) in parts {
        let pc: Vec<ArrayRef> = vec![
            Arc::new(p.file.finish()), Arc::new(p.row.finish()), Arc::new(p.ply.finish()),
            Arc::new(p.token.finish()), Arc::new(p.parent.finish()), Arc::new(p.child.finish()),
            Arc::new(p.epd.finish()),
        ];
        let gc: Vec<ArrayRef> = vec![
            Arc::new(g.file.finish()), Arc::new(g.row.finish()), Arc::new(g.drop.finish()),
            Arc::new(g.n_tokens.finish()), Arc::new(g.outcome.finish()),
            Arc::new(g.failed_ply.finish()), Arc::new(g.failed_token.finish()),
            Arc::new(g.term_hash.finish()), Arc::new(g.term_kind.finish()),
            Arc::new(g.reason.finish()),
        ];
        let pbatch = RecordBatch::try_new(ps.clone(), pc)?;
        let gbatch = RecordBatch::try_new(gs.clone(), gc)?;
        np += pbatch.num_rows() as u64;
        ng += gbatch.num_rows() as u64;
        pb.push(pbatch);
        gb.push(gbatch);
    }
    let base = cfg.out.to_string_lossy().trim_end_matches(".parquet").to_string();
    write_parquet(Path::new(&format!("{base}.plies.parquet")), &ps, &pb)?;
    write_parquet(Path::new(&format!("{base}.games.parquet")), &gs, &gb)?;
    Ok((ng, np))
}
