"""
Stage A CLI: build per-card annotation fact sheets + chunk assignments.

Reads the training pack (its pack_meta.json names each colour's book and stats
pool), the full lifetime books (trainer settings white_rep/black_rep) for the plan
lookahead (annotate.plan), the course comments that follow our line
(annotate.course_notes), the eval arrays, plan-prior exports (if present), the
Stage-B engine cache (if present), and trainer.db deviations. Writes
trainer_data/annotations/facts_{color}.parquet:

    color | position_hash | canonical_node_id | chunk_id | fact_hash |
    facts_json | has_engine | built_at

Idempotent: rerunning without input changes produces identical fact_hashes
(floats rounded at construction; no tree-node ids inside the hashed JSON).

Usage:
    set PYTHONPATH=<repo>\\python
    .venv\\Scripts\\python.exe -m annotate.build_annotation_facts [--data DIR]
        [--color white|black] [--engine-cache PATH] [--no-deviations]
        [--white-book PATH] [--black-book PATH] [--course-min-agree 4] [--no-courses]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import chess
import polars as pl

from annotate.chunks import build_chunks
from annotate.course_notes import CourseNotes
from annotate.facts import EvalDB, FactsBuilder, fact_hash
from annotate.plan import PlanTree
from eval_arrays import DEFAULT_ARRAY_DIR
from trainer_app import db as trainer_db
from trainer_app.config import DEFAULTS, get_settings, resolve_data_dir
from trainer_app.pack import TrainingPack
from zobrist import zobrist_int64

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PRIORS = {
    "white": "E:/chess/repertoire/_plan/canon_white_prior.parquet",
    "black": "E:/chess/repertoire/_plan/canon_black_prior.parquet",
}
DEFAULT_ENGINE_CACHE = "E:/chess/engine-cache/annotation_evals.parquet"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=None)
    ap.add_argument("--color", choices=["white", "black"], default=None,
                    help="Default: both")
    ap.add_argument("--eval-db", default=str(DEFAULT_ARRAY_DIR),
                    help="An eval-arrays directory (eval_arrays.py) or a legacy "
                         "(position_hash, eval_cp) parquet (default: %(default)s).")
    ap.add_argument("--stats", default=DEFAULTS["stats"])
    ap.add_argument("--engine-cache", default=DEFAULT_ENGINE_CACHE)
    ap.add_argument("--no-deviations", action="store_true")
    ap.add_argument("--white-book", default=None,
                    help="Full book for the plan lookahead (default: the trainer's white_rep setting)")
    ap.add_argument("--black-book", default=None, help="likewise for Black")
    ap.add_argument("--course-min-agree", type=int, default=4,
                    help="plies a course must keep following our line for its comment to count")
    ap.add_argument("--no-courses", action="store_true")
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data)
    out_dir = data_dir / "annotations"
    out_dir.mkdir(exist_ok=True)
    pack = TrainingPack(data_dir / "pack")
    con = trainer_db.connect(data_dir)
    settings = get_settings(con)
    con.close()

    t0 = time.time()
    print(f"Loading eval DB: {args.eval_db}", flush=True)
    evaldb = EvalDB(args.eval_db)
    print(f"  {len(evaldb.ph):,} evals, {evaldb.status} ({time.time()-t0:.0f}s)", flush=True)

    colors = [args.color] if args.color else ["white", "black"]
    for color in colors:
        print(f"\n{color}:", flush=True)
        chunks = build_chunks(pack, color)
        chunk_of = {}
        for ch in chunks:
            for h in ch.card_hashes:
                chunk_of[h] = ch.chunk_id
        sizes = sorted(len(c.card_hashes) for c in chunks)
        print(f"  chunks: {len(chunks)} (sizes min {sizes[0]} / median "
              f"{sizes[len(sizes)//2]} / max {sizes[-1]})", flush=True)

        # the pack records which book and stats pool each colour was built from
        rep_path = pack.meta.get("colors", {}).get(color, {}).get("rep") or DEFAULTS[f"{color}_rep"]
        stats_path = pack.meta.get(f"{color}_stats") or args.stats
        book_path = getattr(args, f"{color}_book") or settings[f"{color}_rep"]
        print(f"  book {rep_path}\n  stats {stats_path}\n  plan lookahead {book_path}",
              flush=True)
        plan_tree = PlanTree(color, book_path, stats_path)
        notes = None
        if not args.no_courses:
            targets = {}
            for card in pack.cards[color].iter_rows(named=True):
                pl_ = plan_tree.plan(card["position_hash"])
                b = chess.Board(card["epd"] + " 0 1")
                b.push_san(card["best_move"])
                targets[zobrist_int64(b)] = (card["best_move"],
                                             (pl_ or {}).get("continuation", [])[1:])
            notes = CourseNotes(targets, min_agree=args.course_min_agree)
            print(f"  course notes on {len(notes.notes)} of {len(targets)} cards", flush=True)
        fb = FactsBuilder(
            pack, color, evaldb=evaldb,
            rep_path=rep_path, stats_path=stats_path,
            prior_path=PRIORS[color], engine_cache_path=args.engine_cache,
            trainer_db_path=None if args.no_deviations else str(data_dir / "trainer.db"),
            plan_tree=plan_tree, course_notes=notes)
        print(f"  inputs loaded ({time.time()-t0:.0f}s)", flush=True)

        rows = []
        n_engine = 0
        import json as _json
        for card in pack.cards[color].iter_rows(named=True):
            facts, has_engine = fb.build(card)
            n_engine += has_engine
            rows.append({
                "color": color, "position_hash": card["position_hash"],
                "canonical_node_id": card["canonical_node_id"],
                "chunk_id": chunk_of.get(card["position_hash"], ""),
                "fact_hash": fact_hash(facts),
                "facts_json": _json.dumps(facts, separators=(",", ":")),
                "has_engine": bool(has_engine),
                "built_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        df = pl.DataFrame(rows)

        out = out_dir / f"facts_{color}.parquet"
        if out.exists():                       # idempotency report
            old = pl.read_parquet(out, columns=["position_hash", "fact_hash"])
            j = df.select(["position_hash", "fact_hash"]).join(
                old, on="position_hash", how="inner", suffix="_old")
            same = (j["fact_hash"] == j["fact_hash_old"]).sum()
            print(f"  vs previous: {same}/{j.height} fact_hashes unchanged", flush=True)
        tmp = out.with_suffix(".parquet.tmp")
        df.write_parquet(tmp)
        tmp.replace(out)
        print(f"  {df.height} fact sheets ({n_engine} with engine data) -> {out.name} "
              f"({time.time()-t0:.0f}s)", flush=True)

    print(f"\nDone in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
