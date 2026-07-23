"""
Stage A CLI: build per-card annotation fact sheets + chunk assignments.

Reads the training pack, repertoire parquets, position stats, unified eval DB,
plan-prior exports, the Stage-B engine cache (if present), and trainer.db
deviations. Writes trainer_data/annotations/facts_{color}.parquet:

    color | position_hash | canonical_node_id | chunk_id | fact_hash |
    facts_json | has_engine | built_at

Idempotent: rerunning without input changes produces identical fact_hashes
(floats rounded at construction; no tree-node ids inside the hashed JSON).

Usage:
    set PYTHONPATH=<repo>\\python
    .venv\\Scripts\\python.exe -m annotate.build_annotation_facts [--data DIR]
        [--color white|black] [--engine-cache PATH] [--no-deviations]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from annotate.chunks import build_chunks
from annotate.facts import EvalDB, FactsBuilder, fact_hash
from trainer_app.config import DEFAULTS, resolve_data_dir
from trainer_app.pack import TrainingPack

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
    ap.add_argument("--eval-db", default="E:/chess/unified_eval_db.parquet")
    ap.add_argument("--stats", default=DEFAULTS["stats"])
    ap.add_argument("--engine-cache", default=DEFAULT_ENGINE_CACHE)
    ap.add_argument("--no-deviations", action="store_true")
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data)
    out_dir = data_dir / "annotations"
    out_dir.mkdir(exist_ok=True)
    pack = TrainingPack(data_dir / "pack")

    t0 = time.time()
    print(f"Loading eval DB: {args.eval_db}", flush=True)
    evaldb = EvalDB(args.eval_db)
    print(f"  {len(evaldb.ph):,} evals ({time.time()-t0:.0f}s)", flush=True)

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

        fb = FactsBuilder(
            pack, color, evaldb=evaldb,
            rep_path=DEFAULTS[f"{color}_rep"], stats_path=args.stats,
            prior_path=PRIORS[color], engine_cache_path=args.engine_cache,
            trainer_db_path=None if args.no_deviations else str(data_dir / "trainer.db"))
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
