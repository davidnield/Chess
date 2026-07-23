"""
Export annotations as a markdown "course" — one chapter per chunk (opening
variation), positions in line order with the annotation and a memory-rule
callout. Doubles as the human review surface for generated annotations.

Usage:
    set PYTHONPATH=<repo>\\python
    .venv\\Scripts\\python.exe -m annotate.export_course [--data DIR]
        [--color white|black] [--out PATH] [--include-flagged]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from annotate.chunks import build_chunks
from trainer_app.config import resolve_data_dir
from trainer_app.pack import TrainingPack

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def _num_line(sans: list[str]) -> str:
    return " ".join((f"{i//2+1}.{s}" if i % 2 == 0 else s)
                    for i, s in enumerate(sans))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=None)
    ap.add_argument("--color", choices=["white", "black"], default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--include-flagged", action="store_true")
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data)
    ann_dir = data_dir / "annotations"
    store = pl.read_parquet(ann_dir / "annotations.parquet")
    ann = {(r["color"], r["position_hash"]): r for r in store.iter_rows(named=True)}
    pack = TrainingPack(data_dir / "pack")

    colors = [args.color] if args.color else ["white", "black"]
    out_path = Path(args.out) if args.out else ann_dir / (
        f"course_{args.color}.md" if args.color else "course.md")

    lines: list[str] = ["# Repertoire Course\n",
                        "_Auto-generated study notes. Positions are grouped by "
                        "variation; each shows our move, the idea, and a memory "
                        "rule where one applies._\n"]
    facts_by = {}
    for color in colors:
        fp = ann_dir / f"facts_{color}.parquet"
        if fp.exists():
            facts_by[color] = {r["position_hash"]: json.loads(r["facts_json"])
                               for r in pl.read_parquet(fp).iter_rows(named=True)}

    for color in colors:
        lines.append(f"\n# {color.capitalize()} repertoire\n")
        chunks = build_chunks(pack, color)
        facts = facts_by.get(color, {})
        # order chapters by the reach of their first card (mainlines first)
        def chunk_reach(ch):
            f = facts.get(ch.card_hashes[0], {})
            return f.get("reach_pct", 0.0)
        for ch in sorted(chunks, key=lambda c: -chunk_reach(c)):
            chap = _num_line(ch.chapter_sans) or "Starting position"
            lines.append(f"\n## {chap}\n")
            for h in ch.card_hashes:
                a = ann.get((color, h))
                f = facts.get(h)
                if f is None:
                    continue
                if a is None or not a["annotation"]:
                    continue
                if a["flagged"] and not args.include_flagged:
                    continue
                lines.append(f"**{f['line']}  →  {f['our_move']['san']}**"
                             + ("  ⚠️" if a["flagged"] else "") + "  ")
                lines.append(a["annotation"])
                if a["memory_rule"]:
                    lines.append(f"\n> 🔑 {a['memory_rule']}")
                lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")
    n = sum(1 for k, a in ann.items() if a["annotation"]
            and (args.include_flagged or not a["flagged"]))
    print(f"Wrote {n} annotations -> {out_path}")


if __name__ == "__main__":
    main()
