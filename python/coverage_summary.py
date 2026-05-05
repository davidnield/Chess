"""
Aggregate analyze_coverage.py outputs across all 6 variants into a single
markdown summary table.

Reads the per-variant text reports written by run_2016_coverage.sh (which
end with a CROSS-SLICE COVERAGE SUMMARY block) and produces a consolidated
table with one row per (event, elo_band) and columns for ply-8 / ply-16
coverage across each variant.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


VARIANTS = [
    ("white", "w000"), ("white", "w010"), ("white", "w020"),
    ("black", "w000"), ("black", "w010"), ("black", "w020"),
]


# Match summary table lines:
#   "  Blitz           1100      1,640,403    99.6%    74.3%    26.2%     7.5%    ply 20"
ROW_RE = re.compile(
    r"^\s+(?P<event>\S+)\s+(?P<elo>\d+)\s+(?P<games>[\d,]+)\s+"
    r"(?P<p4>[\d.]+)%\s+(?P<p8>[\d.]+)%\s+(?P<p12>[\d.]+)%\s+(?P<p16>[\d.]+)%"
)


def parse_summary(text: str) -> dict[tuple[str, int], dict]:
    """Pull each (event, elo_band) row out of the cross-slice summary block."""
    out: dict[tuple[str, int], dict] = {}
    in_summary = False
    for ln in text.splitlines():
        if "CROSS-SLICE COVERAGE SUMMARY" in ln:
            in_summary = True
            continue
        if not in_summary:
            continue
        m = ROW_RE.match(ln)
        if not m:
            continue
        ev = m.group("event")
        eb = int(m.group("elo"))
        out[(ev, eb)] = {
            "games": int(m.group("games").replace(",", "")),
            "p4":    float(m.group("p4")),
            "p8":    float(m.group("p8")),
            "p12":   float(m.group("p12")),
            "p16":   float(m.group("p16")),
        }
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--logs", default="logs/2016_pipeline",
                    help="Directory with coverage_<perspective>_<wsfx>.txt files")
    ap.add_argument("--year", type=int, default=2016)
    ap.add_argument("--output", default="logs/2016_pipeline/coverage_summary.md")
    args = ap.parse_args()

    log_dir = Path(args.logs)
    out_path = Path(args.output)

    per_variant: dict[tuple[str, str], dict[tuple[str, int], dict]] = {}
    for persp, wsfx in VARIANTS:
        p = log_dir / f"coverage_{persp}_{wsfx}.txt"
        if not p.exists():
            print(f"  WARN: missing {p}")
            continue
        per_variant[(persp, wsfx)] = parse_summary(p.read_text(encoding="utf-8"))

    if not per_variant:
        print("No coverage reports found. Run run_2016_coverage.sh first.")
        return

    # Union of all slices across variants (should be the same set, but be safe)
    all_slices = sorted({k for v in per_variant.values() for k in v.keys()})

    out: list[str] = []
    out.append(f"# Coverage summary for year={args.year}\n")
    out.append("Coverage = % of opponent games still in our prepared tree at "
               "ply N. Computed by walking the recommended tree from the "
               "start position and weighting branch flow by the empirical "
               "distribution of opponent replies. See `analyze_coverage.py`.\n")

    for ply, label in [(8, "Ply-8"), (16, "Ply-16")]:
        out.append(f"\n## {label} coverage (% of games still in tree)\n")
        # Header
        header_cells = ["event", "elo", "games"]
        for persp, wsfx in VARIANTS:
            header_cells.append(f"{persp[0]} {wsfx}")
        out.append("| " + " | ".join(header_cells) + " |")
        out.append("|" + "|".join(["---"] * len(header_cells)) + "|")
        for ev, eb in all_slices:
            # Use any variant for slice_total games (should match across variants)
            games = next(
                (v[(ev, eb)]["games"] for v in per_variant.values() if (ev, eb) in v),
                0,
            )
            row = [ev, str(eb), f"{games:,}"]
            for persp, wsfx in VARIANTS:
                slice_data = per_variant.get((persp, wsfx), {}).get((ev, eb))
                if slice_data is None:
                    row.append("—")
                else:
                    val = slice_data["p8"] if ply == 8 else slice_data["p16"]
                    row.append(f"{val:.1f}%")
            out.append("| " + " | ".join(row) + " |")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"Summary written to {out_path}")
    print()
    print("\n".join(out))


if __name__ == "__main__":
    main()
