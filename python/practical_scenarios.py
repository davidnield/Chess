"""Compare same-color books using matched simulation seeds in explicit player populations.

plan prints the table extractions and referee commands. prepare builds missing development
tables. score verifies each table's population before running the referee and reporting
paired gains and learning cost. --safety LABEL=validator.json adds a separate audit column.
"""
import argparse
import hashlib
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys

import polars as pl
import convertibility_eval as cv
from exit_difficulty import read_exits

HERE = Path(__file__).resolve().parent
MONTHS = "2026_8,2026_9"
BRC = "Blitz,Rapid,Classical"


@dataclass(frozen=True)
class Scenario:
    ours: tuple
    theirs: tuple
    events: str = BRC
    recall: float = 1.


WEAK = ("1000:1800", 1450)
STRONG = ("1800:4000", 2000)
SCENARIOS = {
    "baseline": Scenario(WEAK, STRONG),
    "recall85": Scenario(WEAK, STRONG, recall=.85),
    "future": Scenario(("1800:2000", 1900), STRONG),
    "strong-opponent": Scenario(WEAK, ("2200:4000", 2300)),
    "blitz": Scenario(WEAK, STRONG, "Blitz"),
    "rapid": Scenario(WEAK, STRONG, "Rapid"),
    "classical": Scenario(WEAK, STRONG, "Classical"),
}


def cohort(root, band, events, colour):
    lo, hi = band.split(":")
    tag = lo if band in (WEAK[0], STRONG[0], "2200:4000") else f"{lo}to{hi}"
    speeds = "" if events == BRC else "_" + events.lower()
    path = root / f"movers{tag}_{colour}_f64{speeds}_2026_08_09"
    args = ["--our-colour", colour, "--our-elo", band, "--opp-min-elo", "0",
            "--months", MONTHS, "--events", events, "--sample", ".35", "--folds", "64",
            "--max-ply", "30", "--out", str(path)]
    return path, [sys.executable, str(HERE/"side_explorer.py"), *args]


def ready(path, band, colour, events, horizon):
    pr = cv.table_params(str(path)) or {}
    expected = {"our_colour": colour, "our_elo": band, "events": events,
                "months": MONTHS, "sample": .35, "folds": 64, "opp_min_elo": 0}
    return (all(pr.get(k) == v for k, v in expected.items()) and pr.get("max_ply", 0) >= horizon
            and all((path/f"_month={m}.DONE").is_file() for m in MONTHS.split(",")))


def decisions(path, colour):
    table = pl.read_parquet(path, columns=["position_hash", "side_to_move", "best_move"])
    return {h: san.rstrip("+#") for h, side, san in table.iter_rows() if side == colour and san}


def paired_summary(rows, base, books, safety):
    groups = cv.paired_differences(rows, base).to_dicts()
    out = []
    for group in groups:
        label = group["label"]
        moves = books[label]
        same = sum(books[base].get(h) == san for h, san in moves.items())
        audit = safety.get(label)
        fresh = audit.get("fresh") if audit else None
        out.append({**group, "mean_score": rows.filter(pl.col("label") == label)["uniform"].mean(),
                    "moves_to_learn": len(moves), "new_or_changed": len(moves) - same,
                    "incumbent_decisions_lost": len(books[base]) - same,
                    "audit_flagged": audit["flagged"] if audit else None,
                    "audit_outside_set": audit["status"].get("outside_set", 0) if audit else None,
                    "audit_fallback": audit["status"].get("fallback", 0) if audit else None,
                    "audit_fresh_confirmed": fresh.get("confirmed", 0) if fresh is not None else None,
                    "audited_worst_cp": safety.get(label, {}).get("best_defence", {}).get("worst_cp_us")})
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("action", choices=["plan", "prepare", "score"])
    ap.add_argument("--book", action="append", required=True, metavar="LABEL=FILE")
    ap.add_argument("--base", required=True)
    ap.add_argument("--colour", required=True, choices=["white", "black"])
    ap.add_argument("--scenario", action="append", choices=SCENARIOS)
    ap.add_argument("--tables", type=Path, default=Path(cv.HX))
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--seeds", default="1,2,3,4")
    ap.add_argument("--rollouts", type=int, default=2)
    ap.add_argument("--horizon", type=int, default=30)
    ap.add_argument("--tau", type=float, default=1e-4)
    ap.add_argument("--safety", action="append", default=[], metavar="LABEL=VALIDATOR_JSON")
    ap.add_argument("--difficulty-exits", type=int, default=0, metavar="N",
                    help="also measure the risk of a 0.10 utility loss in the next three "
                         "unaided moves at the N most frequent exits per book and scenario")
    ap.add_argument("--difficulty-games", type=int, default=32)
    a = ap.parse_args()
    specs = dict(x.split("=", 1) for x in a.book)
    if len(specs) != len(a.book) or a.base not in specs or not 1 <= a.horizon <= 30:
        ap.error("unique labels, a named baseline, and a horizon from 1 through 30 required")
    if a.rollouts < 1 or not 0 < a.tau <= 1:
        ap.error("positive rollouts and tau in (0, 1] required")
    if a.difficulty_exits < 0 or a.difficulty_games < 2:
        ap.error("difficulty-exits must be nonnegative and difficulty-games at least two")
    books = {k:decisions(v, a.colour) for k,v in specs.items()}
    if any(not moves for moves in books.values()):
        ap.error("empty book or wrong colour")
    safety = {}
    for arg in a.safety:
        label, file = arg.split("=", 1)
        audit = json.loads(Path(file).read_text(encoding="utf-8"))
        if (label not in specs or audit["colour"] != a.colour or
                audit.get("book_sha256") != hashlib.sha256(Path(specs[label]).read_bytes()).hexdigest()):
            ap.error("rerun validate_book on the exact file and colour to attach its audit")
        safety[label] = audit
    summary = []
    for name in dict.fromkeys(a.scenario or SCENARIOS):
        s = SCENARIOS[name]
        paths = []
        for player, colour in [(s.ours, a.colour), (s.theirs, "black" if a.colour=="white" else "white")]:
            path, command = cohort(a.tables, player[0], s.events, colour)
            if not ready(path, player[0], colour, s.events, a.horizon):
                print(f"Table required: {subprocess.list2cmdline(command)}", flush=True)
                if a.action == "prepare":
                    subprocess.run(command, check=True)
                if a.action != "plan" and not ready(path, player[0], colour, s.events, a.horizon):
                    raise SystemExit(f"Missing or incompatible table {path}; run prepare first")
            paths.append(path)
        dest = a.out/name
        cmd = [sys.executable, str(HERE/"convertibility_eval.py"), "--compare-to", a.base,
               "--our-moves", str(paths[0]), "--opp-moves", str(paths[1]),
               "--our-elo", str(s.ours[1]), "--opp-elo", str(s.theirs[1]), "--recall", str(s.recall),
               "--seed", a.seeds, "--max-ply", str(a.horizon), "--tau", str(a.tau),
               "--rollouts", str(a.rollouts), "--sf-procs", "1", "--sf-cache", str(dest/"sf.parquet"),
               "--out", str(dest/"scores.parquet"), "--exits-out", str(dest/"exits")]
        for label, file in specs.items():
            cmd.extend(["--book", f"{label}={a.colour}={file}"])
        if a.action == "plan":
            print(subprocess.list2cmdline(cmd))
        if a.action == "score":
            dest.mkdir(parents=True, exist_ok=True)
            subprocess.run(cmd, check=True)
            frame = pl.read_parquet(dest/"scores.parquet")
            for row in paired_summary(frame, a.base, books, safety):
                if a.difficulty_exits:
                    index = list(specs).index(row["label"])
                    partitions = [str(dest/"exits"/f"b{index:03d}_s{int(seed)}.parquet")
                                  for seed in a.seeds.split(",")]
                    available = read_exits(partitions)
                    row["exit_mass"] = available["mass"].sum()
                    row["risk_covered_mass"] = 0.
                    row["conditional_loss_risk"] = None
                    if available.filter(pl.col("ply") < a.horizon).height:
                        risk_path = dest/f"risk-b{index:03d}.parquet"
                        subprocess.run([sys.executable, str(HERE/"exit_difficulty.py"),
                                        "--exits", *partitions, "--colour", a.colour,
                                        "--our-moves", str(paths[0]), "--opp-moves", str(paths[1]),
                                        "--our-elo", str(s.ours[1]), "--opp-elo", str(s.theirs[1]),
                                        "--max-ply", str(a.horizon), "--games", str(a.difficulty_games),
                                        "--top", str(a.difficulty_exits), "--sf-cache", str(dest/"sf.parquet"),
                                        "--out", str(risk_path)], check=True)
                        risk = pl.read_parquet(risk_path)
                        mass = risk["mass"].sum()
                        row["risk_covered_mass"] = mass
                        row["conditional_loss_risk"] = risk["risk"].dot(risk["mass"]) / mass
                summary.append({"scenario": name, "events": s.events, "recall": s.recall, **row})
            # Keep completed scenarios available even if a later cohort is unavailable.
            tmp = a.out/"comparison.csv.tmp"
            pl.DataFrame(summary).write_csv(tmp)
            tmp.replace(a.out/"comparison.csv")


if __name__ == "__main__":
    main()
