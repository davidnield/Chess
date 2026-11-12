"""Prepare a reusable, exact own-move lookup without exporting the whole uncut pool.

Every reachable decision is the initial White board or a child of a named
opponent reply. This superset is independent of gates, budgets and earlier books.
Keep native opponent edges. Restore all native observations at our positions,
including positions without a single move above the original cutoff.
"""

import argparse
import hashlib
import json
import time
from pathlib import Path

import chess
import duckdb
import polars as pl
from pool_from_book import SUMS, slice_files
from zobrist import zobrist_int64


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf8"))


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def stamp(path):
    path = Path(path).resolve()
    st = path.stat()
    return [str(path), st.st_size, st.st_mtime_ns]


def literal(x):
    return "'" + str(x).replace("'", "''") + "'"


def parquet(paths):
    if isinstance(paths, (str, Path)):
        paths = [paths]
    return (
        "read_parquet(["
        + ",".join(literal(p) for p in paths)
        + "],hive_partitioning=false)"
    )


def atomic_json(path, value):
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(value, indent=2), encoding="utf8")
    tmp.replace(path)


def copy(con, sql, path):
    temp = path.with_suffix(".parquet.tmp")
    con.execute(f"COPY ({sql}) TO {literal(temp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    temp.replace(path)


def contract(stats, aux, colour, overlay):
    return {
        "pool": stamp(stats),
        "pool_meta": digest(str(stats) + ".meta.json"),
        "aux": stamp(aux),
        "aux_meta": digest(str(aux) + ".meta.json"),
        "colour": colour,
        "overlay": digest(overlay) if overlay else None,
    }


def load(folder, stats, aux, colour, overlay):
    folder = Path(folder)
    done = read_json(folder / "ready.json")
    if done["recipe_sha256"] != digest(folder / "recipe.json"):
        raise ValueError("Uncut lookup manifest changed")
    if done["contract"] != contract(stats, aux, colour, overlay):
        raise ValueError(
            "Uncut lookup belongs to a different pool, sidecar, colour or opponent overlay"
        )
    for name, expected in done["files"].items():
        if stamp(folder / name)[1:] != expected:
            raise ValueError(f"Uncut lookup file changed: {name}")
    return done


GROUPS = ("term_normal", "term_flag", "term_other", "horizon")


def sums(prefix=""):
    return ", ".join(f"sum({prefix}{c})::BIGINT AS {c}" for c in SUMS)


def source_parts(stats):
    meta = read_json(str(stats) + ".meta.json")
    pop = meta["population"]
    if meta["producer"] == "pool_from_book":
        ar = Path(meta["book"])
        raw = read_json(ar / "_book.meta.json")
        cap = meta["max_ply"]
        if (
            not (ar / "_BOOK.DONE").is_file()
            or digest(ar / "_book.meta.json") != meta["book_meta_sha256"]
            or digest(ar / "_collisions.parquet") != meta["collisions_sha256"]
        ):
            raise ValueError("Archive does not match the pooled export")
        parts = [
            slice_files(ar, pop["events"], pop["elo_bands"], i)
            for i in range(raw["input"]["params"]["buckets"])
        ]
        index = pl.read_parquet(ar / "_slices.parquet").filter(
            pl.col("event").is_in(pop["events"]),
            pl.col("elo_band").is_in(pop["elo_bands"]),
        )
        actual = {}
        for files in parts:
            for file in files:
                path = Path(file)
                key = (
                    path.parent.parent.name.split("=")[1],
                    int(path.parent.name.split("=")[1]),
                )
                count, size = actual.get(key, (0, 0))
                actual[key] = (count + 1, size + path.stat().st_size)
        expected = {
            (e, b): (count, size)
            for e, b, count, size in index.select(
                "event", "elo_band", "files", "bytes"
            ).iter_rows()
        }
        if not expected or actual != expected:
            raise ValueError(
                "Archive slice files disagree with the published file counts or sizes"
            )
        return (
            "archive",
            [(files, []) for files in parts if files],
            cap,
            ar / "_collisions.parquet",
        )
    if meta["producer"] == "build_pooled_stats":
        base = Path(meta["monthly_dir"])
        months = pop["month_list"]
        if len(set(months)) != len(months) or len(months) != pop["n_months"]:
            raise ValueError("Invalid month manifest for the native pool")
        parts = []
        for month in months:
            year, num = month.split("_")
            path = base / f"year={year}_month={int(num)}"
            parts.append(([str(path) + ".ps.parquet"], [str(path) + ".term.parquet"]))
        return "monthly", parts, None, None
    raise ValueError(f"No uncut source for {meta['producer']}")


def domain(con, stats, aux, colour, overlay, collisions):
    theirs = "b" if colour == "white" else "w"
    possible = (
        f"SELECT child_hash h FROM {parquet(stats)} "
        f"WHERE split_part(parent_epd,' ',2)='{theirs}' AND child_hash IS NOT NULL"
    )
    if colour == "white":
        possible += f" UNION ALL SELECT {zobrist_int64(chess.Board())}::BIGINT h"
    if overlay:
        # Include the whole overlay support, independent of the pseudocount.
        possible += f" UNION ALL SELECT child_hash h FROM {parquet(overlay)} WHERE child_hash IS NOT NULL"
    omit = (
        f" ANTI JOIN {parquet(collisions)} c ON c.parent_hash=p.h" if collisions else ""
    )
    # Parents whose native sidecar already accounts for every move need no patch.
    con.execute(f"""CREATE TEMP TABLE wanted AS SELECT DISTINCT p.h,
        a.position_hash IS NULL AS needs_term FROM ({possible}) p {omit}
        LEFT JOIN {parquet(aux)} a ON a.position_hash=p.h
        WHERE a.position_hash IS NULL OR a.other_total>0""")


def partial(con, files, term_files, cap, target):
    """One independently resumable month or group of banded archive slices."""
    src = parquet(files)
    # For a raw-only parent reconstruct the native sidecar. The banded exporter
    # defines endings as arrivals through the cap minus departures on plies
    # 2..cap+1, with departures on cap+1 also recorded as horizon observations.
    if cap is None:
        # The monthly extract already grouped its edges. Stream its observations
        # into the cross-month merge without hashing millions of long EPDs twice.
        moves = f"""SELECT parent_hash,move_san,parent_epd AS epd_lo,NULL::VARCHAR AS epd_hi,
            child_hash AS ch_lo,NULL::BIGINT AS ch_hi,{",".join(SUMS)}
            FROM {src} s SEMI JOIN wanted w ON w.h=s.parent_hash"""
        flow = f"""SELECT position_hash h, CASE WHEN kind=1 THEN 'horizon'
            WHEN reason=0 THEN 'term_normal' WHEN reason=1 THEN 'term_flag'
            ELSE 'term_other' END AS category,{sums("s.")}
            FROM {parquet(term_files)} s SEMI JOIN wanted w ON w.h=s.position_hash AND w.needs_term
            GROUP BY h,category"""
    else:
        moves = f"""SELECT parent_hash,move_san,min(parent_epd) AS epd_lo,max(parent_epd) AS epd_hi,
                min(child_hash) AS ch_lo,max(child_hash) AS ch_hi,{sums("s.")}
            FROM {src} s SEMI JOIN wanted w ON w.h=s.parent_hash WHERE s.ply<={int(cap)}
            GROUP BY parent_hash,move_san"""
        vals = ", ".join(SUMS)
        negative = ", ".join(f"-{c} AS {c}" for c in SUMS)
        terms = [
            f"SELECT child_hash h,'term_other' category,{vals} FROM {src} s SEMI JOIN wanted w ON w.h=s.child_hash AND w.needs_term WHERE ply<={cap}",
            f"SELECT parent_hash h,'term_other' category,{negative} FROM {src} s SEMI JOIN wanted w ON w.h=s.parent_hash AND w.needs_term WHERE ply BETWEEN 2 AND {cap + 1}",
            f"SELECT parent_hash h,'horizon' category,{vals} FROM {src} s SEMI JOIN wanted w ON w.h=s.parent_hash AND w.needs_term WHERE ply={cap + 1}",
        ]
        flow = f"SELECT h,category,{sums()} FROM ({' UNION ALL '.join(terms)}) t GROUP BY h,category"
    for sql, path in [
        (moves, target.with_suffix(".moves.parquet")),
        (flow, target.with_suffix(".flow.parquet")),
    ]:
        if not path.exists():
            copy(con, sql, path)


def finalize(con, folder, stats, aux, colour):
    parts = folder / "parts"
    con.execute(f"""CREATE TEMP TABLE recovered AS SELECT parent_hash,move_san,
        min(epd_lo) AS parent_epd,max(coalesce(epd_hi,epd_lo)) AS epd_hi,
        min(ch_lo) AS child_hash,max(coalesce(ch_hi,ch_lo)) AS ch_hi,
        {sums()} FROM {parquet(parts / "*.moves.parquet")} GROUP BY parent_hash,move_san""")
    ours = "w" if colour == "white" else "b"
    bad = con.execute(f"""SELECT parent_hash FROM recovered WHERE parent_epd IS NULL OR move_san IS NULL
        OR child_hash IS NULL OR parent_epd!=epd_hi OR child_hash!=ch_hi OR least(white_wins,draws,black_wins)<0
        OR split_part(parent_epd,' ',2)!='{ours}' OR white_wins+draws+black_wins!=total OR total<=0
        UNION ALL SELECT parent_hash FROM recovered GROUP BY parent_hash HAVING min(parent_epd)!=max(parent_epd)
        LIMIT 1""").fetchone()
    if bad:
        raise ValueError(
            f"Conflicting raw move identity, side or outcome counts at {bad}"
        )
    # Verify the old named rows and every component of Other before replacing them.
    retained = (
        f"SELECT s.* FROM {parquet(stats)} s SEMI JOIN wanted w ON s.parent_hash=w.h"
    )
    different = " OR ".join(
        f"s.{c} IS DISTINCT FROM r.{c}" for c in (*SUMS, "child_hash", "parent_epd")
    )
    bad = con.execute(f"""SELECT s.parent_hash FROM ({retained}) s LEFT JOIN recovered r
        USING(parent_hash,move_san) WHERE {different} LIMIT 1""").fetchone()
    if bad:
        raise ValueError(f"Native source disagrees with retained pool row at {bad}")
    hidden = f"""SELECT r.parent_hash,{sums("r.")} FROM recovered r ANTI JOIN ({retained}) s
        USING(parent_hash,move_san) GROUP BY r.parent_hash"""
    different = " OR ".join(f"coalesce(u.{c},0)!=a.other_{c}" for c in SUMS)
    bad = con.execute(f"""SELECT w.h FROM wanted w JOIN {parquet(aux)} a ON a.position_hash=w.h
        LEFT JOIN ({hidden}) u ON u.parent_hash=w.h WHERE {different} LIMIT 1""").fetchone()
    if bad:
        raise ValueError(
            f"Uncovered moves disagree with native Other outcomes at {bad}"
        )

    # Rare alternate SAN spellings can describe an identical child. Normalize
    # only the duplicate groups, using the same legal board as the builder.
    def canonical(epd: str, san: str) -> str:
        b = chess.Board(epd + " 0 1")
        return b.san(b.parse_san(san))

    con.create_function("uncut_canonical", canonical)
    cols = "parent_hash,move_san,child_hash,parent_epd,total,white_wins,draws,black_wins,white_score_avg"
    named = f"""SELECT parent_hash, CASE WHEN count(*)=1 THEN min(move_san)
        ELSE uncut_canonical(min(parent_epd),min(move_san)) END AS move_san,
        child_hash,min(parent_epd) AS parent_epd,{sums()},
        (sum(white_wins)+0.5*sum(draws))/sum(total) AS white_score_avg
        FROM recovered GROUP BY parent_hash,child_hash"""
    copy(
        con,
        f"""SELECT {cols} FROM {parquet(stats)} s ANTI JOIN wanted w ON w.h=s.parent_hash
        UNION ALL SELECT {cols} FROM ({named}) n ORDER BY parent_hash,move_san""",
        folder / "graph.parquet",
    )
    # Keep native termination/horizon values for old parents. Clear their Other
    # only after the exact W/D/L conservation check above. Opponents pass through.
    con.execute(f"""CREATE TEMP TABLE flows AS SELECT h,category,{sums()}
        FROM {parquet(parts / "*.flow.parquet")} GROUP BY h,category""")
    bad = con.execute(
        "SELECT h FROM flows WHERE least(white_wins,draws,black_wins)<0 OR white_wins+draws+black_wins!=total LIMIT 1"
    ).fetchone()
    if bad:
        raise ValueError(f"Unexpected terminal flow at {bad}")
    cols = [
        row[0]
        for row in con.execute(f"DESCRIBE SELECT * FROM {parquet(aux)}").fetchall()
    ]
    old, new = [], []
    for col in cols:
        if col == "position_hash":
            old.append("a.position_hash")
            new.append("r.parent_hash AS position_hash")
        elif col.startswith("other_"):
            zero = (
                "NULL"
                if col in ("other_eval_mean", "other_eval_min", "other_eval_max")
                else "0"
            )
            old.append(f"CASE WHEN w.h IS NULL THEN a.{col} ELSE {zero} END AS {col}")
            new.append(f"{zero} AS {col}")
        else:
            cat = next((g for g in GROUPS if col.startswith(g + "_")), None)
            if not cat or col[len(cat) + 1 :] not in SUMS:
                raise ValueError(f"Unknown aux column {col}")
            c = col[len(cat) + 1 :]
            old.append("a." + col)
            new.append(
                f"coalesce(sum(f.{c}) FILTER (WHERE f.category='{cat}'),0)::BIGINT AS {col}"
            )
    copy(
        con,
        f"""SELECT {",".join(old)} FROM {parquet(aux)} a LEFT JOIN wanted w ON w.h=a.position_hash
        UNION ALL SELECT {",".join(new)} FROM (SELECT DISTINCT parent_hash FROM recovered) r
        JOIN wanted w ON w.h=r.parent_hash AND w.needs_term LEFT JOIN flows f ON f.h=w.h
        GROUP BY r.parent_hash ORDER BY position_hash""",
        folder / "aux.parquet",
    )
    atomic_json(
        folder / "aux.parquet.meta.json",
        {
            "producer": "uncut_own_moves",
            "native_aux": str(Path(aux).resolve()),
            "term_reasons": read_json(str(aux) + ".meta.json").get(
                "term_reasons", "split"
            ),
        },
    )


def prepare(stats, aux, colour, folder, overlay=None, threads=2, memory="6GB"):
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    identity = contract(stats, aux, colour, overlay)
    mode, parts, cap, collisions = source_parts(stats)
    sources = [stamp(p) for fs, ts in parts for p in [*fs, *ts]]
    recipe = {
        "contract": identity,
        "source_digest": hashlib.sha256(json.dumps(sources).encode()).hexdigest(),
        "producer": digest(__file__),
        "mode": mode,
        "cap": cap,
    }
    # DuckDB provides an exclusive writer lock across independent preparations.
    with duckdb.connect(str(folder / "prepare.duckdb")) as con:
        con.execute(f"SET threads={int(threads)}; SET memory_limit={literal(memory)}")
        con.execute("SET preserve_insertion_order=false")
        manifest = folder / "recipe.json"
        if manifest.exists() and read_json(manifest) != recipe:
            raise ValueError("Source/recipe changed; use a fresh output directory")
        atomic_json(manifest, recipe)
        if (folder / "ready.json").exists():
            return load(folder, stats, aux, colour, overlay)
        (folder / "parts").mkdir(exist_ok=True)
        domain(con, stats, aux, colour, overlay, collisions)
        t0 = time.monotonic()
        for i, (files, term_files) in enumerate(parts):
            partial(con, files, term_files, cap, folder / "parts" / f"part-{i:04d}")
            if i % 10 == 0 or i + 1 == len(parts):
                print(
                    f"Prepared {i + 1}/{len(parts)} source parts ({time.monotonic() - t0:.0f}s)",
                    flush=True,
                )
        finalize(con, folder, stats, aux, colour)
        done = {
            "contract": identity,
            "recipe_sha256": digest(manifest),
            "files": {
                n: stamp(folder / n)[1:]
                for n in ("graph.parquet", "aux.parquet", "aux.parquet.meta.json")
            },
        }
        atomic_json(folder / "ready.json", done)
        return done


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for arg in ("stats", "aux-stats", "out"):
        p.add_argument("--" + arg, type=Path, required=True)
    p.add_argument("--perspective", choices=["white", "black"], required=True)
    p.add_argument("--opponent-prior", type=Path)
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--mem", default="6GB")
    args = p.parse_args()
    prepare(
        args.stats,
        args.aux_stats,
        args.perspective,
        args.out,
        args.opponent_prior,
        args.threads,
        args.mem,
    )
