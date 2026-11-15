"""Verify the lookahead certificate independently using the saved pre-veto graph."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import polars as pl

from lookahead_policy import sha256


def artifacts(book, meta, receipt=True):
    book = Path(book)
    spec = meta["lookahead_floor"]
    if spec.get("schema") != 2:
        raise ValueError("legacy lookahead book has no auditable pre-veto menu")
    name = spec["evidence_file"]
    if Path(name).name != name:
        raise ValueError("lookahead evidence must be beside the book")
    veto = Path(str(book) + ".lookahead.json")
    evidence = book.parent / name
    digests = {}
    for p, key in ((veto,"veto_sha256"), (evidence,"evidence_sha256")):
        digests[p] = sha256(p) if p.is_file() else None
        if not digests[p] or digests[p] != spec[key]:
            raise ValueError(f"{p}: missing or changed lookahead artifact")
    rp = book.parent / "build.receipt.json"
    if receipt and rp.exists():
        out = json.loads(rp.read_text(encoding="utf-8"))["outputs"]
        for p in (book, Path(str(book)+".meta.json"), veto, evidence, Path(str(book)+".lookahead-audit.json")):
            if not p.is_file() or out.get(p.name) != (digests[p] if p in digests else sha256(p)):
                raise ValueError(f"{rp}: missing or changed {p.name}")
    return veto,evidence


def policy_paths(chosen, replies, cap, depth):
    """For each actual prescription, follow only other prescribed moves, bounded by ply."""
    from functools import lru_cache
    @lru_cache(None)
    def walk(h, plies):
        ok, expanded, child, _, _ = chosen[h]
        if ok is False:
            return h, False
        unknown = ok is None
        if plies < 2 or not expanded or child not in replies:
            return None, True
        followed = replies[child]
        unknown |= sum(p for _,_,p in followed) < 1-1e-9
        for target, inside, _ in followed:
            if not inside or target not in chosen:
                unknown = True
                continue
            witness, boundary = walk(target, plies-2)
            if witness is not None:
                return witness, unknown or boundary
            unknown |= boundary
        return None, unknown or not followed

    bad, unresolved = [], []
    for h, (_, _, _, earliest, reach) in chosen.items():
        witness, boundary = walk(h, min(2*depth, max(0, cap-earliest)))
        if witness is not None:
            bad.append((reach,h,witness))
        elif boundary:
            unresolved.append((reach, h))
    return {"below_floor_reachable":len(bad),
            "highest_reach_witnesses":[{"reach":r,"from":h,"below_floor_at":w} for r,h,w in sorted(bad, reverse=True)[:20]],
            "incomplete_without_witness":len(unresolved),
            "max_reach_incomplete": max((r for r, _ in unresolved), default=0.),
            "highest_reach_incomplete": [{"reach": r, "from": h} for r, h in sorted(unresolved, reverse=True)[:20]]}


def audit(book, meta, *, check_receipt=True, high_reach=0.002, spill_parent=None):
    """Check every rank and veto, then play the optimized book across the followed replies."""
    book = Path(book)
    veto, evidence = artifacts(book, meta, check_receipt)
    depth = int(meta["lookahead_floor"]["depth"])
    white = meta["perspective"] == "white"
    prior, tol = float(meta["slice_prior"]), float(meta["robustness_floor"])
    if meta.get("max_score_loss") is not None:
        side = "score" if white else "(1-score)"
        test = f"{side}+1e-12 >= {(prior if white else 1-prior)-tol!r}"
    else:
        test = f"score >= {prior-tol!r}" if white else f"score <= {prior+tol!r}"
    with (TemporaryDirectory(prefix="lookahead-", dir=spill_parent) as scratch,
          duckdb.connect(config={"threads": 2, "memory_limit": "4GB", "temp_directory": scratch}) as db):
        db.read_parquet(str(evidence)).create_view("edges")
        db.execute("""CREATE TEMP TABLE ranks AS SELECT parent, min(dead_at) AS rank,
                   max(dead_at) AS highest, count(dead_at) AS labelled,count(*) AS n
                   FROM edges WHERE ours GROUP BY parent""")
        if db.execute("SELECT 1 FROM ranks WHERE labelled>0 AND (labelled<>n OR rank<>highest OR rank<0 OR rank>?) LIMIT 1",[depth]).fetchone():
            raise ValueError("inconsistent lookahead ranks")
        # An opponent needs one reply; our side needs every available action to fail.
        # Positive distances must descend to a rank-zero decision, including on cyclic graphs.
        db.execute("""CREATE TEMP TABLE forcing AS SELECT e.parent, min(r.rank)+1 AS steps
                   FROM edges e JOIN ranks r ON e.child=r.parent
                   WHERE NOT e.ours AND e.expanded AND r.rank IS NOT NULL GROUP BY e.parent""")
        db.execute("""CREATE TEMP TABLE reply_coverage AS SELECT parent,
                   coalesce(sum(reply_p) FILTER (WHERE expanded), 0) AS followed
                   FROM edges WHERE NOT ours GROUP BY parent""")
        db.execute(f"""CREATE TEMP TABLE actions AS
                   SELECT e.parent, e.san, e.child, e.expanded, e.pre_reach, e.pre_ply, coalesce(rc.followed, 0) AS followed, {test} AS ok,
                     CASE WHEN san IS NULL OR NOT ({test}) THEN 0 WHEN expanded AND steps<=? AND e.pre_ply + 2*steps<=? THEN steps END AS cost
                   FROM edges e LEFT JOIN forcing f ON f.parent=e.child LEFT JOIN reply_coverage rc ON rc.parent=e.child WHERE e.ours""",[depth, int(meta["max_ply"])])
        db.execute("""CREATE TEMP TABLE states AS
                   SELECT parent, max(pre_reach) AS reach,
                     CASE WHEN count(cost)=count(*) THEN max(cost) END AS expected,
                     count(*) FILTER (WHERE ok AND cost IS NULL)>0 AS alternative,
                     count(*) FILTER (WHERE san IS NOT NULL AND cost IS NOT NULL)>0 AS danger,
                     count(san)=0 AS empty_menu,
                     bool_or(san IS NULL OR ok IS NULL OR NOT expanded OR followed<1-1e-9) AS unresolved
                   FROM actions GROUP BY parent""")
        mismatch = db.execute("SELECT parent FROM states JOIN ranks USING(parent) WHERE expected IS DISTINCT FROM rank LIMIT 1").fetchone()
        if mismatch:
            raise ValueError(f"lookahead floor propagation disagrees at {mismatch[0]}")
        expected = {(h, san): reach for h, san, reach in db.execute("""
            SELECT DISTINCT a.parent, rtrim(a.san,'+#'), s.reach FROM actions a
            JOIN states s USING(parent)
            WHERE s.alternative AND a.cost IS NOT NULL AND a.san IS NOT NULL""").fetchall()}
        published = {}
        seen = set()
        for row in json.loads(veto.read_text(encoding="utf-8")):
            h = row["position_hash"]
            moves = row["moves"]
            if h in seen or not moves or len(set(moves)) != len(moves):
                raise ValueError(f"empty or duplicate veto list at {h}")
            seen.add(h)
            published.update({(h, san): row["reach"] for san in moves})
        if expected != published:
            raise ValueError("veto list or its reach disagrees with the admitted menu")
        picks = (pl.read_parquet(book, columns=["position_hash","side_to_move","best_move"])
                 .filter((pl.col("side_to_move")==meta["perspective"]) & pl.col("best_move").is_not_null())
                 .select(pl.col("position_hash").alias("parent"), pl.col("best_move").alias("move")))
        db.register("booked",picks.to_arrow())
        matches = db.execute("""SELECT b.parent, a.ok, a.expanded, a.child, a.pre_ply, a.pre_reach
                         FROM booked b JOIN actions a ON b.parent=a.parent AND b.move=a.san""").fetchall()
        if len(matches)!=len(picks) or len({r[0] for r in matches})!=len(picks):
            raise ValueError("booked move missing or ambiguous in the admitted menu")
        if any((h,m.rstrip("+#")) in expected for h,m in picks.iter_rows()):
            raise ValueError("optimized book plays a vetoed move")
        chosen = {h: tuple(row) for h,*row in matches}
        replies = {}
        for par, ch, inside, p in db.execute("""SELECT e.parent,e.child,e.expanded,e.reply_p FROM edges e
                  SEMI JOIN (SELECT a.child FROM actions a JOIN booked b ON a.parent=b.parent AND a.san=b.move AND a.expanded) k
                  ON e.parent=k.child WHERE NOT e.ours AND e.child IS NOT NULL AND e.reply_p>0""").fetchall():
            replies.setdefault(par,[]).append((ch,inside,p))
        counts = {}
        for label,where in (("void","danger AND NOT alternative"),("empty_menu","empty_menu"),("unresolved_menu","unresolved")):
            n,hi,total,top = db.execute(f"SELECT count(*),count(*) FILTER (WHERE reach>=?),sum(reach),max(reach) FROM states WHERE {where}",[high_reach]).fetchone()
            examples = db.execute(f"SELECT parent, reach FROM states WHERE {where} ORDER BY reach DESC,parent LIMIT 20").fetchall()
            counts[label] = dict(nodes=n, high_reach_nodes=hi, sum_optimistic_reach=total or 0.,
                                 max_reach=top or 0., highest_reach=[{"parent": h, "reach": r} for h, r in examples])
        high = db.execute("SELECT count(*) FROM states WHERE reach>=?",[high_reach]).fetchone()[0]
    return {"checked_decisions":len(chosen), "checked_high_reach_menus":high,
            "high_reach_threshold":high_reach, "verified_vetoes":len(expected), **counts,
            "chosen_policy":policy_paths(chosen,replies,int(meta["max_ply"]),depth)}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("book", type=Path)
    ap.add_argument("out", type=Path)
    ap.add_argument("--depth", required=True, type=int)
    a = ap.parse_args()
    meta = json.loads(Path(str(a.book)+".meta.json").read_text(encoding="utf-8"))
    if meta.get("lookahead_floor", {}).get("depth") != a.depth:
        raise ValueError("built book differs from the requested lookahead depth")
    summary = audit(a.book, meta, check_receipt=False, spill_parent=a.book.parent)
    a.out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary))
