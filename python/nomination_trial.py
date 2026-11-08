"""Checkpointed targeted versus broad nomination experiment on this machine.

Use the same frozen builder, calibration policy and verifier for both arms.
The targeted arm owns its graph, calibration, score cache, and feedback history.
"""
import argparse
import json
import msvcrt
from pathlib import Path
import shutil
import sys
import time

import chess
import duckdb
import polars as pl

import run_practical_rebuild as r
from zobrist import zobrist_int64

HERE = Path(__file__).resolve().parent
if (HERE/"trial-runtime.json").exists():
    # The Python files live in an immutable snapshot. The original stage receipts
    # record absolute paths in the implementation checkout.
    r.ROOT=Path(json.loads((HERE/"trial-runtime.json").read_text())["checkout"])
    r.OUT=r.ROOT/"outputs/rebuild-2026-11-06"
    r.CODE,r.DEPTH,r.SELECTION=r.OUT/"code",r.OUT/"depth-code",r.OUT/"selection-code-v2"
    r.STATUS_FILE=r.OUT/"status.json"
BROAD = r.OUT
TRIAL = BROAD / "nomination-trial"
FRONTIER_BUDGET = 128
MIN_REACH = .001
PINNED_PROVISIONAL = r.provisional


def files(path):
    return [path, Path(str(path)+".meta.json")]


def save(frame, path):
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():
        if not pl.read_parquet(path).equals(frame):
            raise RuntimeError(f"Changed frozen inputs: {path}")
        return
    tmp = path.with_suffix(".tmp")
    frame.write_parquet(tmp)
    tmp.replace(path)


def evidence(colour):
    """First-pass raw searches only, even when the broad arm has since learned more."""
    first = BROAD / colour / "depth-policy/round-00/hybrid-proposals-01.parquet"
    if colour == "white":
        return []  # White runs the chosen method for real.
    meta = json.loads((BROAD/"stages/black-depth-00-hybrid-01.json").read_text())
    if r.digest(first) != meta["outputs"][str(first)]:
        raise ValueError("Broad raw nomination checkpoint changed")
    return [first]


def edges_for(stats, parents, floor):
    if not parents:
        return []
    with duckdb.connect() as con:
        con.execute("SET threads=3")
        con.register("wanted",pl.DataFrame({"h":sorted(parents)},schema={"h":pl.Int64}).to_arrow())
        return con.execute("SELECT parent_hash, move_san, child_hash, total FROM read_parquet(?) "
                           "SEMI JOIN wanted ON parent_hash=h WHERE total>=? ORDER BY 1,2",
                           [str(stats),floor]).fetchall()


def requested(book, audit, targets, searched, frontier_used, stats, floor):
    """Choose requests using the targeted book and graph before opening replay evidence."""
    ours = dict(book.filter(pl.col("best_move").is_not_null()).select("position_hash","best_move").iter_rows())
    by_hash = {row["position_hash"]:row for row in targets.iter_rows(named=True)}
    parent_data = dict(book.select("position_hash","position_epd").iter_rows())
    chosen = {}
    for row in audit.iter_rows(named=True):
        h, loss = row["position_hash"], row["cached_loss"]
        if h in searched:
            continue
        if row["status"] in ("unknown","fallback","outside_set") or row["flagged"] or (
                loss is not None and abs(loss - floor["loss"]) <= 15):
            chosen[h] = {"position_hash":h,"epd":parent_data[h],"colour":floor["colour"],
                         "status":row["status"],"reach":by_hash.get(h,{}).get("reach",0.0),
                         "ply":by_hash.get(h,{}).get("ply",0),"request_reason":"book"}
    alternatives = {ch for h,san,ch,n in edges_for(stats,set(ours),floor["games"])
                    if ch is not None and san.rstrip("+#") != ours[h].rstrip("+#")}
    frontier = {ch for _,_,ch,_ in edges_for(stats,alternatives,floor["games"])}
    available = [row for h,row in by_hash.items() if h in frontier and h not in ours and
                 h not in searched and row["reach"] >= MIN_REACH]
    for row in sorted(available,key=lambda x:(-x["reach"],x["position_hash"]))[:max(0,FRONTIER_BUDGET-frontier_used)]:
        chosen[row["position_hash"]] = {**row,"request_reason":"frontier"}
    return pl.DataFrame(list(chosen.values()),schema={**targets.schema,"request_reason":pl.String}).sort("position_hash")


def combine(previous, new, out):
    from cloud_feedback import normalize
    chunks = [normalize(pl.read_parquet(p)) for p in (previous,new) if p is not None]
    raw = pl.concat(chunks).sort("position_hash","child_hash")
    if raw.n_unique(subset=["position_hash","child_hash"]) != raw.height:
        raise ValueError("Duplicate proposal edge")
    save(raw,out)
    return out


def query(colour, label, kind, targets, dest):
    command = r.cmd(HERE / "targeted_candidates.py",kind,"--targets",targets,"--colour",colour,"--out",dest)
    if kind == "local":
        command += ["--replay",*map(str,evidence(colour))]
    r.stage(label,command,files(dest))


def exact_provisional(colour, work, label, base, proposals):
    try:
        return PINNED_PROVISIONAL(colour,work,label,base,proposals)
    except RuntimeError as exc:
        if str(exc) != f"Expected exactly 2000 legal decisions for {colour}":
            raise
    # The DP can jump from 1,999 to 2,001. Apply the same existing polish to
    # either arm if needed. Keep the original and record the extra build cost.
    prefix = work/f"exact_{colour}"
    book = work/f"exact_{colour}_b2000.parquet"
    extra = ["--engine-candidates",proposals] if proposals else []
    r.stage(label+"-exact",r.cmd(r.SELECTION/"build_budget_books.py",*base,*extra,
            "--sampled-stop",work/"stop-table.parquet","--sampled-other",work/"other-table.parquet",
            "--polish","--force","--out-prefix",prefix),files(book))
    rows = pl.read_parquet(book).filter((pl.col("side_to_move")==colour)&pl.col("best_move").is_not_null())
    if rows.height != 2000 or rows["position_hash"].n_unique() != 2000:
        raise RuntimeError(f"Exact-budget extraction failed at {book}")
    return book


def build(colour):
    base = r.recipe(colour)
    seen_cloud, seen_local, frontier_used = set(), set(), 0
    props = None
    initial = (BROAD / "black/targets-01.parquet" if colour == "black" else
               BROAD / "white/depth-policy/round-00/targets-01.parquet")
    for epoch in range(100):
        label = f"{colour}-target-{epoch:02d}"
        work = TRIAL / colour / f"round-{epoch:02d}"
        work.mkdir(parents=True,exist_ok=True)
        for step in range(100):
            prefix = f"{label}-cloud-{step:02d}"
            if epoch == step == 0:
                targets = initial
            else:
                targets = work/f"graph-{step:02d}.parquet"
                overlay = ["--engine-candidates",props] if props else []
                r.stage(prefix+"-graph",r.cmd(r.SELECTION/"build_budget_books.py",*base,*overlay,
                        "--out-prefix",work/"graph","--dump-engine-targets",targets),[targets])
            graph = pl.read_parquet(targets)
            new = graph.filter(~pl.col("position_hash").is_in(sorted(seen_cloud)))
            if not new.height:
                break
            ask = work/f"cloud-requests-{step:02d}.parquet"
            save(new,ask)
            hits = work/f"cloud-{step:02d}.parquet"
            seed = TRIAL / colour / "seed"
            if epoch == step == 0 and (seed/"cloud.parquet.meta.json").exists():
                design = json.loads((seed/"cloud.parquet.meta.json").read_text())
                hits = seed/"cloud.parquet"
                if (not pl.read_parquet(seed/"requests.parquet").equals(new) or design["kind"]!="cloud"
                        or design["targets"]!=r.digest(seed/"requests.parquet") or design["output"]!=r.digest(hits)
                        or design["script"]!=r.digest(HERE/"targeted_candidates.py")):
                    raise RuntimeError("Cloud seed identity changed")
            else:
                query(colour,prefix,"cloud",ask,hits)
            seen_cloud.update(new["position_hash"])
            # Verified roots already contain the unrestricted engine move and all admissible moves.
            added = pl.read_parquet(hits)
            if props:
                occupied = set(pl.read_parquet(props,columns=["position_hash"])["position_hash"])
                added = added.filter(~pl.col("position_hash").is_in(sorted(occupied)))
            if not added.height:
                break
            usable = work/f"cloud-new-{step:02d}.parquet"
            save(added,usable)
            props = r.resolve_proposals(colour,prefix,combine(props,usable,work/f"cloud-all-{step:02d}.parquet"))
        else:
            raise RuntimeError("Cloud nomination did not stabilize")
        if props is None:
            import engine_candidates as ec
            props = work/"empty.parquet"
            save(pl.DataFrame(schema=ec.SCHEMA),props)
        book = exact_provisional(colour,work,label,base,props)
        prefix = work/"screen"
        rows = Path(str(prefix)+".rows.parquet")
        r.stage(label+"-screen",r.cmd(r.SELECTION/"validate_book.py","--book",book,"--colour",colour,
                "--nodes",0,"--out",prefix),[rows,Path(str(prefix)+".json")])
        meta = json.loads(Path(str(book)+".meta.json").read_text())
        from cloud_feedback import normalize
        known = normalize(pl.read_parquet(props))
        verified = set(known.filter(pl.col("parent_cp_white").is_not_null())["position_hash"])
        nominations = set(known["position_hash"])
        requests = requested(pl.read_parquet(book),pl.read_parquet(rows),graph,seen_local | verified | nominations,
                             frontier_used,r.POOL[colour],{"games":meta["min_games"],"loss":meta["max_eval_loss"],
                             "colour":colour})
        ask = work/"local-requests.parquet"
        save(requests,ask)
        if requests.height:
            result = work/"local.parquet"
            query(colour,label+"-local","local",ask,result)
            props = r.resolve_proposals(colour,label+"-local",combine(props,result,work/"local-all.parquet"))
            seen_local.update(requests["position_hash"])
            seen_cloud.update(requests["position_hash"])
            frontier_used += requests.filter(pl.col("request_reason")=="frontier").height
            continue
        verified_path = work/"verified.parquet"
        r.stage(label+"-feedback",r.cmd(r.SELECTION/"cloud_feedback.py","--book",book,"--proposals",props,
                "--colour",colour,"--cache",TRIAL/colour/"verification-cache","--out",verified_path),files(verified_path))
        if json.loads(files(verified_path)[1].read_text())["searched_positions"]:
            props = r.resolve_proposals(colour,label+"-feedback",verified_path)
            continue
        if r.digest(verified_path)!=r.digest(props):
            raise RuntimeError("Stable feedback changed the overlay")
        audits = r.audit(colour,{"NEW":book})
        r.publish(TRIAL/colour/"selected.json",{"book":book,"sha256":r.digest(book),"audit":audits["NEW"],
                  "cloud_queries":len(seen_cloud),"local_requests":len(seen_local),"frontier_requests":frontier_used})
        return book
    raise RuntimeError("Targeted feedback did not stabilize")


def setup():
    r.setup()
    r.setup_selection()
    r.OUT = TRIAL
    r.STATUS_FILE = TRIAL/"status.json"
    (TRIAL/"logs").mkdir(parents=True,exist_ok=True)
    cache = TRIAL/"training-sf.parquet"
    src = Path("E:/chess/calibration/convertibility_sf_cache_sampled.parquet")
    for orig,dest in zip(files(src),files(cache)):
        if not dest.exists():
            tmp = dest.with_suffix(".copying")
            shutil.copy2(orig,tmp)
            tmp.replace(dest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("colour",choices=["black","white"])
    parser.add_argument("--after-broad",action="store_true")
    a = parser.parse_args()
    TRIAL.mkdir(parents=True,exist_ok=True)
    with (TRIAL/"workflow.lock").open("a+b") as lock:
        lock.seek(0)
        msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
        if a.after_broad:
            r.publish(TRIAL/"queue.json",{"state":"waiting-for-broad-black","since":r.now()})
            while json.loads((BROAD/"status.json").read_text())["state"] != "black-broad-complete":
                state = json.loads((BROAD/"status.json").read_text())
                if state["state"]=="failed":
                    raise RuntimeError(f"Broad Black needs attention: {state}")
                time.sleep(30)
        setup()
        try:
            book = build(a.colour)
            r.status(state="targeted-complete",book=book)
        except Exception as exc:
            r.status(state="failed",error=str(exc))
            raise


if __name__ == "__main__":
    main()
