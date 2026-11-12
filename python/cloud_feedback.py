"""Independently score provisional book decisions and write a new proposal overlay.

Keep the source book and every prior overlay immutable. Repeat selection with the
new overlay until a pass requests no new searches. Searches are checkpointed per
position and reused when their root move set covers the new request.
"""
import argparse
import datetime
import json
from pathlib import Path

import chess
import chess.engine
import duckdb
import numpy as np
import polars as pl

import budget_core as bc
import engine_candidates as ec
from eval_arrays import MISSING, lookup_evals, open_eval_arrays
from hybrid_candidates import CLOUD, sha, write_frame, write_json
from zobrist import zobrist_int64

SCREEN_CP = 30
GATE_MARGIN_CP = 15
EXTRA = {"cloud_origin":pl.Boolean, "parent_cp_white":pl.Int32, "verified_loss_cp":pl.Int32,
         "verification_nodes":pl.Int64, "verification_best":pl.String,
         "verification_free_cp_white":pl.Int32, "verification_unstable":pl.Boolean,
         "verification_engine_sha256":pl.String, "transposition_cp_white":pl.Int32}
SCHEMA = ec.SCHEMA | EXTRA


def normalize(frame):
    if "cloud_origin" in frame.columns:
        if "transposition_cp_white" not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Int32).alias("transposition_cp_white"))
        return frame.select(SCHEMA.keys())
    return frame.with_columns((pl.col("engine") == CLOUD).alias("cloud_origin"),
        *[pl.lit(None, dtype=t).alias(k) for k, t in EXTRA.items() if k != "cloud_origin"])


def search(engine, board, nodes, san=None):
    engine.configure({"Clear Hash": None})
    kw = {"root_moves":[board.parse_san(san)]} if san else {}
    info = engine.analyse(board, chess.engine.Limit(nodes=nodes), **kw)
    return {"san":board.san(info["pv"][0]), "cp":info["score"].white().score(mate_score=2000),
            "depth":info["depth"], "nodes":info["nodes"]}


def score_moves(engine, board, moves, nodes, limit):
    sign = 1 if board.turn else -1
    free = search(engine, board, nodes)
    sans = set(moves) | {free["san"]}
    scores = {san:search(engine, board, nodes, san) for san in sorted(sans)} if len(sans) > 1 else {free["san"]:free}
    best = max(scores, key=lambda m:(sign * scores[m]["cp"], m == free["san"]))
    unstable = abs(free["cp"] - scores[free["san"]]["cp"]) > limit
    return {"best":best, "free":free, "scores":scores, "budget":nodes, "unstable":unstable}


def needs_check(selected, moves, signed_cp, parent, cloud, limit):
    if cloud:
        return True
    chosen = signed_cp.get(selected)
    if chosen is None or parent is None or any(m not in signed_cp for m in moves):
        return True
    best = max(parent, *signed_cp.values())
    return any(abs(best - cp - limit) <= GATE_MARGIN_CP or
               (m != selected and abs(cp - chosen) <= SCREEN_CP)
               for m, cp in signed_cp.items())


def decisions(book, meta, proposals, colour):
    ours = book.filter((pl.col("side_to_move") == colour) & pl.col("best_move").is_not_null())
    if ours["position_hash"].n_unique() != ours.height:
        raise ValueError("Duplicate book decision")
    parents = ours.select("position_hash")
    with duckdb.connect() as con:
        con.execute("SET threads=4")
        con.register("wanted", parents.to_arrow())
        # the menu the builder chose from (book_graph), restored rivals included
        from book_graph import edge_source
        source, where = edge_source(meta, colour)
        edges = con.execute(f"""SELECT s.parent_hash, s.move_san, s.child_hash FROM read_parquet(?) s
            JOIN wanted ON s.parent_hash=position_hash WHERE {where}""", [source]).fetchall()
    source = pl.scan_parquet(meta["source_rep"]).filter(pl.col("position_hash").is_in(parents["position_hash"].to_list())
                & (pl.col("side_to_move") == colour)).select("position_hash","best_move").collect()
    choices = {h:{} for h in parents["position_hash"]}
    for h, san, child in edges:
        choices[h][san] = child
    src = dict(source.iter_rows())
    cloud_kids = set(proposals.filter(pl.col("cloud_origin"))["child_hash"])
    prior = {}
    for row in proposals.join(parents, on="position_hash", how="semi").iter_rows(named=True):
        h, san = row["position_hash"], row["move_san"]
        choices[h][san] = row["child_hash"]
        if row["verification_nodes"]:
            prior.setdefault(h, {})[san] = row
    positions = []
    wanted = set(choices)
    for h, epd, selected in ours.select("position_hash","position_epd","best_move").iter_rows():
        board = chess.Board(epd + " 0 1")
        if board.turn != (colour == "white") or zobrist_int64(board) != h or not board.is_valid():
            raise ValueError(f"Book identity mismatch at {h}")
        for san in [selected, src.get(h)]:
            if san:
                choices[h].setdefault(san, None)
        moves = {}
        for san, expected in choices[h].items():
            move = board.parse_san(san)
            canonical = board.san(move)
            board.push(move)
            child = zobrist_int64(board)
            board.pop()
            if expected is not None and expected != child:
                raise ValueError(f"Child mismatch at {h} {san}")
            moves[canonical] = child
        wanted.update(moves.values())
        positions.append((h, epd, board.san(board.parse_san(selected)), moves))
    keys = np.array(sorted(wanted), dtype=np.int64)
    cp = {int(h):int(v) for h,v in zip(keys,lookup_evals(keys,*open_eval_arrays())) if v != MISSING}
    cp.update(bc.proposal_cp(proposals))
    sign = 1 if colour == "white" else -1
    for h, epd, selected, moves in positions:
        old = prior.get(h, {})
        if old and set(moves) <= old.keys():
            if old[selected]["verified_loss_cp"] > meta["max_eval_loss"]:
                raise ValueError(f"Builder selected a verified over-limit move at {h} {selected}")
            continue
        signed = {m:sign*cp[ch] for m,ch in moves.items() if ch in cp}
        if old or needs_check(selected, moves, signed, sign*cp[h] if h in cp else None,
                              cloud_kids.intersection(moves.values()), meta["max_eval_loss"]):
            yield h, epd, moves


def corrected_rows(h, epd, result, originals, engine_name, engine_sha):
    b = chess.Board(epd + " 0 1")
    sign = 1 if b.turn else -1
    root = result["scores"][result["best"]]["cp"]
    for san, info in result["scores"].items():
        b.push_san(san)
        child = zobrist_int64(b)
        b.pop()
        old = originals.get((h, child), {})
        yield dict(position_hash=h, position_epd=epd, move_san=san, child_hash=child,
            child_cp_white=info["cp"], nodes=info["nodes"], depth=info["depth"], engine=engine_name,
            searched=result["date"], reason=old.get("reason","book verification"), cloud_origin=old.get("cloud_origin",False),
            parent_cp_white=root, verified_loss_cp=sign*(root-info["cp"]), verification_nodes=result["budget"],
            verification_best=result["best"], verification_free_cp_white=result["free"]["cp"],
            verification_unstable=result["unstable"], verification_engine_sha256=engine_sha)


def run(book, proposals, colour, cache, out, nodes=15_000_000, threads=6):
    meta = json.loads(Path(str(book)+".meta.json").read_text())
    if meta.get("engine_candidates_sha256") != sha(proposals):
        raise ValueError("Provisional book and nomination table differ")
    frame = normalize(pl.read_parquet(proposals))
    book_frame = pl.read_parquet(book)
    requests = list(decisions(book_frame, meta, frame, colour))
    cache.mkdir(parents=True, exist_ok=True)
    contract = {"engine":sha(ec.SF), "nodes":nodes, "threads":threads, "hash_mb":1024,
                "limit":meta["max_eval_loss"], "colour":colour, "verifier":sha(__file__)}
    config = cache / "config.json"
    if config.exists() and json.loads(config.read_text()) != contract:
        raise ValueError("Verification cache settings changed")
    write_json(contract, config)
    print(f"{len(requests)} positions need verification", flush=True)
    originals = {(r["position_hash"],r["child_hash"]):r for r in frame.iter_rows(named=True)}
    records = []
    engine = None
    try:
        for i, (h, epd, moves) in enumerate(requests, 1):
            file = cache / f"{h}.json"
            saved = json.loads(file.read_text()) if file.exists() else None
            if saved and saved["epd"] != epd:
                raise ValueError(f"EPD collision at {h}")
            if not saved or not set(moves) <= saved["scores"].keys():
                if engine is None:
                    engine = chess.engine.SimpleEngine.popen_uci(ec.SF)
                    engine.configure({"Threads":threads, "Hash":1024})
                b = chess.Board(epd + " 0 1")
                sans = set(moves) | (set(saved["scores"]) if saved else set())
                saved = score_moves(engine, b, sans, nodes, meta["max_eval_loss"])
                if saved["unstable"]:
                    saved = score_moves(engine, b, sans | set(saved["scores"]), nodes*2, meta["max_eval_loss"])
                saved.update(epd=epd, engine=engine.id["name"], date=datetime.date.today().isoformat())
                write_json(saved, file)
            records.extend(corrected_rows(h, epd, saved, originals, saved["engine"], contract["engine"]))
            if i % 25 == 0:
                print(f"{i}/{len(requests)}", flush=True)
    finally:
        if engine:
            engine.quit()
    if records:
        update = pl.DataFrame(records, schema=SCHEMA)
        frame = pl.concat([frame.join(update.select("position_hash").unique(), on="position_hash", how="anti"), update])
        frame = frame.sort(["position_hash","child_hash"])
        write_frame(frame, out)
    else:
        # Preserve the very same proposal bytes and fingerprint when the book is stable.
        import shutil
        tmp = out.with_suffix(".tmp")
        shutil.copyfile(proposals, tmp)
        tmp.replace(out)
    write_json({"book_sha256":sha(book), "input_sha256":sha(proposals), "output_sha256":sha(out),
                "searched_positions":len(requests), "corrected_edges":len(records), "contract":contract},
               Path(str(out)+".meta.json"))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for arg in ["book","proposals","cache","out"]:
        p.add_argument("--"+arg, type=Path, required=True)
    p.add_argument("--colour", choices=["white","black"], required=True)
    run(**vars(p.parse_args()))
