"""Search afresh the decisions of a provisional book that rest on an unverified rare move.

With the uncut own-move lookup every observed move is a candidate, and a move seen a handful of
times carries a stored eval that is a selected maximum over many thin candidates. The builder's
--rare-verification keeps such a move from setting a gate reference until it has been searched
(budget_core.gate_candidates), so no rival is pruned on its word and the graph stays a superset.
This script closes the loop for the decisions the book actually depends on:

  A  the book PRESCRIBES a move with fewer than rare_floor games and no verified score;
  B  a booked decision is DISPUTED: an unverified rare move's stored eval, had it bound, would
     have excluded a rival the gate currently admits (<book>.rare.json, written by the build);
  C  (--frontier-reach R, --frontier N) unbooked disputed decisions with optimistic reach >= R,
     the most-reached first: searched now so the next build's choice there is already settled,
     which saves rebuild rounds. Nothing depends on C for correctness.

At each such parent: one unrestricted search, and a search restricted to each move whose ordering
matters (the prescription, the rare threats, EVERY rival the threats put at risk, the best trusted
rival, the source move), all at the same node budget from a cleared hash. A move's loss is measured
like for like: against the best RESTRICTED score at that parent (the engine's own first choice is
always searched restricted too), never against the unrestricted score, which is a different kind of
search and reads about 7 cp apart from the restricted one on the same move.

The COMPARISON SET at a parent is defined by the recipe and the evidence, never by what happens
to be in the cache (comparison_set): the moves asked for, the moves already verified there in the
overlay the book was built with, every trusted move the stored scores admit, the engine's own
first choice, and then any trusted move the fresh reference would admit or cut within 2*BAND of the
limit. Only that set enters the reference, the retry decision, the deeper searches and the overlay.
So a common move is never cut by a fresh reference on its stored score, and it is settled in the
same pass that creates the reference, whether or not its parent stays in the next book. A search
cached for another gate or a pre-warm is reused when the set calls for it and ignored otherwise.

A verdict within BAND cp of the limit, or one that flips between the two references, is noise at
this budget: the whole parent is searched again at --deep-nodes (its own cache, restricted searches
only) and the deeper scores decide. verification_nodes records which budget a row rests on, and
verification_unstable that the two budgets disagreed about the move.

Score-policy books compare the converted parent and move evaluations. The retry band remains
in centipawns around each move, testing whether it straddles the absolute or relative boundary.
When an earlier seed supplies a shallow verification, a booked observed move near the new
boundary goes back through the depth check.

The budget never goes down at a parent. If the overlay the book was built with already holds a
deeper verdict there, a later pass that adds a rival compares the WHOLE set at that deeper budget
again (the earlier deep searches are cache hits), whatever the base scores say: evidence that
selected the book is not replaced by shallower evidence. The decision reads the applied overlay
and this pass's base scores only, never what the deep cache happens to hold. Results are cached per parent position and are independent
of the gate and the book, so another cap or colour reuses them; a later request for more moves at
a cached parent searches only the missing ones.

The output is a proposal overlay in cloud_feedback's schema, which the builder reads through
--engine-candidates: each searched move's child takes its fresh score (one value per board,
budget_core.proposal_cp), the parent takes the best score found, and the loss of a move belongs
to its (parent, move) row. An engine first choice nobody has played is recorded but NOT added as a
candidate: the menu stays the observed moves. Rebuild with the overlay and run this again until
it asks for nothing.

    python rare_verify.py --book B --colour black --cache DIR --out OVERLAY [--nodes 5000000]
"""
from __future__ import annotations

import argparse
import datetime
import json
import time
from pathlib import Path

import chess
import chess.engine
import duckdb
import numpy as np
import polars as pl

import budget_core as bc
import cloud_feedback as fb
import engine_candidates as ec
from book_graph import edges_at
from eval_arrays import MISSING, lookup_evals, open_eval_arrays
from hybrid_candidates import sha, write_frame, write_json
from zobrist import zobrist_int64

THREATS = 3
BAND = 10            # cp either side of the limit that a deeper search must settle


def bare(san: str) -> str:
    return san.rstrip("+#")


def near_score_boundary(best, move, free, limit, band_cp, score_floor):
    """Would a cp perturbation of the child, or the alternate reference, change the verdict?"""
    lo, hi = move - band_cp, move + band_cp
    return (bc.engine_loss(best, hi, True) - bc.SCORE_EPS <= limit <= bc.engine_loss(best, lo, True) + bc.SCORE_EPS
            or (score_floor is not None and bc.cp_to_expected_score(lo) - bc.SCORE_EPS
                <= score_floor <= bc.cp_to_expected_score(hi) + bc.SCORE_EPS)
            or (free is not None and bc.fails_engine_gate(best, move, limit, True)
                != bc.fails_engine_gate(free, move, limit, True)))


def plan(book, meta, frame, colour, cps, edges, disputes, frontier_reach=None, frontier=0,
         deep_nodes=20_000_000, band=BAND):
    """{parent hash: (reason, epd, moves to order)}; moves are bare SANs of observed moves.
    cps: White-view stored or overridden cp by position hash."""
    floor = meta["rare_floor"]
    sign = 1 if colour == "white" else -1
    verified = bc.verified_moves(frame)
    ours = book.filter((pl.col("side_to_move") == colour) & pl.col("best_move").is_not_null())
    booked = {h: (epd, bare(m)) for h, epd, m in
              ours.select("position_hash", "position_epd", "best_move").iter_rows()}
    src = {}
    if meta.get("source_rep"):
        s = (pl.scan_parquet(meta["source_rep"]).filter(
            pl.col("position_hash").is_in(list(booked)) & (pl.col("side_to_move") == colour))
            .select("position_hash", "best_move").collect())
        src = {h: bare(m) for h, m in s.iter_rows() if m}
    dis = {d["position_hash"]: d for d in disputes}
    # A seed searched under a different gate can already carry a 5M verdict on
    # a booked move that now needs 20M. Reuse its cp evidence at the new boundary.
    revisit = set()
    limit = meta.get("max_score_loss")
    if limit is not None and frame is not None and frame.height:
        floor_score = bc.score_floor_from_meta(meta, colour == "white")
        shallow = frame.filter((pl.col("verification_nodes") < deep_nodes)
                               & pl.col("position_hash").is_in(list(booked)))
        for h, san, p, c, free in shallow.select("position_hash", "move_san", "parent_cp_white",
                "child_cp_white", "verification_free_cp_white").iter_rows():
            if (booked[h][1] == bare(san) and p is not None and c is not None and
                    near_score_boundary(sign*p, sign*c, None if free is None else sign*free,
                                        limit, band, floor_score)):
                revisit.add(h)

    def trusted_rival(h, exclude):
        best = None
        for e in edges.get(h, ()):
            m = bare(e["move_san"])
            if m in exclude or e["child_hash"] not in cps:
                continue
            if e["total"] >= floor or m in verified.get(h, ()):
                v = sign * cps[e["child_hash"]]
                if best is None or v > best[0]:
                    best = (v, m)
        return best[1] if best else None

    out = {}
    for h, (epd, move) in booked.items():
        menu = {bare(e["move_san"]): e for e in edges.get(h, ())}
        done = verified.get(h, set())
        want, reason = set(), None
        if h in revisit and move in menu:
            want.add(move)
            reason = "score gate boundary"
        e = menu.get(move)
        if e is not None and e["total"] < floor and move not in done:
            want.add(move)
            reason = "rare prescription"
        d = dis.get(h)
        if d and d["at_risk"]:
            threats = [bare(m) for m, _ in sorted(d["threats"], key=lambda t: -t[1])[:THREATS]]
            # the rivals facing exclusion get their own comparable search: a verdict on them
            # must not rest on a stored score against a fresh reference
            want |= set(threats) | {bare(m) for m in d["at_risk"]} | {move}
            reason = reason or "disputed gate"
        if not want:
            continue
        rival = trusted_rival(h, want)
        if rival:
            want.add(rival)
        if src.get(h) in menu:
            want.add(src[h])
        if reason == "score gate boundary" or not want <= done:
            out[h] = (reason, epd, sorted(want))
    if frontier and frontier_reach is not None:
        extra = [d for d in disputes if d["position_hash"] not in booked and d["at_risk"]
                 and d["reach"] >= frontier_reach and d.get("epd")]
        for d in sorted(extra, key=lambda d: (-d["reach"], d["position_hash"]))[:frontier]:
            h = d["position_hash"]
            want = {bare(m) for m, _ in sorted(d["threats"], key=lambda t: -t[1])[:THREATS]}
            want |= {bare(m) for m in d["at_risk"]}
            rival = trusted_rival(h, want)
            if rival:
                want.add(rival)
            if not want <= verified.get(h, set()):
                out[h] = ("frontier dispute", d["epd"], sorted(want))
    return out


class Searches:
    """Per-parent cache of an unrestricted search and restricted searches, one node budget."""

    def __init__(self, cache: Path, nodes: int, threads: int, hash_mb: int = 1024,
                 free: bool = True):
        """free=False: restricted searches only (the deeper retry tier, its own directory)."""
        self.dir, self.nodes, self.free = cache, nodes, free
        cache.mkdir(parents=True, exist_ok=True)
        self.contract = {"engine": sha(ec.SF), "nodes": nodes, "threads": threads,
                         "hash_mb": hash_mb, "mate_cp": 2000,
                         "kind": "rare_verify-1" if free else "rare_verify-deep-1"}
        cfg = cache / "config.json"
        if cfg.exists() and json.loads(cfg.read_text()) != self.contract:
            raise ValueError("Rare-move search cache was made with other settings")
        write_json(self.contract, cfg)
        self.engine = None
        self.stats = {"parents_searched": 0, "parent_cache_hits": 0, "restricted_searched": 0,
                      "restricted_cache_hits": 0, "nodes": 0, "search_seconds": 0.0}

    def _one(self, board, move=None):
        if self.engine is None:
            self.engine = chess.engine.SimpleEngine.popen_uci(ec.SF)
            self.engine.configure({"Threads": self.contract["threads"],
                                   "Hash": self.contract["hash_mb"]})
            self.name = self.engine.id.get("name", "stockfish")
        t = time.monotonic()
        r = fb.search(self.engine, board, self.nodes, move)
        self.stats["search_seconds"] += time.monotonic() - t
        self.stats["nodes"] += r["nodes"]
        return r

    def get(self, h: int, epd: str, moves, count: bool = True) -> dict:
        """count=False: a repeat call for the same parent in the same pass; its cache hits are
        not counted, so the cost report shows real reuse only."""
        file = self.dir / f"{h}.json"
        saved = json.loads(file.read_text()) if file.exists() else None
        board = chess.Board(epd + " 0 1")
        if zobrist_int64(board) != h or not board.is_valid():
            raise ValueError(f"Position and hash disagree at {h}")
        if saved and saved["epd"] != epd:
            raise ValueError(f"Cached search belongs to another position at {h}")
        if saved is None:
            first = self._one(board) if self.free else None
            saved = {"epd": epd, "free": first, "moves": {},
                     "engine": self.name if first else None,
                     "date": datetime.date.today().isoformat()}
            self.stats["parents_searched"] += 1
        elif count:
            self.stats["parent_cache_hits"] += 1
        legal = {bare(board.san(m)): board.san(m) for m in board.legal_moves}
        todo = set(moves)
        if saved["free"]:
            todo.add(bare(saved["free"]["san"]))      # a comparable score for the engine's choice
        changed = False
        for m in sorted(todo):
            if m in saved["moves"]:
                self.stats["restricted_cache_hits"] += count
                continue
            if m not in legal:
                raise ValueError(f"Illegal move {m} requested at {h}")
            saved["moves"][m] = self._one(board, legal[m])
            saved["engine"] = saved["engine"] or self.name
            self.stats["restricted_searched"] += 1
            changed = True
        if changed or not file.exists():
            write_json(saved, file)
        return saved

    def close(self):
        if self.engine is not None:
            self.engine.quit()


def comparison_set(search, requested, done, menu, stored, floor, limit, sign, band=BAND, score_units=False):
    """(the moves compared at one parent, the cached searches covering them).
    search(moves, first) -> the parent's cache entry with at least `moves` (Searches.get);
    requested: this pass's moves; done: moves verified here in the applied overlay; menu: bare SAN
    -> edge; stored: White-view stored cp by position. Depends on those and on the scores of the
    set's own moves only, so a cold and a warm cache give the same set."""
    trusted = {m: sign * stored[e["child_hash"]] for m, e in menu.items()
               if e["total"] >= floor and e["child_hash"] in stored}
    use = set(requested) | set(done)
    if trusted and limit is not None:
        top = max(trusted.values())
        eps = bc.SCORE_EPS if score_units else bc.CP_EPS
        use |= {m for m, c in trusted.items() if bc.engine_loss(top, c, score_units) <= limit + eps}
    first = True
    while True:
        saved = search(sorted(use), first)
        first = False
        use.add(bare(saved["free"]["san"]))
        if limit is None:
            return use, saved
        root = max(sign * saved["moves"][m]["cp"] for m in use)
        more = {m for m, c in trusted.items() if m not in use
                and not bc.fails_engine_gate(root, c + 2 * band, limit, score_units)}
        if not more:
            return use, saved
        use |= more


def boundary(saved, limit, band=BAND, moves=None, score_units=False, score_floor=None) -> list[str]:
    """The moves (of `moves`; default all cached) at a searched parent whose verdict this budget
    cannot settle: the loss against the best restricted score is within `band` of the limit, or
    the verdict differs when the unrestricted score is the reference instead."""
    sign = 1 if chess.Board(saved["epd"] + " 0 1").turn else -1
    scores = {m: sign * r["cp"] for m, r in saved["moves"].items()
              if moves is None or m in moves}
    best, free = max(scores.values()), sign * saved["free"]["cp"]
    if score_units:
        return sorted(m for m, c in scores.items() if near_score_boundary(best, c, free, limit, band, score_floor))
    return sorted(m for m, c in scores.items()
                  if abs(best - c - limit) <= band or (best - c > limit) != (free - c > limit))


def rows_for(h, saved, menu, limit, nodes, engine_sha, reason, deep=None, deep_nodes=None,
             moves=None, score_units=False, score_floor=None):
    """Overlay rows for the OBSERVED moves of the comparison set `moves` (default: all cached) at
    one parent. The reference is the set's best restricted score. deep: the same parent's
    restricted searches at the larger budget; when given, its scores are the parent's and every
    move's, and decide."""
    board = chess.Board(saved["epd"] + " 0 1")
    sign = 1 if board.turn else -1
    base = {m: r for m, r in saved["moves"].items() if moves is None or m in moves}
    use = {m: r for m, r in deep["moves"].items() if m in base} if deep else base
    if deep and not set(base) <= set(use):
        raise ValueError(f"Deeper searches at {h} do not cover every move searched there")
    root = sign * max(sign * r["cp"] for r in use.values())
    base_root = sign * max(sign * r["cp"] for r in base.values())
    free = saved["free"]["san"]
    for m, r in sorted(use.items()):
        e = menu.get(m)
        if e is None:
            continue                # an engine choice nobody has played: reported, not a candidate
        loss = sign * (root - r["cp"])
        flipped = bool(deep) and (bc.fails_engine_gate(sign * base_root, sign * base[m]["cp"],
                                                       limit, score_units, score_floor)
                                  != bc.fails_engine_gate(sign * root, sign * r["cp"],
                                                          limit, score_units, score_floor))
        yield dict(position_hash=h, position_epd=saved["epd"], move_san=e["move_san"],
                   child_hash=e["child_hash"], child_cp_white=r["cp"], nodes=r["nodes"],
                   depth=r["depth"], engine=saved["engine"], searched=saved["date"], reason=reason,
                   cloud_origin=False, parent_cp_white=root,
                   verified_loss_cp=loss, verification_nodes=deep_nodes if deep else nodes,
                   verification_best=free, verification_free_cp_white=saved["free"]["cp"],
                   verification_unstable=flipped, verification_engine_sha256=engine_sha,
                   transposition_cp_white=None)


def run(book, colour, cache, out, nodes=5_000_000, threads=6, frontier_reach=None, frontier=0,
        deep_nodes=20_000_000, band=BAND):
    t0 = time.monotonic()
    book, out = Path(book), Path(out)
    meta = json.loads(Path(str(book) + ".meta.json").read_text())
    if meta.get("rare_floor") is None:
        raise ValueError("The book was not built with --rare-verification")
    prior = meta.get("engine_candidates")
    if prior and sha(prior) != meta["engine_candidates_sha256"]:
        raise ValueError("The book's proposal overlay changed after the build")
    frame = (fb.normalize(pl.read_parquet(prior)) if prior
             else pl.DataFrame(schema=fb.SCHEMA))
    b = pl.read_parquet(book)
    disputes = json.loads(Path(str(book) + ".rare.json").read_text())
    parents = set(b.filter((pl.col("side_to_move") == colour)
                           & pl.col("best_move").is_not_null())["position_hash"])
    parents |= {d["position_hash"] for d in disputes}
    with duckdb.connect() as con:
        con.execute("SET threads=4; SET memory_limit='8GB'")
        edges = edges_at(con, meta, colour, parents)
        epds = dict(con.execute(
            f"""SELECT DISTINCT s.parent_hash, s.parent_epd FROM read_parquet(?) s
            WHERE s.parent_hash IN ({",".join(map(str, sorted({d["position_hash"] for d in disputes}))) or "NULL"})""",
            [__import__("book_graph").edge_source(meta, colour)[0]]).fetchall())
    for d in disputes:
        d["epd"] = epds.get(d["position_hash"])
    keys = np.array(sorted(parents | {e["child_hash"] for es in edges.values() for e in es
                                      if e["child_hash"] is not None}), dtype=np.int64)
    cps = {int(h): int(v) for h, v in zip(keys, lookup_evals(keys, *open_eval_arrays()))
           if v != MISSING}
    stored = dict(cps)
    cps.update(bc.proposal_cp(frame))
    wanted = plan(b, meta, frame, colour, cps, edges, disputes, frontier_reach, frontier, deep_nodes, band)
    verified = bc.verified_moves(frame)
    sign = 1 if colour == "white" else -1
    # the deepest budget already applied at each parent, from the overlay this book was built with
    applied = dict(frame.filter(pl.col("verification_nodes").is_not_null())
                   .group_by("position_hash").agg(pl.col("verification_nodes").max()).iter_rows())
    print(f"{len(wanted)} parents to verify: "
          f"{dict(pl.Series([w[0] for w in wanted.values()]).value_counts().iter_rows()) if wanted else {}}",
          flush=True)
    searches = Searches(Path(cache), nodes, threads)
    deeper = Searches(Path(cache) / f"deep-{deep_nodes}", deep_nodes, threads, free=False)
    records, unplayed, retried = [], [], []
    score_units = meta.get("max_score_loss") is not None
    limit = meta["max_score_loss"] if score_units else meta["max_eval_loss"]
    score_floor = bc.score_floor_from_meta(meta, colour == "white")
    try:
        for i, (h, (reason, epd, moves)) in enumerate(sorted(wanted.items()), 1):
            menu = {bare(e["move_san"]): e for e in edges.get(h, ())}
            use, saved = comparison_set(
                lambda ms, first: searches.get(h, epd, ms, count=first), moves,
                verified.get(h, ()), menu, stored, meta["rare_floor"], limit, sign, band, score_units)
            if bare(saved["free"]["san"]) not in menu:
                unplayed.append({"position_hash": h, "epd": epd, "engine_move": saved["free"]["san"],
                                 "cp_white": saved["free"]["cp"]})
            again = boundary(saved, limit, band, use, score_units, score_floor) if limit is not None else []
            was = applied.get(h, 0)
            if was > deep_nodes:
                raise ValueError(f"The applied overlay holds a {was:,}-node verdict at {h}; "
                                 f"this run's deepest budget is {deep_nodes:,}")
            carried = was > nodes
            deep = None
            if again or carried:
                before = dict(deeper.stats)
                deep = deeper.get(h, epd, sorted(use))
            new = list(rows_for(h, saved, menu, limit, nodes, searches.contract["engine"], reason,
                                deep, deep_nodes, use, score_units, score_floor))
            if deep:
                retried.append({"position_hash": h,
                                "why": "boundary" if again else "carried forward",
                                "previously_deep": carried, "moves": again,
                                "compared": sorted(use), "budget": deep_nodes,
                                "deep_searched": deeper.stats["restricted_searched"]
                                - before["restricted_searched"],
                                "deep_cache_hits": deeper.stats["restricted_cache_hits"]
                                - before["restricted_cache_hits"],
                                "verdict_changed": sorted(r["move_san"] for r in new
                                                          if r["verification_unstable"])})
            records.extend(new)
            if i % 25 == 0:
                print(f"  {i}/{len(wanted)} ({time.monotonic() - t0:.0f}s)", flush=True)
    finally:
        searches.close()
        deeper.close()
    if records:
        update = pl.DataFrame(records, schema=fb.SCHEMA)
        # earlier rows at a re-searched parent are superseded move by move
        keep = frame.join(update.select("position_hash", "move_san"),
                          on=["position_hash", "move_san"], how="anti")
        # one parent score per position: the newest search's root
        roots = dict(update.select("position_hash", "parent_cp_white").unique().iter_rows())
        keep = keep.with_columns(
            pl.when(pl.col("position_hash").is_in(list(roots)) & pl.col("parent_cp_white").is_not_null())
            .then(pl.col("position_hash").replace_strict(roots, default=None, return_dtype=pl.Int32))
            .otherwise(pl.col("parent_cp_white")).alias("parent_cp_white"))
        keep = keep.with_columns(
            pl.when(pl.col("verified_loss_cp").is_not_null() & pl.col("position_hash").is_in(list(roots)))
            .then(sign * (pl.col("parent_cp_white") - pl.col("child_cp_white")))
            .otherwise(pl.col("verified_loss_cp")).cast(pl.Int32).alias("verified_loss_cp"))
        result = pl.concat([keep, update]).sort("position_hash", "child_hash")
    else:
        result = frame
    write_frame(result, out)
    reasons = {}
    for reason, _, _ in wanted.values():
        reasons[reason] = reasons.get(reason, 0) + 1
    write_json({"book_sha256": sha(book), "input_sha256": sha(prior) if prior else None,
                "output_sha256": sha(out), "requested_parents": len(wanted), "by_reason": reasons,
                "rows": len(records), "engine_choice_unplayed": unplayed,
                "contract": searches.contract, "searches": searches.stats,
                "deep_contract": deeper.contract, "deep_searches": deeper.stats, "band_cp": band,
                "max_score_loss": meta.get("max_score_loss"), "absolute_score_floor": score_floor,
                "retried_parents": retried,
                "seconds": time.monotonic() - t0}, Path(str(out) + ".meta.json"))
    print(f"{len(wanted)} parents, {searches.stats} -> {out}", flush=True)
    return len(wanted)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for arg in ("book", "cache", "out"):
        p.add_argument("--" + arg, type=Path, required=True)
    p.add_argument("--colour", choices=["white", "black"], required=True)
    p.add_argument("--nodes", type=int, default=5_000_000)
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--frontier-reach", type=float, default=None)
    p.add_argument("--frontier", type=int, default=0)
    p.add_argument("--deep-nodes", type=int, default=20_000_000)
    p.add_argument("--band", type=int, default=BAND)
    run(**vars(p.parse_args()))
