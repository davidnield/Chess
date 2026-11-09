"""Black opponent-policy development pilot. Run inventory, policy, holdout, counts, score.

Run from the practical-repertoire worktree. Only 'policy' uses the Maia venv.
The October/November blind evaluation and frozen rebuild inputs are separate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import chess
import numpy as np

PYTHON = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON))
from zobrist import zobrist_int64

RUN = Path("outputs/rebuild-2026-11-06")
OUT = RUN / "opponent-prior-pilot"
ROUND = RUN / "black/depth-policy/round-00"
STATS = Path("E:/chess/position-stats")
ARCHIVE = STATS / "explorer_banded_2013_2026"
POOL = STATS / "position_stats_pooled_ge2000_2013_2026_brc_p30_mg20.parquet"
HOLD = Path("E:/chess/holdout_explorer/movers1800_white_f64_2026_08_09")
BOOKS = [Path("E:/chess/repertoire/final/lifetime_black_2026-10-24.parquet"),
         ROUND / "practical_black_b2000.parquet"]
INV = OUT / "positions.parquet"
VALID = OUT / "validation.parquet"
POLICY = OUT / "policy.parquet"
WEIGHTS = (0, 30, 100)


def digest(path):
    with path.open("rb") as fp:
        return hashlib.file_digest(fp, "sha256").hexdigest()


def write_json(path, obj):
    temp = path.with_suffix(".json.tmp")
    temp.write_text(json.dumps(obj, indent=2, allow_nan=False), encoding="utf8")
    temp.replace(path)


def parquet(table, path):
    import pyarrow.parquet as pq
    temp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, temp, compression="zstd")
    temp.replace(path)


def engine():
    import duckdb
    return duckdb.connect(config={"threads": 3})


def inventory():
    import pyarrow as pa
    db = engine()
    parents, candidates, positions = {}, {}, {}

    def record(board, label):
        h, epd = zobrist_int64(board), board.epd()
        pos = positions.setdefault(h, {"position_hash": h, "epd": epd, "booked": False,
                                       "population": False, "vienna": ""})
        assert board.turn and pos["epd"] == epd
        if label == "booked":
            pos["booked"] = True
        if label in ("booked", "alternative"):
            pos["population"] = True
        else:
            pos["vienna"] = label

    for book in BOOKS:
        rows = db.execute("""SELECT position_hash, position_epd, best_move FROM read_parquet(?)
                             WHERE side_to_move='black' AND best_move IS NOT NULL""", [str(book)]).fetchall()
        assert len(rows) == 2000
        for h, epd, move in rows:
            b = chess.Board(epd + " 0 1")
            assert not b.turn and zobrist_int64(b) == h and parents.setdefault(h, epd) == epd
            candidates.setdefault((h, move), set()).add("booked")
    keys = pa.table({"h": sorted(parents)})
    db.register("keys", keys)
    for source, hcol in [(POOL, "parent_hash"), (ROUND / "all-proposals-03-resolved.parquet", "position_hash")]:
        q = f"SELECT DISTINCT {hcol}, move_san, child_hash FROM read_parquet(?) JOIN keys ON h={hcol}"
        for h, move, child in db.execute(q, [str(source)]).fetchall():
            candidates.setdefault((h, move), set()).add(child)
    for (h, move), refs in candidates.items():
        b = chess.Board(parents[h] + " 0 1")
        b.push_san(move)
        assert refs.difference({"booked", zobrist_int64(b)}) == set()
        record(b, "booked" if "booked" in refs else "alternative")
    record(chess.Board(), "booked")
    b = chess.Board()
    for move in ("e4", "e5", "Nc3", "Qh4", "Nf3", "Bc5"):
        b.push_san(move)
        if move in ("Qh4", "Bc5"):
            record(b, move)
    table = pa.Table.from_pylist([positions[h] for h in sorted(positions)])
    parquet(table, INV)
    design = {"inventory_sha256": digest(INV), "books": {str(b): digest(b) for b in BOOKS},
              "population": sum(p["population"] for p in positions.values()),
              "including_diagnostics": len(positions), "booked_union_including_root": sum(p["booked"] for p in positions.values()),
              "weights": WEIGHTS, "policy": "Maia-3 79m, position repeated, White 2000 vs Black 1450",
              "primary": "Multiclass Brier at 1..99 historical moves, equal weight per observed position. Also report move-weighted Brier and all count strata.",
              "zero_history": "Report separately. K=0 has no empirical prediction.",
              "validation": "August and September 2026 development sample, folds 48..63, White 1800+, unrestricted Black, Blitz/Rapid/Classical.",
              "training": "All recorded named next moves, mean Elo 2000+, B/R/C, Jan 2013 through July 2026, conditional on a move being played."}
    write_json(OUT / "design.json", design)
    print(json.dumps(design), flush=True)


def holdout():
    db = engine()
    paths = sorted(str(f) for f in HOLD.glob("month=*/bkt=*/*.parquet"))
    assert len(paths) == 2
    q = """SELECT parent_hash h, rtrim(move_san,'+#') san, fold, sum(total)::BIGINT n
           FROM read_parquet(?,hive_partitioning=false) JOIN read_parquet(?) ON parent_hash=position_hash
           WHERE fold>=48 AND fold<64 GROUP BY ALL ORDER BY h, san, fold"""
    table = db.execute(q, [paths, str(INV)]).to_arrow_table()
    parquet(table, VALID)
    print(f"{len(table):,} held-out counts", flush=True)


def counts(extra=False):
    """Range-search sorted hash columns; decode SAN/count only in matching row groups."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from collections import Counter
    dest = OUT / ("counts-extra" if extra else "counts")
    dest.mkdir(exist_ok=True)
    inventory_rows = pq.read_table(INV).to_pylist()
    valid_hashes = set(pq.read_table(VALID, columns=["h"]).column("h").unique().to_pylist())
    wanted = valid_hashes | {r["position_hash"] for r in inventory_rows if r["vienna"]}
    if extra:
        wanted = {r["position_hash"] for r in inventory_rows} - wanted
    collision_keys = pq.read_table(ARCHIVE / "_collisions.parquet", columns=["parent_hash"])
    # The supervised pilot uses the same unambiguous keys as the original pool.
    assert not wanted.intersection(collision_keys.column(0).to_pylist())
    identity = {"inventory": digest(INV), "validation": digest(VALID), "archive": digest(ARCHIVE / "_book.meta.json")}
    receipt = dest / "sources.json"
    if receipt.exists() and json.loads(receipt.read_text()) != identity:
        raise ValueError("Count inputs changed; use a separate pilot directory")
    write_json(receipt, identity)
    schema = pa.schema([("h", pa.int64()), ("san", pa.string()), ("n", pa.int64())])
    for bucket in range(512):
        out = dest / f"b{bucket:03}.parquet"
        if out.exists():
            continue
        keys = np.array(sorted(h for h in wanted if h % 512 == bucket), dtype="int64")
        result = Counter()
        for event in ("Blitz", "Rapid", "Classical"):
            for band in (2000, 2200, 2500):
                if not len(keys):
                    continue
                src = ARCHIVE / f"ps/event={event}/elo_band={band}/bkt{bucket:03}.parquet"
                f = pq.ParquetFile(src)
                hi = f.schema_arrow.names.index("parent_hash")
                for g in range(f.num_row_groups):
                    stats = f.metadata.row_group(g).column(hi).statistics
                    targets = keys[keys.searchsorted(stats.min):keys.searchsorted(stats.max, "right")]
                    if not len(targets):
                        continue
                    hs = f.read_row_group(g, columns=["parent_hash"]).column(0).to_numpy()
                    if np.any(hs[1:] < hs[:-1]):
                        raise ValueError(f"Unsorted hashes in {src} group {g}")
                    left, right = hs.searchsorted(targets), hs.searchsorted(targets, "right")
                    hits = [(int(h), int(l), int(r)) for h,l,r in zip(targets,left,right) if r>l]
                    if hits:
                        payload = f.read_row_group(g, columns=["move_san", "total", "ply"])
                        for h,l,r in hits:
                            segment = payload.slice(l, r-l)
                            for san, n, ply in zip(*(c.to_pylist() for c in segment.columns)):
                                if ply <= 30:
                                    result[h,san.rstrip("+#")] += n
        table = pa.Table.from_pylist([{"h": h, "san": s, "n": n} for (h,s),n in sorted(result.items())], schema=schema)
        parquet(table, out)
        if bucket % 16 == 0 or bucket == 511:
            print(f"{bucket+1}/512 training buckets", flush=True)


def policy():
    import inspect
    import polars as pl
    import torch
    import maia_batch as mb
    torch.set_num_threads(2)
    cfg, model = mb.load("79m", "D:/chess/maia/hf_cache/hub")
    identity = {"inventory": digest(INV), "checkpoint_sha256": digest(Path(cfg.checkpoint_path)),
                "encoder_and_inference_sha256": digest(Path(inspect.getfile(mb))), "ratings": [2000,1450],
                "history": int(cfg.history), "encoding": "current position repeated", "model": "Maia-3 79m"}
    recs = pl.read_parquet(INV, columns=["position_hash","epd"]).rows()
    mb.check(cfg, model, recs[:32], 2000, 1450)
    cache, prior = OUT / "policy-cache.json", None
    if cache.exists():
        prior = json.loads(cache.read_text())
    if prior and prior["identity"] == identity and POLICY.exists() and prior["sha256"] == digest(POLICY):
        print("Reused validated policy cache", flush=True)
        return
    hashes, sans, probs = [], [], []
    for start in range(0,len(recs),128):
        hs, tokens, moves = mb.prep(recs[start:start+128])
        if not moves:
            continue
        p,_ = mb.run_batch(cfg,model,tokens,moves,2000,1450)
        mass = np.bincount([i for i,_,_ in moves], weights=p, minlength=len(hs))
        assert np.isfinite(p).all() and (p >= 0).all() and np.all((mass==0) | (abs(mass-1)<1e-5))
        hashes.extend(hs[i] for i,_,_ in moves)
        sans.extend(s for _,_,s in moves)
        probs.extend(p.tolist())
    tmp = POLICY.with_suffix(".tmp")
    pl.DataFrame({"h":hashes, "san":sans, "p":probs},
                 schema={"h":pl.Int64,"san":pl.String,"p":pl.Float32}).write_parquet(tmp)
    tmp.replace(POLICY)
    write_json(cache, {"identity":identity,"sha256":digest(POLICY),"legal_replies":len(hashes),"device":str(cfg.device)})
    print(f"Cached {len(hashes):,} legal replies at {len(recs):,} positions",flush=True)


def score():
    import math
    from collections import defaultdict
    import pyarrow.parquet as pq
    db = engine()
    buckets = sorted((OUT / "counts").glob("b*.parquet"))
    if len(buckets) != 512:
        raise ValueError(f"Only {len(buckets)}/512 complete training buckets")
    db.execute("CREATE TABLE history AS SELECT * FROM read_parquet(?)", [[str(f) for f in buckets]])
    db.execute("CREATE TABLE obs AS SELECT * FROM read_parquet(?)", [str(VALID)])
    db.execute("CREATE TABLE policy AS SELECT * FROM read_parquet(?)", [str(POLICY)])
    db.execute("CREATE TABLE positions AS SELECT * FROM read_parquet(?)", [str(INV)])
    db.execute("""CREATE TABLE wanted AS SELECT DISTINCT h FROM obs UNION
                  SELECT position_hash FROM positions WHERE vienna<>''""")
    # Independently reconcile the reconstructed history against *every* named
    # 20+ game edge in the exact pool used to build the provisional book.
    discrepancy = db.execute("""WITH retained AS (
        SELECT parent_hash h, rtrim(move_san,'+#') san, sum(total) n FROM read_parquet(?)
        JOIN wanted ON wanted.h=parent_hash GROUP BY ALL
    ), exact AS (SELECT * FROM history WHERE n >= 20)
    SELECT count(*) FROM retained FULL JOIN exact USING(h,san) WHERE retained.n IS DISTINCT FROM exact.n""",
        [str(POOL)]).fetchone()[0]
    if discrepancy:
        raise ValueError(f"{discrepancy} historical counts disagree with the production pool")
    aux = STATS / ("position_stats_aux_" + POOL.name.removeprefix("position_stats_"))
    discrepancy = db.execute("""WITH rare AS (SELECT h,sum(n) n FROM history WHERE n<20 GROUP BY h)
        SELECT count(*) FROM read_parquet(?) JOIN wanted ON position_hash=wanted.h
        LEFT JOIN rare USING(h) WHERE other_total<>coalesce(n,0)""", [str(aux)]).fetchone()[0]
    assert discrepancy == 0, "Historical minor replies disagree with the original pooled OTHER counts"
    # Some historical exports disambiguate pinned pieces (e.g. Rfe1 when Re1
    # is already unambiguous). Validate by replay before joining Maia's SAN.
    import pyarrow as pa
    aliases = {}
    for h,s,epd in db.execute("""SELECT h,san,epd FROM
            (SELECT h,san FROM history UNION SELECT h,san FROM obs)
            ANTI JOIN policy USING(h,san) JOIN positions ON h=position_hash""").fetchall():
        board = chess.Board(epd + " 0 1")
        aliases[h,s] = board.san(board.parse_san(s)).rstrip("+#")
    if aliases:
        db.register("aliases", pa.Table.from_pylist([{"h":h,"san":s,"canonical":v}
                                                    for (h,s),v in aliases.items()]))
        for name,extra in (("history",""),("obs",", fold")):
            db.execute(f"""CREATE OR REPLACE TABLE {name} AS SELECT h,coalesce(canonical,san) san{extra},
                    sum(n)::BIGINT n FROM {name} LEFT JOIN aliases USING(h,san) GROUP BY ALL""")
        assert db.execute("""SELECT count(*) FROM (SELECT h,san FROM history UNION SELECT h,san FROM obs)
                             ANTI JOIN policy USING(h,san)""").fetchone()[0] == 0
    vals = defaultdict(list)
    query = """SELECT wanted.h,policy.san,p,coalesce(n,0) FROM wanted JOIN policy ON wanted.h=policy.h
               LEFT JOIN history ON policy.h=history.h AND policy.san=history.san ORDER BY wanted.h,policy.san"""
    for h,s,p,n in db.execute(query).fetchall():
        vals[h].append((s,p,n))
    observed = defaultdict(list)
    for h,s,fold,n in db.execute("SELECT * FROM obs").fetchall():
        observed[h].append((s,fold-48,n))
    pos = {r["position_hash"]: r for r in pq.read_table(INV).to_pylist()}
    groups = defaultdict(list)
    vienna = {}
    missing = defaultdict(int)
    for h, moves in vals.items():
        legal, model, counts = zip(*moves)
        lookup = {s:i for i,s in enumerate(legal)}
        model = np.asarray(model, dtype="float64")
        model /= model.sum()
        counts = np.asarray(counts)
        N = counts.sum()
        freq = np.array([(counts+k*model)/(N+k) if N+k else model for k in WEIGHTS])
        assert np.allclose(freq.sum(axis=1),1)
        obs = np.zeros((len(legal),16), dtype="int64")
        for s,fold,n in observed.get(h, ()):
            obs[lookup[s],fold] += n
        if pos[h]["vienna"]:
            key = pos[h]["vienna"]
            san = {"Qh4":"Nf3","Bc5":"Nxh4"}[key]
            i = lookup[san]
            vienna[key] = {"white_reply":san,"training_moves":int(N),"observed_reply":int(counts[i]),
                           "model":float(model[i]),"probability_by_k":dict(zip(WEIGHTS,freq[:,i].tolist())),
                           "validation_reply":int(obs[i].sum()),"validation_moves":int(obs.sum())}
        if not pos[h]["population"] or not obs.sum():
            continue
        nfold = obs.sum(axis=0)
        losses = ((1 + (freq**2).sum(axis=1)[:,None] - 2*freq) @ obs)
        if not N:
            # The two nonzero K values coincide. No empirical comparator exists.
            missing["positions"] += 1
            missing["moves"] += int(obs.sum())
            missing["brier_sum"] += float(losses[1].sum())
            missing["brier_pos_sum"] += float(losses[1].sum()/obs.sum())
            continue
        rec = (nfold, losses)
        groups["all_positive"].append(rec)
        stratum = "1..99" if N<100 else "100..999" if N<1000 else "1000+"
        groups[stratum].append(rec)
        if N<100:
            groups["thin_booked" if pos[h]["booked"] else "thin_alternatives_only"].append(rec)
            groups["thin_1..9" if N<10 else "thin_10..29" if N<30 else "thin_30..99"].append(rec)
        groups["booked" if pos[h]["booked"] else "alternatives_only"].append(rec)

    def describe(records):
        nfold = np.stack([r[0] for r in records])
        loss = np.stack([r[1] for r in records])
        def metrics(n, l):
            take = n>0
            return np.array([np.average(l[take]/n[take,None],axis=0), l[take].sum(axis=0)/n[take].sum()])
        n = nfold.sum(axis=1)
        l = loss.sum(axis=2)
        full = metrics(n,l)
        # Delete one complete game fold at a time. Some single-observation
        # positions drop out of a replicate; the position-weighted statistic
        # pertains to positions observed at least once in each such sample.
        leave = np.stack([metrics(n-nfold[:,i],l-loss[:,:,i]) for i in range(16)])
        out = {"positions":len(n),"moves":int(n.sum())}
        for metric,row in enumerate(("position_weighted","move_weighted")):
            contrasts = {}
            for a,b in ((1,0),(2,0),(2,1)):
                diff = full[metric,a] - full[metric,b]
                samples = leave[:,metric,a] - leave[:,metric,b]
                se = math.sqrt(15/16 * ((samples-samples.mean())**2).sum())
                margin = 2.131449546 * se  # two-sided 95%, Student's t with 15 df
                contrasts[f"{WEIGHTS[a]} minus {WEIGHTS[b]}"] = {"delta":float(diff),"approx_95":[diff-margin,diff+margin]}
            out[row] = {"brier": dict(zip(WEIGHTS,full[metric].tolist())), "paired":contrasts}
        return out

    cohort = {k:describe(v) for k,v in groups.items()}
    zero = {"positions":missing["positions"], "moves":missing["moves"]}
    if zero["moves"]:
        zero.update({"maia_move_weighted_brier":missing["brier_sum"]/zero["moves"],
                     "maia_position_weighted_brier":missing["brier_pos_sum"]/zero["positions"]})
    prev = RUN / "ungated-black-diagnostic/vienna-maia-sensitivity.json"
    sensitivity = json.loads(prev.read_text())
    replay = {}
    for key, line in (("Qh4","early Qh4"),("Bc5","late Bc5")):
        by_move = {row[0].rstrip("+#"):row for row in sensitivity["inputs"][line]["replies"]}
        check = vienna[key]
        s = by_move[check["white_reply"]]
        check["earlier_model_probability"] = s[2]
        # The earlier small batch and this CUDA AMP cache can differ slightly.
        assert s[1] == check["observed_reply"] and abs(s[2]-check["model"])<0.001
    for k in WEIGHTS:
        ranks = sensitivity["rankings"][str(k)]["anchor"]
        replay[k] = {stage: ranked[0] for stage,ranked in ranks.items()}
    report = {"design":json.loads((OUT/"design.json").read_text()),"validation_positions_including_diagnostics":len(observed),
              "held_moves_including_diagnostics":sum(n for rows in observed.values() for _,_,n in rows),
              "cohorts":cohort,"zero_history":zero,"vienna":vienna,"vienna_fixed_book_anchor":replay,
              "historical_or_validation_SAN_aliases":len(aliases),
              "count_audits":"All sampled 20+ edges and all available pooled OTHER totals agree with the production inputs before legal SAN normalization.",
              "uncertainty":"Paired delete-one-game-fold jackknife, approximate 95% t(15) intervals. These months have been used for development."}
    write_json(OUT/"results.json",report)
    print(json.dumps({k:[v["positions"],v["moves"],v["position_weighted"]["brier"]] for k,v in cohort.items()}))


def matched():
    """Secondary sensitivity on the same test positions at the archive's mean rating."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    db = engine()
    root = HOLD.parent / "ge1800_brc_2026_08_09"
    src = sorted(root.glob("month=*/bkt=*/*.parquet"))
    assert len(src)==1024
    # Keep the primary test's positions fixed for a population sensitivity.
    db.execute("""CREATE TABLE keys AS SELECT DISTINCT h, epd FROM read_parquet(?)
        JOIN read_parquet(?) ON position_hash=h WHERE population""", [str(VALID),str(INV)])
    q = """SELECT h, rtrim(move_san,'+#') san, event, elo_band, month, sum(total)::BIGINT n
           FROM read_parquet(?) JOIN keys ON parent_hash=h AND parent_epd=epd
           WHERE elo_band IN (2000,2200,2500) GROUP BY ALL"""
    path = OUT / "matched-observations.parquet"
    parquet(db.execute(q,[[str(f) for f in src]]).to_arrow_table(),path)
    db.execute("CREATE TABLE obs AS SELECT * FROM read_parquet(?)", [str(path)])
    db.execute("CREATE TABLE history AS SELECT * FROM read_parquet(?)", [str(OUT/"counts/*.parquet")])
    db.execute("CREATE TABLE policy AS SELECT * FROM read_parquet(?)", [str(POLICY)])
    aliases = []
    for h,s,epd in db.execute("""SELECT h,san,epd FROM (SELECT h,san FROM history
          UNION SELECT h,san FROM obs) ANTI JOIN policy USING(h,san) JOIN keys USING(h)""").fetchall():
        b = chess.Board(epd+" 0 1")
        aliases.append({"h":h,"san":s,"canonical":b.san(b.parse_san(s)).rstrip("+#")})
    db.register("aliases",pa.Table.from_pylist(aliases, schema=pa.schema([("h",pa.int64()),("san",pa.string()),("canonical",pa.string())])))
    db.execute("""CREATE TABLE actual AS SELECT h,coalesce(canonical,san) san,event,elo_band,month,sum(n) n
               FROM obs LEFT JOIN aliases USING(h,san) GROUP BY ALL""")
    assert db.execute("SELECT count(*) FROM actual ANTI JOIN policy USING(h,san)").fetchone()[0]==0
    db.execute("""CREATE TABLE pred AS WITH canon AS (SELECT h,coalesce(canonical,san) san,sum(n) n
            FROM history LEFT JOIN aliases USING(h,san) GROUP BY ALL),
        base AS (SELECT h,san,p/sum(p) OVER(PARTITION BY h) m,coalesce(n,0) n,
                 sum(coalesce(n,0)) OVER(PARTITION BY h) train FROM policy LEFT JOIN canon USING(h,san)),
        pr AS (SELECT h,san,k,train,(n+k*m)/(train+k) prob FROM base CROSS JOIN (VALUES(0),(30),(100)) v(k)
               WHERE train BETWEEN 1 AND 99)
        SELECT h,san,k,1+sum(prob*prob) OVER(PARTITION BY h,k)-2*prob loss FROM pr""")
    def summary(field):
        s = field if field else "'all'"
        q = f"""WITH per_position AS (SELECT h,k,{s} cohort,sum(loss*n)/sum(n) brier,sum(n)::BIGINT n
                    FROM actual JOIN pred USING(h,san) GROUP BY ALL)
                SELECT cohort,k,count(*),sum(n),avg(brier),sum(brier*n)/sum(n) FROM per_position
                GROUP BY cohort,k ORDER BY cohort,k"""
        out = {}
        for key,k,positions,n,brier,weighted in db.execute(q).fetchall():
            row = out.setdefault(str(key),{"positions":positions,"moves":int(n),"position_brier":{},"move_brier":{}})
            row["position_brier"][k],row["move_brier"][k] = brier,weighted
        return out
    report = {"scope":"Same primary test positions with 1..99 historical games. All Aug/Sep games with mean rating >=2000; overlaps the primary sample. Descriptive, no independent game-fold intervals.",
              "overall":summary(None),"speed":summary("event"),"month":summary("month"),"rating":summary("elo_band")}
    write_json(OUT/"matched-results.json", report)
    print(json.dumps(report))


def overlay():
    """Materialize the exact named-reply channel for the controlled challenger."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    db = engine()
    files = sorted((OUT/"counts").glob("b*.parquet")) + sorted((OUT/"counts-extra").glob("b*.parquet"))
    if len(files) != 1024:
        raise ValueError("Complete the training counts for the full inventory first")
    db.execute("CREATE TABLE hist AS SELECT * FROM read_parquet(?)", [[str(f) for f in files]])
    db.execute("CREATE TABLE pol AS SELECT * FROM read_parquet(?)",[str(POLICY)])
    db.execute("CREATE TABLE pos AS SELECT * FROM read_parquet(?)",[str(INV)])
    db.execute("""CREATE TABLE retained AS SELECT parent_hash h,child_hash,move_san,total,
                 white_wins, draws,black_wins FROM read_parquet(?) JOIN pos ON parent_hash=position_hash
                 WHERE population""",[str(POOL)])
    aux = STATS / POOL.name.replace("position_stats_pooled","position_stats_aux_pooled")
    db.execute("""CREATE TABLE aux AS SELECT position_hash h,other_total,
                 term_normal_total+term_flag_total+term_other_total+horizon_total AS nonmove
                 FROM read_parquet(?) SEMI JOIN retained ON position_hash=h""",[str(aux)])
    db.execute("""CREATE TABLE mass AS SELECT h,sum(n)::BIGINT n FROM hist GROUP BY h""")
    discrepancy = db.execute("""WITH real AS (SELECT h,sum(total) total FROM retained GROUP BY h)
                 SELECT count(*) FROM real LEFT JOIN mass USING(h) LEFT JOIN aux USING(h)
                 WHERE n IS NULL OR n<>total+coalesce(other_total,0)""").fetchone()[0]
    if discrepancy:
        raise ValueError(f"Raw counts for {discrepancy} positions disagree with retained + OTHER")
    from collections import defaultdict
    hist, pol, old = defaultdict(dict),defaultdict(dict),defaultdict(dict)
    for h,s,n in db.execute("SELECT * FROM hist").fetchall():
        hist[h][s] = n
    for h,s,p in db.execute("SELECT * FROM pol").fetchall():
        pol[h][s] = p
    for h,ch,_,n,win,draw,loss in db.execute("SELECT * FROM retained").fetchall():
        a = old[h].get(ch,(0,0,0,0))
        old[h][ch] = tuple(x+y for x,y in zip(a,(n,win,draw,loss)))
    aux_rows = dict(db.execute("SELECT h,nonmove FROM aux").fetchall())
    out = OUT/"challenger-replies.parquet"
    tmp = out.with_suffix(".parquet.tmp")
    writer = None
    buffer = []
    named_rare = 0
    for rec in pq.read_table(INV).to_pylist():
        h,epd = rec["position_hash"],rec["epd"]
        if h not in old:
            continue
        b = chess.Board(epd+" 0 1")
        history = defaultdict(int)
        for s,n in hist[h].items():
            history[b.parse_san(s)] += n
        normalizer = sum(pol[h].values())
        assert abs(normalizer-1) < 1e-5
        for move in list(b.legal_moves):
            s = b.san(move)
            b.push(move)
            ch = zobrist_int64(b)
            b.pop()
            retained,w,d,l = old[h].get(ch,(0,0,0,0))
            named_rare += int(history[move]>retained)
            buffer.append({"parent_hash":h,"parent_epd":epd,"move_san":s,"child_hash":ch,
                           "total":retained,"white_wins":w,"draws":d,"black_wins":l,
                           "white_score_avg": (w + .5*d)/retained if retained else .5,
                           "reply_observed":history[move],"maia_p":pol[h][s.rstrip("+#")]/normalizer,
                           "nonmove":aux_rows.get(h,0)})
        if len(buffer)>=16000:
            table=pa.Table.from_pylist(buffer)
            if writer is None:
                writer=pq.ParquetWriter(tmp,table.schema,compression="zstd")
            writer.write_table(table)
            buffer.clear()
    if buffer:
        table=pa.Table.from_pylist(buffer)
        if writer is None:
            writer=pq.ParquetWriter(tmp,table.schema,compression="zstd")
        writer.write_table(table)
    writer.close()
    tmp.replace(out)
    info={"source_pool":str(POOL.resolve()),"source_aux":str(aux.resolve()),"min_games":20,"colour":"black","positions":len(old),
          "scope":"Canonical and provisional book decision alternatives with historical replies retained by the production pool; empirical policy elsewhere.",
          "rare_named_edges":named_rare,"input_policy":digest(POLICY),"model_identity":json.loads((OUT/"policy-cache.json").read_text())["identity"],
          "sha256":digest(out),"schema":1}
    write_json(Path(str(out)+".json"),info)
    print(json.dumps(info))


def coverage():
    """How much probability can land on an additional reply with no cached engine score?"""
    import polars as pl
    from eval_arrays import MISSING,lookup_evals,open_eval_arrays
    overlay=pl.read_parquet(OUT/"challenger-replies.parquet")
    overlay=overlay.with_columns(((pl.col("reply_observed")+30*pl.col("maia_p")) /
        (pl.col("reply_observed").sum().over("parent_hash")+30+pl.col("nonmove"))).alias("weight"))
    keys=overlay.select("child_hash").unique().sort("child_hash").to_series().to_numpy()
    known=lookup_evals(keys,*open_eval_arrays())!=MISSING
    supplementary=set()
    for f in (RUN/"training-sf.parquet",ROUND/"stop-table.parquet.boundary.parquet"):
        supplementary.update(pl.read_parquet(f,columns=["hash","cp"]).drop_nulls()["hash"].to_list())
    table=pl.DataFrame({"child_hash":keys,"eval_array":known,
                        "any_cached":known | np.isin(keys,list(supplementary))})
    overlay=overlay.join(table,on="child_hash")
    extra=overlay.filter(pl.col("total")==0)
    missing_mass=(extra.group_by("parent_hash").agg(
        pl.when(~pl.col("any_cached")).then(pl.col("weight")).otherwise(0).sum().alias("missing"))
        .join(overlay.select("parent_hash").unique(),on="parent_hash",how="right").fill_null(0))
    mass=missing_mass["missing"].to_numpy()
    report={"scope_positions":overlay["parent_hash"].n_unique(),"additional_named_or_model_only_edges":extra.height,
            "additional_edges_with_array_eval":extra["eval_array"].sum(),"additional_edges_with_any_cached_eval":extra["any_cached"].sum(),
            "additional_edges_at_least_2pct":extra.filter(pl.col("weight")>=.02).height,
            "additional_edges_at_least_2pct_no_cached_eval":extra.filter((pl.col("weight")>=.02)&~pl.col("any_cached")).height,
            "positions_with_at_least_1pct_unscored_mass":int((mass>=.01).sum()),
            "mean_unscored_mass_equal_per_position":float(mass.mean()),"p95_unscored_mass":float(np.quantile(mass,.95)),
            "definition":"Additional means no named edge in the original 20-game pool. Cached means an eval-array score or an existing training Stockfish or round-00 horizon score. This measures engine availability, not the accuracy of sampled human continuation values."}
    write_json(OUT/"boundary-coverage.json",report)
    print(json.dumps(report))


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("stage", choices=["inventory","policy","holdout","counts","counts-extra","score","matched","overlay","coverage"])
    args = cli.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    if args.stage == "counts-extra":
        counts(extra=True)
    else:
        globals()[args.stage]()
