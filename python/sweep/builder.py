"""Prepare independent source headers and concrete commands from frozen sweep recipes."""
from pathlib import Path
from .spec import digest

def empty_source(path, *, prior):
    """A source header, with no inherited prescriptions or historical choice constraints."""
    import chess
    import polars as pl
    from consumer_keys import stored_key
    from .stages import atomic_json
    path=Path(path)
    if path.exists(): raise ValueError("source header already exists")
    path.parent.mkdir(parents=True,exist_ok=True)
    frame=pl.DataFrame([dict(position_hash=stored_key(chess.Board(),True),
            position_epd=chess.Board().epd(),side_to_move="white",best_move=None,
            value=prior,value_robust=prior,value_worst=prior,eval_score=prior,
            event="Pooled",elo_band=0)],schema_overrides={"best_move":pl.String})
    frame.write_parquet(path,compression="zstd")
    atomic_json(str(path)+".meta.json",dict(source="independent-empty-header",eval_weight=.75,
                reply_shrink=0,other_cov_blend=False,key_bits=128))

def command(arm, *, python, code, inputs, out_prefix, tmp):
    """No paths are inferred from an old canonical/experimental book."""
    r=arm["recipe"]
    if arm["id"]!=digest(r)[:20]: raise ValueError("changed arm recipe")
    required=("source","stats","aux","weak","opponent_overlay","engine_overlay")
    if any(not inputs.get(k) for k in required): raise ValueError("shared inputs are not bound")
    if r["mode"]=="reduced" and not inputs.get("allowed_book"):
        raise ValueError("reduced arm needs its fixed-prescription parent")
    args=[str(python),"-X","utf8","-u",str(Path(code)/"python/build_budget_books.py"),
          "--rep",str(inputs["source"]),"--stats",str(inputs["stats"]),
          "--aux-stats",str(inputs["aux"]),"--weak-pool",str(inputs["weak"]),
          "--opponent-prior",str(inputs["opponent_overlay"]),"--opponent-pseudogames","0",
          "--engine-candidates",str(inputs["engine_overlay"]),"--perspective",r["colour"],
          "--out-prefix",str(out_prefix),"--tmp-dir",str(tmp),"--budget",str(r["budget"]),
          "--max-ply",str(r["max_ply"]),"--min-games","1","--eval-weight",str(r["eval_weight"]),
          "--weak-min-games",str(r["weak_min_games"]),"--grid-cap",str(r["grid_cap"]),
          "--match-inflate",str(r["match_inflate"]),"--prior-strength",str(r["prior_strength"]),"--slice-prior",str(r["slice_prior"]),
          "--robustness-floor",str(r["robustness_floor"]),"--max-score-loss",str(r["max_score_loss"]),
          "--epsilon",str(r["epsilon"]),"--share-floor",str(r["share_floor"]),
          "--lookahead-floor",str(r["lookahead_floor"]),"--gated-reach",
          "--verification-policy","quality","--verification-score-band",str(r["verification"]["score_band"]),
          "--method","price" if r["colour"]=="white" else "dp"]
    if r["opponent_policy"]["name"]!="otb-ensemble-v1":
        # The control overlay uses the existing schema-2 conditional recipe.
        args[args.index("--opponent-pseudogames")+1]=str(r["opponent_policy"]["pseudogames"])
    if r["colour"]=="black": args.append("--match-distinct")
    if r["mode"]=="reduced": args.extend(["--allowed-book",str(inputs["allowed_book"])])
    return args
