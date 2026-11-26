"""Own post-book policy calibration (plan 2026-11-26, step 5).

Question: which of model_contract.calibration_grid()'s 108 transforms best predicts the move a
1000-1799 MOVER actually plays at their own turn in held-out games, given the training weak counts
(game-mean bands 1000-1799, 2013-01..2026-07) and Maia-3?

Stages, each atomic and receipted under --out (a stage is skipped when its receipt matches, and a
changed identity is refused: use a new --out):
  panel   Held-out games, Blitz/Rapid/Classical, no BOT titles, sampled by blake2b(game_id) below a
          per-(month, event) fraction. The first month is development, the second confirmation. Every
          move at ply <= cap whose mover is rated in [lo, hi) is a row: game, split, ply, colour,
          mover/opponent rating, event, opening family, board (FEN4 + full key), played move (UCI).
  counts  Unique boards -> complete legal weak-count vectors (human_providers.count_vector: full key
          and child replay checked) from the published weak pool.
  maia    Unique boards x the six rating pairs -> raw Maia-3 vectors (MaiaPolicy and its raw cache).
  score   Every transform's probability of every played move; tables; the pre-registered selection on
          development; the paired confirmation; the contamination study.

Pre-registered on 26 Nov 2026, before any score was computed:
  - Metric: mean negative log-likelihood (nats) per played move.
  - Selection set: development moves at positions whose training support n < 1000 (above that every
    transform is essentially the counts, so including them only dilutes the comparison).
  - Candidates: transforms with own rating 1200 or 1450 (David, 26 Nov: model our side weaker than
    current). The best 1700 transform and the best overall are reported beside the choice.
  - Confirmation: on the second month, chosen minus each alternative, paired per move, with a
    bootstrap that resamples whole opening families.
  - Contamination (David's in-book concern): a mover is "off book" at a position once an earlier own
    move in the same game had training share < 5% (or no training games). At positions with n >= 20
    played by both groups, compare how often each plays the training-modal move and the count
    policy's log-likelihood. A lower modal share off book is the contamination the cap is meant for.
    The flag is defined from the counts themselves, so it also marks idiosyncratic players; read the
    difference as an upper bound on the book effect.
Everything here is diagnostic: nothing is promoted automatically.
"""
from __future__ import annotations

import argparse
from hashlib import blake2b
import json
import math
from pathlib import Path
import time

import chess
import numpy as np
import pandas as pd

from .model_contract import calibration_grid
from .spec import digest
from .stages import atomic_json, file_sha

SPLITS = ("development", "confirmation")
EVENTS = ("Blitz", "Rapid", "Classical")
PAIRS = [(a, b) for a in (1200, 1450, 1700) for b in (1800, 2200)]
SUPPORT_BINS = [(0, 0), (1, 9), (10, 99), (100, 999), (1000, None)]
OFF_BOOK_SHARE = 0.05


def sampled(game_id: str, fraction: float) -> bool:
    h = blake2b(game_id.encode(), digest_size=8, person=b"owncal").digest()
    return int.from_bytes(h, "little") / 2**64 < fraction


def fen4(board):
    return " ".join(board.fen(en_passant="fen").split()[:4])


def panel_rows(games, *, cap, lo, hi):
    """games: dicts with game_id, split, movetext, white_elo, black_elo, event, opening_family."""
    from consumer_keys import stored_key
    from stage1_extract_positions import iter_san_moves
    rows, bad = [], 0
    for g in games:
        board = chess.Board()
        try:
            for ply, san in enumerate(iter_san_moves(g["movetext"]), 1):
                if ply > cap:
                    break
                move = board.parse_san(san)
                white = board.turn == chess.WHITE
                mover, opp = (g["white_elo"], g["black_elo"]) if white else (g["black_elo"], g["white_elo"])
                if lo <= mover < hi:
                    k = stored_key(board, True)
                    rows.append(dict(game_id=g["game_id"], split=g["split"], ply=ply,
                                     colour="white" if white else "black", mover_elo=int(mover),
                                     opp_elo=int(opp), event=g["event"],
                                     family=g.get("opening_family") or "?", fen=fen4(board),
                                     lo=k["lo"], hi=k["hi"], move=move.uci()))
                board.push(move)
        except ValueError:
            bad += 1   # a malformed game keeps the rows before the bad token
    return rows, bad


def read_games(source: Path, months, per_event: int):
    """Hash-sampled games from the canonical source, about per_event per (month, event)."""
    import pyarrow.parquet as pq
    cols = ["game_id", "movetext", "white_elo", "black_elo", "white_title", "black_title", "opening_family"]
    out, fractions = [], {}
    for split, (year, month) in zip(SPLITS, months):
        for event in EVENTS:
            files = sorted((source / f"year={year}" / f"month={month}" / f"event={event}").glob("*.parquet"))
            if not files:
                raise ValueError(f"no source files for {year}-{month} {event}")
            total = sum(pq.read_metadata(f).num_rows for f in files)
            frac = min(1.0, per_event / total)
            fractions[f"{year}-{month:02d} {event}"] = dict(rows=total, fraction=frac)
            for f in files:
                for batch in pq.ParquetFile(f).iter_batches(columns=cols, batch_size=250_000):
                    t = batch.to_pydict()
                    for i, gid in enumerate(t["game_id"]):
                        if not sampled(gid, frac):
                            continue
                        if "BOT" in (t["white_title"][i], t["black_title"][i]):
                            continue
                        out.append(dict(game_id=gid, split=split, event=event, movetext=t["movetext"][i],
                                        white_elo=t["white_elo"][i], black_elo=t["black_elo"][i],
                                        opening_family=t["opening_family"][i]))
    return out, fractions


def boards_of(panel: pd.DataFrame) -> pd.DataFrame:
    # The key holds the en-passant square only when a capture is pseudo-legal; FEN writes it after
    # every double push. So one key may carry two FEN strings, which must be the same position.
    legal_fen = panel.fen.map(lambda f: board_of(f).fen(en_passant="legal"))
    b = (panel.assign(_same=legal_fen).groupby(["lo", "hi"], sort=True)
         .agg(fen=("fen", "first"), positions=("_same", "nunique")).reset_index())
    if (b.positions != 1).any():
        raise ValueError("one full key with two positions")
    return b.drop(columns="positions").assign(board=np.arange(len(b)))


def board_of(fen):
    return chess.Board(fen + " 0 1")


def count_stage(boards: pd.DataFrame, fetch, batch=2000):
    """fetch(list[chess.Board]) -> per board the weak pool rows (WeakCounts.fetch)."""
    from .human_providers import count_vector
    legal, counts = [], []
    for s in range(0, len(boards), batch):
        bs = [board_of(f) for f in boards.fen.iloc[s:s + batch]]
        recs = fetch(bs)
        if len(recs) != len(bs):
            raise ValueError("weak count batch differs")
        for b, r in zip(bs, recs):
            l, c = count_vector(b, r)
            legal.append(l)
            counts.append(c)
    return boards.assign(legal=legal, counts=counts)


def maia_stage(boards: pd.DataFrame, predict, batch=1024):
    """predict(list[chess.Board]) -> raw {uci: p} over the legal moves, in board order."""
    raw = []
    for s in range(0, len(boards), batch):
        bs = [board_of(f) for f in boards.fen.iloc[s:s + batch]]
        got = predict(bs)
        if len(got) != len(bs):
            raise ValueError("Maia batch differs")
        for legal, q in zip(boards.legal.iloc[s:s + batch], got):
            if set(q) != set(legal):
                raise ValueError("Maia vector does not cover the legal moves")
            raw.append([float(q[u]) for u in legal])
    return raw


def played_index(panel, boards):
    """Per panel row: board index, played move's index in that board's legal list."""
    pos = {(lo, hi): i for lo, hi, i in zip(boards.lo, boards.hi, boards.board)}
    bi = np.array([pos[(lo, hi)] for lo, hi in zip(panel.lo, panel.hi)])
    idx = []
    for b, m in zip(bi, panel.move):
        legal = boards.legal.iloc[b]
        j = legal.index(m) if m in legal else -1
        if j < 0:
            raise ValueError("played move is not legal on its board")
        idx.append(j)
    return bi, np.array(idx)


def own_probability(c, n, q, strength, cap):
    """Vectorised book_builder_rs.own_policy at the played move: (c*s + K*q)/(n*s + K), s = cap/n
    when n > cap. Checked against the native function in score()."""
    s = np.ones_like(n) if cap is None else np.where(n > cap, cap / np.maximum(n, 1e-300), 1.0)
    return (c * s + strength * q) / (n * s + strength)


def _bin(n):
    for lo, hi in SUPPORT_BINS:
        if n >= lo and (hi is None or n <= hi):
            return f"{lo}+" if hi is None else (f"{lo}" if lo == hi else f"{lo}-{hi}")


def family_bootstrap(delta, families, reps=5000, seed=20261126):
    ids, inv = np.unique(np.asarray(families), return_inverse=True)
    sums = np.bincount(inv, weights=delta, minlength=len(ids))
    cnts = np.bincount(inv, minlength=len(ids)).astype(float)
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(ids), size=(reps, len(ids)))
    m = sums[draws].sum(1) / cnts[draws].sum(1)
    return dict(mean=float(delta.mean()), ci95=[float(np.quantile(m, .025)), float(np.quantile(m, .975))],
                families=int(len(ids)), moves=int(len(delta)))


def score(panel, boards, raw, *, check_native=200):
    """raw: {(a, b): list of per-board raw vectors}. Returns (per-transform table, row NLL matrix)."""
    from book_builder_rs import own_policy, prediction_temperature
    bi, mi = played_index(panel, boards)
    cnt = [np.asarray(c) for c in boards.counts]
    n_b = np.array([c.sum() for c in cnt])
    c_row = np.array([cnt[b][j] for b, j in zip(bi, mi)])
    n_row = n_b[bi]
    grid = calibration_grid()
    q_row = {}
    for pair in PAIRS:
        for t in sorted({g["temperature"] for g in grid}):
            qb = [prediction_temperature(list(v), t) for v in raw[pair]]
            q_row[(pair, t)] = np.array([qb[b][j] for b, j in zip(bi, mi)])
    rng = np.random.default_rng(7)
    nll = np.empty((len(panel), len(grid)))
    for k, g in enumerate(grid):
        pair, cap = tuple(g["ratings"]), g.get("observation_cap")
        p = own_probability(c_row, n_row, q_row[(pair, g["temperature"])], g["strength"], cap)
        for r in rng.choice(len(panel), size=min(check_native, len(panel)), replace=False):
            q = prediction_temperature(list(raw[pair][bi[r]]), g["temperature"])
            native = own_policy(list(cnt[bi[r]]), q, g["strength"], cap)[mi[r]]
            if not math.isclose(native, p[r], rel_tol=1e-12, abs_tol=1e-15):
                raise ValueError("vectorised own probability differs from the native policy")
        if (p <= 0).any():
            raise ValueError("a played move has zero own probability")
        nll[:, k] = -np.log(p)
    return grid, nll, n_row, c_row, bi, mi


def summarise(panel, grid, nll, n_row):
    dev = (panel.split == "development").to_numpy()
    sel = dev & (n_row < 1000)
    band = np.where(panel.mover_elo < 1400, "1000-1399", "1400-1799")
    bins = np.array([_bin(v) for v in n_row])
    names = sorted(set(bins), key=lambda x: int(x.split("-")[0].rstrip("+")))
    rows = []
    for k, g in enumerate(grid):
        r = dict(transform=k, own_rating=g["ratings"][0], opp_rating=g["ratings"][1],
                 temperature=g["temperature"], strength=g["strength"], cap=g.get("observation_cap"),
                 selection_nll=float(nll[sel, k].mean()), dev_all_nll=float(nll[dev, k].mean()))
        for s in names:
            m = dev & (bins == s)
            r[f"dev_n_{s}"] = float(nll[m, k].mean()) if m.any() else None
        for b in ("1000-1399", "1400-1799"):
            m = sel & (band == b)
            r[f"sel_mover_{b}"] = float(nll[m, k].mean()) if m.any() else None
        rows.append(r)
    return pd.DataFrame(rows)


def select_and_confirm(panel, grid, nll, n_row, table):
    conf = ((panel.split == "confirmation").to_numpy()) & (n_row < 1000)
    allowed = table[table.own_rating.isin([1200, 1450])]
    chosen = int(allowed.sort_values("selection_nll")["transform"].iloc[0])
    alts = {"best 1700": int(table[table.own_rating == 1700].sort_values("selection_nll")["transform"].iloc[0]),
            "best overall": int(table.sort_values("selection_nll")["transform"].iloc[0])}
    g = grid[chosen]
    other = 1450 if g["ratings"][0] == 1200 else 1200
    alts[f"best {other}"] = int(table[table.own_rating == other].sort_values("selection_nll")["transform"].iloc[0])
    for k, h in enumerate(grid):
        same = all(h[x] == g[x] for x in ("ratings", "temperature", "strength"))
        if same and h.get("observation_cap") != g.get("observation_cap"):
            alts[f"cap {h.get('observation_cap')}"] = k
    fam = panel.family.to_numpy()[conf]
    confirmation = {}
    for name, k in alts.items():
        if k == chosen:
            confirmation[name] = "same as chosen"
            continue
        confirmation[name] = dict(transform=k, recipe=grid[k],
                                  **family_bootstrap(nll[conf, chosen] - nll[conf, k], fam))
    return dict(chosen=chosen, recipe=g, development_selection_nll=float(table.selection_nll[chosen]),
                confirmation_nll=float(nll[conf, chosen].mean()), confirmation_moves=int(conf.sum()),
                chosen_minus_alternative=confirmation)


def contamination(panel, boards, n_row, c_row, bi, mi, nll_chosen):
    p = panel.assign(n=n_row, c=c_row, b=bi, nll=nll_chosen).sort_values(["game_id", "colour", "ply"])
    share = np.where(p.n > 0, p.c / np.maximum(p.n, 1), 0.0)
    rare = (share < OFF_BOOK_SHARE).astype(int)
    # off book at a move = some EARLIER own move in this game was rare
    prior = p.assign(rare=rare).groupby(["game_id", "colour"]).rare.transform(lambda s: s.shift(fill_value=0).cummax())
    p = p.assign(off=prior.astype(bool))
    cnt = [np.asarray(c) for c in boards.counts]
    modal = np.array([int(np.argmax(cnt[b])) for b in p.b])
    L = np.array([len(cnt[b]) for b in p.b])
    count_nll = -np.log((p.c + 0.5) / (p.n + 0.5 * L))
    p = p.assign(modal_hit=(modal == mi[p.index.to_numpy()]).astype(float), count_nll=count_nll.to_numpy())
    q = p[p.n >= 20]
    both = q.groupby("b").off.agg(lambda s: s.any() and not s.all())
    q = q[q.b.isin(both[both].index)]
    out = dict(rows=int(len(p)), off_book_rows=int(p.off.sum()), matched_positions=int(both.sum()),
               matched_rows=int(len(q)))
    if q.empty:
        return out   # no position with >= 20 training games was reached by both groups
    for name in ("modal_hit", "count_nll", "nll"):
        g = q.groupby(["b", "off"])[name].mean().unstack()
        w = q.groupby("b").size().loc[g.index].to_numpy()
        d = (g[True] - g[False]).to_numpy()
        fam = q.groupby("b").family.first().loc[g.index].to_numpy()
        out[name] = dict(on_book=float(q[~q.off][name].mean()), off_book=float(q[q.off][name].mean()),
                         matched_off_minus_on=family_bootstrap(np.repeat(d, w), np.repeat(fam, w)))
    return out


# ── stages with receipts ──────────────────────────────────────────────────────

def _stage(out: Path, name: str, identity: dict, outputs, work):
    receipt = out / f"{name}.receipt.json"
    ident = digest(identity)
    if receipt.exists():
        r = json.loads(receipt.read_text(encoding="utf8"))
        if r["identity"] != ident:
            raise ValueError(f"{name}: inputs or settings changed; use a new --out")
        for o in outputs:
            if file_sha(out / o) != r["files"][o]:
                raise ValueError(f"{name}: output {o} changed after its receipt")
        return r
    t = time.time()
    extra = work() or {}
    r = dict(stage=name, identity=ident, settings=identity, seconds=round(time.time() - t, 1),
             files={o: file_sha(out / o) for o in outputs}, **extra)
    atomic_json(receipt, r)
    return r


def _write_parquet(frame: pd.DataFrame, path: Path):
    tmp = path.with_name(path.name + ".tmp")
    frame.to_parquet(tmp, index=False)
    tmp.replace(path)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stage", choices=["panel", "counts", "maia", "score", "all"])
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--source", type=Path, default=Path("D:/data/chess/standard-chess-games-compressed"))
    ap.add_argument("--months", default="2026-8,2026-9", help="development,confirmation")
    ap.add_argument("--per-event", type=int, default=10_000, help="games per (month, event)")
    ap.add_argument("--cap", type=int, default=30)
    ap.add_argument("--mover-elo", default="1000:1800")
    ap.add_argument("--weak-pool", type=Path, help="the weak count pool's pool.json")
    ap.add_argument("--maia-python", type=Path, default=Path("D:/chess/maia/.venv/Scripts/python.exe"))
    ap.add_argument("--maia-checkpoint", type=Path, default=Path(
        "D:/chess/maia/hf_cache/hub/models--UofTCSSLab--Maia3-79M/snapshots/"
        "a107d6ceb7b298cb04ae1da4edffe2939858b894/maia3-79m.pt"))
    a = ap.parse_args()
    out = a.out
    out.mkdir(parents=True, exist_ok=True)
    months = [tuple(map(int, m.split("-"))) for m in a.months.split(",")]
    lo, hi = map(int, a.mover_elo.split(":"))
    if len(months) != 2:
        ap.error("--months takes the development and the confirmation month")
    stages = ["panel", "counts", "maia", "score"] if a.stage == "all" else [a.stage]
    base = dict(months=months, per_event=a.per_event, cap=a.cap, mover_elo=[lo, hi],
                source=str(a.source), code=file_sha(__file__))

    if "panel" in stages:
        def work():
            games, fractions = read_games(a.source, months, a.per_event)
            rows, bad = panel_rows(games, cap=a.cap, lo=lo, hi=hi)
            panel = pd.DataFrame(rows)
            _write_parquet(panel, out / "panel.parquet")
            _write_parquet(boards_of(panel)[["board", "lo", "hi", "fen"]], out / "boards.parquet")
            return dict(games=len(games), malformed_games=bad, rows=len(panel), fractions=fractions)
        print(_stage(out, "panel", base, ["panel.parquet", "boards.parquet"], work))
    panel_sha = lambda: json.loads((out / "panel.receipt.json").read_text())["files"]

    if "counts" in stages:
        if not a.weak_pool:
            ap.error("counts needs --weak-pool")
        ident = dict(panel=panel_sha(), weak_pool=str(a.weak_pool), weak_pool_sha256=file_sha(a.weak_pool))
        def work():
            from .human_providers import WeakCounts
            boards = pd.read_parquet(out / "boards.parquet")
            weak = WeakCounts(a.weak_pool)
            try:
                got = count_stage(boards, weak.fetch)
            finally:
                weak.close()
            _write_parquet(got, out / "counts.parquet")
            return dict(boards=len(got), unseen=int(sum(sum(c) == 0 for c in got.counts)))
        print(_stage(out, "counts", ident, ["counts.parquet"], work))

    if "maia" in stages:
        ident = dict(counts=json.loads((out / "counts.receipt.json").read_text())["files"],
                     checkpoint_sha256=file_sha(a.maia_checkpoint), pairs=PAIRS)
        names = [f"maia-{x}-{y}.parquet" for x, y in PAIRS]
        def work():
            from .maia_policy import MaiaPolicy, runtime_identity
            boards = pd.read_parquet(out / "counts.parquet")
            runtime = runtime_identity(a.maia_python)
            for (x, y), name in zip(PAIRS, names):
                if (out / name).exists():
                    continue   # finished pair from an interrupted stage; the receipt hashes it
                cfg = dict(python=str(a.maia_python), checkpoint=str(a.maia_checkpoint),
                           raw_cache=str(out / f"maia-raw-{x}-{y}.sqlite"))
                pol = MaiaPolicy(cfg, dict(ratings=[x, y], temperature=1.0), runtime=runtime)
                try:
                    raw = maia_stage(boards, lambda bs: pol.raw_cache.predict(bs, pol.infer_raw))
                finally:
                    pol.close()
                _write_parquet(pd.DataFrame(dict(board=boards.board, raw=raw)), out / name)
                print(f"maia {x}/{y}: {len(raw):,} boards", flush=True)
            return dict(runtime=runtime)
        print(_stage(out, "maia", ident, names, work))

    if "score" in stages:
        ident = dict(maia=json.loads((out / "maia.receipt.json").read_text())["files"],
                     counts=json.loads((out / "counts.receipt.json").read_text())["files"],
                     grid=digest(calibration_grid()))
        def work():
            panel = pd.read_parquet(out / "panel.parquet")
            boards = pd.read_parquet(out / "counts.parquet")
            boards["legal"] = boards.legal.map(list)
            boards["counts"] = boards.counts.map(list)
            raw = {(x, y): list(pd.read_parquet(out / f"maia-{x}-{y}.parquet").raw.map(list)) for x, y in PAIRS}
            grid, nll, n_row, c_row, bi, mi = score(panel, boards, raw)
            table = summarise(panel, grid, nll, n_row)
            table.to_csv(out / "transforms.csv", index=False)
            result = select_and_confirm(panel, grid, nll, n_row, table)
            result["contamination"] = contamination(panel, boards, n_row, c_row, bi, mi, nll[:, result["chosen"]])
            atomic_json(out / "report.json", result)
            return dict(chosen=result["chosen"])
        print(_stage(out, "score", ident, ["transforms.csv", "report.json"], work))


if __name__ == "__main__":
    main()
