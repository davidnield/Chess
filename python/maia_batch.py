"""Fast batch Maia-3 move probabilities for many positions (runs in the Maia venv):

    HF_HOME=D:/chess/maia/hf_cache D:/chess/maia/.venv/Scripts/python.exe -I -X utf8 \
        python/maia_batch.py --input POS.parquet --out OUT.parquet \
        --self-elo 2000 --oppo-elo 2000 [--model 79m] [--workers 6] [--min-p 0.002]

--input holds position_hash and epd (four-field EPD). Worker processes parse each
position, list its legal moves (SAN, check marks stripped, and the model's vocabulary
index) and tokenise it in numpy exactly as maia3.dataset does (12 piece channels,
board mirrored when Black is to move, the single position repeated as its 8-ply
history); the GPU runs the batches. Writes OUT (position_hash, san, p) keeping moves
with p >= --min-p, and OUT with _wdl (position_hash, win, draw, loss for the side to
move). --check N compares N positions against maia3's own code path first.
"""
from __future__ import annotations

import argparse
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import chess
import numpy as np
import polars as pl
import torch
from torch.amp import autocast

PROMO = {"q": 0, "r": 1, "b": 2, "n": 3}


def vocab_index(uci: str) -> int:
    """maia3.utils.get_all_possible_moves order, for a move already in White's view."""
    f, t = chess.parse_square(uci[:2]), chess.parse_square(uci[2:4])
    if len(uci) == 5:
        return 4096 + ((f % 8) * 8 + (t % 8)) * 4 + PROMO[uci[4]]
    return f * 64 + t


def mirror_uci(uci: str) -> str:
    return uci[0] + str(9 - int(uci[1])) + uci[2] + str(9 - int(uci[3])) + uci[4:]


def prep(chunk):
    """(hashes, tokens uint8 (n, 64, 12), move lists [(row, vocab_idx, san)])"""
    hs, toks, mv = [], [], []
    for row, (h, epd) in enumerate(chunk):
        b = chess.Board(epd + " 0 1")
        view = b.mirror() if b.turn == chess.BLACK else b
        t = np.zeros((64, 12), np.uint8)
        for sq, pc in view.piece_map().items():
            t[sq, pc.piece_type - 1 + (6 if pc.color == chess.BLACK else 0)] = 1
        hs.append(h)
        toks.append(t)
        for m in b.legal_moves:
            u = m.uci() if b.turn == chess.WHITE else mirror_uci(m.uci())
            mv.append((row, vocab_index(u), b.san(m).rstrip("+#")))
    return hs, np.stack(toks) if toks else np.zeros((0, 64, 12), np.uint8), mv


def load(model_name: str, cache_dir: str):
    from maia3.model_registry import resolve_checkpoint_path
    from maia3.uci import load_model, parse_args
    cfg = parse_args(["--model", f"maia3-{model_name}", "--local-files-only", "--cache-dir", cache_dir])
    cfg.checkpoint_path = resolve_checkpoint_path(
        cfg.model_spec, checkpoint_filename=None, cache_dir=cache_dir, revision=None,
        local_files_only=True, force_download=False, token=None)
    return cfg, load_model(cfg)


@torch.no_grad()
def run_batch(cfg, model, toks, mv, self_elo, oppo_elo):
    dev = cfg.device
    n = toks.shape[0]
    x = torch.from_numpy(toks).to(dev).float().repeat(1, 1, cfg.history)       # (n, 64, 96)
    se = torch.full((n,), self_elo, dtype=torch.long, device=dev)
    oe = torch.full((n,), oppo_elo, dtype=torch.long, device=dev)
    with autocast("cuda", enabled=cfg.use_amp and dev.startswith("cuda")):
        lm, lv, _ = model(x, se, oe)
    rows = torch.tensor([r for r, _i, _s in mv], device=dev)
    idx = torch.tensor([i for _r, i, _s in mv], device=dev)
    mask = torch.zeros(lm.shape, dtype=torch.bool, device=dev)
    mask[rows, idx] = True
    p = torch.softmax(lm.float().masked_fill(~mask, float("-inf")), dim=-1)[rows, idx]
    wdl = torch.softmax(lv.float(), dim=-1)                                       # [loss, draw, win]
    return p.cpu().numpy(), wdl.cpu().numpy()


def check(cfg, model, recs, self_elo, oppo_elo):
    """The fast path against maia3's own tokenisation and legal mask."""
    from collections import deque
    from maia3.dataset import get_historical_tokens, get_legal_moves_mask, tokenize_board
    from maia3.utils import get_all_possible_moves
    vidx = {m: i for i, m in enumerate(get_all_possible_moves())}
    hs, toks, mv = prep(recs)
    p_fast, _ = run_batch(cfg, model, toks, mv, self_elo, oppo_elo)
    worst = 0.0
    for row, (_h, epd) in enumerate(recs):
        b = chess.Board(epd + " 0 1")
        t = get_historical_tokens(deque([tokenize_board(b)]), cfg, 0.0, 0.0, 0.0, 0.0)
        ref = torch.from_numpy(toks[row]).float().repeat(1, cfg.history)
        assert torch.equal(t[:, :96], ref), f"token mismatch at {epd}"
        m = get_legal_moves_mask(b, vidx)
        mine = {i for r, i, _s in mv if r == row}
        assert set(torch.nonzero(m).flatten().tolist()) == mine, f"legal-mask mismatch at {epd}"
    return len(recs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--self-elo", type=int, required=True)
    ap.add_argument("--oppo-elo", type=int, required=True)
    ap.add_argument("--model", default="79m")
    ap.add_argument("--cache-dir", default="D:/chess/maia/hf_cache/hub")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--batch", type=int, default=1024)
    ap.add_argument("--min-p", type=float, default=0.002)
    ap.add_argument("--check", type=int, default=200)
    a = ap.parse_args()
    pos = pl.read_parquet(a.input, columns=["position_hash", "epd"]).unique("position_hash")
    recs = pos.rows()
    cfg, model = load(a.model, a.cache_dir)
    if a.check:
        print(f"checked {check(cfg, model, recs[:a.check], a.self_elo, a.oppo_elo)} positions "
              f"against maia3's tokeniser and legal mask", flush=True)
    t0 = time.time()
    out_h, out_s, out_p, wdl_rows = [], [], [], []
    chunks = [recs[i:i + a.batch] for i in range(0, len(recs), a.batch)]
    with Pool(a.workers) as pool:
        for k, (hs, toks, mv) in enumerate(pool.imap(prep, chunks, chunksize=1)):
            if not len(hs):
                continue
            p, wdl = run_batch(cfg, model, toks, mv, a.self_elo, a.oppo_elo)
            for (r, _i, san), pp in zip(mv, p):
                if pp >= a.min_p:
                    out_h.append(hs[r])
                    out_s.append(san)
                    out_p.append(float(pp))
            for h, w in zip(hs, wdl):
                wdl_rows.append((h, float(w[2]), float(w[1]), float(w[0])))
            if k % 200 == 0:
                done = min((k + 1) * a.batch, len(recs))
                print(f"  {done:,}/{len(recs):,} positions ({done / (time.time() - t0):,.0f}/s)",
                      flush=True)
    pl.DataFrame({"position_hash": out_h, "san": out_s, "p": out_p},
                 schema={"position_hash": pl.Int64, "san": pl.Utf8, "p": pl.Float32}).write_parquet(a.out)
    pl.DataFrame(wdl_rows, schema={"position_hash": pl.Int64, "win": pl.Float32, "draw": pl.Float32,
                                   "loss": pl.Float32}, orient="row").write_parquet(
        str(a.out).replace(".parquet", "_wdl.parquet"))
    print(f"{len(recs):,} positions, {len(out_h):,} moves kept in {time.time() - t0:,.0f}s "
          f"({len(recs) / (time.time() - t0):,.0f}/s) -> {a.out}")


if __name__ == "__main__":
    sys.exit(main())
