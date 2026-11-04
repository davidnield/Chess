"""Maia-3 move-probability server over stdin/stdout (runs in the Maia venv):

    HF_HOME=D:/chess/maia/hf_cache D:/chess/maia/.venv/Scripts/python.exe -I -X utf8 \
        python/maia_server.py [--model 79m]

Each request is one JSON line: {"epd": [...], "self": [...], "oppo": [...]}; the reply is
one JSON line {"probs": [{san: p, ...}, ...]} with every legal move (SAN, check marks
stripped). Uses maia_batch's tokeniser and batching, so it matches maia3 exactly.
"""
from __future__ import annotations

import argparse
import json
import sys

import torch

sys.path.insert(0, __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0])
import maia_batch as mb  # noqa: E402


@torch.no_grad()
def answer(cfg, model, req, batch=256):  # short GPU kernels: 1024 tripped a driver timeout (2026-10-23)
    epds, se, oe = req["epd"], req["self"], req["oppo"]
    out = [None] * len(epds)
    for s in range(0, len(epds), batch):
        hs, toks, mv = mb.prep([(i, e) for i, e in enumerate(epds[s:s + batch])])
        if not len(hs):
            continue
        dev = cfg.device
        x = torch.from_numpy(toks).to(dev).float().repeat(1, 1, cfg.history)
        sel = torch.tensor(se[s:s + batch], dtype=torch.long, device=dev)
        opp = torch.tensor(oe[s:s + batch], dtype=torch.long, device=dev)
        with torch.amp.autocast("cuda", enabled=cfg.use_amp and dev.startswith("cuda")):
            lm, _lv, _ = model(x, sel, opp)
        rows = torch.tensor([r for r, _i, _s in mv], device=dev)
        idx = torch.tensor([i for _r, i, _s in mv], device=dev)
        mask = torch.zeros(lm.shape, dtype=torch.bool, device=dev)
        mask[rows, idx] = True
        p = torch.softmax(lm.float().masked_fill(~mask, float("-inf")), dim=-1)[rows, idx].cpu().tolist()
        for (r, _i, san), pp in zip(mv, p):
            d = out[s + r]
            if d is None:
                d = out[s + r] = {}
            d[san] = pp
    return [d or {} for d in out]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="79m")
    ap.add_argument("--cache-dir", default="D:/chess/maia/hf_cache/hub")
    a = ap.parse_args()
    cfg, model = mb.load(a.model, a.cache_dir)
    print(json.dumps({"ready": True}), flush=True)
    for line in sys.stdin:
        if not line.strip():
            continue
        print(json.dumps({"probs": answer(cfg, model, json.loads(line))}), flush=True)


if __name__ == "__main__":
    main()
