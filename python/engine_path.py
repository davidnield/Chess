"""The one Stockfish every tool runs: Stockfish 18 (David, 2026-11-22; Stockfish 19 for one day before).

Why not Stockfish 19: at the same depth its centipawn scores are ~1.47x Stockfish 18's, and Stockfish
18 is on the scale of everything else the project measures in centipawns (the eval DB's cloud and
fishnet scores, the fitted eval-to-score curve, the 30/60 cp soundness limits, the critical-move gap,
the extension severities). Mixing SF19 searches into any of them would be off by that factor.
Measured 2026-11-22 (docs/overnight-log-2026-11-22.md; scratch/chessdb_import/sf_scale_check.py).

D:/chess/bin/stockfish-18 holds a copy of the WinGet Stockfish 18 that every tool used until
2026-11-21 (same sha256 c86215fa..., PROVENANCE.txt there), so every cache that pins the binary's
sha256 stays valid. CHESS_STOCKFISH overrides the path, e.g. on another machine. Every engine cache
pins the engine it was filled with (binary sha256 in the search contracts, UCI name in the referee
cache's sidecar), so another engine's results are never reused: they refuse to load or live under
another key. The Stockfish 19 binary stays pinned under D:/chess/bin/stockfish-19, and its one day of
results in the *_sf19 cache files stays apart.
"""
from __future__ import annotations

import os

STOCKFISH = os.environ.get("CHESS_STOCKFISH",
                           "D:/chess/bin/stockfish-18/stockfish-windows-x86-64-avx2.exe")
STOCKFISH_NAME = "Stockfish 18"     # the UCI `id name` expected from STOCKFISH
