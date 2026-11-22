"""The one Stockfish every tool runs (David, 2026-11-21: Stockfish 19 everywhere).

D:/chess/bin/stockfish-19 holds the official sf_19 release (stockfish-windows-x86-64-universal,
sha256 45bc8e49...; PROVENANCE.txt there). CHESS_STOCKFISH overrides the path, e.g. on another
machine. Every engine cache already pins the engine it was filled with (binary sha256 in the search
contracts, UCI name in the referee cache's sidecar), so Stockfish 18 results are never reused under
this engine: they either refuse to load or live under another key. Caches without such a record use
engine-specific default file names (see the callers).
"""
from __future__ import annotations

import os

STOCKFISH = os.environ.get("CHESS_STOCKFISH",
                           "D:/chess/bin/stockfish-19/stockfish-windows-x86-64-universal.exe")
STOCKFISH_NAME = "Stockfish 19"     # the UCI `id name` expected from STOCKFISH
