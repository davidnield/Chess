"""One Stockfish for every tool (engine_path.py, David 2026-11-21: Stockfish 19 everywhere).

Every module that launches Stockfish resolves to engine_path.STOCKFISH; no Python file names another
Stockfish binary; the default caches that carry no engine record are engine-specific files; and the
configured binary answers with the expected UCI name (skipped where the binary is absent).
"""
import re
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import engine_path  # noqa: E402


class EnginePath(unittest.TestCase):
    def test_modules_share_the_setting(self):
        import convertibility_eval
        import engine_candidates
        import extend_book
        import transposition_search
        import validate_book
        for name, value in (("engine_candidates.SF", engine_candidates.SF), ("extend_book.SF", extend_book.SF),
                            ("validate_book.SF", validate_book.SF),
                            ("convertibility_eval.SF_PATH", convertibility_eval.SF_PATH),
                            ("transposition_search.SF", transposition_search.SF)):
            self.assertEqual(value, engine_path.STOCKFISH, name)

    def test_no_other_binary_is_named(self):
        bad = re.compile(r"WinGet|stockfish[^\"'\s]*\.exe|default=\"stockfish\"|default='stockfish'", re.I)
        hits = []
        for f in HERE.rglob("*.py"):
            if f.name in {"engine_path.py", Path(__file__).name}:
                continue
            for i, line in enumerate(f.read_text(encoding="utf8", errors="replace").splitlines(), 1):
                if bad.search(line) and "stockfish*.exe" not in line:
                    hits.append(f"{f.relative_to(HERE)}:{i}: {line.strip()}")
        self.assertEqual(hits, [])

    def test_engineless_caches_are_engine_specific(self):
        import annotate.build_annotation_facts as facts
        import annotate.engine_pass as ep
        self.assertTrue(str(ep.CACHE).endswith("annotation_evals_sf19.parquet"))
        self.assertEqual(Path(facts.DEFAULT_ENGINE_CACHE), Path(ep.CACHE))

    def test_binary_identity(self):
        if not Path(engine_path.STOCKFISH).exists():
            self.skipTest(f"no engine at {engine_path.STOCKFISH}")
        import chess.engine
        e = chess.engine.SimpleEngine.popen_uci(engine_path.STOCKFISH)
        try:
            self.assertEqual(e.id["name"], engine_path.STOCKFISH_NAME)
        finally:
            e.quit()


if __name__ == "__main__":
    unittest.main()
