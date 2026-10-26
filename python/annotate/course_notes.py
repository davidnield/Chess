"""Course comments that describe OUR plan, for repertoire annotations.

Opening courses (E:/chess/courses, built by course_ingest.py) comment on moves, but a
comment at one of our positions is only about our plan if the course goes on to play
our line. A comment on our move is kept only when the course's own move tree (main
line or sidelines) continues with our book's continuation for at least `min_agree`
plies after our move; the plies it follows are reported with it.

Course text is copyrighted: it is input for paraphrase in local annotations only,
never shipped anywhere.

comments.parquet keys a comment by the position AFTER the move it follows
(position_hash) plus that move (move_san).
"""

from __future__ import annotations

from collections import defaultdict

import polars as pl

COURSES = "E:/chess/courses"


class CourseNotes:
    def __init__(self, targets: dict[int, tuple[str, list[str]]], courses_dir: str = COURSES,
                 min_agree: int = 4, min_chars: int = 60, max_chars: int = 600):
        """targets: position-after-our-move hash -> (our move SAN, our continuation
        after it, as SANs)."""
        self.min_agree, self.max_chars = min_agree, max_chars
        cm = (pl.scan_parquet(f"{courses_dir}/comments.parquet")
                .filter(pl.col("position_hash").is_in(list(targets))
                        & (pl.col("text").str.len_chars() >= min_chars))
                .collect())
        cm = cm.filter(pl.struct("position_hash", "move_san").map_elements(
            lambda r: targets[r["position_hash"]][0] == r["move_san"], return_dtype=pl.Boolean))
        self.notes: dict[int, list[dict]] = defaultdict(list)
        if cm.is_empty():
            return
        items = cm.select("course_id", "file_idx", "item_idx").unique()
        ed = (pl.scan_parquet(f"{courses_dir}/edges.parquet")
                .select("course_id", "file_idx", "item_idx", "node_id", "parent_node", "move_san")
                .join(items.lazy(), on=["course_id", "file_idx", "item_idx"])
                .collect())
        kids: dict[tuple, dict[int, dict[str, int]]] = defaultdict(lambda: defaultdict(dict))
        for r in ed.iter_rows(named=True):
            kids[(r["course_id"], r["file_idx"], r["item_idx"])][r["parent_node"]][r["move_san"]] = r["node_id"]
        names = dict(pl.read_parquet(f"{courses_dir}/courses.parquet",
                                     columns=["course_id", "course_name"]).iter_rows())
        for r in cm.iter_rows(named=True):
            key = (r["course_id"], r["file_idx"], r["item_idx"])
            cont = targets[r["position_hash"]][1]
            n, k = r["node_id"], 0
            for san in cont:                       # walk our line through the course tree
                nxt = kids[key].get(n, {}).get(san)
                if nxt is None:
                    break
                n, k = nxt, k + 1
            if k >= min(self.min_agree, len(cont)) and k >= 2:
                self.notes[r["position_hash"]].append({
                    "course": names.get(r["course_id"], r["course_id"]),
                    "follows_our_line_plies": k,
                    "text": r["text"][:max_chars],
                })
        for h, v in self.notes.items():
            v.sort(key=lambda d: (-d["follows_our_line_plies"], -len(d["text"])))
            seen, uniq = set(), []
            for d in v:                            # duplicate editions repeat a comment
                if d["text"][:80] not in seen:
                    seen.add(d["text"][:80])
                    uniq.append(d)
            self.notes[h] = uniq

    def get(self, after_hash: int, limit: int = 2) -> list[dict]:
        return self.notes.get(after_hash, [])[:limit]
