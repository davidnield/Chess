"""How our players move at one board, read by full key from the merged explorer book.

The extension search's player-severity term (``extensions.SEVERITY_MODES``) needs our side's move
counts at positions past the pools' ply-30 cap, so it reads the ply-51 book directly instead of a
pool: the recipe's own post-book population (``own_policy`` -> the weak pool recipe's events and
game-mean bands), summed over every ply as the explorer's counts are.

Book read contract (book README): bucket = low half mod 512; each slice file is sorted by
(parent_hash, parent_hash_hi), so a key touches at most a few row groups. Row groups are pruned on
the low half's min/max statistics only to exclude impossible groups (missing statistics keep the
group); rows then match on BOTH halves, so a 64-bit collision twin never contributes.
"""
from __future__ import annotations
import hashlib
from pathlib import Path
import pyarrow.compute as pc
import pyarrow.parquet as pq
from zobrist128 import zobrist128
from .spec import pool_recipes

COLUMNS=["parent_hash","parent_hash_hi","move_san","total"]


def population(own_policy):
    from .bindings import OWN_POLICY_POOL
    base=OWN_POLICY_POOL.get(own_policy)
    if base is None: raise ValueError(f"unknown own post-book policy {own_policy!r}")
    r=next(r for r in pool_recipes() if r["id"]==f"{base}-p30")
    return dict(own_policy=own_policy,events=list(r["events"]),elo_bands=list(r["elo_bands"]),
                rating_semantics=r["rating_semantics"],plies="all")


class BookOwnMoves:
    def __init__(self, book, *, own_policy, expected_meta_sha=None):
        self.book=Path(book).resolve()
        if not (self.book/"_BOOK.DONE").is_file():
            raise ValueError(f"{self.book}: not a finished explorer book (no _BOOK.DONE)")
        meta=(self.book/"_book.meta.json").read_bytes()
        self.meta_sha=hashlib.sha256(meta).hexdigest()
        if expected_meta_sha is not None and self.meta_sha!=expected_meta_sha:
            raise ValueError("explorer book differs from the one the pool was built from")
        self.population=population(own_policy)
        self._groups={}   # file -> list of (lo_min, lo_max) or None per row group, or [] if absent
        self.stats=dict(queries=0,files=0,groups_read=0,rows_read=0)

    def contract(self):
        return dict(book_meta_sha256=self.meta_sha,**self.population)

    def _bounds(self, path):
        if path not in self._groups:
            if not path.is_file():
                self._groups[path]=[]
            else:
                md=pq.ParquetFile(path).metadata;groups=[]
                for i in range(md.num_row_groups):
                    rg=md.row_group(i);b=None
                    for j in range(rg.num_columns):
                        c=rg.column(j)
                        if c.path_in_schema=="parent_hash":
                            s=c.statistics
                            if s is not None and s.has_min_max: b=(s.min,s.max)
                            break
                    groups.append(b)
                self._groups[path]=groups
        return self._groups[path]

    def moves(self, board):
        """dict(games=total, moves={uci: games}); moves the book never recorded are absent."""
        lo,hi=zobrist128(board)
        bucket=lo%512
        self.stats["queries"]+=1
        counts={}
        for event in self.population["events"]:
            for band in self.population["elo_bands"]:
                path=self.book/"ps"/f"event={event}"/f"elo_band={band}"/f"bkt{bucket:03d}.parquet"
                bounds=self._bounds(path)
                keep=[i for i,b in enumerate(bounds) if b is None or b[0]<=lo<=b[1]]
                if not keep: continue
                self.stats["files"]+=1;self.stats["groups_read"]+=len(keep)
                t=pq.ParquetFile(path).read_row_groups(keep,columns=COLUMNS)
                self.stats["rows_read"]+=t.num_rows
                t=t.filter(pc.and_(pc.equal(t["parent_hash"],lo),pc.equal(t["parent_hash_hi"],hi)))
                for san,n in zip(t["move_san"].to_pylist(),t["total"].to_pylist()):
                    counts[san]=counts.get(san,0)+n
        out={}
        for san,n in counts.items():
            u=board.parse_san(san).uci();out[u]=out.get(u,0)+n
        return dict(games=sum(out.values()),moves=out)
