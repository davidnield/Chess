"""Opening families: group the pack tree into tabiya-rooted blocks for drilling.

The trainer drills a few lines from one family before moving to the next, so related
lines are rehearsed together (e.g. the Two Knights, then the Italian, then White's
other 3rd moves, then the Caro-Kann) instead of jumping across the repertoire.

Split rule, top-down from the start position. Our moves never split (we have one
move). At an opponent turn whose reach is above `split_reach`, each reply with reach
>= `min_reach` becomes its own branch (split again further down while it is still
above `split_reach`); the smaller replies are pooled into one "other moves" family
of that node. A node at or below `split_reach` is a family with its whole subtree.

Reach is the pack tree's path_reach (the share of games reaching the node along
that move order); a card belongs to the family of its canonical node.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .pack import TrainingPack

SPLIT_REACH = 0.10
MIN_REACH = 0.02


@dataclass
class Family:
    family_id: str
    label: str                       # "1.e4 e5 2.Nf3 Nc6 3.Bc4 Nf6" / "... (other moves)"
    root: int                        # node id of the tabiya (the opponent node, for pools)
    nodes: list[int] = field(default_factory=list)


def _line(pack: TrainingPack, rep: str, nid: int) -> str:
    sans = pack.line_sans(rep, pack.path_to_root(rep, nid))
    return " ".join((f"{i // 2 + 1}.{s}" if i % 2 == 0 else s) for i, s in enumerate(sans))


def build_families(pack: TrainingPack, rep: str, split_reach: float = SPLIT_REACH,
                   min_reach: float = MIN_REACH) -> list[Family]:
    cols, kids = pack.tree[rep], pack.children[rep]
    reach = cols["path_reach"]
    fams: list[Family] = []

    def subtree(n: int) -> list[int]:
        out, st = [], [n]
        while st:
            m = st.pop()
            out.append(m)
            st.extend(kids.get(m, []))
        return out

    def whole(n: int, label: str) -> None:
        fams.append(Family(f"{rep}:{n}", label, n, subtree(n)))

    def split(n: int) -> None:
        # walk our single moves down to the next opponent decision
        prefix = []
        while cols["is_our_turn"][n] and kids.get(n):
            prefix.append(n)
            n = kids[n][0]
        if (reach[n] or 0) <= split_reach or not kids.get(n):
            fams.append(Family(f"{rep}:{n}", _line(pack, rep, n) or "(start)", n,
                               prefix + subtree(n)))
            return
        big = [c for c in kids[n] if (reach[c] or 0) >= min_reach]
        small = [c for c in kids[n] if (reach[c] or 0) < min_reach]
        # the decision node and our moves leading to it live with the pooled replies
        fams.append(Family(f"{rep}:{n}:other", (_line(pack, rep, n) + " (other moves)").strip(),
                           n, prefix + [n] + [m for c in small for m in subtree(c)]))
        for c in big:
            split(c)

    split(0)
    return fams


def family_of_nodes(fams: list[Family]) -> dict[int, str]:
    return {n: f.family_id for f in fams for n in f.nodes}
