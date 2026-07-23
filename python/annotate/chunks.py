"""Subtree chunking: split the training-pack tree into coherent-variation
chunks of MIN..MAX cards for annotation generation.

Each card (distinct our-turn position) is chunked at its canonical tree node.
Chunk roots are the shallowest nodes whose subtree holds <= MAX cards; cards on
internal nodes above every chunk root are pushed down the highest-reach child
until they land in a chunk. Small sibling chunks are merged (DFS-adjacent,
same parent) until >= MIN where possible.

chunk_id = sha1(color + sorted member position_hashes)[:12] — depends only on
membership, so pack rebuilds that renumber nodes without changing positions do
NOT churn chunk ids (and therefore don't trigger regeneration).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from trainer_app.pack import TrainingPack

MAX_CARDS = 25
MIN_CARDS = 8


@dataclass
class Chunk:
    chunk_id: str
    color: str
    root_node: int              # tree node the chapter line points at
    chapter_sans: list[str]     # SANs root -> chunk root
    card_hashes: list[int] = field(default_factory=list)   # DFS order
    card_nodes: list[int] = field(default_factory=list)


def _chunk_id(color: str, hashes: list[int]) -> str:
    key = color + ":" + ",".join(str(h) for h in sorted(hashes))
    return hashlib.sha1(key.encode()).hexdigest()[:12]


def build_chunks(pack: TrainingPack, color: str,
                 max_cards: int = MAX_CARDS, min_cards: int = MIN_CARDS) -> list[Chunk]:
    cols = pack.tree[color]
    kids = pack.children[color]
    n_nodes = len(cols["parent_id"])

    # card node -> hash (each card counted once, at its canonical arrival)
    card_at: dict[int, int] = {
        row["canonical_node_id"]: row["position_hash"]
        for row in pack.cards[color].iter_rows(named=True)
    }

    # subtree card counts, bottom-up (children have higher node ids? not
    # guaranteed — do an explicit post-order via stack)
    count = [0] * n_nodes
    order: list[int] = []
    stack = [0]
    while stack:
        n = stack.pop()
        order.append(n)
        stack.extend(kids.get(n, []))
    for n in reversed(order):                     # children before parents
        count[n] = (1 if n in card_at else 0) + sum(count[c] for c in kids.get(n, []))

    # DFS assigning cards to chunk roots. `carry` holds cards on internal
    # nodes above the split point; they ride down the highest-reach child.
    chunks: list[Chunk] = []

    def reach(n: int) -> float:
        return cols["path_reach"][n] or 0.0

    def collect(n: int, carry: list[tuple[int, int]]) -> None:
        """Emit one chunk = carry + all cards in subtree(n), DFS order."""
        members: list[tuple[int, int]] = list(carry)
        st = [n]
        while st:
            m = st.pop()
            if m in card_at:
                members.append((m, card_at[m]))
            st.extend(reversed(kids.get(m, [])))   # keep DFS order
        if not members:
            return
        ch = Chunk(chunk_id="", color=color, root_node=n,
                   chapter_sans=pack.line_sans(color, pack.path_to_root(color, n)))
        for node, h in members:
            ch.card_nodes.append(node)
            ch.card_hashes.append(h)
        ch.chunk_id = _chunk_id(color, ch.card_hashes)
        chunks.append(ch)

    def split(n: int, carry: list[tuple[int, int]]) -> None:
        if count[n] + len(carry) <= max_cards or not kids.get(n):
            collect(n, carry)
            return
        my_carry = list(carry)
        if n in card_at:
            my_carry.append((n, card_at[n]))
        children = sorted(kids.get(n, []), key=lambda c: -reach(c))
        best = children[0] if children else None
        for c in sorted(kids.get(n, [])):          # DFS (node-id) order
            if count[c] == 0 and c != best:
                continue
            split(c, my_carry if c == best else [])
            if c == best:
                my_carry = []                      # carried cards delivered
        if my_carry:                                # best child had no chunk
            collect(n, my_carry)

    split(0, [])

    # merge pass: greedily fold each sub-MIN chunk into its smaller DFS-neighbor
    # (chunks are already in DFS order, so neighbors share the most line prefix).
    # Repeat until no undersized chunk can merge without exceeding MAX.
    def fold(a: Chunk, b: Chunk) -> Chunk:
        common = 0
        for x, y in zip(a.chapter_sans, b.chapter_sans):
            if x != y:
                break
            common += 1
        a.chapter_sans = a.chapter_sans[:common]
        a.card_hashes += b.card_hashes
        a.card_nodes += b.card_nodes
        a.chunk_id = _chunk_id(color, a.card_hashes)
        return a

    merged = list(chunks)
    frozen: set[int] = set()      # id() of chunks that can't merge with any neighbor
    while True:
        # smallest undersized, not-yet-frozen chunk
        cand = [i for i, c in enumerate(merged)
                if len(c.card_hashes) < min_cards and id(c) not in frozen]
        if not cand:
            break
        i = min(cand, key=lambda k: len(merged[k].card_hashes))
        sz = len(merged[i].card_hashes)
        prev_ok = i > 0 and len(merged[i - 1].card_hashes) + sz <= max_cards
        next_ok = i < len(merged) - 1 and sz + len(merged[i + 1].card_hashes) <= max_cards
        if not prev_ok and not next_ok:
            frozen.add(id(merged[i]))         # deep singleton — leave as its own chunk
            continue
        if prev_ok and (not next_ok or
                        len(merged[i - 1].card_hashes) <= len(merged[i + 1].card_hashes)):
            merged[i - 1] = fold(merged[i - 1], merged[i])
            merged.pop(i)
        else:
            merged[i] = fold(merged[i], merged[i + 1])
            merged.pop(i + 1)
    return merged
