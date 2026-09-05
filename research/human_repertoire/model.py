"""Finite-horizon policy evaluation and exact distinct-position book search.

Only Python's standard library is needed. See docs/human_repertoire/MODEL.md
for the estimand and semantics. This is intentionally an exponential tiny-book
benchmark, not a production-scale optimiser.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from functools import lru_cache
from math import isclose, isfinite
from typing import Mapping


@dataclass(frozen=True)
class Edge:
    move: str
    child: str
    probability: float
    sound_cp: float | None = None  # independent engine gate, our perspective


@dataclass(frozen=True)
class Node:
    side: str
    edges: tuple[Edge, ...] = ()
    cp: float | None = None  # observed first-hit marker, our perspective
    wdl: tuple[float, float, float] = (0.4, 0.2, 0.4)
    cost: int = 1


@dataclass(frozen=True)
class Graph:
    root: str
    nodes: Mapping[str, Node]

    def validate(self):
        if self.root not in self.nodes:
            raise ValueError("missing root")
        for key, n in self.nodes.items():
            if n.side not in {"us", "opp", "terminal", "frontier"}:
                raise ValueError(f"invalid side at {key}")
            if type(n.cost) is not int or n.cost <= 0:
                raise ValueError("cost must be a positive integer")
            if n.cp is not None and not isfinite(n.cp):
                raise ValueError("nonfinite evaluation")
            distribution(n.wdl)
            if n.side in {"us", "opp"}:
                if not n.edges or len({e.move for e in n.edges}) != len(n.edges):
                    raise ValueError("nonterminal needs distinct moves")
                distribution(tuple(e.probability for e in n.edges))
            elif n.edges:
                raise ValueError("terminal/frontier cannot have edges")
            for e in n.edges:
                if e.child not in self.nodes:
                    raise ValueError("missing child")
                if e.sound_cp is not None and not isfinite(e.sound_cp):
                    raise ValueError("nonfinite gate evaluation")


def distribution(values):
    if not values or any(not isfinite(x) or x < 0 for x in values) or not isclose(sum(values), 1, abs_tol=1e-10):
        raise ValueError(f"invalid distribution: {values}")


@dataclass(frozen=True)
class Config:
    horizon: int = 8
    recall: float = 1.0
    recall_by_position: Mapping[str, float] = field(default_factory=dict)
    fallback: Mapping[str, Mapping[str, float]] = field(default_factory=dict)
    opponent: Mapping[str, Mapping[str, float]] = field(default_factory=dict)
    reentry: bool = True
    repetition_visits: int | None = 3  # automatic draw on Nth occurrence
    threshold_cp: float = 300
    discount: float = 0.99
    soundness_floor_cp: float = -100
    max_engine_loss_cp: float = 150
    min_points: float = 0.0
    max_disadvantage: float = 1.0
    max_books: int = 200_000

    def validate(self, g: Graph):
        if type(self.horizon) is not int or not 0 <= self.horizon <= 100:
            raise ValueError("horizon must be an integer in [0,100]")
        if self.repetition_visits is not None and (type(self.repetition_visits) is not int or self.repetition_visits < 2):
            raise ValueError("repetition_visits must be >=2 or null")
        for x in [self.recall, self.discount, self.min_points, self.max_disadvantage, *self.recall_by_position.values()]:
            if not isfinite(x) or not 0 <= x <= 1:
                raise ValueError("probability/configuration outside [0,1]")
        if not isfinite(self.threshold_cp) or self.threshold_cp <= 0 or not isfinite(self.soundness_floor_cp) or not isfinite(self.max_engine_loss_cp) or self.max_engine_loss_cp < 0:
            raise ValueError("invalid centipawn threshold")
        if type(self.max_books) is not int or self.max_books < 1:
            raise ValueError("invalid search cap")
        for k in self.recall_by_position:
            if k not in g.nodes or g.nodes[k].side != "us":
                raise ValueError("recall override is not our position")
        for overrides, side in [(self.fallback, "us"), (self.opponent, "opp")]:
            for k, probabilities in overrides.items():
                if k not in g.nodes or g.nodes[k].side != side:
                    raise ValueError("policy override has wrong side")
                if set(probabilities) != {e.move for e in g.nodes[k].edges}:
                    raise ValueError("policy override must contain every move")
                distribution(tuple(probabilities.values()))


def eligible(g: Graph, c: Config, key: str) -> tuple[str, ...]:
    n = g.nodes[key]
    return tuple(e.move for e in n.edges if n.side == "us" and e.sound_cp is not None
                 and e.sound_cp >= c.soundness_floor_cp
                 and n.cp is not None and e.sound_cp >= n.cp - c.max_engine_loss_cp)


def book_cost(g: Graph, book: Mapping[str, str]) -> int:
    return sum(g.nodes[k].cost for k in book)


@dataclass(frozen=True)
class Metrics:
    win: float = 0
    draw: float = 0
    loss: float = 0
    advantage: float = 0
    disadvantage: float = 0
    no_hit: float = 0
    early_advantage: float = 0
    win_after_advantage: float = 0
    terminal_mass: float = 0
    frontier_mass: float = 0
    horizon_mass: float = 0
    repetition_mass: float = 0
    unknown_eval_mass: float = 0

    @property
    def points(self):
        return self.win + 0.5 * self.draw

    def report(self):
        return {**asdict(self), "points": self.points,
                "conversion_given_advantage": self.win_after_advantage / self.advantage if self.advantage else None}


def evaluate(g: Graph, book: Mapping[str, str], c: Config) -> Metrics:
    g.validate()
    c.validate(g)
    for k, move in book.items():
        if k not in g.nodes or move not in eligible(g, c, k):
            raise ValueError(f"uncertified/invalid prepared move: {k} {move}")

    @lru_cache(None)
    def visit(key, ply, history, event, hit_ply, active, unknown):
        n = g.nodes[key]
        unknown = unknown or n.cp is None
        if event == 0 and n.cp is not None and abs(n.cp) >= c.threshold_cp:
            event, hit_ply = (1 if n.cp > 0 else -1), ply
        repeated = c.repetition_visits is not None and history.count(key) + 1 >= c.repetition_visits
        # Exact graph terminals precede repetition; repetition precedes truncation.
        reason = ("terminal" if n.side == "terminal" else "repetition" if repeated else
                  "frontier" if n.side == "frontier" else "horizon" if ply == c.horizon else None)
        if reason:
            w, d, l = (0, 1, 0) if reason == "repetition" else n.wdl
            return Metrics(w, d, l, float(event == 1), float(event == -1), float(event == 0),
                           c.discount ** hit_ply if event == 1 else 0, w if event == 1 else 0,
                           float(reason == "terminal"), float(reason == "frontier"),
                           float(reason == "horizon"), float(reason == "repetition"), float(unknown))
        probs = (c.fallback if n.side == "us" else c.opponent).get(key, {})
        branches = []
        if n.side == "us":
            can_recall = key in book and (active or c.reentry)
            r = c.recall_by_position.get(key, c.recall) if can_recall else 0
            for e in n.edges:
                if r and e.move == book.get(key):
                    branches.append((r, e.child, True))
                branches.append(((1-r) * probs.get(e.move, e.probability), e.child, bool(c.reentry)))
        else:
            branches = [(probs.get(e.move, e.probability), e.child, active) for e in n.edges]
        total = [0.0] * len(Metrics.__dataclass_fields__)
        next_history = history + (key,) if c.repetition_visits is not None else ()
        for probability, child, next_active in branches:
            if probability:
                result = visit(child, ply + 1, next_history, event, hit_ply, next_active, unknown)
                for i, value in enumerate(asdict(result).values()):
                    total[i] += probability * value
        return Metrics(*total)

    return visit(g.root, 0, (), 0, 0, True, False)


def feasible(m: Metrics, c: Config) -> bool:
    return m.points >= c.min_points - 1e-12 and m.disadvantage <= c.max_disadvantage + 1e-12


def objective(m: Metrics, cost: int):
    # Stable ties at a precision well below the empirical uncertainty.
    return round(m.early_advantage, 12), round(m.points, 12), -cost


@dataclass
class Solution:
    book: dict[str, str]
    metrics: Metrics
    cost: int
    evaluated_books: int


def optimise(g: Graph, c: Config, budget: int, allowed: Mapping[str, tuple[str, ...]] | None = None) -> Solution | None:
    """Depth-first exact search. Store the selected book, never reconstruct an argmax."""
    g.validate()
    c.validate(g)
    if type(budget) is not int or budget < 0:
        raise ValueError("budget must be a nonnegative integer")
    choices = []
    for k in sorted(g.nodes):
        moves = eligible(g, c, k)
        if allowed is not None:
            moves = tuple(m for m in moves if m in allowed.get(k, ()))
        if moves:
            choices.append((k, moves))
    best = None
    count = 0
    book = {}

    def search(i, spent):
        nonlocal best, count
        if i == len(choices):
            count += 1
            if count > c.max_books:
                raise RuntimeError("tiny-book search cap exceeded; no approximate answer returned")
            m = evaluate(g, book, c)
            if feasible(m, c) and (best is None or objective(m, spent) > objective(best.metrics, best.cost)):
                best = Solution(dict(book), m, spent, count)
            return
        k, moves = choices[i]
        search(i + 1, spent)
        if spent + g.nodes[k].cost <= budget:
            for move in moves:
                book[k] = move
                search(i + 1, spent + g.nodes[k].cost)
            del book[k]
    search(0, 0)
    if best:
        best.evaluated_books = count
        assert evaluate(g, best.book, c) == best.metrics
    return best


def graph_dict(g: Graph):
    return {"root": g.root, "nodes": {k: asdict(n) for k, n in g.nodes.items()}}


def graph_from_dict(data):
    return Graph(data["root"], {k: Node(**{**n, "edges": tuple(Edge(**e) for e in n.get("edges", [])),
                                         "wdl": tuple(n.get("wdl", [0.4, 0.2, 0.4]))})
                                for k, n in data["nodes"].items()})
