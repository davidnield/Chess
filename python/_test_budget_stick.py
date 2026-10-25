"""Budget-book rebuild stickiness (budget_core.incumbent_move, build_budget_books --incumbent),
2026-10-24. Synthetic graphs through the production solvers (price_book; build_curves +
extract_book).

  A  no stickiness (meta empty, stick absent, or margin 0) is an exact no-op: same book, same values
  B  a near-tie: the incumbent's move is kept when the rival is better by less than the margin...
  C  ...and replaced when the rival is better by more than the margin
  D  only the CHOICE is biased: the node's value is the kept move's true value, and a position is
     still not booked when even the incumbent move is worse than stopping
  E  an incumbent move that is not a candidate here is ignored; check marks do not matter
  F  the curve engine (build_curves + extract_book) follows the same rule, at a deeper node too
  G  incumbent_churn counts kept / changed / dropped / added
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from budget_core import (Graph, OppNode, OurNode, build_curves, extract_book,  # noqa: E402
                         incumbent_move, price_book, topo_up)
from build_budget_books import incumbent_churn  # noqa: E402

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def leaf_graph(va: float, vb: float, stop: float = 0.50) -> Graph:
    """Root (our turn) with two terminal candidates a (va) and b (vb)."""
    return Graph(root=1, our={1: OurNode(l_node=stop, cands=[("a", None, False, va),
                                                             ("b+", None, False, vb)])},
                 opp={}, epd={}, ply={1: 0}, reach={1: 1.0})


def price(g: Graph, lam: float = 1e-6) -> dict:
    return price_book(g, lam, topo_up(g)[0])


def deep_graph() -> Graph:
    """our 1 -> 'x' -> opp 2 -> (p=1) our 3 with near-tied a/b; the book needs 2 positions."""
    return Graph(root=1,
                 our={1: OurNode(l_node=0.50, cands=[("x", 2, True, 0.50)]),
                      3: OurNode(l_node=0.50, cands=[("a", None, False, 0.600),
                                                     ("b", None, False, 0.605)])},
                 opp={2: OppNode(const_base=0.0, kids=[(1.0, 3, True, 0.50)])},
                 epd={}, ply={1: 0, 2: 1, 3: 2}, reach={1: 1.0, 2: 1.0, 3: 1.0})


def dp(g: Graph, budget: int) -> dict:
    curves, _ = build_curves(g, budget)
    return extract_book(g, curves, budget)


def main() -> int:
    base = price(leaf_graph(0.600, 0.605))
    g0 = leaf_graph(0.600, 0.605)
    g0.meta["stick"] = None
    gm = leaf_graph(0.600, 0.605)
    gm.meta["stick"] = ({1: "a"}, 0.0)
    for label, g in (("stick None", g0), ("margin 0", gm)):
        r = price(g)
        check(f"A: {label} is a no-op (price)", r["booked"] == base["booked"]
              and r["root_value_realized"] == base["root_value_realized"])
    check("A: incumbent_move without stick -> (None, 0.0)", incumbent_move(leaf_graph(0.6, 0.6), 1) == (None, 0.0))

    g = leaf_graph(0.600, 0.605)
    g.meta["stick"] = ({1: "a"}, 0.01)
    r = price(g)
    check("B: rival better by 0.005 < margin 0.01 -> incumbent a kept", r["booked"].get(1) == "a",
          str(r["booked"]))
    check("D: the node's value is a's true value (0.600, not 0.610)",
          abs(r["root_value_realized"] - 0.600) < 1e-12, f"{r['root_value_realized']}")
    g = leaf_graph(0.600, 0.620)
    g.meta["stick"] = ({1: "a"}, 0.01)
    check("C: rival better by 0.02 > margin -> replaced by b", price(g)["booked"].get(1) == "b+")
    g = leaf_graph(0.600, 0.580, stop=0.605)
    g.meta["stick"] = ({1: "a"}, 0.01)
    check("D: incumbent worse than stopping -> not booked (stop decided on true values)",
          1 not in price(g)["booked"])

    g = leaf_graph(0.600, 0.605)
    g.meta["stick"] = ({1: "z"}, 0.01)
    check("E: an incumbent move that is not a candidate is ignored", price(g)["booked"].get(1) == "b+")
    g = leaf_graph(0.610, 0.605)
    g.meta["stick"] = ({1: "b"}, 0.01)      # the book's move is 'b+'; the incumbent says 'b'
    check("E: check marks are ignored when matching", price(g)["booked"].get(1) == "b+")

    g = leaf_graph(0.600, 0.605)
    check("F: curve engine, no stick -> b", dp(g, 1)["booked"].get(1) == "b+")
    g.meta["stick"] = ({1: "a"}, 0.01)
    r = dp(g, 1)
    check("F: curve engine keeps the incumbent a", r["booked"].get(1) == "a", str(r["booked"]))
    check("F: ... at a's true value", abs(r["root_value_realized"] - 0.600) < 1e-12)
    g = deep_graph()
    check("F: deeper node, no stick -> b", dp(g, 2)["booked"].get(3) == "b")
    g.meta["stick"] = ({3: "a"}, 0.01)
    r = dp(g, 2)
    check("F: deeper node keeps the incumbent a (and books the root)",
          r["booked"].get(3) == "a" and r["booked"].get(1) == "x", str(r["booked"]))
    g = deep_graph()
    g.meta["stick"] = ({3: "a"}, 0.01)
    rp = price(g)
    check("F: price solve agrees at the deeper node", rp["booked"].get(3) == "a", str(rp["booked"]))

    ch = incumbent_churn({1: "e4", 2: "Nf3+", 3: "d4"}, {1: "e4", 2: "Nf3", 4: "c4", 3: "c3"},
                         {1: 1.0, 2: 0.5, 3: 0.25, 4: 0.1})
    check("G: churn counts", (ch["kept"], ch["changed"], ch["dropped"], ch["added"]) == (2, 1, 1, 0),
          str(ch))
    check("G: churn reach", abs(ch["reach_kept"] - 1.5) < 1e-12 and abs(ch["reach_changed"] - 0.25) < 1e-12
          and abs(ch["reach_dropped"] - 0.1) < 1e-12)
    print("\nALL PASS" if not FAIL else "\nFAILURES")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
