"""Transparent synthetic model predictions, not measurements of chess players."""
from .model import Edge, Graph, Node


def demanding():
    # Three remembered decisions buy a 90% first-hit chance; one easy card buys
    # 50%. Failure to execute a demanding continuation risks disadvantage.
    nodes = {
        "root": Node("us", (Edge("demanding", "reply1", .1, 0), Edge("easy", "easy_reply", .1, 0), Edge("quiet", "quiet", .8, 0)), 0),
        "reply1": Node("opp", (Edge("forced", "d1", 1),), 0),
        "d1": Node("us", (Edge("remember1", "reply2", .1, 0), Edge("slip1", "bad", .9, -400)), 0),
        "reply2": Node("opp", (Edge("forced", "d2", 1),), 0),
        "d2": Node("us", (Edge("remember2", "payoff", .1, 0), Edge("slip2", "bad", .9, -400)), 0),
        "payoff": Node("opp", (Edge("error", "good", .9), Edge("defend", "quiet", .1)), 0),
        "easy_reply": Node("opp", (Edge("error", "good", .5), Edge("defend", "quiet", .5)), 0),
        "good": Node("terminal", cp=400, wdl=(.8, .15, .05)),
        "bad": Node("terminal", cp=-400, wdl=(.1, .2, .7)),
        "quiet": Node("terminal", cp=0, wdl=(.35, .3, .35)),
    }
    return Graph("root", nodes)


def fallback_skill(g, success):
    return {k: {e.move: success if i == 0 else 1-success for i, e in enumerate(g.nodes[k].edges)} for k in ("d1", "d2")}


def shared_reentry():
    return Graph("root", {
        "root": Node("opp", (Edge("direct", "shared", .4), Edge("detour", "unprepared", .6)), 0),
        "unprepared": Node("us", (Edge("natural", "reply", 1, 0),), 0),
        "reply": Node("opp", (Edge("transpose", "shared", 1),), 0),
        "shared": Node("us", (Edge("find", "win", 0, 0), Edge("miss", "flat", 1, 0)), 0),
        "win": Node("terminal", cp=400, wdl=(1, 0, 0)),
        "flat": Node("terminal", cp=0, wdl=(0, 1, 0)),
    })
