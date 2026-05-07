"""
Repertoire explorer -- a Streamlit app for browsing the recommended opening
tree produced by Stage 3. Pick a repertoire variant (perspective +
forcing-weight), pick an (event, elo_band) slice, and walk the tree by
clicking moves.

Run:
    cd C:/Users/David/Documents/Chess
    .venv/Scripts/python.exe -m streamlit run python/explorer_app.py
"""

from __future__ import annotations

import math
from collections import defaultdict
from pathlib import Path

import chess
import chess.polyglot
import chess.svg
import polars as pl
import streamlit as st


REPERTOIRE_DIR = Path("E:/chess/repertoire")
STATS_DIR      = Path("E:/chess/position-stats")

INT64_MAX   = 2**63 - 1
INT64_RANGE = 2**64


def zobrist_int64(b: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(b)
    return h - INT64_RANGE if h > INT64_MAX else h


# ── Data discovery and loading (cached) ────────────────────────────────────

@st.cache_data(show_spinner=False)
def list_repertoires() -> list[str]:
    if not REPERTOIRE_DIR.exists():
        return []
    return sorted(p.name for p in REPERTOIRE_DIR.glob("repertoire_*.parquet"))


@st.cache_data(show_spinner=True)
def load_repertoire(name: str) -> pl.DataFrame:
    return pl.read_parquet(REPERTOIRE_DIR / name)


@st.cache_data(show_spinner=True)
def load_stats_for(dataset: str) -> pl.DataFrame | None:
    """Load position stats for a dataset tag (e.g. '2016', '2025_apr')."""
    p = STATS_DIR / f"position_stats_{dataset}.parquet"
    if not p.exists():
        return None
    return pl.read_parquet(p)


def parse_repertoire_filename(name: str) -> dict:
    """Parse repertoire filename into metadata.

    Supports naming conventions:
      Old:  repertoire_2016_white_w010.parquet              -> {dataset, perspective, fw, ew=0}
      New:  repertoire_2016_white_f010_e030.parquet          -> {dataset, perspective, fw, ew}
      Conv: repertoire_2025_apr_white_f000_e050.parquet      -> {dataset, perspective, fw, ew}
    """
    stem = name.removeprefix("repertoire_").removesuffix(".parquet")
    parts = stem.split("_")
    out: dict = {}
    dataset_parts: list[str] = []
    for p in parts:
        if p in ("white", "black"):
            out["perspective"] = p
        elif p.startswith("f") and len(p) >= 2 and p[1:].isdigit():
            out["fw"] = int(p[1:]) / 100
        elif p.startswith("e") and len(p) >= 2 and p[1:].isdigit():
            out["ew"] = int(p[1:]) / 100
        elif p.startswith("w") and len(p) >= 2 and p[1:].isdigit():
            # Legacy naming: w### = forcing weight, eval weight was 0
            out["fw"] = int(p[1:]) / 100
            out["ew"] = 0.0
        elif "perspective" not in out:
            # Anything before the perspective is part of the dataset tag
            dataset_parts.append(p)
    out["dataset"] = "_".join(dataset_parts) if dataset_parts else "unknown"
    # Back-compat: extract year from dataset if purely numeric
    if out["dataset"].isdigit():
        out["year"] = int(out["dataset"])
    return out


# ── Slice + position indexes ────────────────────────────────────────────────

@st.cache_data(show_spinner=True)
def index_slice(rep_name: str, ev: str, eb: int):
    """Build position-keyed index for one slice of a repertoire.

    Returns a tuple (rep_idx, opp_idx_keys) where rep_idx maps
    position_hash -> repertoire row dict, and opp_idx_keys is unused (kept
    for API symmetry).
    """
    rep = load_repertoire(rep_name)
    rep_slice = rep.filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))
    rep_idx = {r["position_hash"]: r for r in rep_slice.iter_rows(named=True)}
    return rep_idx


@st.cache_data(show_spinner=True)
def index_stats_slice(dataset: str, ev: str, eb: int):
    stats = load_stats_for(dataset)
    if stats is None:
        return {}
    s = stats.filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))
    out: dict[int, list[dict]] = defaultdict(list)
    for r in s.iter_rows(named=True):
        out[r["parent_hash"]].append({
            "move_san":   r["move_san"],
            "total":      r["total"],
            "white_score_avg": r["white_score_avg"],
            "child_hash": r.get("child_hash"),
        })
    # sort each parent's moves by total desc
    for k in out:
        out[k].sort(key=lambda x: -x["total"])
    return dict(out)


# ── Helpers ─────────────────────────────────────────────────────────────────

def board_from_moves(moves: list[str]) -> chess.Board:
    b = chess.Board()
    for san in moves:
        try:
            b.push(b.parse_san(san))
        except (ValueError, AssertionError):
            break
    return b


def render_board_svg(board: chess.Board, last_move: chess.Move | None = None,
                     orient_white: bool = True) -> str:
    return chess.svg.board(
        board=board,
        size=400,
        lastmove=last_move,
        orientation=chess.WHITE if orient_white else chess.BLACK,
    )


# ── App layout ──────────────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="Repertoire explorer", layout="wide")
    st.title("Repertoire Explorer")

    files = list_repertoires()
    if not files:
        st.error(f"No repertoire files found in {REPERTOIRE_DIR}.")
        st.stop()

    # Sidebar: variant + slice pickers ---------------------------------------
    with st.sidebar:
        st.header("Variant")
        rep_name = st.selectbox("Repertoire file", files, index=0)
        meta = parse_repertoire_filename(rep_name)
        st.caption(
            f"dataset={meta.get('dataset','?')}  •  "
            f"perspective={meta.get('perspective','?')}  •  "
            f"forcing={meta.get('fw','?')}  •  "
            f"eval={meta.get('ew','?')}"
        )

        rep = load_repertoire(rep_name)

        slices = (
            rep.select(["event", "elo_band"]).unique()
            .sort(["event", "elo_band"])
        )
        slice_labels = [
            f"{r['event']}  /  elo {r['elo_band']}"
            for r in slices.iter_rows(named=True)
        ]
        slice_label = st.selectbox("Slice", slice_labels, index=0)
        idx = slice_labels.index(slice_label)
        sr = list(slices.iter_rows(named=True))[idx]
        ev, eb = sr["event"], sr["elo_band"]

        if st.button("Reset to start position"):
            st.session_state.move_history = []

    # State ------------------------------------------------------------------
    if "move_history" not in st.session_state:
        st.session_state.move_history = []

    moves: list[str] = st.session_state.move_history
    board = board_from_moves(moves)

    # Indexes for the selected slice
    rep_idx = index_slice(rep_name, ev, eb)
    stats_idx = index_stats_slice(meta.get("dataset", ""), ev, eb)

    # Layout: two columns -- board on the left, info on the right -----------
    col_board, col_info = st.columns([1, 1])

    with col_board:
        last_move = board.peek() if board.move_stack else None
        orient_white = meta.get("perspective", "white") == "white"
        st.markdown(
            render_board_svg(board, last_move, orient_white=orient_white),
            unsafe_allow_html=True,
        )

        # Move history breadcrumbs
        if moves:
            line = []
            for i, san in enumerate(moves):
                if i % 2 == 0:
                    line.append(f"{i//2 + 1}.{san}")
                else:
                    line.append(san)
            st.text("Line:  " + " ".join(line))
        else:
            st.text("Line:  (start position)")

        if moves:
            if st.button("← Back one move"):
                st.session_state.move_history = moves[:-1]
                st.rerun()

    with col_info:
        ph = zobrist_int64(board)
        rep_row = rep_idx.get(ph)
        opp_moves = stats_idx.get(ph, [])

        our_color = chess.WHITE if meta.get("perspective") == "white" else chess.BLACK
        is_our_turn = board.turn == our_color

        if rep_row is None:
            st.warning(
                "Position not in this slice's repertoire. "
                "Use Back to return."
            )
        else:
            st.metric(
                "Value (white_score)",
                f"{rep_row['value']:.4f}",
                help="Smoothed expected white score from this position. "
                     "Higher = better for white."
            )
            if rep_row.get("forcingness") is not None:
                st.metric("Forcingness", f"{rep_row['forcingness']:.3f}")
            if rep_row.get("opponent_error") is not None:
                st.metric(
                    "Opponent error",
                    f"{rep_row['opponent_error']:.4f}",
                    help="Expected centipawn-equivalent loss opponents incur "
                         "at this position. Higher = opponents make bigger "
                         "mistakes. Bayesian-smoothed toward 0.",
                )
            if rep_row.get("eval_score") is not None:
                sf = rep_row["eval_score"]
                # Reverse the Lichess sigmoid to display centipawns
                if 0.001 < sf < 0.999:
                    cp = -math.log(1.0 / sf - 1.0) / 0.00368208
                elif sf >= 0.999:
                    cp = 999.0
                else:
                    cp = -999.0
                sign_str = "+" if cp >= 0 else ""
                st.metric(
                    "Stockfish eval",
                    f"{sign_str}{cp / 100:.2f}",
                    help="Evaluation from Lichess Stockfish annotations. "
                         "+1.00 = one pawn advantage for white.",
                )

            if is_our_turn:
                st.subheader(f"Our recommendation ({meta.get('perspective')})")
                bm = rep_row.get("best_move")
                if bm:
                    st.success(f"Play: **{bm}**")
                    if st.button(f"Follow {bm} →", key="follow_best"):
                        st.session_state.move_history = moves + [bm]
                        st.rerun()
                else:
                    st.info("No recommendation here (likely a leaf).")
            else:
                st.subheader("Opponent's turn")
                st.caption("Pick what your opponent plays. Common choices first:")

        if opp_moves:
            st.markdown("##### Available moves at this position")
            # Build a small dataframe + click buttons
            df_rows = []
            for m in opp_moves[:30]:
                df_rows.append({
                    "move":  m["move_san"],
                    "n":     m["total"],
                    "white_score": round(m["white_score_avg"], 4),
                    "in repertoire": "yes" if (m.get("child_hash") in rep_idx) else "no",
                })
            st.dataframe(df_rows, use_container_width=True, hide_index=True)

            # Buttons to follow any of these moves
            st.caption("Click a move to follow it:")
            n_per_row = 5
            for i in range(0, min(15, len(opp_moves)), n_per_row):
                cols = st.columns(n_per_row)
                for j, m in enumerate(opp_moves[i:i + n_per_row]):
                    with cols[j]:
                        in_rep = (m.get("child_hash") in rep_idx)
                        label = f"{m['move_san']}\nn={m['total']:,}"
                        if st.button(label, key=f"opp_{i+j}_{m['move_san']}"):
                            st.session_state.move_history = moves + [m["move_san"]]
                            st.rerun()
                        st.caption("✓ in tree" if in_rep else "✗ exits")
        else:
            st.caption("(No move data for this position in this slice.)")


if __name__ == "__main__":
    main()
