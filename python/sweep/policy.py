"""OTB ensemble on complete legal-move vectors; never resmooth a completed ensemble."""
from __future__ import annotations
import math
import sqlite3
from pathlib import Path
from .spec import canonical, digest

def _vector(name, values, size, probability=False):
    if len(values)!=size or any(not math.isfinite(v) or v<0 for v in values):
        raise ValueError(f"invalid {name} vector")
    if probability and not math.isclose(sum(values),1.,abs_tol=1e-10):
        raise ValueError(f"{name} probabilities do not sum to one")
    return list(map(float,values))

def support(broad, adult, targeted, *, backup_discount=.10, broad_discount=.01):
    """Effective online support of a board: target games, plus discounted other-1800+ and
    other all-rating games (sums over the legal moves)."""
    B,A,R=sum(broad),sum(adult),sum(targeted)
    return R+backup_discount*(A-R)+broad_discount*(B-A)

def needs_maia(broad, adult, targeted, otb, *, maia_support_gate=None, backup_discount=.10,
               broad_discount=.01):
    """Whether ``ensemble`` reads the Maia vector for these counts (callers skip inference
    otherwise). Boards with no online and no OTB games always need it."""
    if sum(broad)==0 and sum(otb)==0: return True
    if maia_support_gate is None: return True
    return support(broad,adult,targeted,backup_discount=backup_discount,
                   broad_discount=broad_discount)<maia_support_gate

def ensemble(legal, broad, adult, targeted, otb, maia, *, otb_strength=10., empirical_strength=30.,
             maia_strength=3., backup_discount=.10, broad_discount=.01, maia_support_gate=None):
    """Production arithmetic is native; ensemble_reference is the independent oracle."""
    from book_builder_rs import opponent_policy
    if not legal or len(set(legal))!=len(legal): raise ValueError("need unique legal moves")
    if any(len(v)!=len(legal) for v in (broad,adult,targeted,otb)):
        raise ValueError("count vector differs from legal moves")
    return opponent_policy(broad,adult,targeted,otb,maia,otb_strength,empirical_strength,
                           maia_strength,backup_discount,broad_discount,maia_support_gate)


def ensemble_reference(legal, broad, adult, targeted, otb, maia, *, otb_strength=10., empirical_strength=30.,
             maia_strength=3., backup_discount=.10, broad_discount=.01, maia_support_gate=None):
    """Counts are mutually nested b >= a >= r PER MOVE, and OTB is disjoint. The strengths and
    discounts are the recipe's (spec.POLICY); the defaults are its approved values.

    maia_support_gate G (otb-ensemble-v2: 300; None = v1, Maia everywhere): at support >= G the
    Maia term is dropped and ``maia`` may be None. Maia's weight there is S/(support+S), under 1%
    at G=300 and S=3; on the 13 Nov OTB holdout dropping it changed Brier by 2e-6
    (outputs/otb-u2000-2026-11-13/maia-dag/ensemble_options.py)."""
    L=len(legal)
    if not L or len(set(legal))!=L: raise ValueError("need unique legal moves")
    b,a,r,o=[_vector(n,v,L) for n,v in
             zip(("broad","1800+","target","OTB"),(broad,adult,targeted,otb))]
    if any(not (bb>=aa>=rr) for bb,aa,rr in zip(b,a,r)):
        raise ValueError("overlapping/superset counts do not support subtraction")
    for name,v in (("OTB strength",otb_strength),("empirical strength",empirical_strength),
                   ("Maia strength",maia_strength)):
        if not math.isfinite(v) or v<=0: raise ValueError(f"invalid {name}")
    for name,v in (("backup discount",backup_discount),("broad discount",broad_discount)):
        if not math.isfinite(v) or not 0<=v<=1: raise ValueError(f"invalid {name}")
    if maia_support_gate is not None and (not math.isfinite(maia_support_gate) or maia_support_gate<=0):
        raise ValueError("invalid Maia support gate")
    use_maia=needs_maia(b,a,r,o,maia_support_gate=maia_support_gate,backup_discount=backup_discount,
                        broad_discount=broad_discount)
    if use_maia:
        if maia is None: raise ValueError("this board's support is below the gate: Maia is required")
        q=_vector("Maia",maia,L,True)
    B,A,R,O=map(sum,(b,a,r,o))
    if B==0 and O==0: return q
    k,m=empirical_strength,maia_strength
    s=support(b,a,r,backup_discount=backup_discount,broad_discount=broad_discount)
    result=[]
    for i,(bb,aa,rr,oo) in enumerate(zip(b,a,r,o)):
        broad_p=(bb+1/L)/(B+1)
        backup=(aa-rr+k*broad_p)/(A-R+k)
        online=(rr+k*backup)/(R+k)
        pre=(s*online+m*q[i])/(s+m) if use_maia else online
        result.append((oo+otb_strength*pre)/(O+otb_strength))
    return _vector("ensemble",result,L,True)

def overlay_rows(board, distributions, *, empirical, nonmove, move_mass=None, wide=True):
    """Rows for ReplyOverlay schema 3. Counts and nonmove mass refer to ONE outcome pool.
    For an unobserved position the caller explicitly supplies a mass convention and
    matching aux data; no fictitious game result is inserted into the historical counts.
    """
    import polars as pl
    from consumer_keys import stored_key
    if board.is_game_over(): raise ValueError("terminal board has no opponent reply distribution")
    legal=sorted(m.uci() for m in board.legal_moves)
    p=_vector("policy",[distributions[m] for m in legal],len(legal),True)
    if set(distributions)!=set(legal) or set(empirical)-set(legal):
        raise ValueError("policy must cover exactly the legal moves")
    if not math.isfinite(nonmove) or nonmove<0: raise ValueError("invalid nonmove mass")
    counts=[]
    for m in legal:
        w,d,b=empirical.get(m,(0,0,0))
        if any(not math.isfinite(n) or n<0 for n in (w,d,b)):
            raise ValueError("invalid outcome counts")
        counts.append((w,d,b))
    observed=sum(sum(v) for v in counts)
    mass=observed if move_mass is None else move_mass
    if not math.isfinite(mass) or mass<0 or mass+nonmove<=0:
        raise ValueError("explicit continuation mass required at zero empirical support")
    h=stored_key(board,wide)
    rows=[]
    for uci,prob,(w,d,b) in zip(legal,p,counts):
        move=board.parse_uci(uci)
        san=board.san(move)
        child=board.copy(stack=False)
        child.push(move)
        n=w+d+b
        rows.append(dict(parent_hash=h,child_hash=stored_key(child,wide),parent_epd=board.epd(),
                         move_san=san,total=n,white_wins=w,draws=d,black_wins=b,
                         white_score_avg=(w+.5*d)/n if n else .5,
                         reply_observed=n,policy_p=prob,move_mass=mass,nonmove=nonmove))
    return pl.DataFrame(rows)

class PolicyCache:
    """Content identity includes source snapshots, full FEN, model and inference settings."""
    def __init__(self,path,recipe):
        self.recipe=digest(recipe)
        self.db=sqlite3.connect(Path(path))
        self.db.execute("CREATE TABLE IF NOT EXISTS probabilities (k TEXT PRIMARY KEY, value TEXT NOT NULL)")
    def get_or_compute(self,fen,legal,counts,compute,*,commit=True):
        # FEN includes EP/castling and any model-visible counters; hash collisions
        # in the legacy low half cannot alias here.
        key=digest(dict(recipe=self.recipe,fen=fen,legal=legal,counts=counts))
        row=self.db.execute("SELECT value FROM probabilities WHERE k=?",(key,)).fetchone()
        if row:
            import json
            return json.loads(row[0]), True
        result=_vector("cached policy",compute(),len(legal),True)
        if commit:
            with self.db:
                self.db.execute("INSERT OR IGNORE INTO probabilities VALUES (?,?)",(key,canonical(result)))
        else:
            if not self.db.in_transaction:raise ValueError("batched policy cache needs an active transaction")
            self.db.execute("INSERT OR IGNORE INTO probabilities VALUES (?,?)",(key,canonical(result)))
        import json
        return json.loads(self.db.execute("SELECT value FROM probabilities WHERE k=?",(key,)).fetchone()[0]), False
    def close(self): self.db.close()


def validate_overlay_rows(frame, colour):
    """Replay all legal edges before a frozen ensemble can enter the native collector."""
    from consumer_keys import checked_board, stored_key
    import polars as pl
    wide=isinstance(frame.schema["parent_hash"], pl.Struct)
    for group in frame.partition_by("parent_hash", maintain_order=True):
        _vector("ensemble",group["policy_p"].to_list(),len(group),True)
        for name in ("move_mass","nonmove"):
            values=group[name].to_list()
            if len(set(values))!=1 or not math.isfinite(values[0]) or values[0]<0:
                raise ValueError("invalid continuation/nonmove mass")
        if group["move_mass"][0]+group["nonmove"][0]<=0:
            raise ValueError("empty total mass")
        parent=group["parent_hash"][0]
        epds=group["parent_epd"].unique()
        if len(epds)!=1: raise ValueError("ensemble parent has conflicting EPDs")
        board=checked_board(epds[0],parent)
        if board.turn==(colour=="white"):
            raise ValueError("opponent overlay changes our move menu")
        legal=set(m.uci() for m in board.legal_moves)
        actual=set()
        for row in group.to_dicts():
            move=board.parse_san(row["move_san"])
            if move.uci() in actual: raise ValueError("duplicate legal reply")
            actual.add(move.uci())
            board.push(move)
            if stored_key(board,wide)!=row["child_hash"]:
                raise ValueError("ensemble child key does not match its move")
            board.pop()
            values=[row[k] for k in ("total","white_wins","draws","black_wins","reply_observed")]
            if any(not math.isfinite(v) or v<0 or int(v)!=v for v in values):
                raise ValueError("nonintegral or invalid empirical counts")
            if row["total"]!=sum(values[1:4]) or row["reply_observed"]!=row["total"]:
                raise ValueError("empirical outcome mass differs from observations")
        if actual!=legal: raise ValueError("ensemble must include all legal replies")

def write_overlay(path,frame,*,colour,pool,aux,input_artifacts,recipe,scope):
    """Publish schema 3, including the count/model/source receipt hashes supplied by the caller."""
    from .stages import atomic_json, artifacts, file_sha
    validate_overlay_rows(frame,colour)
    pinned=artifacts(input_artifacts)
    if not {str(Path(pool).resolve()),str(Path(aux).resolve())}<=set(pinned):
        raise ValueError("ensemble must pin the outcome pool and aux sidecar")
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+".tmp")
    frame.write_parquet(tmp,compression="zstd")
    tmp.replace(path)
    atomic_json(str(path)+".json",dict(schema=3,sha256=file_sha(path),colour=colour,
                source_pool=str(pool),source_aux=str(aux),min_games=1,scope=scope,
                input_artifacts=input_artifacts,policy_recipe=recipe))
