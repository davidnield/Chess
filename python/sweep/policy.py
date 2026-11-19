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

def ensemble(legal, broad, adult, targeted, otb, maia, *, otb_strength=10.):
    """Counts are mutually nested b >= a >= r PER MOVE, and OTB is disjoint."""
    L=len(legal)
    if not L or len(set(legal))!=L: raise ValueError("need unique legal moves")
    b,a,r,o=[_vector(n,v,L) for n,v in
             zip(("broad","1800+","target","OTB"),(broad,adult,targeted,otb))]
    q=_vector("Maia",maia,L,True)
    if any(not (bb>=aa>=rr) for bb,aa,rr in zip(b,a,r)):
        raise ValueError("overlapping/superset counts do not support subtraction")
    if not math.isfinite(otb_strength) or otb_strength<=0: raise ValueError("invalid OTB strength")
    B,A,R,O=map(sum,(b,a,r,o))
    if B==0 and O==0: return q
    support=R+.10*(A-R)+.01*(B-A)
    result=[]
    for bb,aa,rr,oo,qq in zip(b,a,r,o,q):
        broad_p=(bb+1/L)/(B+1)
        backup=(aa-rr+30*broad_p)/(A-R+30)
        online=(rr+30*backup)/(R+30)
        pre=(support*online+3*qq)/(support+3)
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
    def get_or_compute(self,fen,legal,counts,compute):
        # FEN includes EP/castling and any model-visible counters; hash collisions
        # in the legacy low half cannot alias here.
        key=digest(dict(recipe=self.recipe,fen=fen,legal=legal,counts=counts))
        row=self.db.execute("SELECT value FROM probabilities WHERE k=?",(key,)).fetchone()
        if row:
            import json
            return json.loads(row[0]), True
        result=_vector("cached policy",compute(),len(legal),True)
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO probabilities VALUES (?,?)",(key,canonical(result)))
        return result, False
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
