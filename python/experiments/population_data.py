"""Account-disjoint, time-separated online opponent-policy experiment, and external OTB test."""
import hashlib
import itertools
import json
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
import sys
import chess
import duckdb
import polars as pl
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import run_practical_rebuild as rr
from stage1_extract_positions import iter_san_moves
from zobrist import IncrementalZobrist, zobrist_int64

OUT = rr.OUT / "population-transfer-v1"
RAW = Path("D:/data/chess/standard-chess-games-compressed")
OTB = Path("D:/data/chess/tournament-games-by-source/data")
EVENTS = ("Blitz", "Rapid", "Classical")
# Stable game samples within speed. Account partitions are independent of sampling.
DIV = {"Blitz":2048, "Rapid":1024, "Classical":16}
TARGETS = {"blitz-now":("Blitz",1600,1700), "blitz-goal":("Blitz",2000,2100),
           "rapid-now":("Rapid",1800,1900), "rapid-goal":("Rapid",2000,2100),
           "slow-now":("Classical",1800,1900), "slow-goal":("Classical",2000,2100),
           "slow-stress":("Classical",2200,2300)}
COLS = {"h":pl.Int64,"m":pl.String,"band":pl.Int16,"pooled":pl.Boolean,"player":pl.UInt64}
START = chess.Board()

def band(r):
    return 2200 if r>=2200 else r//200*200 if r>=1600 else 1000 if r>=1000 else 0

@lru_cache(maxsize=100000)
def actor(n):
    return int.from_bytes(hashlib.blake2b(n.strip().casefold().encode(),digest_size=8,person=b'actor-v1').digest(),'big')

def panel():
    p = Path("C:/Users/David/Documents/Chess Opening Dashboard/data/openings_paths.parquet")
    boards = {}
    for sans, in pl.read_parquet(p, columns=['move_san']).iter_rows():
        b=chess.Board()
        for s in sans[:30]:
            boards[zobrist_int64(b)]=b.epd()
            b.push_san(s)
    for c in ("white","black"):
        for h,e in pl.read_parquet(rr.incumbent(c),columns=["position_hash","position_epd"]).drop_nulls().iter_rows():
            boards[h]=e
    return boards

def write(frame,path):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(".tmp")
    frame.write_parquet(tmp); tmp.replace(path)

def game_edges(moves):
    b=chess.Board(); z=IncrementalZobrist(b)
    for i,s in enumerate(itertools.islice(moves,30)):
        try:
            mv=b.parse_san(s)
        except ValueError:
            break
        if not mv:
            break
        h=z.current(b)
        yield i,h,b.san(mv).rstrip("+#")
        z.push_move(b,mv)

def online():
    boards=panel()
    con=duckdb.connect(); con.execute("set threads=2")
    for month in range(4,10):
        split="train" if month<=7 else "tune" if month==8 else "test"
        partitions=range(0,6) if split=="train" else range(6,8) if split=="tune" else range(8,10)
        for event in EVENTS:
            files=sorted((RAW/f"year=2026/month={month}/event={event}").glob("*.parquet"))
            if not files: raise ValueError((event,month))
            # Oversample validation relative to training to offset small unseen-player folds.
            div=DIV[event] if split=="train" else max(1,DIV[event]//4)
            for f in files:
                dest=OUT/"edges"/split/event/f"{month}-{f.stem}.parquet"
                if dest.exists(): continue
                query="""select distinct on(game_id) game_id,white,black,white_elo,black_elo,mean_elo,movetext
                         from read_parquet(?) where hash(game_id)%?=0 and white_score is not null and mean_elo is not null
                         and greatest(white_elo,black_elo)>=1000 and white is not null and black is not null
                         and coalesce(white_title,'') <> 'BOT' and coalesce(black_title,'') <> 'BOT'
                         and coalesce(termination,'') not in ('Rules infraction','Abandoned') and move_count>0"""
                rows=[]
                for gid,w,b,wr,br,mr,mt in con.execute(query,[str(f),div]).fetchall():
                    players=[actor(w),actor(b)]
                    valid=[r is not None and r>=1000 and a%10 in partitions for r,a in zip((wr,br),players)]
                    if not any(valid):continue
                    ratings=[band(r or 0) for r in (wr,br)]
                    for i,h,m in game_edges(iter_san_moves(mt or "")):
                        side=i%2
                        if valid[side] and (split=="train" or h in boards):
                            rows.append((h,m,ratings[side],mr>=1800,players[side]))
                write(pl.DataFrame(rows,schema=COLS,orient="row"),dest)
            print(split,event,month,flush=True)

def otb():
    import io
    import chess.pgn
    boards=panel()
    # Use audited, explicit slow controls only. WhiteElo is a published tournament rating.
    sources={"mechanics-tnm":["G/120;d5","G/90;+30","30/90,SD/30;d5","40/85,SD/30;d5","30/90 SD30","30/90 GAME/30","30/90, SD 30","5400+30"],
             "northwest-washington":["5400+30","3600+10","G/90 + 30sec/move","40/120:0","40/7200:20/3600:1800"],
             "four-nations-league":["40/6000+30:3000+30","40/5400+30:1800+30","40/7200:20/3600:1800"]}
    seen=set(); stats={}
    for source,tcs in sources.items():
        counts=defaultdict(int); rows=[]
        g=pl.scan_parquet(str(OTB/f"data_source={source}/*.parquet")).filter(
            pl.col("TimeControl").is_in(tcs)&pl.col("FEN").is_null()&
            (pl.col("Variant").is_null()|pl.col("Variant").is_in(["Standard","Normal"]))&
            pl.col("Result").is_in(["1-0","0-1","1/2-1/2"])&
            pl.col("White").is_not_null()&pl.col("Black").is_not_null()&
            pl.col("GameDate").is_not_null()&(pl.col("GameDate")<=pl.date(2026,7,31))&
            (pl.col("WhiteElo").is_between(1800,2199)|pl.col("BlackElo").is_between(1800,2199)))
        for w,b,wr,br,dt,mt in g.select("White","Black","WhiteElo","BlackElo","GameDate","movetext").collect().iter_rows():
            counts["candidate_games"]+=1
            if not mt:continue
            game=chess.pgn.read_game(io.StringIO(mt))
            if game is None or game.errors:continue
            moves=list(game.mainline_moves())
            if not moves:continue
            ids=[actor(w),actor(b)]
            signature=(dt,*ids,tuple(m.uci() for m in moves))
            if signature in seen:continue
            seen.add(signature)
            counts["deduplicated_games"]+=1
            board=START.copy()
            for i,m in enumerate(moves[:30]):
                side=i%2; rating=(wr,br)[side]
                h=zobrist_int64(board)
                if h in boards and rating is not None and 1800<=rating<2200:
                    rows.append((h,board.san(m).rstrip("+#"),band(rating),False,ids[side]))
                board.push(m)
        write(pl.DataFrame(rows,schema=COLS,orient='row'),OUT/'otb'/f'{source}.parquet')
        stats[source]=dict(counts,decisions=len(rows))
        print(source,stats[source],flush=True)
    rr.publish(OUT/'otb-coverage.json',stats)
if __name__=="__main__":
    (online if sys.argv[1]=="online" else otb)()
