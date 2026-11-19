"""Explicit-checkpoint Maia client; complete legal vectors, one temperature transform."""
import json
import math
import os
from pathlib import Path
import subprocess
import queue
import threading
from .stages import file_sha

class MaiaPolicy:
    def __init__(self,config,recipe):
        self.config,self.recipe=config,recipe
        self.process=None
        self.checkpoint_sha=file_sha(config["checkpoint"])

    def read(self):
        result=queue.Queue(maxsize=1)
        def worker():
            try: result.put(self.process.stdout.readline())
            except Exception as exc: result.put(exc)
        thread=threading.Thread(target=worker,daemon=True);thread.start()
        try: line=result.get(timeout=self.config.get("timeout_seconds",600))
        except queue.Empty:
            self.close();raise TimeoutError("Maia worker response timeout")
        if isinstance(line,Exception): self.close();raise line
        if not line: self.close();raise RuntimeError("Maia worker exited without a response")
        return json.loads(line)

    def start(self):
        script=Path(__file__).resolve().parents[1]/"maia_server.py"
        cmd=[self.config["python"],"-I","-X","utf8",str(script),"--model","79m",
             "--checkpoint",self.config["checkpoint"]]
        self.process=subprocess.Popen(cmd,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                text=True,encoding="utf8",bufsize=1,
                creationflags=getattr(subprocess,"CREATE_NO_WINDOW",0) if os.name=="nt" else 0)
        ready=self.read()
        if ready.get("checkpoint_sha256")!=self.checkpoint_sha or not ready.get("ready"):
            self.close(); raise ValueError("Maia worker loaded another checkpoint")

    def __call__(self,boards):
        if self.process is None: self.start()
        r=self.recipe["ratings"]
        query=dict(epd=[" ".join(b.fen(en_passant="fen").split()[:4]) for b in boards],
                   self=[r[0]]*len(boards),oppo=[r[1]]*len(boards))
        self.process.stdin.write(json.dumps(query)+"\n"); self.process.stdin.flush()
        answer=self.read()["probs"]
        if len(answer)!=len(boards): raise ValueError("Maia worker returned an incomplete batch")
        result=[]
        for b,p in zip(boards,answer):
            u={b.parse_san(s).uci():v for s,v in p.items()}
            if set(u)!={m.uci() for m in b.legal_moves} or any(not math.isfinite(v) or v<0 for v in u.values()):
                raise ValueError("invalid legal Maia output")
            powered={m:v**(1/self.recipe["temperature"]) for m,v in u.items()}
            total=sum(powered.values())
            if not total: raise ValueError("zero Maia mass")
            result.append({m:v/total for m,v in powered.items()})
        return result

    def close(self):
        if self.process is not None:
            try:
                self.process.stdin.close(); self.process.wait(timeout=20)
            except (OSError,subprocess.TimeoutExpired): self.process.kill(); self.process.wait()
            self.process=None


def runtime_identity(python):
    """No model/GPU load: versions plus Maia package sources bind cache compatibility."""
    script = """
import hashlib,importlib.metadata as md,importlib.util,json,sys
from pathlib import Path
names=['torch','numpy','chess','einops','maia3']
versions={}
for name in names:
    try: versions[name]=md.version(name)
    except md.PackageNotFoundError: versions[name]=None
spec=importlib.util.find_spec('maia3')
if not spec or not spec.origin: raise RuntimeError('Maia package is missing')
root=Path(spec.origin).parent
sources={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.rglob('*.py'))}
print(json.dumps(dict(python=sys.version,versions=versions,maia_sources=sources),sort_keys=True))
"""
    result=subprocess.run([str(python),"-I","-c",script],capture_output=True,text=True,
                          encoding="utf8",check=True,timeout=30,
                          creationflags=getattr(subprocess,"CREATE_NO_WINDOW",0) if os.name=="nt" else 0)
    return json.loads(result.stdout)
