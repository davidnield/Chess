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
    def __init__(self,config,recipe,*,runtime=None):
        self.config,self.recipe=config,recipe
        self.process=None
        self.checkpoint_sha=file_sha(config["checkpoint"])
        self.raw_cache=None
        if config.get("raw_cache"):
            import inspect
            from .spec import digest
            from .prediction_cache import PredictionCache
            code=Path(__file__).resolve().parents[1]
            identity=dict(checkpoint_sha256=self.checkpoint_sha,
                python_sha256=file_sha(config["python"]),runtime=runtime or runtime_identity(config["python"]),
                local_chess=__import__('chess').__version__,
                sources={p.name:file_sha(p) for p in [code/"maia_server.py",code/"maia_batch.py",code/"zobrist128.py"]},
                ratings=list(recipe["ratings"]),protocol=digest(inspect.getsource(self.infer_raw)),
                contract="standard-fen4-repeat-history-legal-softmax-f32-v1")
            self.raw_cache=PredictionCache(config["raw_cache"],identity)

    def read(self):
        result=queue.Queue(maxsize=1)
        def worker():
            try: result.put(self.process.stdout.readline())
            except Exception as exc: result.put(exc)
        thread=threading.Thread(target=worker,daemon=True);thread.start()
        try: line=result.get(timeout=self.config.get("timeout_seconds",600))
        except queue.Empty:
            self.stop_worker();raise TimeoutError("Maia worker response timeout")
        if isinstance(line,Exception): self.stop_worker();raise line
        if not line: self.stop_worker();raise RuntimeError("Maia worker exited without a response")
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
            self.stop_worker(); raise ValueError("Maia worker loaded another checkpoint")

    def infer_raw(self,boards):
        if not boards:return []
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
            if len(u)!=len(p):raise ValueError("duplicate Maia legal move")
            result.append(u)
        return result

    def __call__(self,boards):
        from book_builder_rs import prediction_temperature
        if not boards:return []
        raw=self.raw_cache.predict(boards,self.infer_raw) if self.raw_cache else self.infer_raw(boards)
        return [dict(zip(p,prediction_temperature(list(p.values()),self.recipe["temperature"]))) for p in raw]

    def cache_metrics(self):
        return dict(self.raw_cache.stats) if self.raw_cache is not None else {}

    def stop_worker(self):
        if self.process is not None:
            try:
                self.process.stdin.close(); self.process.wait(timeout=20)
            except (OSError,subprocess.TimeoutExpired): self.process.kill(); self.process.wait()
            self.process=None

    def close(self):
        self.stop_worker()
        if self.raw_cache is not None:self.raw_cache.close();self.raw_cache=None


def runtime_identity(python):
    """No model load: versions, package sources and GPU identity bind compatibility."""
    script = """
import hashlib,importlib.metadata as md,importlib.util,json,sys,torch
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
hardware=dict(cuda=torch.version.cuda,cudnn=torch.backends.cudnn.version(),
              devices=[dict(name=torch.cuda.get_device_name(i),capability=torch.cuda.get_device_capability(i))
                       for i in range(torch.cuda.device_count())])
print(json.dumps(dict(python=sys.version,versions=versions,maia_sources=sources,hardware=hardware),sort_keys=True))
"""
    result=subprocess.run([str(python),"-I","-c",script],capture_output=True,text=True,
                          encoding="utf8",check=True,timeout=30,
                          creationflags=getattr(subprocess,"CREATE_NO_WINDOW",0) if os.name=="nt" else 0)
    return json.loads(result.stdout)
