"""Bounded Windows OpenSSH client for the existing standard-user mini launcher.

Use file-backed stdio, not captured pipes: killing a timed-out PowerShell parent
can leave SSH descendants holding pipe handles, making communicate() wait forever.
Submit is never retried here; an unknown outcome must be reconciled by the caller.
No remote launcher installation, privileges, host keys, or worker limits change.
"""
import base64
import json
import re
import subprocess
import tempfile
from pathlib import Path

SSH = 'C:/Windows/System32/OpenSSH/ssh.exe'
SCP = 'C:/Windows/System32/OpenSSH/scp.exe'
OPTIONS = ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=20',
           '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3']


def run_bounded(command, *, timeout, input_text=None, check=False):
    """Bound the process and its local tree without waiting for pipe EOF."""
    with tempfile.TemporaryFile() as incoming, tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        if input_text is not None:
            incoming.write(input_text.encode('utf-8'))
        incoming.seek(0)
        proc = subprocess.Popen(command, stdin=incoming, stdout=out, stderr=err,
                                creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # This PID is the still-running process we just created, never a
            # name-based kill of unrelated SSH sessions or the remote worker.
            if proc.poll() is None:
                subprocess.run(['taskkill.exe', '/PID', str(proc.pid), '/T', '/F'],
                               stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=10,
                               creationflags=subprocess.CREATE_NO_WINDOW)
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)
            raise TimeoutError(f'Local transport exceeded {timeout}s; reconcile any submission before retrying')
        out.seek(0)
        err.seek(0)
        result = subprocess.CompletedProcess(command, proc.returncode,
                                             out.read().decode('utf-8', errors='replace'),
                                             err.read().decode('utf-8', errors='replace'))
        if check and result.returncode:
            raise RuntimeError(f'Local transport exited {result.returncode}: {result.stderr[-3000:]}')
        return result


def remote(script, *, timeout=180):
    script = "$ErrorActionPreference='Stop';$ProgressPreference='SilentlyContinue';" + script
    encoded = base64.b64encode(script.encode('utf-16le')).decode('ascii')
    prefix = 'powershell.exe -NoProfile -NonInteractive -ExecutionPolicy RemoteSigned -OutputFormat Text '
    if len(encoded) <= 6000:
        command = prefix + '-EncodedCommand ' + encoded
    else:
        # Avoid Windows SSH stdin delivery and command-line length limits.
        # Retain the unique request script as reconciliation evidence.
        import uuid
        remote("[IO.Directory]::CreateDirectory('C:/chess-worker/inputs/transport-requests')|Out-Null", timeout=30)
        name = uuid.uuid4().hex + '.ps1'
        target = 'C:/chess-worker/inputs/transport-requests/' + name
        with tempfile.TemporaryDirectory() as td:
            request = Path(td)/name
            request.write_text(script, encoding='utf-8-sig')
            scp(request, 'chess-mini:' + target, timeout=60)
        command = prefix + '-File ' + target
    return run_bounded([SSH, *OPTIONS, '-T', 'chess-mini', command],
                       timeout=timeout, check=True).stdout



def scp(source, destination, *, recursive=False, timeout=900):
    return run_bounded([SCP, *OPTIONS, '-q', *(['-r'] if recursive else []),
                        str(source), str(destination)], timeout=timeout, check=True)


def client(action, **kwargs):
    if action not in {'Health', 'Status', 'List', 'Logs', 'Submit', 'Collect'}:
        raise ValueError('unsupported transport action')
    job_id = kwargs.get('JobId')
    if job_id is not None and not re.fullmatch('[a-f0-9]{32}', job_id):
        raise ValueError('invalid job ID')
    if action == 'Collect':
        before = client('Status', JobId=job_id)
        if before['status'] not in {'succeeded', 'failed', 'cancelled', 'timed_out', 'interrupted'}:
            raise ValueError('collection requires a terminal attempt')
        destination = Path(kwargs['Destination']).resolve()
        if destination.exists() or not destination.parent.is_dir():
            raise ValueError('collection needs a new directory under an existing parent')
        destination.mkdir()
        scp('chess-mini:C:/chess-worker/jobs/'+job_id+'/.', destination, recursive=True, timeout=1800)
        after = client('Status', JobId=job_id)
        if (after['request_id'], after['status']) != (before['request_id'], before['status']):
            raise ValueError('attempt changed during collection; preserve partial copy')
        return dict(job_id=job_id, request_id=after['request_id'], collected_to=str(destination))
    params = dict(Action=action, Root='C:\\chess-worker', Tail=kwargs.get('Tail', 60))
    if job_id:
        params['JobId'] = job_id
    if action == 'Submit':
        data = Path(kwargs['ManifestPath']).read_bytes()
        if len(data) > 262144:
            raise ValueError('manifest too large')
        params['ManifestBase64'] = base64.b64encode(data).decode('ascii')
    payload = base64.b64encode(json.dumps(params).encode('utf-8')).decode('ascii')
    script = ("$o=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('"+payload+"'))|ConvertFrom-Json;"
              "$a=@{};foreach($p in $o.PSObject.Properties){$a[$p.Name]=$p.Value};"
              "& 'C:\\chess-worker\\launcher-v1\\Jobs.ps1' @a")
    return json.loads(remote(script, timeout=120))
