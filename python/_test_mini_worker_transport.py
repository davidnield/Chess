import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import psutil
import mini_worker_transport as transport


@unittest.skipUnless(os.name == 'nt', 'Windows transport contract')
class TransportTests(unittest.TestCase):
    def test_large_stdout_does_not_fill_a_pipe(self):
        r = transport.run_bounded([sys.executable, '-c', "print('x'*300000)"], timeout=10, check=True)
        self.assertEqual(len(r.stdout.strip()), 300000)

    def test_input_is_complete_and_closed(self):
        text = 'hello\n' * 50000
        r = transport.run_bounded([sys.executable, '-c', 'import sys;print(len(sys.stdin.read()))'], input_text=text, timeout=10)
        self.assertEqual(int(r.stdout), len(text))

    def test_timeout_kills_inheriting_child_and_returns(self):
        with tempfile.TemporaryDirectory() as td:
            pid_file = Path(td)/'child.pid'
            code = ('import subprocess,sys,time,pathlib;'
                    'p=subprocess.Popen([sys.executable,"-c","import time;time.sleep(60)"]);'
                    f'pathlib.Path({str(pid_file)!r}).write_text(str(p.pid));time.sleep(60)')
            start = time.monotonic()
            with self.assertRaises(TimeoutError):
                transport.run_bounded([sys.executable, '-c', code], timeout=2)
            self.assertLess(time.monotonic()-start, 15)
            self.assertTrue(pid_file.exists())
            self.assertFalse(psutil.pid_exists(int(pid_file.read_text())))

    def test_submission_timeout_is_not_retried(self):
        with tempfile.TemporaryDirectory() as td:
            f=Path(td)/'manifest.json';f.write_text('{}')
            with patch.object(transport, 'remote', side_effect=TimeoutError('unknown')) as remote:
                with self.assertRaises(TimeoutError):transport.client('Submit', ManifestPath=f)
                self.assertEqual(remote.call_count, 1)

    def test_legacy_capture_timeout_reproduces_inherited_pipe_hang(self):
        with tempfile.TemporaryDirectory() as td:
            pid_file = Path(td)/'legacy-child.pid'
            parent = ('import subprocess,sys,time,pathlib;'
                      'p=subprocess.Popen([sys.executable,"-c","import time;time.sleep(60)"]);'
                      f'pathlib.Path({str(pid_file)!r}).write_text(str(p.pid));time.sleep(60)')
            legacy = ('import subprocess,sys;'
                      f'subprocess.run([sys.executable,"-c",{parent!r}],capture_output=True,timeout=1)')
            try:
                # The old inner one-second timeout cannot return while its
                # orphan holds the capture pipe. An independent guard bounds it.
                with self.assertRaises(TimeoutError):
                    transport.run_bounded([sys.executable,'-c',legacy],timeout=3)
            finally:
                if pid_file.exists():
                    pid=int(pid_file.read_text())
                    if psutil.pid_exists(pid):
                        child=psutil.Process(pid);child.kill();child.wait(timeout=5)

    def test_job_id_rejected_before_network(self):
        with patch.object(transport, 'remote') as remote:
            with self.assertRaises(ValueError):transport.client('Status', JobId='../wrong')
            remote.assert_not_called()

    def test_changed_collection_attempt_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            states = [dict(request_id='old',status='succeeded'),dict(request_id='new',status='succeeded')]
            with patch.object(transport,'remote',side_effect=[json.dumps(s) for s in states]),patch.object(transport,'scp'):
                with self.assertRaisesRegex(ValueError,'attempt changed'):
                    transport.client('Collect', JobId='a'*32, Destination=Path(td)/'new')


if __name__ == '__main__':unittest.main()
