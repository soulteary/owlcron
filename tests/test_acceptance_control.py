"""Host-controller protocol and authority tests; no Docker daemon is used."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import uuid


class AcceptanceControlTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.ipc = self.base / "ipc"
        self.ipc.mkdir()
        self.log = self.base / "docker-log.jsonl"
        self.state = self.base / "state.json"
        self.state.write_text(json.dumps({"Paused": False, "Running": True}))
        executable = self.base / "docker"
        executable.write_text("#!" + sys.executable + "\n" + r'''
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ["FAKE_DOCKER_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
state_path = Path(os.environ["FAKE_DOCKER_STATE"])
state = json.loads(state_path.read_text())
if args == ["inspect", "owned-service"]:
    print(json.dumps([{"Config": {"Labels": {"io.owlcron.acceptance": "true"}}, "State": state}]))
elif args == ["pause", "owned-service"]:
    state["Paused"] = True
    state_path.write_text(json.dumps(state))
else:
    sys.exit(97)
''')
        executable.chmod(0o700)
        env = dict(os.environ, PATH=str(self.base) + os.pathsep + os.environ.get("PATH", ""),
                   FAKE_DOCKER_LOG=str(self.log), FAKE_DOCKER_STATE=str(self.state))
        self.process = subprocess.Popen(
            [sys.executable, str(Path(__file__).with_name("acceptance_control.py")),
             "--directory", str(self.ipc), "--container", "owned-service"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, umask=0o077)
        self.addCleanup(self.stop_controller)
        self.assertTrue(self.request({"container": "owned-service", "action": "inspect"})["ok"])

    def stop_controller(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        try:
            out, err = self.process.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate()
            self.fail("controller did not stop after SIGTERM")
        self.assertEqual(self.process.returncode, 0, (out, err))

    def read_log(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def request(self, payload=None, make_file=None):
        request = self.ipc / (uuid.uuid4().hex + ".request")
        if make_file is not None:
            make_file(request)
        else:
            data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            temporary = request.with_suffix(".tmp")
            temporary.write_bytes(data)
            temporary.rename(request)
        response = request.with_suffix(".response")
        deadline = time.monotonic() + 5
        while not response.exists() and time.monotonic() < deadline:
            self.assertIsNone(self.process.poll(), "controller exited on IPC input")
            time.sleep(0.01)
        self.assertTrue(response.exists(), "controller failed to answer IPC request")
        self.assertEqual(response.stat().st_mode & 0o044, 0o044,
                         "metadata response must be readable across host/container UIDs")
        result = json.loads(response.read_text())
        request.unlink(missing_ok=True)
        response.unlink()
        return result

    def assert_rejected_without_docker(self, payload=None, make_file=None):
        before = self.read_log()
        result = self.request(payload, make_file)
        self.assertFalse(result["ok"])
        self.assertEqual(self.read_log(), before, "rejected request invoked Docker")
        self.assertTrue(self.request({"container": "owned-service", "action": "inspect"})["ok"],
                        "malformed input prevented later valid requests")

    def test_malformed_requests_do_not_stop_controller(self):
        for payload in (None, [], b"{", b"\xff", {"container": [], "action": "pause"},
                        {"container": "owned-service", "action": []}):
            with self.subTest(payload=payload):
                self.assert_rejected_without_docker(payload)

    def test_special_files_do_not_block_or_escape(self):
        self.assert_rejected_without_docker(make_file=lambda path: os.mkfifo(path))
        outside = self.base / "outside.json"
        original = b'{"container":"owned-service","action":"pause"}'
        outside.write_bytes(original)
        self.assert_rejected_without_docker(make_file=lambda path: path.symlink_to(outside))
        self.assert_rejected_without_docker(make_file=lambda path: os.link(outside, path))
        self.assertEqual(outside.read_bytes(), original)

    def test_container_and_action_authority(self):
        for payload in ({"container": "unrelated-service", "action": "pause"},
                        {"container": "owned-service", "action": "exec"},
                        {"container": "owned-service", "action": "rm"}):
            with self.subTest(payload=payload):
                self.assert_rejected_without_docker(payload)

    def test_pause_is_idempotent_and_sigterm_stops(self):
        for _ in range(2):
            self.assertTrue(self.request({"container": "owned-service", "action": "pause"})["ok"])
        mutations = [entry for entry in self.read_log() if entry[0] != "inspect"]
        self.assertEqual(mutations, [["pause", "owned-service"]])
        self.stop_controller()


if __name__ == "__main__":
    unittest.main()
