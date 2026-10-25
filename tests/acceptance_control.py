#!/usr/bin/env python3
"""Narrow host-side fault control for explicitly owned acceptance containers.

The Linux runner gets only this run's IPC directory, never the Docker socket.
"""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", required=True)
    parser.add_argument("--container", action="append", required=True)
    args = parser.parse_args()
    directory = Path(args.directory).resolve(strict=True)
    allowed = frozenset(args.container)
    actions = {"inspect", "pause", "unpause", "stop", "start", "restart", "kill"}
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def docker(*arguments):
        return subprocess.run(["docker", *arguments], capture_output=True,
                              text=True, timeout=60, check=True).stdout

    def inspect(name):
        value = json.loads(docker("inspect", name))[0]
        labels = value["Config"].get("Labels") or {}
        if labels.get("io.owlcron.acceptance") != "true":
            raise ValueError("container is not an owned acceptance service")
        return labels, value.get("State", {})

    for name in allowed:
        inspect(name)
    deadline = time.monotonic() + 3600
    while running and time.monotonic() < deadline:
        for request in directory.glob("*.request"):
            if not re.fullmatch(r"[a-f0-9]{32}\.request", request.name):
                continue
            response = request.with_suffix(".response")
            if response.exists():
                continue
            result = {"ok": False, "error": "invalid control request"}
            try:
                descriptor = os.open(request, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(descriptor) as source:
                    metadata = os.fstat(source.fileno())
                    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_size > 4096:
                        raise ValueError("invalid request file")
                    payload = json.loads(source.read(4097))
                if not isinstance(payload, dict):
                    raise ValueError("request must be an object")
                name, action = payload.get("container"), payload.get("action")
                if not isinstance(name, str) or not isinstance(action, str):
                    raise ValueError("invalid request fields")
                if name not in allowed or action not in actions:
                    raise ValueError("container or action is outside this acceptance run")
                labels, state = inspect(name)
                already_done = (action == "unpause" and not state.get("Paused")) or (action == "pause" and state.get("Paused")) or (action == "start" and state.get("Running")) or (action == "stop" and not state.get("Running"))
                if action != "inspect" and not already_done:
                    arguments = [action]
                    if action in {"stop", "restart"}:
                        arguments += ["--time", "1"]
                    if action == "kill":
                        # The IPC peer cannot choose a signal or another target.
                        arguments += ["--signal", "KILL"]
                    docker(*arguments, name)
                result = {"ok": True, "labels": labels}
            except (ValueError, KeyError, TypeError, UnicodeError, OSError, subprocess.SubprocessError):
                # Never echo inspect/env output or credentials into test reports.
                result = {"ok": False, "error": "isolated service control failed"}
            # Consume before publishing: the peer may remove its response as soon
            # as it sees it. Leaving the request visible could repeat a restart.
            request.unlink(missing_ok=True)
            temporary = response.with_suffix(".response.tmp")
            try:
                descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
                os.fchmod(descriptor, 0o644)
                with os.fdopen(descriptor, "w") as output:
                    json.dump(result, output)
                os.replace(temporary, response)
            except OSError:
                # A malformed IPC peer must not redirect host writes through links.
                continue
        time.sleep(0.02)


if __name__ == "__main__":
    main()
