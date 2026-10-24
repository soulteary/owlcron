#!/usr/bin/env python3
"""Accept an extracted single-node distribution using only Python's standard library.

The application and its child plugins run with an empty PATH, without source,
Go, Bun, or node_modules. Python is test infrastructure, not a runtime dependency.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request
from check_frontend_assets import check_frontend_assets


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def accept_distribution(distribution):
    distribution = Path(distribution).resolve()
    binary = distribution / 'owlcron-engine'
    require(binary.is_file(), 'Distribution engine missing')
    with tempfile.TemporaryDirectory(prefix='owlcron-release-accept-') as directory:
        work = Path(directory)
        empty_path = work / 'empty-path'
        empty_path.mkdir()
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(('OWLCRON_', 'GO', 'BUN', 'NODE'))}
        env.update(PATH=str(empty_path), HOME=str(work), TMPDIR=str(work))
        password = secrets.token_urlsafe(24)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        config = dict(http_port=port, base_app_url=f'http://127.0.0.1:{port}',
                      hostname='release-node', secret_key=secrets.token_hex(32),
                      storage_engine='Filesystem', storage_base_dir=str(work / 'original/data'),
                      log_dir=str(work / 'original/logs'), queue_dir=str(work / 'original/queue'),
                      udp_broadcast_port=-1, scheduler_startup_grace=1)
        path = work / 'config.json'
        session = ''
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        processes = []

        def write_config():
            path.write_text(json.dumps(config))
            path.chmod(0o600)

        def cli(*args):
            return subprocess.run([str(binary), '--config', str(path), *args],
                                  cwd=distribution, env=env, capture_output=True,
                                  check=True, timeout=30)

        def api(endpoint, data=None):
            request = urllib.request.Request(config['base_app_url'] + '/api/' + endpoint,
                data=json.dumps(data or {}).encode(),
                headers={'Content-Type': 'application/json', 'X-Session-ID': session})
            with opener.open(request, timeout=5) as response:
                result = json.load(response)
            require(result.get('code') == 0, f'{endpoint} failed')
            return result

        def start(bootstrap=False):
            nonlocal session
            launch_env = dict(env)
            if bootstrap:
                launch_env.update(OWLCRON_ADMIN_USERNAME='release-admin', OWLCRON_ADMIN_PASSWORD=password)
            log = (work / f'daemon-{len(processes)}.log').open('wb')
            process = subprocess.Popen([str(binary), '--config', str(path)],
                                       cwd=distribution, env=launch_env, stdout=log, stderr=log)
            processes.append((process, log))
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                require(process.poll() is None, 'Distribution daemon exited during startup')
                try:
                    with opener.open(config['base_app_url'] + '/', timeout=1) as response:
                        require(b'js/_combo.js' in response.read(), 'Built frontend missing')
                    break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError('Distribution daemon readiness timed out')
            session = api('user/login', dict(username='release-admin', password=password))['session_id']
            while time.monotonic() < deadline:
                if api('app/status').get('master'):
                    return process
                time.sleep(0.1)
            raise RuntimeError('Single node did not become leader')

        def stop(process):
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise RuntimeError('Distribution daemon shutdown timed out')
            require(process.returncode == 0, 'Distribution daemon shutdown failed')

        def run_event(event='release-script', marker=b'RELEASE_SCRIPT_OK'):
            job = api('app/run_event', {'id': event})['ids'][0]
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                status = api('app/get_job_status', {'id': job})
                if status.get('complete'):
                    require(status['job']['code'] == 0, 'Bundled plugin job failed')
                    break
                time.sleep(0.1)
            else:
                raise RuntimeError('Bundled plugin job timed out')
            history = api('app/get_event_history', {'id': event})
            require(any(row['id'] == job for row in history['rows']), 'Job missing from history')
            output = api('app/get_job_log', {'id': job, 'offset': 0, 'limit': 32768})
            require(marker in base64.b64decode(output['log_base64']), 'Plugin output missing')
            return job

        try:
            write_config()
            cli('setup')
            process = start(bootstrap=True)
            check_frontend_assets(config['base_app_url'])
            api('app/create_event', dict(id='release-echo', title='Release Echo acceptance', enabled=0,
                category='general', target='local', plugin='echoplug', timing={}, timezone='UTC',
                max_children=1, params=dict(message='RELEASE_ECHO_OK', repeat='1', delay_ms='0')))
            run_event('release-echo', b'RELEASE_ECHO_OK')
            script = work / 'acceptance.sh'
            script.write_text('#!/bin/sh\nprintf "RELEASE_SCRIPT_OK\\n"\n')
            script.chmod(0o700)
            api('app/create_plugin', dict(id='release-adapter', title='Release script adapter',
                enabled=True, command=f'{distribution / "owlcron-script"} -- /bin/sh {script}', params=[]))
            api('app/create_event', dict(id='release-script', title='Release acceptance', enabled=0,
                category='general', target='local', plugin='release-adapter', timing={}, timezone='UTC',
                max_children=1, params={}))
            first_job = run_event()
            stop(process)
            process = start()
            history = api('app/get_event_history', {'id': 'release-script'})
            require(any(row['id'] == first_job for row in history['rows']), 'Restart lost job history')
            second_job = run_event()
            stop(process)
            backup = work / 'backup.json'
            cli('export', str(backup))
            data = json.loads(backup.read_text())
            require(data.get('format') == 'owlcron-storage-v1' and data.get('records'), 'Backup format invalid')
            require(backup.stat().st_mode & 0o077 == 0, 'Backup permissions expose private data')
            # Both source and destination daemons are stopped during backup/restore.
            # Import only the application's own format, into a separate empty store.
            shutil.rmtree(work / 'original')
            config['storage_base_dir'] = str(work / 'restored/data')
            config['log_dir'] = str(work / 'restored/logs')
            config['queue_dir'] = str(work / 'restored/queue')
            write_config()
            cli('import', str(backup))
            process = start()
            history = api('app/get_event_history', {'id': 'release-script'})
            for prior_job in (first_job, second_job):
                require(any(row['id'] == prior_job for row in history['rows']), 'Restore lost prior job history')
                output = api('app/get_job_log', {'id': prior_job, 'offset': 0, 'limit': 32768})
                require(b'RELEASE_SCRIPT_OK' in base64.b64decode(output['log_base64']),
                        'Restore lost persisted job log')
            run_event()
            stop(process)
        finally:
            for process, log in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                log.close()
    print('Release acceptance passed: empty runtime PATH, frontend asset graph, login, Echo Plugin, script adapter, '
          'history, restart, offline backup and fresh-store restore')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('distribution', type=Path)
    accept_distribution(parser.parse_args().distribution)


if __name__ == '__main__':
    main()
