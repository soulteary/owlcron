#!/usr/bin/env python3
"""Exercise a built distribution with isolated data and a real daemon/plugin."""
import base64
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.request
from check_frontend_assets import check_frontend_assets

ROOT = Path(__file__).resolve().parent.parent


def main():
    subprocess.run(['/bin/sh', str(Path(__file__).resolve().parent / 'smoke_script_adapter.sh'),
                    str(ROOT / 'owlcron-script')], check=True)
    with tempfile.TemporaryDirectory(prefix='owlcron-smoke-') as directory:
        work = Path(directory)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        config = dict(http_port=port, base_app_url=f'http://127.0.0.1:{port}',
                      hostname='smoke-node', secret_key=secrets.token_hex(32),
                      storage_engine='Filesystem', storage_base_dir=str(work / 'data'),
                      log_dir=str(work / 'logs'), queue_dir=str(work / 'queue'),
                      udp_broadcast_port=-1, scheduler_startup_grace=1)
        path = work / 'config.json'
        path.write_text(json.dumps(config))
        path.chmod(0o600)
        command = [str(ROOT / 'owlcron-engine'), '-config', str(path)]
        subprocess.run(command + ['setup'], cwd=ROOT, check=True, capture_output=True)
        password = secrets.token_urlsafe(24)
        env = {key: value for key, value in os.environ.items() if not key.startswith('OWLCRON_')}
        env.update(OWLCRON_ADMIN_USERNAME='smoke-admin', OWLCRON_ADMIN_PASSWORD=password)
        session = ''
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def api(endpoint, data=None):
            request = urllib.request.Request(config['base_app_url'] + endpoint,
                data=json.dumps(data or {}).encode(),
                headers={'Content-Type': 'application/json', 'X-Session-ID': session})
            with opener.open(request, timeout=5) as response:
                result = json.load(response)
            if result.get('code') != 0:
                raise RuntimeError(f'{endpoint} failed: {result.get("description", "unknown error")}')
            return result

        with (work / 'daemon.log').open('wb') as log:
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=log)
            try:
                for _ in range(100):
                    if process.poll() is not None:
                        raise RuntimeError('Daemon exited before becoming ready')
                    try:
                        with opener.open(config['base_app_url'] + '/', timeout=1) as response:
                            assert b'js/_combo.js' in response.read(), 'Distribution frontend missing'
                        break
                    except (OSError, AssertionError):
                        time.sleep(0.1)
                else:
                    details = (work / 'daemon.log').read_text()[-6000:]
                    details = details.replace(password, '[redacted]').replace(config['secret_key'], '[redacted]')
                    raise RuntimeError('Daemon startup timed out: ' + details)
                for asset in ['/js/_combo.js', '/css/_combo.css', '/js/external/jquery.min.js']:
                    with opener.open(config['base_app_url'] + asset, timeout=5) as response:
                        assert response.status == 200 and response.read(), f'Missing frontend asset: {asset}'
                check_frontend_assets(config['base_app_url'])
                session = api('/api/user/login', dict(username='smoke-admin', password=password))['session_id']
                for _ in range(100):
                    if api('/api/app/status').get('master'):
                        break
                    time.sleep(0.1)
                else:
                    raise RuntimeError('Node did not become leader')
                api('/api/app/create_event', dict(id='smoke-event', title='Install smoke', enabled=0,
                    category='general', target='local', plugin='echoplug', timing={}, timezone='UTC',
                    max_children=1, params=dict(message='OWLCRON_SMOKE_OK', repeat='1', delay_ms='0')))
                job = api('/api/app/run_event', {'id': 'smoke-event'})['ids'][0]
                for _ in range(150):
                    status = api('/api/app/get_job_status', {'id': job})
                    if status.get('complete'):
                        assert status['job']['code'] == 0, 'Plugin failed'
                        break
                    time.sleep(0.1)
                else:
                    raise RuntimeError('Plugin completion timed out')
                history = api('/api/app/get_event_history', {'id': 'smoke-event'})
                assert any(row['id'] == job for row in history['rows']), 'Completed job missing from history'
                output = api('/api/app/get_job_log', {'id': job, 'offset': 0, 'limit': 32768})
                assert b'OWLCRON_SMOKE_OK' in base64.b64decode(output['log_base64']), 'Job log missing'
                print('Install smoke passed: frontend, login, leadership, event, plugin, history, log')
            finally:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            if process.returncode != 0:
                raise RuntimeError(f'Daemon shutdown failed: {process.returncode}')


if __name__ == '__main__':
    main()
