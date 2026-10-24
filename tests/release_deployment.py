#!/usr/bin/env python3
"""Exercise an extracted release behind real Nginx, using synthetic migration data.

Requires Python, Nginx and OpenSSL on the test host. No existing configuration,
Cronicle installation, public certificate, SMTP relay or customer data is used.
"""
import argparse
import base64
import hashlib
from http.cookies import SimpleCookie
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from check_frontend_assets import check_frontend_assets
from release_acceptance import require

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class WebSocket:
    """Small bounded RFC 6455 client for the application's JSON text messages."""

    def __init__(self, port, prefix, context):
        self.socket = context.wrap_socket(socket.create_connection(('localhost', port), timeout=5),
                                          server_hostname='localhost')
        self.stream = self.socket.makefile('rb')
        key = base64.b64encode(secrets.token_bytes(16)).decode()
        origin = f'https://localhost:{port}'
        self.socket.sendall((f'GET {prefix}/ws HTTP/1.1\r\nHost: localhost:{port}\r\n'
            f'Origin: {origin}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
            f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        require(self.stream.readline(4096).startswith(b'HTTP/1.1 101 '), 'WSS upgrade failed')
        headers = {}
        for _ in range(50):
            line = self.stream.readline(4096)
            if line == b'\r\n':
                break
            require(line and b':' in line, 'Invalid WSS response')
            name, value = line.decode().split(':', 1)
            headers[name.lower()] = value.strip()
        else:
            raise RuntimeError('Too many WSS response headers')
        expected = base64.b64encode(hashlib.sha1(
            (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        require(headers.get('sec-websocket-accept') == expected, 'Invalid WSS accept header')

    def send_frame(self, opcode, data):
        require(len(data) < 65536, 'Test WebSocket message too large')
        mask = secrets.token_bytes(4)
        length = bytes([0x80 | len(data)]) if len(data) < 126 else b'\xfe' + struct.pack('!H', len(data))
        self.socket.sendall(bytes([0x80 | opcode]) + length + mask +
                            bytes(value ^ mask[index % 4] for index, value in enumerate(data)))

    def send(self, kind, data):
        self.send_frame(1, json.dumps(dict(type=kind, data=data)).encode())

    def read(self, count):
        value = self.stream.read(count)
        require(len(value) == count, 'WSS closed before expected message')
        return value

    def receive(self, kind, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.socket.settimeout(max(0.1, deadline - time.monotonic()))
            first, second = self.read(2)
            require(first & 0x80 and not second & 0x80, 'Unexpected WSS frame format')
            length = second & 127
            if length == 126:
                length = struct.unpack('!H', self.read(2))[0]
            elif length == 127:
                length = struct.unpack('!Q', self.read(8))[0]
            require(length <= 2 * 1024 * 1024, 'WSS response too large')
            data = self.read(length)
            opcode = first & 15
            if opcode == 9:
                self.send_frame(10, data)
                continue
            require(opcode == 1, 'Unexpected WSS opcode')
            message = json.loads(data)
            require(message['type'] != 'error', 'WSS application rejected request')
            if message['type'] == kind:
                return message['data']
        raise RuntimeError('WSS message timed out: ' + kind)

    def close(self):
        self.stream.close()
        self.socket.close()


def accept_deployment(distribution, checker, prefix):
    distribution, checker = Path(distribution).resolve(), Path(checker).resolve()
    nginx, openssl = shutil.which('nginx'), shutil.which('openssl')
    require(nginx and openssl, 'Install Nginx and OpenSSL on the acceptance host')
    fixture = ROOT / 'migration/testdata/cronicle-export.txt'
    original = fixture.read_bytes()
    reports = [subprocess.check_output([str(checker), '--export', str(fixture)], timeout=15)
               for _ in range(2)]
    require(reports[0] == reports[1] and fixture.read_bytes() == original,
            'Migration preflight must be deterministic and read-only')
    report = json.loads(reports[0])
    require(report['read_only'] and report['counts']['schedule'] == 2, 'Unexpected migration fixture')
    codes = {item['code'] for item in report['findings']}
    require({'storage_format', 'recreate_identity', 'inline_shell_adapter', 'scheduling_subset'} <= codes,
            'Preflight omitted a required conversion warning')
    require(not any(item['status'] == 'unsupported' for item in report['findings']),
            'Synthetic fixture has unsupported data')

    with tempfile.TemporaryDirectory(prefix='owlcron-deployment-') as directory:
        work = Path(directory)
        work.chmod(0o700)
        backend, public = free_port(), free_port()
        while public == backend:
            public = free_port()
        base = f'https://localhost:{public}{prefix}'
        config = dict(http_port=backend, base_app_url=base, base_path=prefix,
            hostname='release-deployment', secret_key=secrets.token_hex(32),
            session_cookie_secure=True, trusted_proxy_cidrs=['127.0.0.1/32'],
            storage_engine='Filesystem', storage_base_dir=str(work / 'original/data'),
            log_dir=str(work / 'original/logs'), queue_dir=str(work / 'original/queue'),
            udp_broadcast_port=-1, scheduler_startup_grace=1)
        config_path = work / 'config.json'
        config_path.write_text(json.dumps(config))
        config_path.chmod(0o600)
        cert, key = work / 'cert.pem', work / 'key.pem'
        subprocess.run([openssl, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
            '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost',
            '-keyout', str(key), '-out', str(cert)], check=True, capture_output=True, timeout=15)
        nginx_config = work / 'nginx.conf'
        nginx_config.write_text(f'''worker_processes 1;
pid {work}/nginx.pid;
error_log {work}/nginx-error.log;
events {{ worker_connections 64; }}
http {{
  access_log off;
  client_body_temp_path {work}/client-body;
  proxy_temp_path {work}/proxy;
  fastcgi_temp_path {work}/fastcgi;
  uwsgi_temp_path {work}/uwsgi;
  scgi_temp_path {work}/scgi;
  map $http_upgrade $owlcron_connection {{ default upgrade; '' close; }}
  server {{
    listen 127.0.0.1:{public} ssl;
    server_name localhost;
    ssl_certificate {cert};
    ssl_certificate_key {key};
    ssl_protocols TLSv1.2 TLSv1.3;
    location / {{
      proxy_pass http://127.0.0.1:{backend};
      proxy_http_version 1.1;
      proxy_set_header Host $http_host;
      proxy_set_header X-Forwarded-For $remote_addr;
      proxy_set_header X-Forwarded-Proto $scheme;
      proxy_set_header Upgrade $http_upgrade;
      proxy_set_header Connection $owlcron_connection;
      proxy_read_timeout 30s;
      proxy_buffering off;
    }}
  }}
}}
''')
        context = ssl.create_default_context(cafile=str(cert))
        cookies = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=context), urllib.request.HTTPCookieProcessor(cookies))
        empty_path = work / 'empty-path'
        empty_path.mkdir()
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(('OWLCRON_', 'GO', 'BUN', 'NODE'))}
        env.update(PATH=str(empty_path), HOME=str(work), TMPDIR=str(work))
        password = secrets.token_urlsafe(24)
        processes, sockets = [], []
        binary = distribution / 'owlcron-engine'

        def cli(*args):
            return subprocess.run([str(binary), '--config', str(config_path), *args],
                cwd=distribution, env=env, capture_output=True, check=True, timeout=30)

        def api(endpoint, data=None, expected=0, origin=None, expected_http=200):
            request = urllib.request.Request(base + '/api/' + endpoint,
                data=json.dumps(data or {}).encode(), headers={
                    'Content-Type': 'application/json', 'Origin': origin or f'https://localhost:{public}'})
            try:
                response = opener.open(request, timeout=5)
            except urllib.error.HTTPError as error:
                if expected == 0:
                    raise
                require(expected != 0 and error.code == expected_http,
                        endpoint + ' returned an unexpected HTTP error')
                error.close()
                return {'code': error.code}, error.headers
            with response:
                require(response.status == expected_http, endpoint + ' returned an unexpected HTTP status')
                result = json.load(response)
                headers = response.headers
            require((result.get('code') == 0) == (expected == 0), endpoint + ' returned unexpected status')
            return result, headers

        def launch(command, environment):
            log = (work / f'process-{len(processes)}.log').open('wb')
            process = subprocess.Popen(command, cwd=distribution, env=environment, stdout=log, stderr=log)
            processes.append((process, log))
            return process

        def start(bootstrap=False):
            launch_env = dict(env)
            if bootstrap:
                launch_env.update(OWLCRON_ADMIN_USERNAME='acceptance-admin', OWLCRON_ADMIN_PASSWORD=password)
            process = launch([str(binary), '--config', str(config_path)], launch_env)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                require(process.poll() is None, 'Daemon exited before readiness')
                try:
                    result, _ = api('app/status')
                    if result.get('master'):
                        return process
                except urllib.error.HTTPError as error:
                    if error.code not in (502, 503, 504):
                        raise
                except OSError:
                    pass
                time.sleep(0.1)
            raise RuntimeError('Deployment readiness timed out')

        def stop(process):
            process.terminate()
            process.wait(timeout=15)
            require(process.returncode == 0, 'Daemon shutdown failed')

        def login(username='acceptance-admin', secret=None):
            result, headers = api('user/login', dict(username=username, password=secret or password))
            cookie = SimpleCookie()
            cookie.load(headers['Set-Cookie'])
            session = cookie['session_id']
            require(session['secure'] and session['httponly'] and session['samesite'].lower() == 'lax',
                    'HTTPS session cookie flags missing')
            require(session['path'] == prefix + '/', 'Session cookie path mismatch')
            return result['session_id']

        def run(event, code=0, marker=b'MIGRATED_SCRIPT_OK'):
            result, _ = api('app/run_event', {'id': event})
            job = result['ids'][0]
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                status, _ = api('app/get_job_status', {'id': job})
                if status.get('complete'):
                    require(status['job']['code'] == code, 'Migrated script exit code changed')
                    output, _ = api('app/get_job_log', {'id': job, 'offset': 0, 'limit': 32768})
                    require(marker in base64.b64decode(output['log_base64']), 'Migrated script output missing')
                    return job
                time.sleep(0.1)
            raise RuntimeError('Migrated task timed out')

        try:
            cli('setup')
            subprocess.run([nginx, '-p', str(work), '-c', str(nginx_config), '-t'],
                           check=True, capture_output=True, timeout=10)
            proxy = launch([nginx, '-p', str(work), '-c', str(nginx_config), '-g', 'daemon off;'], os.environ)
            process = start(bootstrap=True)
            login()
            require(check_frontend_assets(base, opener) > 10, 'Incomplete HTTPS frontend graph')
            api('app/create_event', {}, expected=1, origin='https://attacker.example.invalid', expected_http=403)
            # Commands and script content are reviewed fixtures, never taken from an arbitrary export.
            script = work / 'reviewed-script.sh'
            script.write_text('#!/bin/sh\nprintf "MIGRATED_SCRIPT_OK\\n"\n')
            script.chmod(0o700)
            api('app/create_plugin', dict(id='migrated-shell', title='Reviewed shell adapter', enabled=True,
                command=f'{distribution / "owlcron-script"} -- /bin/sh {script}', params=[]))
            records = dict(line.split(' - ', 1) for line in original.decode().splitlines()
                           if line and not line.startswith('#'))
            source_events = json.loads(records['global/schedule/0'])['items']
            for source in source_events:
                event = dict(id='migrated-' + source['id'], title='Reviewed ' + source['title'], enabled=0,
                    plugin='migrated-shell', category='general', target='local',
                    timing=source['timing'], timezone=source['timezone'], params={}, max_children=1)
                api('app/create_event', event)
                stored, _ = api('app/get_event', {'id': event['id']})
                expected_timing = source['timing'] or None
                actual_timing = stored['event']['timing']
                if isinstance(actual_timing, dict):
                    actual_timing = {key: value for key, value in actual_timing.items() if value is not None}
                require(actual_timing == expected_timing, 'Migration changed timing')
                for field in ('timezone', 'target', 'plugin'):
                    require(stored['event'][field] == event[field], 'Migration changed ' + field)
            first = run('migrated-manual')
            run('migrated-daily')
            failed = work / 'reviewed-failure.sh'
            failed.write_text('#!/bin/sh\nprintf "MIGRATED_FAILURE_OK\\n"\nexit 7\n')
            failed.chmod(0o700)
            api('app/create_plugin', dict(id='failure-shell', title='Exit-code check', enabled=True,
                command=f'{distribution / "owlcron-script"} -- /bin/sh {failed}', params=[]))
            api('app/create_event', dict(id='migration-failure', title='Expected failure', enabled=0,
                plugin='failure-shell', category='general', target='local', timing=False, timezone='UTC', params={}))
            run('migration-failure', code=7, marker=b'MIGRATED_FAILURE_OK')
            api('user/admin_create', dict(username='acceptance-reader', password=password, enabled=True, privileges={}))
            login('acceptance-reader')
            api('app/run_event', {'id': 'migrated-manual'}, expected=1)
            session = login()
            socket_client = WebSocket(public, prefix, context)
            sockets.append(socket_client)
            socket_client.send('auth', {'session_id': session})
            require(socket_client.receive('auth_success')['authenticated'], 'WSS authentication failed')
            api('app/create_event', dict(id='deployment-stream', title='Live log acceptance', enabled=0,
                plugin='echoplug', category='general', target='local', timing=False, timezone='UTC',
                params=dict(message='DEPLOYMENT_WSS_OK', repeat='5', delay_ms='1000')))
            live, _ = api('app/run_event', {'id': 'deployment-stream'})
            socket_client.send('watch_job_log', {'job_id': live['ids'][0]})
            require(socket_client.receive('watching_log')['job_id'] == live['ids'][0], 'WSS subscribed to wrong job')
            deadline = time.monotonic() + 15
            while True:
                chunk = socket_client.receive('log_data', timeout=max(0.1, deadline - time.monotonic()))
                if 'DEPLOYMENT_WSS_OK' in chunk['data']:
                    break
                require(time.monotonic() < deadline, 'WSS log did not traverse proxy')
            socket_client.receive('log_complete')
            socket_client.close()
            sockets.clear()
            stop(process)
            process = start()
            # Persisted cookie must still authorize the same account after restart.
            api('app/get_event', {'id': 'migrated-manual'})
            stop(process)
            backup = work / 'backup.json'
            cli('export', str(backup))
            require(backup.stat().st_mode & 0o077 == 0, 'Backup is not private')
            shutil.rmtree(work / 'original')
            for field, folder in [('storage_base_dir', 'data'), ('log_dir', 'logs'), ('queue_dir', 'queue')]:
                config[field] = str(work / 'restored' / folder)
            config_path.write_text(json.dumps(config))
            cli('import', str(backup))
            process = start()
            login()
            history, _ = api('app/get_event_history', {'id': 'migrated-manual'})
            require(any(row['id'] == first for row in history['rows']), 'Migration restore lost history')
            output, _ = api('app/get_job_log', {'id': first, 'offset': 0, 'limit': 32768})
            require(b'MIGRATED_SCRIPT_OK' in base64.b64decode(output['log_base64']), 'Restore lost old logs')
            run('migrated-manual')
            api('user/logout')
            api('app/get_event', {'id': 'migrated-manual'}, expected=1)
            stop(process)
            require(proxy.poll() is None, 'Nginx exited during acceptance')
        finally:
            for client in sockets:
                client.close()
            for process, log in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                log.close()
    print(f'Release deployment passed ({prefix or "/"}): verified HTTPS, cookie policy, assets, '
          'WSS live logs, reviewed migration, denied reader writes, exit codes, restart and fresh-store restore')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('distribution', type=Path)
    parser.add_argument('--checker', required=True, type=Path)
    args = parser.parse_args()
    for prefix in ('', '/tools/cron'):
        accept_deployment(args.distribution, args.checker, prefix)
