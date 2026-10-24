import assert from 'node:assert/strict';
import {spawn, execFileSync} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {mkdtempSync, writeFileSync, rmSync, openSync, closeSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import net from 'node:net';
import {setTimeout as delay} from 'node:timers/promises';
import {chromium} from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const work = mkdtempSync(path.join(tmpdir(), 'owlcron-account-mail-'));
const messages = [];
const connections = new Set();
// SMTP stays entirely in this process. Messages and action URLs are never logged.
const smtp = net.createServer(socket => {
  connections.add(socket);
  socket.on('close', () => connections.delete(socket));
  socket.on('error', () => {});
  socket.write('220 localhost test SMTP\r\n');
  let buffer = '', data = null;
  socket.on('data', chunk => {
    buffer += chunk.toString();
    while (buffer.includes('\r\n')) {
      const index = buffer.indexOf('\r\n');
      const line = buffer.slice(0, index);
      buffer = buffer.slice(index + 2);
      if (data !== null) {
        if (line === '.') { messages.push(data.join('\n')); data = null; socket.write('250 accepted\r\n'); }
        else data.push(line.replace(/^\.\./, '.'));
      } else if (/^(EHLO|HELO) /i.test(line)) socket.write('250 localhost\r\n');
      else if (/^DATA$/i.test(line)) { data = []; socket.write('354 continue\r\n'); }
      else if (/^QUIT$/i.test(line)) socket.end('221 goodbye\r\n');
      else socket.write('250 OK\r\n');
    }
  });
});
let daemon, browser;
let stage = 'startup';
const log = openSync(path.join(work, 'daemon.log'), 'w');
const username = 'account-mail-admin';
const password = randomBytes(24).toString('hex');
const replacement = randomBytes(24).toString('hex');
const email = 'verified@example.test';
const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('OWLCRON_')));
async function waitFor(check, label) {
  for (let n = 0; n < 150; n++) {
    if (daemon && daemon.exitCode !== null) throw new Error('Daemon exited during ' + label);
    if (await check()) return;
    await delay(100);
  }
  throw new Error('Timed out waiting for ' + label);
}
try {
  await new Promise(resolve => smtp.listen(0, '127.0.0.1', resolve));
  const reserve = net.createServer();
  await new Promise(resolve => reserve.listen(0, '127.0.0.1', resolve));
  const port = reserve.address().port;
  await new Promise(resolve => reserve.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const configPath = path.join(work, 'config.json');
  writeFileSync(configPath, JSON.stringify({http_port:port, base_app_url:base, hostname:'account-mail-test',
    secret_key:randomBytes(32).toString('hex'), storage_engine:'Filesystem',
    storage_base_dir:path.join(work,'data'), log_dir:path.join(work,'logs'), queue_dir:path.join(work,'queue'),
    udp_broadcast_port:-1, scheduler_startup_grace:1, account_email_enabled:true,
    account_email_allow_insecure:true, account_email_cooldown_seconds:1,
    smtp_hostname:'127.0.0.1', smtp_port:smtp.address().port, smtp_tls_mode:'none',
    email_from:'OwlCron <noreply@example.test>'}), {mode:0o600});
  const command = path.join(root, 'owlcron-engine');
  execFileSync(command, ['-config',configPath,'setup'], {cwd:root,env,stdio:'pipe',timeout:15000});
  daemon = spawn(command, ['-config',configPath], {cwd:root,env:{...env,
    OWLCRON_ADMIN_USERNAME:username,OWLCRON_ADMIN_PASSWORD:password},stdio:['ignore',log,log]});
  await waitFor(async () => { try { return (await fetch(base+'/api/app/ping',{signal:AbortSignal.timeout(2000)})).ok; } catch { return false; } }, 'startup');
  await waitFor(async () => (await (await fetch(base+'/api/app/status',{signal:AbortSignal.timeout(2000)})).json()).master, 'leadership');
  async function api(endpoint, body, session) {
    return (await fetch(base+'/api/user/'+endpoint, {method:'POST',signal:AbortSignal.timeout(5000),headers:{'Content-Type':'application/json',
      ...(session ? {'X-Session-ID':session} : {})},body:JSON.stringify(body)})).json();
  }
  browser = await chromium.launch({headless:true,...(process.env.OWLCRON_BROWSER_EXECUTABLE ?
    {executablePath:process.env.OWLCRON_BROWSER_EXECUTABLE} : {})});
  const page = await browser.newPage();
  page.setDefaultTimeout(15000);
  const errors = [];
  const consoleMessages = [];
  page.on('console', message => consoleMessages.push(message.text()));
  page.on('pageerror', () => errors.push('Browser JavaScript exception'));
  async function responseFor(endpoint, action) {
    const [response] = await Promise.all([page.waitForResponse(response =>
      new URL(response.url()).pathname === '/api/user/'+endpoint && response.request().method()==='POST'), action()]);
    const result = await response.json();
    assert.equal(result.code,0,endpoint+' failed: '+result.description);
    return result;
  }
  async function mailAction(purpose) {
    let link;
    await waitFor(() => {
      for (const message of messages) {
        // Support both plain text and quoted-printable SMTP bodies.
        const decoded = /Content-Transfer-Encoding: quoted-printable/i.test(message) ? message.replace(/=\n/g,'').replace(/=([0-9A-F]{2})/gi, (_,hex) => String.fromCharCode(parseInt(hex,16))) : message;
        const urls = decoded.match(/https?:\/\/[^\s<>]+/g) || [];
        link = urls.find(value => { try { return new URLSearchParams(new URL(value).hash.split('?')[1]).get(purpose)==='1'; } catch { return false; } });
        if (link) return true;
      }
      return false;
    }, purpose+' SMTP delivery');
    return link;
  }
  stage = 'initial login';
  await page.goto(base);
  assert.equal(await page.locator('#fe_ca_username').count(),0,'Registration remains closed');
  await page.locator('#fe_login_username').fill(username);
  await page.locator('#fe_login_password').fill(password);
  const login = await responseFor('login', () => page.locator('.button:visible').filter({hasText:'Login'}).click());
  stage = 'email verification request';
  await page.goto(base+'/#MyAccount');
  await page.locator('#fe_ma_email').fill(email);
  await page.locator('#fe_ma_old_password').fill(password);
  await responseFor('request_email_verification', () => page.getByRole('button',{name:'Send verification email'}).click());
  stage = 'verification SMTP delivery';
  const verificationLink = await mailAction('verify');
  const verificationURL = new URL(verificationLink);
  const verificationParams = new URLSearchParams(verificationURL.hash.split('?')[1]);
  stage = 'verification confirmation';
  await page.goto('about:blank');
  await page.goto(verificationLink);
  await page.locator('#confirm_email_verification').waitFor();
  assert.ok(!page.url().includes('token='),'Verification token removed from URL before rendering');
  await responseFor('verify_email', () => page.locator('#confirm_email_verification').click());
  const replayVerification = await api('verify_email',{user_id:verificationParams.get('u'),token:verificationParams.get('token')});
  assert.notEqual(replayVerification.code,0,'Verification token must be single-use');
  await page.goto(base+'/#MyAccount');
  await page.getByText('Verified',{exact:true}).waitFor();
  await delay(1100); // Per-account cooldown is intentionally one second in this fixture.
  stage = 'password recovery request';
  const absent = await api('forgot_password',{username:'missing-account',email});
  await page.evaluate(() => app.doUserLogout(true));
  await page.goto(base+'/#Login?recover=1');
  await page.locator('#fe_recovery_username').fill(username);
  await page.locator('#fe_recovery_email').fill(email);
  const known = await responseFor('forgot_password', () => page.locator('#request_password_recovery').click());
  assert.deepEqual(known,absent,'Recovery response must not disclose account existence');
  stage = 'recovery SMTP delivery';
  const recoveryLink = await mailAction('reset');
  const recoveryParams = new URLSearchParams(new URL(recoveryLink).hash.split('?')[1]);
  stage = 'password reset form';
  // Reusing the open tab must scrub the token before the application router.
  await page.goto(recoveryLink);
  await page.locator('#fe_reset_password').waitFor();
  assert.ok(!page.url().includes('token='),'Recovery token removed from URL before rendering');
  // Opening an email link must not consume it or change the current password.
  const beforeReset = await api('login',{username,password});
  assert.equal(beforeReset.code,0,'Password remains valid until explicit confirmation');
  await page.locator('#fe_reset_password').fill(replacement);
  await page.locator('#fe_reset_confirm').fill(replacement);
  await responseFor('reset_password', () => page.locator('#confirm_password_reset').click());
  await page.locator('#fe_login_username').waitFor();
  await waitFor(() => messages.some(message => message.includes('OwlCron password changed')), 'password change notification');
  stage = 'revocation and replay checks';
  const replay = await api('reset_password',{user_id:recoveryParams.get('u'),token:recoveryParams.get('token'),new_password:replacement,confirm_password:replacement});
  assert.notEqual(replay.code,0,'Recovery token must be single-use');
  assert.notEqual((await api('login',{username,password})).code,0,'Old password is revoked');
  assert.notEqual((await api('resume_session',{session_id:login.session_id})).code,0,'Existing sessions are revoked');
  await page.locator('#fe_login_username').fill(username);
  await page.locator('#fe_login_password').fill(replacement);
  await responseFor('login', () => page.locator('.button:visible').filter({hasText:'Login'}).click());
  await page.locator('#fe_login_username').waitFor({state:'hidden'});
  assert.deepEqual(errors,[]);
  for (const token of [verificationParams.get('token'),recoveryParams.get('token')]) {
    assert.ok(!consoleMessages.some(message => message.includes(token)), 'Action tokens must not appear in browser console');
  }
  console.log('Account mail passed: local SMTP, verification, private recovery response, explicit reset, replay protection, session revocation, new login and closed registration');
} catch (error) {
  console.error(String(error.message).replace(/https?:\/\/[^\s]+/g, '[redacted URL]').replace(/[a-f0-9]{48,}/gi, '[redacted secret]'));
  // Errors, screenshots and daemon logs can contain token-bearing URLs. Keep this test output secret-free.
  throw new Error('Account mail check failed during ' + stage + '; reproduce locally to inspect its isolated fixture');
} finally {
  if (browser) await browser.close();
  if (daemon && daemon.exitCode===null) {
    daemon.kill('SIGTERM');
    for (let n=0;n<100 && daemon.exitCode===null;n++) await delay(100);
    if (daemon.exitCode===null) { daemon.kill('SIGKILL'); await new Promise(resolve => daemon.once('exit',resolve)); }
  }
  for (const socket of connections) socket.destroy();
  await new Promise(resolve => smtp.close(resolve));
  closeSync(log);
  rmSync(work,{recursive:true,force:true});
}
