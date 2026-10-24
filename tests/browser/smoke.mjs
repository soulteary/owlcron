import assert from 'node:assert/strict';
import {spawn, execFileSync} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {mkdtempSync, writeFileSync, rmSync, openSync, closeSync, readFileSync, mkdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import net from 'node:net';
import {setTimeout as delay} from 'node:timers/promises';
import {chromium} from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const work = mkdtempSync(path.join(tmpdir(), 'owlcron-browser-'));
const socket = net.createServer();
await new Promise(resolve => socket.listen(0, '127.0.0.1', resolve));
const port = socket.address().port;
await new Promise(resolve => socket.close(resolve));
const base = `http://127.0.0.1:${port}`;
const password = randomBytes(24).toString('hex');
const secret = randomBytes(32).toString('hex');
const configPath = path.join(work, 'config.json');
writeFileSync(configPath, JSON.stringify({http_port:port, base_app_url:base, hostname:'browser-smoke',
  secret_key:secret, storage_engine:'Filesystem',
  storage_base_dir:path.join(work,'data'), log_dir:path.join(work,'logs'), queue_dir:path.join(work,'queue'),
  udp_broadcast_port:-1, scheduler_startup_grace:1, prompt_before_run:false}), {mode:0o600});
const command = path.join(root, 'owlcron-engine');
let daemon, browser, page;
const log = openSync(path.join(work, 'daemon.log'), 'w');
const errors = [];
async function poll(check, description) {
  for (let count=0; count<150; count++) {
    if (daemon.exitCode !== null) throw new Error('Daemon exited during ' + description);
    try { if (await check()) return; } catch (error) { if (count === 149) throw error; }
    await delay(100);
  }
  throw new Error('Timed out waiting for ' + description);
}
async function responseFor(endpoint, action) {
  const [response] = await Promise.all([
    page.waitForResponse(response => new URL(response.url()).pathname.replace(/\/$/, '') === '/api/' + endpoint && response.request().method() === 'POST'),
    action()
  ]);
  const result = await response.json();
  assert.equal(result.code, 0, endpoint + ': ' + result.description);
  return result;
}
const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('OWLCRON_')));
try {
  execFileSync(command, ['-config',configPath,'setup'], {cwd:root, env, stdio:'pipe', timeout:15000});
  daemon = spawn(command, ['-config',configPath], {cwd:root, env:{...env,
    OWLCRON_ADMIN_USERNAME:'browser-admin', OWLCRON_ADMIN_PASSWORD:password}, stdio:['ignore',log,log]});
  await poll(async () => (await fetch(base + '/api/app/ping', {signal:AbortSignal.timeout(2000)})).ok, 'startup');
  await poll(async () => (await (await fetch(base + '/api/app/status', {signal:AbortSignal.timeout(2000)})).json()).master, 'leadership');
  browser = await chromium.launch({headless:true, ...(process.env.OWLCRON_BROWSER_EXECUTABLE ? {executablePath:process.env.OWLCRON_BROWSER_EXECUTABLE} : {})});
  page = await browser.newPage({viewport:{width:1440,height:1100}});
  page.setDefaultTimeout(15000);
  page.on('pageerror', error => errors.push(error.stack || error.message));
  await page.goto(base);
  await page.locator('#fe_login_username').fill('browser-admin');
  await page.locator('#fe_login_password').fill(password);
  const login = await responseFor('user/login', () => page.locator('.button').filter({hasText:'Login'}).click());
  await page.locator('#fe_login_username').waitFor({state:'hidden'});
  await page.locator('#theme-preference').selectOption('dark');
  assert.match(await page.locator('body').getAttribute('class'), /dark-theme/);
  await page.reload();
  assert.match(await page.locator('body').getAttribute('class'), /dark-theme/);
  await page.locator('#theme-preference').selectOption('light');
  assert.doesNotMatch(await page.locator('body').getAttribute('class') || '', /dark-theme/);
  await page.goto(base + '/#Schedule?sub=new_event');
  await page.locator('#fe_ee_title').fill('[browser_smoke] Browser smoke');
  await page.locator('#fe_ee_enabled').uncheck();
  await page.locator('#fe_ee_cat').selectOption('general');
  await page.locator('#fe_ee_target').selectOption('all');
  await page.locator('#fe_ee_plugin').selectOption('echoplug');
  await page.locator('#fe_ee_pp_message').fill('OWLCRON_BROWSER_SMOKE_OK');
  await page.locator('#fe_ee_pp_repeat').fill('6');
  await page.locator('#fe_ee_pp_delay_ms').fill('1000');
  await responseFor('app/create_event', () => page.locator('.button').filter({hasText:'Create Event'}).click());
  await page.waitForURL(/#Schedule$/);
  await page.goto(base + '/#Schedule?sub=edit_event&id=browser_smoke');
  await page.locator('#fe_ee_title').fill('Browser smoke edited');
  await page.locator('#fe_ee_log_expire_days').fill('7');
  await responseFor('app/update_event', () => page.locator('.button').filter({hasText:/Save Changes/}).click());
  await page.reload();
  assert.equal(await page.locator('#fe_ee_title').inputValue(), 'Browser smoke edited');
  assert.equal(await page.locator('#fe_ee_log_expire_days').inputValue(), '7');
  const result = await responseFor('app/run_event', () => page.locator('.button').filter({hasText:/^Run Now$/}).click());
  const id = result.ids[0];
  assert.ok(id, 'Manual run did not create a job');
  await page.waitForFunction(id => !!app.activeJobs[id], id);
  await page.evaluate(id => Nav.go('#JobDetails?id=' + id), id);
  await page.locator('#c_live_progress').waitFor();
  assert.deepEqual(await page.evaluate(() => ['c_live_progress', 'c_live_cpu', 'c_live_mem'].map(id => {
    const chart = Chart.getChart(id);
    return {type:chart.config.type, legend:chart.options.plugins.legend.display, points:chart.data.datasets[0].data.length};
  })), Array.from({length:3}, () => ({type:'doughnut',legend:false,points:2})));
  await poll(async () => {
    const status = await (await fetch(base + '/api/app/get_job_status', {method:'POST', signal:AbortSignal.timeout(2000),
      headers:{'Content-Type':'application/json','X-Session-ID':login.session_id}, body:JSON.stringify({id})})).json();
    if (!status.complete) return false;
    assert.equal(status.job.code,0);
    return true;
  }, 'job completion');
  await page.goto(base + '/#History');
  const row = page.locator('#d_history_table tr').filter({hasText:'Browser smoke edited'});
  await row.getByRole('link', {name:new RegExp(id)}).click();
  await page.locator('#i_arch_job_log').filter({hasText:'OWLCRON_BROWSER_SMOKE_OK'}).waitFor();
  assert.deepEqual(await page.evaluate(() => ['c_arch_perf', 'c_arch_cpu', 'c_arch_mem'].map(id => {
    const chart = Chart.getChart(id);
    return {type:chart.config.type, legend:chart.options.plugins.legend.display};
  })), [{type:'pie',legend:false},{type:'doughnut',legend:false},{type:'doughnut',legend:false}]);
  await page.goto(base + '/#History?sub=event_stats&id=browser_smoke');
  await page.locator('#c_graph_hist_perf').waitFor();
  const chartState = await page.evaluate(() => ['c_graph_hist_perf', 'c_graph_hist_cpu', 'c_graph_hist_mem'].map(id => {
    const chart = Chart.getChart(id);
    const value = chart.data.datasets[0].data[0];
    chart.tooltip.setActiveElements([{datasetIndex:0,index:0}], {x:100,y:100});
    chart.update('none');
    return {type:chart.config.type,x:chart.scales.x.type,zero:chart.options.scales.y.beginAtZero,
      finite:Number.isFinite(value.x) && Number.isFinite(value.y),
      rendered:chart.width > 0 && chart.height > 0 && chart.getDatasetMeta(0).data.length > 0,
      tooltip:chart.tooltip.body.flatMap(item => item.lines).join(' '),
      title:chart.tooltip.title.join(' ')};
  }));
  for (const state of chartState) {
    assert.equal(state.type, 'line');
    assert.equal(state.x, 'time');
    assert.equal(state.zero, true);
    assert.equal(state.finite, true);
    assert.equal(state.rendered, true);
    assert.ok(state.title && !state.title.includes('Invalid'), 'Date adapter must format the tooltip title');
    assert.ok(state.tooltip && !/undefined|NaN/.test(state.tooltip), 'Tooltip must use parsed Chart.js values');
  }
  assert.match(chartState[0].tooltip, /sec/);
  assert.match(chartState[1].tooltip, /%/);
  await page.locator('#theme-preference').selectOption('dark');
  assert.deepEqual(await page.evaluate(() => {
    const chart = Chart.getChart('c_graph_hist_cpu');
    return [chart.options.scales.x.ticks.color, chart.options.scales.y.grid.color, chart.options.plugins.legend.labels.color];
  }), ['#d4dae3','#38404d','#d4dae3']);
  await page.locator('#theme-preference').selectOption('light');
  assert.deepEqual(await page.evaluate(() => {
    const chart = Chart.getChart('c_graph_hist_cpu');
    return [chart.options.scales.x.ticks.color, chart.options.scales.y.grid.color, chart.options.plugins.legend.labels.color];
  }), ['#555555','rgba(0,0,0,0.1)','#555555']);
  assert.deepEqual(errors, [], 'Browser JavaScript errors');
  console.log('Browser smoke passed: bootstrap login, persistent theme, create/edit/run event, history, job log, live/archive charts, time axes, tooltips and chart themes');
} catch (error) {
  if (page) {
    const artifacts = path.join(root, 'tests/browser/test-results');
    mkdirSync(artifacts, {recursive:true});
    await page.screenshot({path:path.join(artifacts,'failure.png'),fullPage:true}).catch(() => {});
  }
  // Redact bootstrap credentials before exposing daemon diagnostics.
  const diagnostics = readFileSync(path.join(work, 'daemon.log'),'utf8').replaceAll(password,'[redacted]').replaceAll(secret,'[redacted]');
  console.error(diagnostics.slice(-5000));
  throw error;
} finally {
  if (browser) await browser.close();
  if (daemon && daemon.exitCode === null) {
    daemon.kill('SIGTERM');
    for (let count=0; count<100 && daemon.exitCode === null; count++) await delay(100);
    if (daemon.exitCode === null) { daemon.kill('SIGKILL'); await new Promise(resolve => daemon.once('exit',resolve)); }
  }
  closeSync(log);
  rmSync(work,{recursive:true,force:true});
}
