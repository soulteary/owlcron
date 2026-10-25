import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {writeFileSync} from 'node:fs';
import path from 'node:path';

// Reuse the smoke runner's isolated daemon, never an operator's deployment.
export async function checkSingleNodePages(page, responseFor, {base, session, poll, completedID, work}) {
  const api = async (endpoint, data, sid = session) => {
    const response = await fetch(base + '/api/' + endpoint, {method:'POST',signal:AbortSignal.timeout(5000),
      headers:{'Content-Type':'application/json','X-Session-ID':sid}, body:JSON.stringify(data)});
    return response.json();
  };
  const okay = async (endpoint, data) => {
    const result = await api(endpoint, data);
    assert.equal(result.code, 0, endpoint + ': ' + result.description);
    return result;
  };
  const button = text => page.locator('.button:visible').filter({hasText:typeof text === 'string' ? text : text.source.replace(/^\^|\$$/g, '')});
  const complete = async id => {
    let job;
    await poll(async () => {
      const result = await okay('app/get_job_status', {id});
      if (!result.complete) return false;
      job = result.job;
      return true;
    }, 'single-node job completion');
    return job;
  };
  const checkChain = async id => {
    let rows;
    await poll(async () => {
      rows = (await okay('app/get_event_history', {id})).rows;
      return rows.length > 0;
    }, 'chain completion');
    assert.equal(rows.length, 1, 'Each final completion should trigger one chain job');
    assert.equal(rows[0].code, 0, 'Chain target must complete successfully');
  };
  for (const id of ['browser_chain_success', 'browser_chain_error']) {
    await okay('app/create_event', {id,title:id,enabled:true,timing:null,category:'general',target:'all',
      plugin:'echoplug',params:{message:id,repeat:1,delay_ms:1},timeout:10,max_children:1});
  }
  await page.waitForFunction(() => ['browser_chain_success', 'browser_chain_error'].every(id =>
    app.schedule.some(event => event.id === id)));
  await okay('app/update_event', {id:'browser_smoke',chain:'',chain_error:''});
  // The API response precedes the schedule's WebSocket update. Editing the old
  // array fixture renders API-managed chains without the scalar select controls.
  await page.waitForFunction(() => {
    const event = app.schedule.find(event => event.id === 'browser_smoke');
    return event && event.chain === '' && event.chain_error === '';
  });
  await page.goto(base + '/#Schedule?sub=edit_event&id=browser_smoke');
  await page.locator('#fe_ee_timezone').selectOption('America/New_York');
  await page.locator('#fe_ee_timing').selectOption('daily');
  await page.locator('#fe_ee_timeout_units').selectOption('60');
  await page.locator('#fe_ee_timeout').fill('2');
  await page.locator('#fe_ee_retries').selectOption('1');
  await page.locator('#fe_ee_retry_delay_units').selectOption('1');
  await page.locator('#fe_ee_retry_delay').fill('1');
  await page.locator('#fe_ee_queue').check();
  await page.locator('#fe_ee_queue_max').fill('3');
  await page.locator('.link:visible').filter({hasText:'Chain Options'}).click();
  await page.locator('#fe_ee_chain').selectOption('browser_chain_success');
  await page.locator('#fe_ee_chain_error').selectOption('browser_chain_error');
  await responseFor('app/update_event', () => button(/^Save Changes$/).click());
  await page.reload();
  assert.equal(await page.locator('#fe_ee_timezone').inputValue(), 'America/New_York');
  assert.equal(await page.locator('#fe_ee_timing').inputValue(), 'daily');
  assert.equal(await page.locator('#fe_ee_retries').inputValue(), '1');
  assert.equal(await page.locator('#fe_ee_queue').isChecked(), true);
  const saved = (await okay('app/get_event', {id:'browser_smoke'})).event;
  assert.equal(saved.timeout, 120);
  assert.equal(saved.retry_delay, 1);
  assert.equal(saved.queue_max, 3);
  assert.equal(saved.chain, 'browser_chain_success');
  assert.equal(saved.chain_error, 'browser_chain_error');
  assert.ok(saved.timing.hours.length && saved.timing.minutes.length);
  assert.equal(saved.enabled, false, 'Scheduled fixture must remain disabled');

  await okay('app/update_event', {id:'browser_smoke',chain:[],chain_error:[]});

  // Archived job reruns and live aborts use the actual visible controls.
  await page.goto(base + '/#JobDetails?id=' + completedID);
  const rerun = await responseFor('app/run_event', () => page.locator('.link:visible').filter({hasText:'Run Again'}).click());
  const rerunID = rerun.ids[0];
  await page.locator('.link:visible').filter({hasText:'Abort Job'}).waitFor();
  await page.locator('.link:visible').filter({hasText:'Abort Job'}).click();
  await responseFor('app/abort_job', () => button(/^Abort$/).click());
  const aborted = await complete(rerunID);
  assert.notEqual(aborted.code, 0);
  await page.goto(base + '/#JobDetails?id=' + rerunID);
  await page.locator('#i_arch_job_log').waitFor();

  // Script fixture fails fast, exercising a real failed job without minute-long waits.
  const failureScript = path.join(work, 'failure.sh');
  writeFileSync(failureScript, '#!/bin/sh\nread input\necho OWLCRON_EXPECTED_FAILURE\nexit 7\n', {mode:0o700});
  await okay('app/create_plugin', {id:'browser_failure_plugin',title:'Failure fixture',enabled:true,command:'/bin/sh ' + failureScript,params:[]});
  await okay('app/create_event', {id:'browser_failure',title:'Browser failure',enabled:0,
    category:'general',target:'all',plugin:'browser_failure_plugin',params:{},
    timeout:10,retries:1,retry_delay:1,max_children:1,chain_error:'browser_chain_error'});
  const failedID = (await okay('app/run_event', {id:'browser_failure'})).ids[0];
  const failed = await complete(failedID);
  assert.equal(failed.code, 7);
  assert.equal(failed.retries_used, 1);
  await checkChain('browser_chain_error');
  // Run a separate fast success fixture to verify the other chain path.
  await okay('app/create_event', {id:'browser_chain_source',title:'Success chain source',enabled:false,
    category:'general',target:'all',plugin:'echoplug',params:{repeat:1,delay_ms:1},
    timeout:10,max_children:1,chain:'browser_chain_success'});
  const successID = (await okay('app/run_event', {id:'browser_chain_source'})).ids[0];
  assert.equal((await complete(successID)).code, 0);
  await checkChain('browser_chain_success');
  await page.goto(base + '/#JobDetails?id=' + failedID);
  await page.locator('#i_arch_job_log').filter({hasText:'OWLCRON_EXPECTED_FAILURE'}).waitFor();

  // Enforce a real timeout, then exercise queue admission and its visible flush action.
  await okay('app/create_event', {id:'browser_timeout',title:'Browser timeout',enabled:false,
    category:'general',target:'all',plugin:'echoplug',params:{repeat:30,delay_ms:1000},
    timeout:1,retries:0,max_children:1});
  const timeoutID = (await okay('app/run_event', {id:'browser_timeout'})).ids[0];
  const timedOut = await complete(timeoutID);
  assert.notEqual(timedOut.code, 0);
  assert.match(timedOut.abort_reason, /Exceeded maximum run time/);
  await okay('app/update_event', {id:'browser_smoke',params:{message:'QUEUE_FIXTURE',repeat:30,delay_ms:1000}});
  const queueLiveID = (await okay('app/run_event', {id:'browser_smoke'})).ids[0];
  try {
    assert.deepEqual((await okay('app/run_event', {id:'browser_smoke'})).ids, [], 'Second singleton execution must queue');
    await page.goto(base + '/#Home');
    await page.locator('#d_home_upcoming_events:not(.loading)').waitFor();
    const flush = page.locator('#d_home_queued_jobs .link:visible').filter({hasText:'Flush Queue'});
    await flush.waitFor();
    await flush.click();
    const flushed = await responseFor('app/flush_event_queue', () => button(/^Flush$/).click());
    assert.equal(flushed.flushed, 1);
    await page.locator('#d_home_queue_container').waitFor({state:'hidden'});
  } finally { await okay('app/abort_job', {id:queueLiveID}); await complete(queueLiveID); }

  // Limited account: a separate browser context avoids sharing admin cookies/storage.
  const password = randomBytes(24).toString('hex');
  await okay('user/admin_create', {username:'browser-reader',password,enabled:true,privileges:{}});
  const context = await page.context().browser().newContext();
  const reader = await context.newPage();
  reader.setDefaultTimeout(15000);
  const readerErrors = [];
  reader.on('pageerror', error => readerErrors.push(error.message));
  try {
    await reader.goto(base);
    await reader.locator('#fe_login_username').fill('browser-reader');
    await reader.locator('#fe_login_password').fill(password);
    const [response] = await Promise.all([
      reader.waitForResponse(r => new URL(r.url()).pathname === '/api/user/login'),
      reader.locator('.button:visible').filter({hasText:'Login'}).click()
    ]);
    const loggedIn = await response.json();
    assert.equal(loggedIn.code, 0);
    await reader.locator('#d_home_header_stats').waitFor();
    await reader.locator('#d_home_upcoming_events:not(.loading)').waitFor();
    assert.equal(await reader.locator('a:visible').filter({hasText:/^Admin$/}).count(), 0);
    await reader.goto(base + '/#Admin?sub=users');
    await reader.waitForURL(/#Home$/);
    assert.equal(await reader.locator('#fe_eu_username:visible').count(), 0);
    await reader.goto(base + '/#Schedule');
    await reader.locator('#fe_sch_keywords').waitFor();
    assert.equal(await reader.locator('.button:visible,.link:visible').filter({hasText:/Add New Event|Add Event|New Event|Run/}).count(), 0);
    assert.equal(await reader.locator('#page_Schedule input[type=checkbox]:visible').count(), 0);
    for (const sub of ['new_event', 'edit_event&id=browser_smoke']) {
      await reader.goto(base + '/#Schedule?sub=' + sub);
      await reader.waitForURL(/#Schedule$/);
      await reader.locator('#page_Schedule:not(.loading) #fe_sch_keywords').waitFor();
      assert.equal(await reader.locator('#fe_ee_title:visible').count(), 0);
    }
    await reader.goto(base + '/#JobDetails?id=' + completedID);
    await reader.locator('#i_arch_job_log').waitFor();
    assert.equal(await reader.locator('.link:visible').filter({hasText:/Run Again|Delete Job/}).count(), 0);
    await okay('app/update_event', {id:'browser_smoke', params:{message:'READONLY_LIVE_FIXTURE',repeat:30,delay_ms:1000}});
    const liveID = (await okay('app/run_event', {id:'browser_smoke'})).ids[0];
    try {
      await reader.goto(base + '/#Home');
      await reader.locator('#d_home_upcoming_events:not(.loading)').waitFor();
      await reader.locator('#d_home_active_jobs').getByText('Browser smoke edited', {exact:false}).first().waitFor();
      assert.equal(await reader.locator('.link:visible').filter({hasText:/Abort Job|Flush Queue/}).count(), 0);
      await reader.goto(base + '/#JobDetails?id=' + liveID);
      await reader.locator('#c_live_progress').waitFor();
      assert.equal(await reader.locator('.link:visible').filter({hasText:/Abort Job|Watch Job/}).count(), 0);
      assert.notEqual((await api('app/abort_job',{id:liveID},loggedIn.session_id)).code, 0);
    } finally { await okay('app/abort_job', {id:liveID}); await complete(liveID); }
    for (const [endpoint, data] of [['user/admin_get_users',{}],['app/run_event',{id:'browser_smoke'}],['app/delete_event',{id:'browser_smoke'}],['app/update_event',{id:'browser_smoke',enabled:true}]]) {
      assert.notEqual((await api(endpoint,data,loggedIn.session_id)).code, 0, endpoint + ' must reject a reader');
    }
    assert.deepEqual(readerErrors, [], 'Read-only browser JavaScript errors');
  } finally { await context.close(); }
}
