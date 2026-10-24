import assert from 'node:assert/strict';

// Use physical key events, never invoke handlers or focus the destination control.
async function tabTo(page, target) {
  await target.waitFor();
  for (let n = 0; n < 100; n++) {
    if (await target.evaluate(element => element === document.activeElement)) return;
    await page.keyboard.press('Tab');
  }
  throw new Error('Control is not reachable with Tab: ' + await target.getAttribute('id'));
}

export async function checkKeyboardPages(page, base, session) {
  await page.goto(base + '/#Home');
  await page.locator('#d_home_header_stats').waitFor();
  const schedule = page.locator('#tab_Schedule');
  await tabTo(page, schedule);
  await page.keyboard.press('Enter');
  await page.waitForURL(/#Schedule$/);
  await page.locator('#fe_sch_keywords').waitFor();
  const add = page.locator('.button:visible').filter({hasText:/Add Event|Add New Event/});
  await tabTo(page, add);
  await page.keyboard.press('Space');
  await page.locator('#fe_ee_title').waitFor();

  const plugin = await fetch(base + '/api/app/create_plugin', {method:'POST', signal:AbortSignal.timeout(5000),
    headers:{'Content-Type':'application/json','X-Session-ID':session},
    body:JSON.stringify({id:'keyboard_textarea',title:'Keyboard textarea',enabled:true,command:'/bin/true',
      params:[{id:'script',title:'Script',type:'textarea',rows:3,value:''}]})});
  assert.equal((await plugin.json()).code, 0);
  await page.reload();
  await page.locator('#fe_ee_plugin').selectOption('keyboard_textarea');
  const script = page.locator('#fe_ee_pp_script');
  await script.fill('printf hello');
  await page.keyboard.press('Tab');
  assert.equal(await script.evaluate(element => element === document.activeElement), false, 'Tab must leave a script editor');
  assert.equal(await script.inputValue(), 'printf hello');

  await page.goto(base + '/#Schedule');
  await page.locator('#fe_sch_keywords').waitFor();
  const run = page.locator('#page_Schedule .link:visible').filter({hasText:/^Run$/}).first();
  await tabTo(page, run);
  await page.keyboard.press('Shift+Enter');
  await page.locator('#dialog_container .dt_fs').first().waitFor();
  assert.equal(await page.locator('#dialog_container .dt_fs').count(), 5);
  await page.keyboard.press('Escape');
  await page.locator('#dialog_container').waitFor({state:'hidden'});

  await page.goto(base + '/#Schedule?sub=edit_event&id=browser_smoke');
  await page.locator('#fe_ee_catch_up').check();
  const eventClock = page.locator('#fe_ee_rc_time');
  const resetClock = page.locator('#s_ee_rc_reset');
  assert.equal(await eventClock.isDisabled(), true);
  assert.equal(await resetClock.isVisible(), false);
  await page.locator('#fe_ee_rc_enabled').check();
  await tabTo(page, eventClock);
  await page.keyboard.press('Enter');
  await page.locator('#dialog_container #fe_dt_year').waitFor();
  await page.keyboard.press('Escape');
  await page.locator('#dialog_container').waitFor({state:'hidden'});
  assert.equal(await eventClock.evaluate(element => element === document.activeElement), true);
  await page.locator('#fe_ee_rc_enabled').uncheck();
  assert.equal(await resetClock.isVisible(), false);

  // Enter must activate the focused Cancel, not the framework's default confirm.
  await page.goto(base + '/#Home');
  await page.locator('#d_home_header_stats').waitFor();
  const toggle = page.locator('#d_tab_master');
  await tabTo(page, toggle);
  const previous = await toggle.textContent();
  await page.keyboard.press('Space');
  const dialog = page.locator('#dialog_container');
  await dialog.waitFor();
  assert.equal(await dialog.getAttribute('role'), 'dialog');
  assert.equal(await dialog.getAttribute('aria-modal'), 'true');
  const cancel = dialog.locator('.button').filter({hasText:/^Cancel$/});
  await tabTo(page, cancel);
  await page.keyboard.press('Enter');
  await dialog.waitFor({state:'hidden'});
  assert.equal(await toggle.textContent(), previous);
  assert.equal(await toggle.evaluate(element => element === document.activeElement), true, 'Dialog restores its opener');

  await page.keyboard.press('Enter');
  await dialog.waitFor();
  // A complete cycle, including backwards navigation, cannot escape the modal.
  for (let n = 0; n < 6; n++) {
    await page.keyboard.press(n % 2 ? 'Shift+Tab' : 'Tab');
    assert.equal(await dialog.evaluate(element => element.contains(document.activeElement)), true);
  }
  await page.keyboard.press('Escape');
  await dialog.waitFor({state:'hidden'});
  assert.equal(await toggle.textContent(), previous);
  assert.equal(await toggle.evaluate(element => element === document.activeElement), true);
  console.log('Keyboard acceptance passed: Tab, Shift+Tab, Enter, Space, modal cancellation and focus restoration');
}
