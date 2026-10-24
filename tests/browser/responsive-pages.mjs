import assert from 'node:assert/strict';

async function fitsViewport(page, label) {
  await page.evaluate(() => document.fonts.ready);
  const dimensions = await page.evaluate(() => ({
    viewport:innerWidth, width:document.documentElement.scrollWidth,
    overflow:[...document.querySelectorAll('body *')].filter(element => {
      const rect = element.getBoundingClientRect();
      // A wide cell inside a scrollable table is intentional.
      return rect.width && (rect.left < -2 || rect.right > innerWidth + 2) &&
        !element.closest('table.data_table');
    }).slice(0, 8).map(element => element.id || element.tagName + '.' + element.className)
  }));
  assert.ok(dimensions.width <= dimensions.viewport + 2, label + ': page overflows: ' + JSON.stringify(dimensions));
}

export async function checkCompactLogin(page) {
  for (const width of [320, 375, 768]) {
    await page.setViewportSize({width, height:900});
    await page.locator('#fe_login_username').waitFor();
    await fitsViewport(page, 'Login at ' + width);
  }
  await page.setViewportSize({width:1440, height:1100});
}

export async function checkResponsivePages(page, base, jobID) {
  const pages = [
    ['Home', '#d_home_header_stats'], ['Schedule', '#fe_sch_keywords'],
    ['Schedule?sub=new_event', '#fe_ee_title'],
    ['Schedule?sub=edit_event&id=browser_smoke', '#fe_ee_title'],
    ['History', '#d_history_table'], ['JobDetails?id=' + jobID, '#i_arch_job_log'],
    ['MyAccount', '#fe_ma_fullname'], ['Admin?sub=activity', '.activity_desc'],
    ['Admin?sub=api_keys', '.subtitle'], ['Admin?sub=categories', '.subtitle'],
    ['Admin?sub=plugins', '.subtitle'], ['Admin?sub=servers', '.subtitle'],
    ['Admin?sub=users', '#fe_ul_search'], ['Admin?sub=new_api_key', '#fe_ak_title'],
    ['Admin?sub=new_category', '#fe_ec_title'], ['Admin?sub=new_plugin', '#fe_ep_command'],
    ['Admin?sub=new_user', '#fe_eu_username']
  ];
  for (const width of [320, 375, 768]) {
    await page.setViewportSize({width, height:900});
    for (const [route, marker] of pages) {
      await page.goto(base + '/?layout=' + width + '#'+ route);
      await page.locator(marker + ':visible').first().waitFor();
      await fitsViewport(page, route + ' at ' + width);
      // Navigation must remain present; clipping overflow is not an acceptable fix.
      assert.equal(await page.locator('#tab_Admin').isVisible(), true);
    }
    await page.goto(base + '/#Home');
    await page.locator('#d_home_header_stats').waitFor();
    await page.locator('#d_tab_master').click();
    await page.locator('#dialog_container').waitFor();
    await fitsViewport(page, 'Confirmation at ' + width);
    await page.locator('#dialog_container .button').filter({hasText:/^Cancel$/}).click();
    await page.locator('#dialog_container').waitFor({state:'hidden'});
  }
  await page.setViewportSize({width:1440, height:1100});
  console.log('Responsive acceptance passed: login, main/admin pages and confirmation at 320, 375 and 768 CSS pixels');
}
