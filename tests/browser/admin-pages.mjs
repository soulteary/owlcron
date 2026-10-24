import assert from 'node:assert/strict';

// Every record belongs to the smoke runner's temporary, isolated daemon.
export async function checkAdminPages(page, responseFor) {
  const base = new URL(page.url()).origin;
  const open = async (sub, selector) => {
    await page.goto(base + '/?admin_smoke=' + encodeURIComponent(sub) + '#Admin?sub=' + sub);
    await page.locator(selector).first().waitFor();
  };
  const button = text => page.locator('.button:visible').filter({hasText:typeof text === 'string' ? text : text.source.replace(/^\^|\$$/g, '')});
  const confirmDelete = async endpoint => {
    await responseFor(endpoint, () => page.locator('.button:visible').filter({hasText:/^Delete$/}).click());
  };
  for (const [sub, text] of [['activity','Activity Log'],['api_keys','API Keys'],['categories','Categories'],['plugins','Plugins'],['servers','Server Cluster'],['users','User Accounts']]) {
    await page.goto(base + '/?admin_smoke=' + encodeURIComponent(sub) + '#Admin?sub=' + sub);
    await page.locator('.subtitle').filter({hasText:text}).first().waitFor();
  }

  await open('new_api_key', '#fe_ak_title');
  assert.equal(await page.locator('#fe_ak_desc').count(), 0);
  await page.locator('#fe_ak_title').fill('Admin smoke key');
  const createdKey = await responseFor('app/create_api_key', () => button('Create Key').click());
  await page.locator('#fe_ak_status').waitFor();
  assert.equal(await page.locator('#fe_ak_status').inputValue(), '1');
  await page.locator('#fe_ak_status').selectOption('0');
  const savedKey = await responseFor('app/update_api_key', () => button(/^Save Changes$/).click());
  assert.equal(savedKey.api_key.enabled, false);
  await open('edit_api_key&id=' + createdKey.id, '#fe_ak_status');
  assert.equal(await page.locator('#fe_ak_status').inputValue(), '0');
  await button('Delete Key...').click();
  await confirmDelete('app/delete_api_key');

  await open('new_plugin', '#fe_ep_title');
  await page.locator('#fe_ep_title').fill('[admin_smoke_plugin] Admin smoke plugin');
  await page.locator('#fe_ep_enabled').uncheck();
  await page.locator('#fe_ep_command').fill('/bin/true');
  await page.locator('.link:visible').filter({hasText:'Advanced Options'}).click();
  await page.locator('#fe_ep_uid').fill('10001');
  await page.locator('#fe_ep_gid').fill('10002');
  await responseFor('app/create_plugin', () => button(/^Create Plugin$/).click());
  await open('edit_plugin&id=admin_smoke_plugin', '#fe_ep_uid');
  assert.equal(await page.locator('#fe_ep_uid').inputValue(), '10001');
  assert.equal(await page.locator('#fe_ep_gid').inputValue(), '10002');
  await button('Delete Plugin...').click();
  await confirmDelete('app/delete_plugin');

  await open('new_category', '#fe_ec_title');
  await page.locator('#fe_ec_title').fill('[admin_smoke_category] Admin smoke category');
  await responseFor('app/create_category', () => button(/^Add Category$/).click());
  await open('edit_category&id=admin_smoke_category', '#fe_ec_title');
  await page.locator('#fe_ec_title').fill('Admin smoke category edited');
  await responseFor('app/update_category', () => button(/^Save Changes$/).click());
  await button('Delete Category...').click();
  await confirmDelete('app/delete_category');

  await open('new_user', '#fe_eu_username');
  assert.equal(await page.locator('#fe_eu_send_email').count(), 0);
  await page.locator('#fe_eu_username').fill('AdminSmoke.Mixed');
  await page.locator('#fe_eu_password').fill('BrowserSmoke.Password!123');
  const createdUser = await responseFor('user/admin_create', () => button(/^Create User$/).click());
  assert.equal(createdUser.username, 'AdminSmoke.Mixed');
  await page.locator('#fe_eu_username:disabled').waitFor();
  assert.equal(await page.locator('#fe_eu_username').inputValue(), 'AdminSmoke.Mixed');
  assert.equal(await page.locator('#fe_eu_password').inputValue(), '');
  await page.locator('#fe_eu_fullname').fill('Admin smoke account');
  const savedUser = await responseFor('user/admin_update', () => button(/^Save Changes$/).click());
  assert.equal(savedUser.user.full_name, 'Admin smoke account');
  await button('Delete Account...').click();
  await confirmDelete('user/admin_delete');
  await open('activity', '.activity_desc');
  assert.ok((await page.locator('.activity_desc').allTextContents()).some(text => text.includes('AdminSmoke.Mixed')));
}
