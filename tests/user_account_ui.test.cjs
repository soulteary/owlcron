const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {webcrypto} = require('node:crypto');

function fixture({enabled = true, user = {}, crypto = webcrypto} = {}) {
  const values = {'#fe_eu_username':'new-user','#fe_eu_status':'1','#fe_eu_email':'new@example.test','#fe_eu_password':''};
  const errors = [], page = {};
  const app = {config:{account_email_enabled:enabled},categories:[],server_groups:[],
    badField(id,text){errors.push(text);return false;},doError(text){errors.push(text);return false;}};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/admin/Users.js'),'utf8'), {
    Class:{add(target,methods){Object.assign(target,methods);}},Page:{Admin:page},app,
    config:{privilege_list:[]},window:{crypto},Uint8Array,TextEncoder,
    $:selector=>({val(value){if(arguments.length)values[selector]=value;return values[selector] || '';},is(){return false;}}),
    trim:value=>String(value || '').trim(),escape_text_field_value:value=>String(value || ''),
    render_menu_options:()=>'',get_form_table_row:(label,html)=>label+html,get_form_table_caption:text=>text,get_form_table_spacer:()=>''
  });
  page.user = {privileges:{},...user};
  return {page,values,errors};
}

test('generated admin passwords use secure randomness and satisfy the form policy',()=>{
  let requested = 0;
  const f = fixture({crypto:{getRandomValues(bytes){requested=bytes.length;bytes.fill(0xab);return bytes;}}});
  f.page.generate_password();
  assert.equal(requested,16);
  assert.equal(f.values['#fe_eu_password'],'ab'.repeat(16));
  assert.equal(f.page.get_user_form_json().password,'ab'.repeat(16));
  const live = fixture();live.page.generate_password();
  assert.match(live.values['#fe_eu_password'],/^[0-9a-f]{32}$/);
});

test('unavailable or failed secure randomness preserves the existing password and reports an actionable error',()=>{
  for (const crypto of [null,{}, {getRandomValues(){throw new Error('Unavailable');}}]) {
    const f = fixture({crypto});f.values['#fe_eu_password']='keep-this-password';
    assert.equal(f.page.generate_password(),false);
    assert.equal(f.values['#fe_eu_password'],'keep-this-password');
    assert.match(f.errors[0],/trusted password manager/);
  }
});

test('email-enabled edits show verification instructions and omit even stale or altered email values',()=>{
  for (const verified of [true,false]) {
    const f = fixture({user:{username:'x',email:'existing@example.test',email_verified:verified}});
    const html = f.page.get_user_edit_html();
    assert.match(html,/readonly="readonly"/);
    assert.match(html,verified ? /Verified email address/ : /has not been verified/);
    assert.match(html,/My Account/);
    f.values['#fe_eu_email']='not even a valid address';
    f.values['#fe_eu_username']='attempted-rename';
    const payload = f.page.get_user_form_json();
    assert.equal(payload.username,'x');
    assert.equal(Object.hasOwn(payload,'email'),false);
    assert.equal(f.errors.length,0);
  }
});

test('new accounts retain initial unverified email and enforce the creation username policy',()=>{
  const f=fixture();
  assert.doesNotMatch(f.page.get_user_edit_html(),/readonly="readonly"/);
  assert.match(f.page.get_user_edit_html(),/must verify it from My Account/);
  assert.equal(f.page.get_user_form_json().email,'new@example.test');
  f.values['#fe_eu_username']='x';
  assert.equal(f.page.get_user_form_json(),false);
  assert.match(f.errors[0],/3-50/);
});

test('disabled email features retain editable optional contact information and administrator reset guidance',()=>{
  const f=fixture({enabled:false,user:{username:'existing'}});
  assert.doesNotMatch(f.page.get_user_edit_html(),/readonly="readonly"/);
  assert.match(f.page.get_user_edit_html(),/Account email features are disabled/);
  assert.equal(f.page.get_user_form_json().email,'new@example.test');
});
