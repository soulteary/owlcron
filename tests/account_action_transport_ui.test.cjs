const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

test('account action transport avoids credential logs and preserves session transport and callbacks', () => {
 for (const plainText of [true, false]) {
  const calls = [], logs = [];
  const app = {api:{request(...args){calls.push(args);return 'request-result';}},extend(value){Object.assign(this,value);},getPref(){return 'session-secret';},base_api_url:'/cron/api'};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../htdocs/js/app.js'), 'utf8'), {window:{app},app,hash_keys_to_array:Object.keys,Debug:{trace(...args){logs.push(args);}}});
  app.plain_text_post=plainText;
  const params={token:'token-secret',current_password:'password-secret'};
  const success=()=>{}, error=()=>{};
  assert.equal(app.api.postAccountAction('user/verify_email',params,success,error),'request-result');
  assert.equal(calls[0][0],'/cron/api/user/verify_email');
  assert.equal(calls[0][1].type,'POST');
  assert.equal(calls[0][1].contentType,plainText?'text/plain':'application/json');
  const body=JSON.parse(calls[0][1].data);
  assert.equal(body.token,params.token); assert.equal(body.current_password,params.current_password);
  assert.equal(body.session_id,plainText?'session-secret':undefined);
  assert.equal(params.session_id,undefined);
  assert.equal(calls[0][2],success);assert.equal(calls[0][3],error);assert.deepEqual(logs,[]);
 }
});
