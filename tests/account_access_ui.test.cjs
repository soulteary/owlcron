const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function loginFixture() {
 let definition, html = '', requests = [];
 const field = {focus(){}, keypress(){}, val(){return 'CaseSensitiveUser';}};
 const context = {
  Page:{Base:{}}, Class:{subclass(base,name,value){definition=value;}},
  app:{user:null, getPref(){return '';}, showProgress(){}, setWindowTitle(){}, showTabBar(){}, get_password_type(){return 'password';}, get_password_toggle_html(){return '';}, api:{post(...args){requests.push(args);}}},
  config:{free_accounts:1}, $(){return field;}, setTimeout(fn){fn();}
 };
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/Login.class.js'),'utf8'), context);
 const page = {...definition, div:{css(){}, html(value){html=value;}}};
 return {page, get html(){return html;}, requests};
}

test('login offers administrator-assisted access instead of unavailable self-service forms', () => {
 const f=loginFixture(); f.page.onActivate({});
 assert.match(f.html,/Account access help/);
 assert.doesNotMatch(f.html,/Forgot Password|Create Account|Send Email/);
 for (const args of [{recover:1},{create:1},{u:'legacy-user',h:'legacy-token'}]) {
  f.page.onActivate(args);
  assert.match(f.html,/contact your administrator/i);
  assert.match(f.html,/Back to Login/);
  assert.doesNotMatch(f.html,/fe_pr_email|fe_reset_password|fe_ca_username/);
 }
 assert.equal(f.requests.length,0);
});

test('login preserves the case-sensitive account name', () => {
 const f=loginFixture(); f.page.doLogin = f.page.doLogin.bind(f.page);
 // Only the request matters here; the callback is not executed.
 f.page.doLogin();
 assert.equal(f.requests[0][1].username, 'CaseSensitiveUser');
});
