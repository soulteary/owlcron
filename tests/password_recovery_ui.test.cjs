const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../htdocs/js/pages/Login.class.js'), 'utf8');
function fixture(enabled = true) {
 let definition, html = '', requests = [], messages = [], logout = 0;
 const values = {'#fe_reset_password':'long-new-password', '#fe_reset_confirm':'long-new-password', '#fe_recovery_username':'CaseUser', '#fe_recovery_email':'verified@example.test'};
 const context = {Page:{Base:{}},Class:{subclass:(_a,_b,d)=>definition=d},config:{password_recovery:enabled},TextEncoder,
  app:{user:null,getPref(){return '';},get_password_type(){return 'password';},get_password_toggle_html(){return '';},setWindowTitle(){},showTabBar(){},showMessage:(...args)=>messages.push(args),doUserLogout:()=>logout++,api:{post:(...args)=>requests.push(args)}},
  $:id=>({val:()=>values[id],focus(){},keypress(){}}),window:{},setTimeout(fn){fn();}};
 vm.runInNewContext(source,context);
 const page=Object.assign({div:{css(){},html:value=>html=value}},definition);
 return {page,values,requests,messages,get html(){return html;},get logout(){return logout;}};
}
const action=()=>({purpose:'reset',userID:'user1',token:'a'.repeat(64)});
test('reset requires explicit confirmation and consumes its in-memory token once',()=>{
 const f=fixture(); f.page.showPasswordReset(action());
 assert.equal(f.requests.length,0); assert.match(f.html,/confirm_password_reset/); assert.doesNotMatch(f.html,/a{64}/);
 f.page.confirmPasswordReset();f.page.confirmPasswordReset();
 assert.equal(f.requests.length,1);assert.equal(f.requests[0][0],'user/reset_password');
 assert.equal(f.requests[0][1].confirm_password,'long-new-password');assert.equal(f.page.pendingResetAction,null);
 f.requests[0][2]();assert.equal(f.logout,1);assert.match(f.messages.at(-1)[1],/Sign in/);
});
test('reset validates byte limits and confirmation before sending',()=>{
 const f=fixture();f.page.showPasswordReset(action());
 for (const value of ['short','界'.repeat(25)]) { f.values['#fe_reset_password']=value; f.values['#fe_reset_confirm']=value; f.page.confirmPasswordReset(); }
 f.values['#fe_reset_password']='long-new-password';f.values['#fe_reset_confirm']='different-password';f.page.confirmPasswordReset();
 assert.equal(f.requests.length,0);assert.equal(f.messages.length,3);assert.ok(f.page.pendingResetAction);
 f.values['#fe_reset_password']='界'.repeat(24);f.values['#fe_reset_confirm']='界'.repeat(24);f.page.confirmPasswordReset();assert.equal(f.requests.length,1);
});
test('disabled reset and invalid links never retain an action or send requests',()=>{
 for(const enabled of [false,true]) {const f=fixture(enabled);f.page.showPasswordReset(enabled?{...action(),purpose:'verify'}:action());f.page.confirmPasswordReset();assert.equal(f.requests.length,0);assert.equal(f.page.pendingResetAction,null);assert.doesNotMatch(f.html,/confirm_password_reset/);}
});
test('recovery preserves username case and uses a generic acknowledgement',()=>{
 const f=fixture();f.page.requestPasswordRecovery();f.page.requestPasswordRecovery();assert.equal(f.requests.length,1);
 assert.equal(f.requests[0][1].username,'CaseUser');f.requests[0][2]();assert.match(f.html,/If an enabled account matches/);
});
test('deactivation discards all pending account actions',()=>{
 const f=fixture();f.page.showPasswordReset(action());f.page.onDeactivate();assert.equal(f.page.pendingResetAction,null);assert.equal(f.page.pendingEmailAction,null);
});
test('bootstrap strips verification and reset tokens before loading assets',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../htdocs/index-dev.html'),'utf8');const script=html.match(/<script>([\s\S]*?)<\/script>/)[1];
 for(const purpose of ['reset','verify']) {
  const token='a'.repeat(64), context={URLSearchParams,window:{},location:{hash:'#Login?'+purpose+'=1&u=user1&token='+token,pathname:'/cron/',search:''},history:{replaceState(_a,_b,url){context.url=url;}}};
  vm.runInNewContext(script,context);assert.equal(context.window.owlcronAccountAction.purpose,purpose);assert.equal(context.window.owlcronAccountAction.token,token);assert.equal(context.url,'/cron/#Login?'+purpose+'=1');
 }
 assert.ok(html.indexOf('<script>')<html.indexOf('<link'));
});

test('login exposes recovery only when enabled and keeps registration closed',()=>{
 for(const enabled of [false,true]) {
  const f=fixture(enabled); f.page.onActivate({});
  assert.equal(f.html.includes('Forgot Password?'),enabled);
  f.page.onActivate({recover:1});assert.equal(f.html.includes('fe_recovery_email'),enabled);
  f.page.onActivate({create:1});assert.match(f.html,/Self-service registration is not available/);assert.doesNotMatch(f.html,/fe_ca_username/);
 }
});
