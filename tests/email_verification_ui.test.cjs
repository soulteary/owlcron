const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../htdocs/js/pages/Login.class.js'), 'utf8');

test('verification link waits for explicit confirmation and sends the token once', () => {
 let definition, calls = [], html = '';
 const context = {Page:{Base:{}}, Class:{subclass:(_a,_b,d)=>definition=d}, config:{email_verification:true},
 app:{setWindowTitle(){},showTabBar(){},api:{postAccountAction:(...args)=>calls.push(args)}},window:{}};
 vm.runInNewContext(source,context);
 const page = Object.assign({div:{html:value=>html=value}},definition);
 page.showEmailVerification({purpose:'verify',userID:'user1',token:'a'.repeat(64)});
 assert.equal(calls.length,0); assert.match(html,/confirm_email_verification/);
 page.confirmEmailVerification(); page.confirmEmailVerification();
 assert.equal(calls.length,1); assert.equal(calls[0][0],'user/verify_email');
 assert.equal(page.pendingEmailAction,null);
});
test('disabled capability never offers verification or retains a token', () => {
 let definition, html='';
 vm.runInNewContext(source,{Page:{Base:{}},Class:{subclass:(_a,_b,d)=>definition=d},config:{email_verification:false},app:{setWindowTitle(){},showTabBar(){}}});
 const page=Object.assign({div:{html:value=>html=value}},definition);
 page.showEmailVerification({purpose:'verify',userID:'u',token:'a'.repeat(64)});
 assert.equal(page.pendingEmailAction,null); assert.doesNotMatch(html,/confirm_email_verification/);
});


test('profile saves omit cached email when verification manages the address', () => {
 for (const enabled of [true,false]) {
  let definition, sent;
  const values={'#fe_ma_old_password':'current-password','#fe_ma_new_password':'','#fe_ma_fullname':'Updated Name','#fe_ma_email':'pending@example.test'};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/MyAccount.class.js'),'utf8'),{
   Page:{Base:{}},Class:{subclass:(_a,_b,d)=>definition=d},trim:value=>value.trim(),
   $:selector=>({val:()=>values[selector]}),
   app:{config:{account_email_enabled:enabled},username:'user',user:{email:'stale@example.test'},clearError(){},showProgress(){},api:{postAccountAction:(_endpoint,body)=>sent=body}}
  });
  definition.save_changes();
  assert.equal(sent.full_name,'Updated Name');
  assert.equal(Object.hasOwn(sent,'email'),!enabled);
  if (!enabled) assert.equal(sent.email,'pending@example.test');
 }
});


test('verification fragments are scrubbed on same-document navigation', () => {
 const html=fs.readFileSync(path.join(__dirname,'../htdocs/index-dev.html'),'utf8');
 const script=html.match(/<script>([\s\S]*?)<\/script>/)[1];
 let listener;
 const context={URLSearchParams,location:{hash:'#Home',pathname:'/cron/',search:''},window:{Nav:{loc:'Home'},addEventListener(name,fn,capture){assert.equal(name,'hashchange');assert.equal(capture,true);listener=fn;}},history:{replaceState(_a,_b,url){context.url=url;context.location.hash=url.slice(url.indexOf('#'));}}};
 vm.runInNewContext(script,context);
 for(const token of ['a'.repeat(64),'b'.repeat(64)]) {
  context.location.hash='#Login?verify=1&u=user1&token='+token;listener();
  assert.equal(context.location.hash,'#Login?verify=1');
  assert.equal(context.window.owlcronAccountAction.token,token);
  assert.equal(context.window.Nav.loc,'account-action');
 }
 listener();assert.equal(context.window.owlcronAccountAction.token,'b'.repeat(64));
});
