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
