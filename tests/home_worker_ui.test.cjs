const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

test('Home worker accepts the missing scheduler snapshot during first login', () => {
 for (const state of [undefined, null, {}, {cursors:null}, {cursors:{}}]) {
  const messages = [];
  const context = {
   importScripts(){},
   normalize_time(value){return value;},
   time_now(){return 1700000000;},
   hires_time_now(){return 0;},
   postMessage(value){messages.push(value);}
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/home-worker.js'),'utf8'),context);
  context.onmessage({data:{state,schedule:[],categories:[],plugins:[],default_tz:'UTC'}});
  assert.equal(messages.length,1);
  assert.equal(messages[0].length,0);
 }
});
