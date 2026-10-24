const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function fixture(start = true) {
  let page;
  const listeners = {};
  const workers = [];
  class Worker {
    constructor(url) { this.url = url; this.messages = []; this.terminated = false; workers.push(this); }
    postMessage(message) { assert.equal(this.terminated, false); this.messages.push(message); }
    terminate() { this.terminated = true; }
  }
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../htdocs/js/pages/Home.class.js'), 'utf8'), {
    Class: {subclass(base, name, methods) { page = methods; }}, Page: {Base: {}}, Worker,
    window: {addEventListener(type, listener) { listeners[type] = listener; }},
    app: {tz:'UTC', schedule:[], state:{}, categories:[], plugins:[]}, hires_time_now:()=>1
  });
  page.div = {html() {}};
  page.onInit();
  if (start) page.start_worker();
  return {page, workers, listeners};
}

test('Home initialization does not load a worker before the page is needed', () => {
  const {page, workers} = fixture(false);
  assert.equal(page.worker, null);
  assert.equal(workers.length, 0);
  page.start_worker();
  page.start_worker();
  assert.equal(workers.length, 1);
  assert.equal(page.onDeactivate(), true);
  assert.equal(workers[0].terminated, true);
  page.start_worker();
  assert.equal(workers.length, 2);
  assert.equal(page.worker, workers[1]);
});

test('document navigation terminates its worker and late updates cannot post to it', () => {
  const {page, workers, listeners} = fixture();
  assert.equal(workers[0].url, 'js/home-worker.js');
  page.refresh_upcoming_events();
  assert.equal(workers[0].messages.length, 1);
  listeners.pagehide({persisted:false});
  assert.equal(workers[0].terminated, true);
  assert.equal(page.worker, null);
  page.refresh_upcoming_events();
  listeners.pagehide({persisted:false});
  assert.equal(workers[0].messages.length, 1);
  assert.equal(workers.length, 1);
});

test('back/forward cache restoration creates one fresh worker and refreshes an active Home page', () => {
  const {page, workers, listeners} = fixture();
  page.active = true;
  listeners.pagehide({persisted:true});
  listeners.pageshow({persisted:true});
  assert.equal(workers.length, 2);
  assert.equal(workers[1].messages.length, 1);
  assert.equal(typeof workers[1].onmessage, 'function');
  page.start_worker();
  assert.equal(workers.length, 2);
});

test('restoring a different tab does not render hidden Home and ordinary pageshow does not replace its worker', () => {
  const {page, workers, listeners} = fixture();
  listeners.pageshow({persisted:false});
  assert.equal(workers.length, 1);
  page.active = false;
  listeners.pagehide({persisted:true});
  listeners.pageshow({persisted:true});
  assert.equal(workers.length, 1);
  assert.equal(page.worker, null);
});

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
