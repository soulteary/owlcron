// Run with bun test tests/job_log_ui.test.cjs.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
function fixture() {
 const requests=[], timers=[], errors=[]; let methods, displayed='';
 const element={length:1,text(v){displayed=v;return this;},empty(){return this;},append(v){return this.text(v.value);},prop(){return 0;},scrollTop(){return 0;},height(){return 0;}};
 const context={Class:{subclass(base,name,defs){methods=defs;}}, Page:{Base:{}}, TextDecoder, Uint8Array, atob,
 setTimeout(fn){timers.push(fn);},setInterval(fn){timers.push(fn);return 1;},clearInterval(){},
 $:selector=>selector.startsWith('<')?{text(value){return {value};}}:element,
 app:{getPref(){return 'session';},api:{post(path,params,success,error){requests.push({path,params,success,error});}},doError(e){errors.push(e);}},window:{}};
 vm.runInNewContext(fs.readFileSync(require('node:path').join(__dirname,'../htdocs/js/pages/JobDetails.class.js'),'utf8'),context);
 const page=Object.assign({active:true,job:{id:'job'},setup_live_log_links(){}},methods);
 return {page,requests,timers,errors,context,get displayed(){return displayed;}};
}
function response(bytes,next,eof) {return {log_base64:Buffer.from(bytes).toString('base64'),next_offset:next,eof};}
test('archive advances byte offsets and decodes UTF-8 across pages',()=>{
 const f=fixture(), bytes=Buffer.from('日志'); f.page.load_archive_log();
 assert.equal(f.requests[0].params.offset,0);
 f.requests.shift().success(response(bytes.subarray(0,1),1,false));
 f.timers.shift()(); assert.equal(f.requests[0].params.offset,1);
 f.requests.shift().success(response(bytes.subarray(1),6,true)); assert.equal(f.displayed,'日志');
});
test('live polling appends pages, caps display, and ignores callbacks from old watcher',()=>{
 const f=fixture(); f.page.start_live_log_watcher(f.page.job);
 const old=f.requests.shift(); f.requests.shift();
 f.page.start_live_log_watcher(f.page.job);
 old.success(response('old',3,true)); assert.equal(f.displayed,'');
 const current=f.requests.shift(); f.requests.shift();
 current.success(response('a'.repeat(1048576),1048576,false));
 f.timers.at(-1)(); const next=f.requests.shift(); assert.equal(next.params.offset,1048576);
 next.success(response('new',1048579,true));
 assert.ok(f.displayed.endsWith('new')); assert.ok(f.displayed.length<1048700);
});
test('download writes bounded raw byte pages and closes writer',async()=>{
 const f=fixture(), writes=[];let closed=false;const offsets=[];
 f.context.window.showSaveFilePicker=async()=>({createWritable:async()=>({write:async b=>writes.push(Buffer.from(b)),close:async()=>{closed=true;},abort:async()=>{throw Error('unexpected abort');}})});
 f.context.fetch=async url=>{offsets.push(new URL(url,'http://local').searchParams.get('offset'));const second=offsets.length===2;return {ok:true,headers:new Map([['X-Log-Next-Offset',second?'6':'1'],['X-Log-EOF',String(second)]]),arrayBuffer:async()=>Buffer.from('日志').subarray(second?1:0,second?6:1)};};
 await f.page.do_download_log(); assert.deepEqual(offsets,['0','1']); assert.equal(Buffer.concat(writes).toString(),'日志');assert.equal(closed,true);
});
test('download failure aborts and reports original error even if abort fails',async()=>{
 const f=fixture();let aborted=false;
 f.context.window.showSaveFilePicker=async()=>({createWritable:async()=>({abort:async()=>{aborted=true;throw Error('abort failure');}})});
 f.context.fetch=async()=>{throw Error('network failure');};
 await f.page.do_download_log();assert.equal(aborted,true);assert.deepEqual(f.errors,['network failure']);
});

test('browsers without file picker navigate to authenticated streaming download',async()=>{
 const f=fixture(); f.context.window.location={};
 await f.page.do_download_log();const req=f.requests.shift();
 assert.equal(req.path,'app/get_log_watch_auth');req.success({token:'fresh-token'});
 const url=new URL(f.context.window.location.href,'http://local');
 assert.equal(url.searchParams.get('stream'),'1');assert.equal(url.searchParams.get('download'),'1');
 assert.equal(url.searchParams.get('t'),'fresh-token');assert.equal(url.searchParams.has('session_id'),false);
});
