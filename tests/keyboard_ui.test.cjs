const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');

function fixture() {
 let keydown, observer, resize, dialog=null, active;
 const regions=[];
 const events=[];
 const matches=(n,s)=>s.includes('onmouseup') ? n.hasAttribute('onmouseup')||n.hasAttribute('onclick')||/^tab_/.test(n.attrs.id||'') : s==='input, select, textarea, [contenteditable="true"]' ? /INPUT|TEXTAREA|SELECT/.test(n.tagName)||n.attrs.contenteditable==='true' : s==='button, a[href]' ? n.tagName==='BUTTON'||(n.tagName==='A'&&n.hasAttribute('href')) : false;
 function element(tag='DIV',attrs={},children=[]) {
  const n={tagName:tag,attrs:{...attrs},dataset:{},children,textContent:'Action',nodeType:1,isConnected:true,disabled:false,title:'',
   hasAttribute(k){return k in this.attrs;},getAttribute(k){return this.attrs[k]??null;},setAttribute(k,v){this.attrs[k]=v;},removeAttribute(k){delete this.attrs[k];},
   get tabIndex(){return this.attrs.tabindex!==undefined?Number(this.attrs.tabindex):/^(A|INPUT|SELECT|TEXTAREA|BUTTON)$/.test(tag)?0:-1;},
   matches(s){return matches(this,s);},closest(s){return this.matches(s)?this:null;},getClientRects(){return this.hidden?[]:[{}];},
   contains(other){return other===this||this.children.includes(other);},querySelector(){return null;},
   querySelectorAll(s){return s.includes('onmouseup')?this.children.filter(c=>matches(c,s)):this.children.filter(c=>c.tabIndex>=0);},
   focus(){active=this;},dispatchEvent(e){events.push({node:this,type:e.type});}};
  return n;
 }
 const trigger=element('DIV',{onmouseup:'open'});active=trigger;
 const document={body:{},readyState:'complete',get activeElement(){return active;},
  querySelectorAll(s){return s==='table.data_table, .pagination'?regions:[trigger];},getElementById(){return dialog;},addEventListener(type,fn){if(type==='keydown')keydown=fn;}};
 const Dialog={active:false,clickBlock:false,show(){dialog=element('DIV',{},this.next||[]);this.active=true;},hide(){this.active=false;dialog=null;}};
 const app={confirm_click(value){events.push({confirm:value});Dialog.hide();}};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/keyboard.js'),'utf8'),{document,window:{addEventListener(type,fn){resize=fn;}},Dialog,app,
  MouseEvent:class{constructor(type){this.type=type;}},MutationObserver:class{constructor(fn){observer=fn;}observe(){}}});
 return {element,trigger,events,Dialog,document,regions,resize(){resize();}, get dialog(){return dialog;},
  add(n){observer([{addedNodes:[n]}]);},key(target,key,extra={}){const e={target,key,preventDefault(){this.prevented=true;},stopImmediatePropagation(){this.stopped=true;},stopPropagation(){this.bubbleStopped=true;},...extra};keydown(e);return e;}};
}

test('legacy actions are tabbable and activate once with Enter or Space, including dynamic pages',()=>{
 const f=fixture();assert.equal(f.trigger.attrs.role,'button');assert.equal(f.trigger.tabIndex,0);
 for(const key of ['Enter',' ']) {const e=f.key(f.trigger,key);assert.equal(e.prevented,true);assert.equal(e.stopped,true);}
 assert.deepEqual(f.events.map(e=>e.type),['mouseup','mouseup']);
 f.key(f.trigger,' ',{repeat:true});assert.equal(f.events.length,2);
 const tab=f.element('DIV',{id:'tab_Schedule'});f.add(tab);assert.equal(tab.tabIndex,0);f.key(tab,'Enter');assert.equal(f.events[2].type,'click');
 const added=f.element('SPAN',{onclick:'action'});f.add(added);assert.equal(added.tabIndex,0);
 f.key(added,'Enter');assert.equal(f.events[3].type,'click');
});

test('native form controls, real links and disabled actions retain their own semantics',()=>{
 const f=fixture();
 for(const n of [f.element('INPUT',{onmouseup:'action'}),f.element('TEXTAREA'),f.element('A',{href:'#Home',onclick:'go'}),f.element('DIV',{onmouseup:'action','aria-disabled':'true'})]){
  f.add(n);assert.equal(f.key(n,'Enter').prevented,undefined);
 }
 assert.equal(f.events.length,0);
});

test('modal focuses its first control, traps both tab boundaries and restores the opener',()=>{
 const f=fixture(),first=f.element('INPUT'),last=f.element('DIV',{onmouseup:'cancel'});
 f.Dialog.next=[first,last];f.Dialog.show();
 assert.equal(f.dialog.attrs.role,'dialog');assert.equal(f.dialog.attrs['aria-modal'],'true');assert.equal(f.document.activeElement,first);
 assert.equal(f.key(first,'Tab',{shiftKey:true}).prevented,true);assert.equal(f.document.activeElement,last);
 assert.equal(f.key(last,'Tab').prevented,true);assert.equal(f.document.activeElement,first);
 f.Dialog.hide();assert.equal(f.document.activeElement,f.trigger);
});

test('Escape cancels confirmations but cannot dismiss a blocking progress dialog',()=>{
 const f=fixture();f.Dialog.show();f.Dialog.active='confirmation';f.key(f.dialog,'Escape');
 assert.deepEqual(f.events,[{confirm:false}]);assert.equal(f.document.activeElement,f.trigger);
 f.Dialog.show();f.Dialog.clickBlock=true;assert.equal(f.key(f.dialog,'Escape').stopped,true);assert.equal(f.Dialog.active,true);
});

test('Enter on the focused Cancel control invokes Cancel rather than the framework global confirmation shortcut',()=>{
 const f=fixture(),cancel=f.element('DIV',{onmouseup:'app.confirm_click(false)'});
 f.Dialog.next=[cancel];f.Dialog.show();f.Dialog.active='confirmation';
 assert.equal(f.key(cancel,'Enter').stopped,true);assert.deepEqual(f.events.map(e=>e.type),['mouseup']);
});

test('event script editor allows Tab to leave the textarea',()=>{
 const source=fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/Schedule.class.js'),'utf8');
 assert.doesNotMatch(source,/onkeydown="return catchTab\(this,event\)"/);
});

 test('native dialog button and link Enter retain browser defaults without global confirm',()=>{
 const f=fixture();f.Dialog.show();f.Dialog.active='confirmation';
 for(const n of [f.element('BUTTON'),f.element('A',{href:'#Home'})]) {
  const e=f.key(n,'Enter');assert.equal(e.bubbleStopped,true);assert.equal(e.prevented,undefined);
 }
 assert.equal(f.events.length,0);
});

test('overflow regions are keyboard reachable only while overflowing and retain existing labels',()=>{
 const f=fixture(),table=f.element('TABLE',{'aria-label':'Jobs'});
 table.clientWidth=300;table.scrollWidth=700;f.regions.push(table);f.resize();
 assert.equal(table.tabIndex,0);assert.equal(table.attrs.role,'region');assert.equal(table.attrs['aria-label'],'Jobs');
 table.scrollWidth=300;f.resize();assert.equal(table.tabIndex,-1);assert.equal(table.attrs['aria-label'],'Jobs');
});
