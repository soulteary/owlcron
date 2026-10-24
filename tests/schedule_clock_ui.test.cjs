const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/Schedule.class.js'),'utf8');
function fixture(){
 let page,enabled=false,choice;
 const controls={};
 function $(selector){
  if(!controls[selector])controls[selector]={attrs:{},props:{},epoch:123,value:'original',
   attr(k,v){this.attrs[k]=v;return this;},removeAttr(k){delete this.attrs[k];return this;},
   prop(k,v){this.props[k]=v;return this;},css(){return this;},is(){return enabled;},
   data(k,v){if(v===undefined)return this.epoch;this.epoch=v;return this;},val(v){if(v===undefined)return this.value;this.value=v;return this;}};
  return controls[selector];
 }
 vm.runInNewContext(source,{Class:{subclass(a,b,value){page=value;}},Page:{Base:{}},$,app:{tz:'UTC',state:{}},normalize_time:()=>456,time_now:()=>456});
 page.event={timezone:'UTC'};page.choose_date_time=args=>{choice=args;};page.rc_get_short_date_time=epoch=>'time '+epoch;
 return {page,$,set enabled(value){enabled=value;},get choice(){return choice;}};
}
test('event clock uses a native keyboard button without blur-on-focus',()=>{
 const input=source.match(/<input type="button" id="fe_ee_rc_time"[^>]+>/)[0];
 assert.match(input,/onClick="\$P\(\).rc_click\(\)"/);assert.match(input,/aria-haspopup="dialog"/);
 assert.doesNotMatch(input,/onFocus|onMouseUp/);
 assert.match(source,/id="s_ee_rc_reset"[^>]+hidden[^>]+tabindex="-1"/);
});
test('clock picker preserves the epoch and value contract and reset follows the enabled state',()=>{
 const f=fixture();f.page.rc_click();assert.equal(f.choice,undefined);f.enabled=true;f.page.rc_click();
 assert.equal(f.choice.when,123);assert.equal(f.choice.title,'Set Event Clock');f.choice.callback(789);
 assert.equal(f.$('#fe_ee_rc_time').epoch,789);assert.equal(f.$('#fe_ee_rc_time').value,'time 789');
 f.page.toggle_rc_textfield(true);assert.equal(f.$('#s_ee_rc_reset').props.hidden,false);assert.equal(f.$('#s_ee_rc_reset').attrs.tabindex,'0');
 f.page.toggle_rc_textfield(false);assert.equal(f.$('#s_ee_rc_reset').props.hidden,true);assert.equal(f.$('#s_ee_rc_reset').attrs.tabindex,'-1');
 f.enabled=false;f.page.reset_rc_time_now();assert.equal(f.$('#fe_ee_rc_time').epoch,789);
 f.enabled=true;f.page.reset_rc_time_now();assert.equal(f.$('#fe_ee_rc_time').epoch,456);
});
