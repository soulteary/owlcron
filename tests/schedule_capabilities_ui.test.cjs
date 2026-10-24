const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
let page;
vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/Schedule.class.js'),'utf8'),{
 Class:{subclass(base,name,value){page=value;}}, Page:{Base:{}},
 encode_entities(value){return value.replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');},
 render_menu_options(){return '<option value="single" selected>Single</option>';},
 $(){return {val(){return 'single';}};}
});
test('editing an event preserves API-managed chain arrays instead of erasing targets',()=>{
 const original=['first','second'];
 const value=page.get_chain_form_value('fe_ee_chain',original);
 assert.deepEqual(Array.from(value),original);
 assert.notEqual(value,original);
 assert.match(page.get_chain_editor('fe_ee_chain',original,[]),/API-managed chain targets/);
 assert.doesNotMatch(page.get_chain_editor('fe_ee_chain',original,[]),/<select/);
 assert.equal(page.get_chain_form_value('fe_ee_chain','old'),'single');
 assert.match(page.get_chain_editor('fe_ee_chain','single',[]),/<select/);
});
