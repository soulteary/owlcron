const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
let page;
vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/Base.class.js'),'utf8'),{
 Class:{subclass(base,name,value){page=value;}}, Page:{}, encode_entities(value){return value.replaceAll('<','&lt;').replaceAll('>','&gt;');},
 get_nice_date(value){return 'date '+value;}, get_nice_date_time(value){return 'time '+value;}
});
test('records without timestamps remain renderable without fabricated dates',()=>{
 for(const value of [undefined,null,0,'bad',Infinity,1e20]) assert.equal(page.getNiceRecordDate(value,true),'Not recorded');
 assert.equal(page.getNiceRecordDate(100,true),'date 100');
 assert.equal(page.getNiceRecordDate(100,false),'time 100');
});

test('API key display treats titles as text',()=>{ assert.match(page.getNiceAPIKey({title:'<b>app</b>'},false), /&lt;b&gt;app&lt;\/b&gt;/); });
