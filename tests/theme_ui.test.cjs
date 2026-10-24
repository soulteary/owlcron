const {test}=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const path=require('node:path');
function fixture(saved,denied=false,Chart){
 let dark=false,listener,change,stored=saved;
 const select={value:'',addEventListener(type,fn){change=fn;}};
 const media={matches:true,addEventListener(type,fn){listener=fn;}};
 const context={Chart,window:{Chart,matchMedia(){return media;}},document:{body:{classList:{toggle(cls,value){dark=value;}},style:{}},getElementById(){return select;}},localStorage:{getItem(){if(denied)throw Error('denied');return stored;},setItem(k,v){if(denied)throw Error('denied');stored=v;}}};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/theme.js'),'utf8'),context);
 return {get dark(){return dark;},get stored(){return stored;},media,select,system(){listener();},choose(value){select.value=value;change();}};
}
test('system preference follows OS and can be restored after explicit override',()=>{
 const f=fixture();assert.equal(f.dark,true);f.choose('light');assert.equal(f.dark,false);assert.equal(f.stored,'light');
 f.media.matches=true;f.system();assert.equal(f.dark,false);f.choose('system');assert.equal(f.dark,true);
 f.media.matches=false;f.system();assert.equal(f.dark,false);
});
test('stored preference wins and denied storage does not break the page',()=>{
 assert.equal(fixture('light').dark,false);assert.equal(fixture('invalid').select.value,'system');
 const f=fixture(null,true);f.choose('dark');assert.equal(f.dark,true);f.choose('light');assert.equal(f.dark,false);
});

test('theme applies Chart.js 4 defaults and updates existing scales without animation',()=>{
 const updates=[];
 const chart={options:{plugins:{legend:{labels:{}}},scales:{x:{ticks:{},grid:{}},y:{ticks:{},grid:{}}}},update(mode){updates.push(mode);}};
 const Chart={defaults:{scale:{ticks:{},grid:{}}},instances:{chart}};
 const f=fixture('light',false,Chart);
 assert.equal(Chart.defaults.color,'#555555');
 f.choose('dark');
 assert.equal(Chart.defaults.color,'#d4dae3');
 assert.equal(Chart.defaults.scale.grid.color,'#38404d');
 assert.equal(chart.options.plugins.legend.labels.color,'#d4dae3');
 assert.equal(chart.options.scales.x.ticks.color,'#d4dae3');
 assert.equal(chart.options.scales.y.grid.color,'#38404d');
 assert.deepEqual(updates,['none','none']);
});
