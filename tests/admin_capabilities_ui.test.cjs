const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function fixture(file, values = {}, checked = {}) {
 const errors = [], page = {}, rendered = [];
 const field = selector => ({val(){return values[selector] || '';},is(){return !!checked[selector];},focus(){},prop(){return this;}});
 const app = {categories:[],server_groups:[],schedule:[],badField(id,text){errors.push({id,text});return false;},doError(text){errors.push({text});return false;},clearError(){}};
 const context = {Class:{add(target, methods){Object.assign(target, methods);}},Page:{Admin:page},app,
   config:{privilege_list:[{id:'admin'},{id:'run_events'}]},$ : field,trim:s=>String(s || '').trim(),TextEncoder,
   escape_text_field_value:value=>String(value ?? ''),setTimeout(){},render_menu_options(){return '';},get_form_table_row(label,html){return label + html;},get_form_table_caption:s=>s,get_form_table_spacer:()=>'',
   copy_object:obj=>({...obj}),find_object:(rows,query)=>rows.find(row=>Object.keys(query).every(key=>row[key]===query[key])),
   encode_entities:value=>String(value ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;'),
   get_nice_date_time:value=>'TIME:'+value,summarize_event_timing:()=>'',get_unique_id:()=> 'test-key'};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/admin',file+'.js'),'utf8'),context);
 page.div={removeClass(){},html(value){rendered.push(value);}};
 page.getSidebarTabs=()=>'';
 page.getNiceUsername=()=>'';
 return {page,app,context,errors,rendered};
}

test('plugin form keeps numeric OS identities as strings and explains the script adapter',()=>{
 const f=fixture('Plugins',{'#fe_ep_title':'Script','#fe_ep_command':'/opt/owlcron/owlcron-script -- /bin/sh /opt/jobs/task.sh','#fe_ep_uid':'10001','#fe_ep_gid':'10002'}, {'#fe_ep_enabled':true});
 f.page.plugin={params:[]};
 const plugin=f.page.get_plugin_form_json();
 assert.equal(plugin.uid,'10001');assert.equal(plugin.gid,'10002');
 assert.equal(typeof plugin.uid,'string');
 f.page.plugin.command='invalid\ncommand';
 f.context.$=selector=>({val(){return selector==='#fe_ep_command'?'invalid\ncommand':selector==='#fe_ep_title'?'Script':'';},is(){return true;}});
 assert.equal(f.page.get_plugin_form_json(),false);
 assert.match(f.errors[0].text,/owlcron-script/);
 assert.doesNotMatch(f.errors[0].text,/built-in Shell Plugin/);
});

test('duplicate plugin parameters report a field error without throwing',()=>{
 const f=fixture('Plugins');f.page.plugin={params:[{id:'existing'}]};
 f.page.ctype_labels={};f.page.get_plugin_param_editor_html=()=>'';
 f.page.get_plugin_param_values=()=>({id:'existing'});
 let submit;f.app.confirm=(title,html,label,callback)=>{submit=callback;};
 f.page.edit_plugin_param(-1);submit(true);
 assert.equal(f.errors[0].id,'fe_epp_id');
 assert.match(f.errors[0].text,/already taken/);
});

test('API key status updates both supported aliases and omits unsupported description',()=>{
 const f=fixture('APIKeys',{'#fe_ak_key':'test-key','#fe_ak_status':'0','#fe_ak_title':'Integration'}, {'#fe_ak_priv_run_events':true});
 f.page.api_key={enabled:true,active:true,description:'not persisted',privileges:{}};
 const key=f.page.get_api_key_form_json();
 assert.equal(key.enabled,false);assert.equal(key.active,false);assert.equal(key.description,undefined);
 assert.equal(key.privileges.run_events,1);assert.equal(key.privileges.admin,0);
 assert.doesNotMatch(f.page.get_api_key_edit_html(),/fe_ak_desc/);
});

test('user form preserves username case and permits optional contact details',()=>{
 const f=fixture('Users',{'#fe_eu_username':'Mixed.Case','#fe_eu_status':'1','#fe_eu_password':'TwelveBytes!!'});
 const user=f.page.get_user_form_json();
 assert.equal(user.username,'Mixed.Case');assert.equal(user.email,'');assert.equal(user.full_name,'');
 assert.equal(user.active,1);assert.equal(user.privileges.admin,0);
 assert.equal(f.errors.length,0);
});

test('user password validation counts UTF-8 bytes and allows unchanged passwords',()=>{
 for(const [password,valid] of [['',true],['short',false],['界'.repeat(25),false],['界'.repeat(4),true]]) {
  const f=fixture('Users',{'#fe_eu_username':'ExampleUser','#fe_eu_password':password});
  assert.equal(!!f.page.get_user_form_json(),valid);
 }
});

test('server group preview preserves case sensitivity and supports leading Go flags',()=>{
 const f=fixture('Servers');
 assert.equal(f.page.get_server_group_regexp('^Worker').test('worker'),false);
 assert.equal(f.page.get_server_group_regexp('(?i)^Worker').test('worker'),true);
 assert.equal(f.page.get_server_group_regexp('(?i)(?m)^Worker').test('other\nworker'),true);
 assert.equal(f.page.get_server_group_regexp('[[:alpha:]]+'),null);
 assert.equal(f.page.get_server_group_regexp('['),null);
});

test('activity renders current nested records and legacy records without losing dates',()=>{
 const f=fixture('Activity');const rows=[];
 f.page.getPaginatedTable=(response,columns,type,render)=>{for(const row of response.rows)rows.push(render(row));return '';};
 f.page.receive_activity({rows:[
  {action:'user_login',time:1700000000,data:{username:'Mixed.Case'}},
  {action:'job_complete',time:1700000001,data:{job_id:'job1',event_id:'event1',hostname:'worker',code:0}},
  {action:'master_state_update',time:1700000002,data:{enabled:true}},
  {action:'cat_create',epoch:1700000003,cat:{title:'<script>bad</script>'}},
 ]});
 assert.match(rows[0][0],/1700000000/);assert.match(rows[0][2],/Mixed.Case/);
 assert.match(rows[1][2],/#job1/);assert.match(rows[1][2],/worker/);
 assert.match(rows[2][2],/enabled/);
 assert.match(rows[3][2],/&lt;script&gt;/);assert.doesNotMatch(rows[3][2],/<script>/);
});
