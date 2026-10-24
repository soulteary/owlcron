const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
function fixture(privileges=[]) {
  let page;const errors=[],navigation=[];
  const app={hasPrivilege:name=>privileges.includes(name),isAdmin:()=>false,doError(message){errors.push(message);return false;}};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/Schedule.class.js'),'utf8'),{
    Class:{subclass(base,name,methods){page=methods;}},Page:{Base:{}},
    app,
    setTimeout(){},get_inner_window_size:()=>({width:1000}),escape_text_field_value:value=>String(value || ''),
    render_menu_options:()=>'',copy_object:object=>({...object}),find_object:(items,query)=>items.find(item=>item.id===query.id),summarize_event_timing:()=>'',
    config:{},Nav:{go(route){navigation.push(route);}}
  });
  return {page,errors,navigation,app};
}

test('readers retain schedule history and status without event mutation controls',()=>{
  const {page}=fixture();
  const html=page.get_event_row_actions({id:'event'},0);
  assert.match(html,/Stats/);assert.match(html,/History/);
  assert.doesNotMatch(html,/>Run<|>Edit</);
  assert.equal(page.get_event_enabled_control({enabled:true},0),'Enabled');
  assert.equal(page.get_event_enabled_control({enabled:false},0),'Disabled');
});

test('run and edit controls follow their independent privileges',()=>{
  const runner=fixture(['run_events']).page;
  assert.match(runner.get_event_row_actions({id:'event'},0),/>Run</);
  assert.doesNotMatch(runner.get_event_row_actions({id:'event'},0),/>Edit</);
  assert.doesNotMatch(runner.get_event_enabled_control({enabled:true},0),/input/);
  const editor=fixture(['edit_events']).page;
  assert.match(editor.get_event_row_actions({id:'event'},0),/>Edit</);
  assert.doesNotMatch(editor.get_event_row_actions({id:'event'},0),/>Run</);
  assert.match(editor.get_event_enabled_control({enabled:true},0),/checkbox.*checked/);
});

test('direct schedule mutation handlers stop before touching form or event state',()=>{
  const {page,errors}=fixture();
  for(const method of ['change_event_enabled','run_event','run_event_now','delete_event','do_new_event','do_copy_event','run_event_from_edit','do_save_event','show_quick_add_cat_dialog']) {
    assert.equal(page[method](),false,method);
  }
  assert.equal(page.edit_event(0),false);assert.equal(page.edit_event(-1),false);
  assert.equal(errors.length,11);
});

test('direct new/edit routes return readers to the schedule',()=>{
  const {page,navigation}=fixture();
  assert.equal(page.gosub_new_event({}),false);
  assert.equal(page.gosub_edit_event({id:'event'}),false);
  assert.deepEqual(navigation,['Schedule','Schedule']);
});


test('real schedule table rendering uses permission-aware actions for both readers and editors',()=>{
  for(const privileges of [[],['run_events','edit_events','create_events']]) {
    const {page,app}=fixture(privileges);const rows=[];let output;
    Object.assign(app,{categories:[],plugins:[],server_groups:[],schedule:[{id:'event',title:'Scheduled Event',enabled:true}],state:{},setWindowTitle(){},getPref(){return '';}});
    page.div={removeClass(){},html(html){output=html;}};
    page.render_target_menu_options=()=>'';
    page.getBasicTable=(events,columns,label,render)=>{for(let idx=0;idx<events.length;idx++)rows.push(render(events[idx],idx));return 'rendered-event-table';};
    for(const method of ['getNiceEvent','getNiceCategory','getNicePlugin','getNiceGroup'])page[method]=()=>'';
    page.update_job_last_runs=()=>{};
    page.update_live_job_counts=()=>{};
    page.gosub_events({});
    assert.match(output,/rendered-event-table/);
    assert.equal(rows.length,1);
    assert.match(rows[0][7],/Stats/);
    assert.match(rows[0][7],/History/);
    assert.equal(rows[0][7].includes('>Run<'),privileges.includes('run_events'));
    assert.equal(rows[0][7].includes('>Edit<'),privileges.includes('edit_events'));
    assert.equal(rows[0][0].includes('checkbox'),privileges.includes('edit_events'));
  }
});
