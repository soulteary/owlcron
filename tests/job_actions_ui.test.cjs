const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
function fixture(privileges = [], admin = false, email = 'reader@example.test') {
  let page;const errors=[];
  const app={user:{email},hasPrivilege:name=>admin || privileges.includes(name),isAdmin:()=>admin,
    doError(message){errors.push(message);return false;}};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/JobDetails.class.js'),'utf8'),{
    Class:{subclass(base,name,methods){page=methods;}},Page:{Base:{}},app
  });
  return {page,errors};
}

test('read-only users see no mutation actions and direct handlers stop before reading job state',()=>{
  const {page,errors}=fixture();
  assert.equal(page.get_job_actions_html(false,{id:'event'}),'');
  assert.equal(page.get_job_actions_html(true,{id:'event'},false),'');
  for (const action of ['run_again','abort_job','delete_job','toggle_watch']) assert.equal(page[action](),false);
  assert.equal(errors.length,4);
});

test('job actions honor independent run, abort, edit and administrator permissions',()=>{
  for (const [privilege, visible] of [['run_events','Run Again'],['abort_events','Abort Job'],['edit_events','Watch Job']]) {
    const {page}=fixture([privilege]);
    const html=page.get_job_actions_html(false,{id:'event'})+page.get_job_actions_html(true,{id:'event'},false);
    assert.match(html,new RegExp(visible));
    for (const label of ['Run Again','Abort Job','Watch Job','Delete Job']) if(label!==visible) assert.ok(!html.includes(label));
  }
  const {page}=fixture([],true);
  assert.match(page.get_job_actions_html(false,{id:'event'}),/Run Again.*Delete Job/);
  assert.match(page.get_job_actions_html(true,{id:'event'},false),/Abort Job.*Watch Job/);
});

test('rerun stays unavailable for deleted or multiplex events and watching requires an email address',()=>{
  const {page,errors}=fixture(['run_events','edit_events'],false,'');
  assert.equal(page.get_job_actions_html(false,{}),'');
  assert.equal(page.get_job_actions_html(false,{id:'event',multiplex:true}),'');
  assert.equal(page.get_job_actions_html(true,{},false),'');
  assert.equal(page.toggle_watch(),false);
  assert.match(errors[0],/My Account/);
});

test('Home hides abort and flush controls from readers and refuses direct actions',()=>{
  for(const allowed of [false,true]) {
    let page;const errors=[],rows=[];
    const app={hasPrivilege:()=>allowed,activeJobs:{job:{id:'job',pending:true,hostname:'local'}},eventQueue:{event:1},schedule:[{id:'event',title:'Event'}],
      doError(message){errors.push(message);return false;}};
    vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/Home.class.js'),'utf8'),{
      Class:{subclass(base,name,methods){page=methods;}},Page:{Base:{}},app,
      get_inner_window_size:()=>({width:1000}),hash_keys_to_array:Object.keys,commify:String,
      find_object:(items,query)=>items.find(item=>item.id===query.id),$:()=>({html(){return this;},show(){}})
    });
    page.getBasicTable=(items,columns,label,render)=>{for(let i=0;i<items.length;i++)rows.push(render(items[i],i));return '';};
    for(const name of ['getNiceJob','getNiceEvent','getNiceCategory','getNiceGroup','getNicePlugin','getNiceJobElapsedTime','getNiceJobPendingText'])page[name]=()=>'';
    page.get_active_jobs_html();page.refresh_event_queues();
    assert.equal(rows[0].at(-1).includes('Abort Job'),allowed);
    assert.equal(rows[1].at(-1).includes('Flush Queue'),allowed);
    if(!allowed){assert.equal(page.abort_job(0),false);assert.equal(page.flush_event_queue(0),false);assert.equal(errors.length,2);}
  }
});
