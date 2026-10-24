const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function fixture(event = {}) {
 let methods, inserted = false, reads = 0;
 const errors = [];
 const fields = {'#fe_ee_title':'Backup', '#fe_ee_cat':'general', '#fe_ee_plugin':'shell', '#fe_ee_log_expire_days':''};
 const context = {
  Class:{subclass(base, name, defs){methods = defs;}}, Page:{Base:{}},
  app:{categories:[], plugins:[], server_groups:[], schedule:[], zones:[], isAdmin(){return true;}, badField(id, message){errors.push({id,message}); return false;}},
  $(selector){reads++; return {val(){return inserted ? (fields[selector] ?? '0') : undefined;}, is(){return false;}};},
  get_form_table_row(...args){return args.at(-1);}, get_form_table_caption(){return '';}, get_form_table_spacer(){return '';},
  escape_text_field_value(value){return String(value ?? '');}, render_menu_options(){return '';},
  find_object(){return {params:[]};}, normalize_time(value){return value;}, time_now(){return 1;}, trim(value){return value.trim();}, setTimeout(){},
  $P(){return {rc_get_short_date_time(){return 'now';}};}
 };
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../htdocs/js/pages/Schedule.class.js'),'utf8'), context);
 const page = Object.assign({}, methods, {event, render_target_menu_options(){return '';}, get_plugin_params_html(){return '';}, get_timing_params_html(){return '';}, get_relative_time_combo_box(){return '';}, get_relative_size_combo_box(){return '';}});
 return {page, fields, errors, insert(){inserted = true;}, get reads(){return reads;}};
}

test('new and existing event forms render retention without reading unmounted inputs', () => {
 for (const event of [{title:'New'}, {id:'backup',title:'Backup',log_expire_days:7}]) {
  const f = fixture(event);
  const html = f.page.get_event_edit_html();
  assert.match(html, /id="fe_ee_log_expire_days"/);
  assert.match(html, new RegExp('value="' + (event.log_expire_days ?? '') + '" placeholder="Inherit global policy"'));
  assert.equal(f.reads, 0);
  assert.equal(event.log_expire_days, event.id ? 7 : undefined);
 }
});

test('submission reads the current retention value, including inheritance and zero', () => {
 const f = fixture({log_expire_days:7}); f.insert();
 for (const [input, expected] of [['',null], ['  ',null], ['0',0], ['30',30], ['36500',36500]]) {
  f.fields['#fe_ee_log_expire_days'] = input;
  assert.equal(f.page.get_event_form_json().log_expire_days, expected);
 }
 assert.equal(f.errors.length, 0);
});

test('invalid retention stops submission and quiet autosave without a field alert', () => {
 const f = fixture(); f.insert();
 for (const input of ['-1','36501','1.5','abc','Infinity']) {
  f.fields['#fe_ee_log_expire_days'] = input;
  assert.equal(f.page.get_event_form_json(true), false);
  assert.equal(f.errors.length, 0);
 }
 assert.equal(f.page.get_event_form_json(), false);
 assert.equal(f.errors[0].id, 'fe_ee_log_expire_days');
});
