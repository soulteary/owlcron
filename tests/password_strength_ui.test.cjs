const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const zxcvbn = require('../htdocs-deps/node_modules/zxcvbn');

function application() {
  const app = {api:{}, extend(properties) { Object.assign(this, properties); }};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../htdocs/js/app.js'), 'utf8'),
    {app, window:{app}, zxcvbn, hash_keys_to_array:Object.keys});
  return app;
}

test('password warning renders the installed zxcvbn slow offline attack estimate', () => {
  const app = application();
  const result = zxcvbn('password');
  assert.equal(typeof result.crack_times_display.offline_slow_hashing_1e4_per_second, 'string');
  app.last_password_strength = result;
  const warning = app.get_password_warning();
  const estimate = result.crack_times_display.offline_slow_hashing_1e4_per_second;
  assert.ok(warning.includes(estimate === 'instant' ? 'instantly' : 'in about ' + estimate));
  assert.ok(!warning.includes('undefined'));
});

test('password warning retains compatibility with the previous result format', () => {
  const app = application();
  app.last_password_strength = {crack_time_display:'3 days'};
  assert.match(app.get_password_warning(), /in about 3 days/);
});
