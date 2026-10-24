import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {lockedPackages} from '../tools/bun-lock.mjs';

function fixture(text, check) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'owlcron-bun-lock-'));
  try {
    fs.writeFileSync(path.join(directory, 'bun.lock'), text);
    check(directory);
  } finally {
    fs.rmSync(directory, {recursive:true, force:true});
  }
}

test('license lock reader handles JSONC, scoped and nested dependencies', () => {
  fixture(`{
    // Bun text lockfiles allow comments and trailing commas.
    "lockfileVersion": 2,
    "packages": {
      "@scope/root/child": ["child@2.0.0", "", {}, "sha512-example"],
      "@scope/root": ["@scope/root@1.0.0", "", {}, "sha512-example"],
      "parent/@scope/child": ["@scope/child@3.0.0", "", {}, "sha512-example"],
    },
  }`, directory => {
    assert.deepEqual(lockedPackages(directory), [
      {name:'@scope/root',version:'1.0.0',package:'node_modules/@scope/root'},
      {name:'child',version:'2.0.0',package:'node_modules/@scope/root/node_modules/child'},
      {name:'@scope/child',version:'3.0.0',package:'node_modules/parent/node_modules/@scope/child'},
    ]);
  });
});

test('license lock reader fails closed for unsupported or unpinned entries', () => {
  for (const value of [
    {lockfileVersion:99,packages:{}},
    {lockfileVersion:2,packages:{pkg:['pkg@1.0.0','',{},'']}},
    {lockfileVersion:2,packages:{'../pkg':['pkg@1.0.0','',{},'sha512-example']}},
    {lockfileVersion:2,packages:{pkg:['different@1.0.0','',{},'sha512-example']}},
  ]) fixture(JSON.stringify(value), directory => assert.throws(() => lockedPackages(directory)));
});
