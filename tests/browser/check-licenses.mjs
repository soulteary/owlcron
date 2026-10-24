import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {lockedPackages} from '../../tools/bun-lock.mjs';
const directory = path.dirname(fileURLToPath(import.meta.url));
const destination = path.resolve(directory, '../../licenses/browser');
const packages = lockedPackages(directory);
const rows = [];
const copies = new Map();
for (const entry of packages) {
  const name = entry.package;
  const source = path.join(directory, name);
  const metadata = JSON.parse(fs.readFileSync(path.join(source, 'package.json')));
  assert.equal(metadata.version, entry.version, name);
  assert.equal(metadata.name, entry.name, name);
  const files = fs.readdirSync(source).filter(file => /^(license|notice|copying)(\.|$)/i.test(file)).sort();
  assert.ok(files.length, 'Missing license for ' + name);
  const folder = metadata.name.replaceAll('/', '__') + '@' + metadata.version;
  for (const file of files) copies.set(path.join(folder,file), fs.readFileSync(path.join(source,file)));
  rows.push({name:metadata.name,version:metadata.version,license:metadata.license,notices:'licenses/browser/' + folder,files});
}
const inventory = path.resolve(directory, '../../licenses/browser-inventory.json');
if (process.argv.includes('--write')) {
  fs.rmSync(destination,{recursive:true,force:true});
  for (const [name,data] of copies) { fs.mkdirSync(path.dirname(path.join(destination,name)),{recursive:true}); fs.writeFileSync(path.join(destination,name),data); }
  fs.writeFileSync(inventory, JSON.stringify(rows,null,2) + '\n');
} else {
  assert.deepEqual(JSON.parse(fs.readFileSync(inventory)), rows, 'Browser license inventory differs');
  for (const [name,data] of copies) assert.ok(fs.readFileSync(path.join(destination,name)).equals(data), name);
  const actual = fs.readdirSync(destination,{recursive:true}).filter(name => fs.statSync(path.join(destination,name)).isFile()).sort();
  assert.deepEqual(actual, [...copies.keys()].sort(), 'Missing or obsolete browser notices');
}
console.log(`Browser license inventory verified: ${rows.length} development packages`);
