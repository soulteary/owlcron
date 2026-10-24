// Read the committed Bun text lockfile without depending on an installed parser.
import fs from 'node:fs';
import path from 'node:path';

export function lockedPackages(directory) {
  const lock = Bun.JSON5.parse(fs.readFileSync(path.join(directory, 'bun.lock'), 'utf8'));
  if (lock.lockfileVersion !== 2 || !lock.packages || typeof lock.packages !== 'object') {
    throw new Error('Unsupported Bun lockfile format');
  }
  return Object.entries(lock.packages).map(([key, entry]) => {
    if (!Array.isArray(entry) || typeof entry[0] !== 'string' || typeof entry[3] !== 'string' || !entry[3].startsWith('sha512-')) {
      throw new Error('Expected an integrity-pinned registry package: ' + key);
    }
    const separator = entry[0].lastIndexOf('@');
    const name = entry[0].slice(0, separator);
    const version = entry[0].slice(separator + 1);
    const components = key.split('/');
    const packages = [];
    for (let i = 0; i < components.length; i++) {
      const component = components[i];
      if (!component || component === '.' || component === '..') throw new Error('Invalid package path: ' + key);
      if (component.startsWith('@')) {
        const scopedName = components[++i];
        if (!scopedName || scopedName === '.' || scopedName === '..') throw new Error('Invalid scoped package path: ' + key);
        packages.push(component + '/' + scopedName);
      } else packages.push(component);
    }
    if (!name || !version || packages.at(-1) !== name) throw new Error('Invalid registry package identity: ' + key);
    return {name, version, package:packages.map(item => 'node_modules/' + item).join('/')};
  }).sort((a, b) => a.package < b.package ? -1 : a.package > b.package ? 1 : 0);
}

if (import.meta.main) {
  if (process.argv.length !== 3) throw new Error('Usage: bun tools/bun-lock.mjs PACKAGE_DIRECTORY');
  console.log(JSON.stringify(lockedPackages(process.argv[2])));
}
