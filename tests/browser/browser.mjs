import {chromium, firefox, webkit} from 'playwright';

export const browserName = process.env.OWLCRON_BROWSER || 'chromium';
const browsers = {chromium, firefox, webkit};
if (!Object.hasOwn(browsers, browserName)) {
  throw new Error('OWLCRON_BROWSER must be chromium, firefox or webkit');
}
if (process.env.OWLCRON_BROWSER_EXECUTABLE && browserName !== 'chromium') {
  throw new Error('OWLCRON_BROWSER_EXECUTABLE is only supported for Chromium');
}

export function launchBrowser() {
  return browsers[browserName].launch({headless:true,
    // Fixtures bind loopback only; do not inherit the host's Firefox proxy.
    ...(browserName === 'firefox' ? {firefoxUserPrefs:{'network.proxy.type':0}} : {}),
    ...(process.env.OWLCRON_BROWSER_EXECUTABLE ? {executablePath:process.env.OWLCRON_BROWSER_EXECUTABLE} : {})});
}
