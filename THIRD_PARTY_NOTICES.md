# Third-party notices

English | [简体中文](THIRD_PARTY_NOTICES.zh-CN.md)

OwlCron's original code is licensed under MIT (see LICENSE). Third-party code,
fonts and other resources retain their own licenses and copyright notices.
The root MIT license does not replace those terms. Preserve this file and the
licenses directory when distributing source or built releases.

## Cronicle

The browser application, styling, images and frontend build tools derive from
[Joseph Huckaby / PixlCore's Cronicle](https://github.com/jhuckaby/Cronicle).
Existing file-level copyright notices are retained. The upstream MIT notice is
in [licenses/Cronicle.txt](licenses/Cronicle.txt). The upstream license was
checked on 2026-10-24; this does not claim that the local code matches the latest
upstream release. References to Cronicle protocols and compatibility describe
the upstream project and are intentionally retained.

## Frontend dependencies and fonts

Exact installed versions and package-declared licenses are recorded in
[licenses/npm-inventory.json](licenses/npm-inventory.json), with the package's
available license, notice and README files under licenses/npm. bun.lock
pins the dependency tree; these dependencies include build-time tools as well
as browser assets, and not every listed package is shipped to the browser.

Special cases that must not be treated as MIT-only:

- Font Awesome 4.7.0 by Dave Gandy: fonts are SIL OFL 1.1, CSS/LESS/Sass are MIT,
  documentation is CC BY 3.0. See [the upstream policy](https://fontawesome.com/v4/license/),
  licenses/Font-Awesome-font-metadata.txt, licenses/Font-Awesome-OFL.txt and
  licenses/npm/font-awesome@4.7.0/README.md. No endorsement is implied by brand icons.
- Material Design Icons (mdi 1.9.33): fonts are SIL OFL 1.1; package metadata
  declares MIT for code. Copyright Austin Andrews and Google; the upstream
  notice also points to Google's Apache 2.0 icon license. See
  licenses/mdi-LICENSE.txt and licenses/Apache-2.0.txt.
- Lato fonts distributed through pixl-webapp: copyright tyPoland Lukasz Dziedzic,
  reserved font name Lato; SIL OFL 1.1. See licenses/Lato-OFL.txt.
- pixl-webapp's md5.js credits Paul Johnston (1999–2009), Greg Holt, Andrew Kepert,
  Ydnar and Lostinet and uses the BSD license. See licenses/Paul-Johnston-BSD.html.
  The broader pixl-webapp package is MIT; its package-level license does not
  supersede the MD5 file's notice.

Fonts have not been renamed or relicensed. Retain their reserved-name notices.

## Go dependencies

[licenses/go-inventory.json](licenses/go-inventory.json) lists the exact direct
and explicit indirect modules declared in go.mod. Their available LICENSE,
NOTICE and copyright files are copied from the downloaded module sources into
licenses/go. These retain MIT, BSD, Apache and other upstream terms rather than
being relicensed under OwlCron's MIT license. The Go standard library and runtime
remain under the Go project's BSD-style license (licenses/Go-LICENSE).

## Updating dependencies or producing releases

When go.mod, bun.lock or bundled resources change, refresh the relevant
inventory and upstream notices. Include LICENSE, THIRD_PARTY_NOTICES.md and the
licenses directory in release archives and container distributions. Keep
copyright headers when minifying or combining assets. This inventory covers
this repository's current declared Go modules and installed npm dependency tree;
it does not cover optional external plugin executables or services chosen by a
deployer, which require their own notices.

## Browser test tooling

The isolated `tests/browser` development tools are not included in the runtime
image. Exact versions and upstream licenses are recorded in
[licenses/browser-inventory.json](licenses/browser-inventory.json); license and
notice copies are preserved under `licenses/browser`. Chromium installed by
Playwright is a separate test prerequisite and retains its bundled notices.
