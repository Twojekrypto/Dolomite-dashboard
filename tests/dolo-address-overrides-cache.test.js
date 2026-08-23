"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const loader = fs.readFileSync(path.join(__dirname, "..", "route-loader.js"), "utf8");
const doloRoute = fs.readFileSync(path.join(__dirname, "..", "dolo", "index.html"), "utf8");

test("route loader cache-busts the behavioral address override independently", async () => {
  const document = {
    body: {},
    open() {},
    close() {},
    querySelector() { return null; },
    write(html) { this.html = html; },
  };
  const context = {
    document,
    fetch: async () => ({
      ok: true,
      text: async () => "<html><head></head><body>preview</body></html>",
    }),
    setTimeout() {},
    console,
  };
  context.window = context;

  vm.runInNewContext(loader, context);
  await context.window.loadDoloRoute({label: "DOLO", target: "dolo-preview.html"});

  assert.match(
    document.html,
    /dolo-address-overrides\.js\?v=20260823-address-type-normalization-v1/,
  );
});

test("DOLO route requests the address-type normalization release", () => {
  assert.match(
    doloRoute,
    /route-loader\.js\?v=route-loader-table-ux-20260820&release=address-type-normalization-20260823/,
  );
  assert.match(doloRoute, /address-type-normalization-20260823/);
});
