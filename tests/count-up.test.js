"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const source = fs.readFileSync(path.resolve(__dirname, "../count-up.js"), "utf8");

function countUpHarness(){
  let now = 1000;
  let nextFrame = 1;
  const frames = new Map();
  const sandbox = {
    console,
    document:{ hidden:false, getElementById(){ return null; } },
    performance:{ now(){ return now; } },
    requestAnimationFrame(callback){
      const id = nextFrame++;
      frames.set(id, callback);
      return id;
    },
    cancelAnimationFrame(id){ frames.delete(id); },
    matchMedia(){ return {matches:false}; },
  };
  sandbox.window = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(source, sandbox);
  return {
    metric:sandbox.CountUpMetric,
    setNow(value){ now = value; },
    runNext(timestamp){
      const entry = frames.entries().next().value;
      assert.ok(entry, "an animation frame should be queued");
      frames.delete(entry[0]);
      entry[1](timestamp);
    },
  };
}

function element(){
  return {textContent:"", dataset:{}};
}

test("count-up never renders a negative overshoot when the first frame timestamp predates the start timestamp", () => {
  const harness = countUpHarness();
  const target = element();
  harness.metric.text(target, "4,071", {duration:760});

  harness.runNext(999);

  assert.doesNotMatch(target.textContent, /^-/);
  assert.ok(Number(target.textContent.replace(/,/g, "")) >= 0);
});

test("an interrupted count-up resumes from the visible value instead of jumping back to zero", () => {
  const harness = countUpHarness();
  const target = element();
  harness.metric.text(target, "4,071", {duration:760});
  harness.runNext(1200);
  const visible = target.textContent;
  assert.notEqual(visible, "0");

  harness.setNow(1200);
  harness.metric.text(target, "4,200", {duration:760});

  assert.equal(target.textContent, visible);
});
