import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const preview = fs.readFileSync("odolo-preview.html", "utf8");

function extractNamedFunctionSource(name) {
  const marker = `function ${name}(`;
  const start = preview.indexOf(marker);
  assert.notEqual(start, -1, `${name} must exist`);
  const bodyStart = preview.indexOf("{", start);
  let depth = 0;
  let quote = null;
  let escaped = false;
  for(let index = bodyStart; index < preview.length; index += 1){
    const char = preview[index];
    if(quote){
      if(escaped) escaped = false;
      else if(char === "\\") escaped = true;
      else if(char === quote) quote = null;
      continue;
    }
    if(char === '"' || char === "'" || char === "`"){
      quote = char;
      continue;
    }
    if(char === "{") depth += 1;
    if(char === "}"){
      depth -= 1;
      if(depth === 0) return preview.slice(start, index + 1);
    }
  }
  assert.fail(`${name} has no closing brace`);
}

test("empty latest-activity tables render one compact message row", () => {
  const helper = Function(
    `"use strict"; ${extractNamedFunctionSource("latestActivityEmptyRowHtml")}; return latestActivityEmptyRowHtml;`,
  )();
  const html = helper("No activity found.", 6);

  assert.equal((html.match(/<tr\b/g) || []).length, 1);
  assert.match(html, /colspan="6"/);
  assert.match(html, /No activity found\./);
  assert.doesNotMatch(html, /tbl-spacer-row/);
});

test("both latest activity renderers use the compact empty row", () => {
  const exercisesStart = preview.indexOf("function renderLatestExercises(){");
  const pairsStart = preview.indexOf("function renderLatestPairs(){");
  const exercises = preview.slice(exercisesStart, preview.indexOf("function getLatestPairRows", exercisesStart));
  const pairs = preview.slice(pairsStart, preview.indexOf("function bindTableSort", pairsStart));

  assert.match(exercises, /latestActivityEmptyRowHtml\(emptyLabel, 6\)/);
  assert.match(pairs, /latestActivityEmptyRowHtml\(emptyLabel, 4\)/);
  assert.doesNotMatch(exercises, /emptyLabel}[\s\S]*stableTableSpacerRowsHtml\(state\.latestPageSize - 1/);
  assert.doesNotMatch(pairs, /emptyLabel}[\s\S]*stableTableSpacerRowsHtml\(state\.pairPageSize - 1/);
});
