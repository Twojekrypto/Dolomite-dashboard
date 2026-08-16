const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const preview = fs.readFileSync(path.join(root, "vedolo-preview.html"), "utf8");
const route = fs.readFileSync(path.join(root, "vedolo", "index.html"), "utf8");

function sectionBetween(source, start, end){
  const startAt = source.indexOf(start);
  assert.notEqual(startAt, -1, `missing ${start}`);
  const endAt = source.indexOf(end, startAt);
  assert.notEqual(endAt, -1, `missing ${end}`);
  return source.slice(startAt, endAt);
}

const activityCard = sectionBetween(preview, '<section class="card position-activity-card"', '<section class="card claimable-card"');
const activityToolbar = sectionBetween(activityCard, '<div class="toolbar">', '<div class="tbl-wrap position-activity-scroll">');
const leftControls = sectionBetween(activityToolbar, '<div class="tb-left">', '<div class="tb-right">');
const rightControls = activityToolbar.slice(activityToolbar.indexOf('<div class="tb-right">'));

assert.match(leftControls, /id="q-position-activity"/);
assert.match(leftControls, /id="dd-position-activity-kind"/);
assert.ok(
  leftControls.indexOf('id="q-position-activity"') < leftControls.indexOf('id="dd-position-activity-kind"'),
  "Position Activity action control must sit beside the search in the left toolbar group",
);
assert.match(rightControls, /id="dd-position-activity-period"/);
assert.match(rightControls, /data-dd="position-activity-period"/);
assert.match(rightControls, /<span class="lbl">7D<\/span>/);
assert.match(rightControls, /data-value="7" data-short="7D"/);

assert.match(preview, /holder:\{q:"",sort:"rank",asc:true,page:1,perPage:10,/);
assert.match(preview, /claimable:\{q:"",sort:"dolo",asc:false,page:1,perPage:10\}/);
assert.match(preview, /activity:\{q:"",kind:"all",period:"7",sort:"date",asc:false,page:1,perPage:10\}/);
assert.match(preview, /filterActivityRows\(state\.activityRows,\{[\s\S]*?startTimestamp:/);

const periodHandler = sectionBetween(preview, 'else if(root.id === "dd-position-activity-period")', '\n      }\n      return;');
assert.match(periodHandler, /state\.activity\.period = value;/);
assert.match(periodHandler, /state\.activity\.page = 1;/);
assert.match(periodHandler, /renderPositionActivity\(\);/);
assert.doesNotMatch(periodHandler, /state\.flows\./, "Position Activity period changes must not reset Flow state");

assert.match(preview, /\.position-activity-card \.toolbar\{display:grid;grid-template-columns:minmax\(0,1fr\) minmax\(96px,auto\);gap:10px/);
assert.match(preview, /\.position-activity-card \.tb-left,\.position-activity-card \.tb-right\{display:contents/);
assert.match(preview, /\.position-activity-card \.search\{grid-column:1\/-1;width:100%/);
assert.match(preview, /\.position-activity-card #dd-position-activity-kind\{grid-column:1;width:auto;min-width:0/);
assert.match(preview, /\.position-activity-card #dd-position-activity-kind \.dd-btn\{min-width:0/);
assert.match(preview, /\.position-activity-card #dd-position-activity-period\{grid-column:2;width:auto/);
assert.match(preview, /\.position-activity-card #dd-position-activity-period \.dd-panel\{left:auto;right:0/);
assert.doesNotMatch(preview, /\.position-activity-card #dd-position-activity-kind,\.position-activity-card #dd-position-activity-period\{width:100%/);
assert.match(route, /position-activity-helper-20260817/);

console.log("veDOLO filter/table UX contracts: 20 passed");
