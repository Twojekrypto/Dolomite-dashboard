const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const workflowPath = path.join(root, '.github/workflows/update-dolo-liquidity.yml');
const pagesPath = path.join(root, '.github/workflows/pages.yml');

test('six-hour DOLO liquidity workflow is fail-closed and publishes only validated artifacts', () => {
  const yaml = fs.readFileSync(workflowPath, 'utf8');
  assert.match(yaml, /^name: Update DOLO Liquidity$/m);
  assert.match(yaml, /cron: ['"]17 \*\/6 \* \* \*['"]/);
  assert.match(yaml, /workflow_dispatch:/);
  assert.match(yaml, /full_history:/);
  assert.match(yaml, /type: boolean/);
  assert.match(yaml, /cancel-in-progress: true/);
  assert.match(yaml, /timeout-minutes: 55/);
  assert.match(yaml, /ref: master/);
  assert.match(yaml, /python-version: ['"]3\.11['"]/);
  assert.match(yaml, /pip install -r requirements\.txt/);
  assert.match(yaml, /tests\.test_generate_dolo_liquidity tests\.test_validate_dolo_liquidity/);
  assert.match(yaml, /ALCHEMY_ETHEREUM_RPC:/);
  assert.match(yaml, /DRPC_ETHEREUM_RPC_2_JEFF: \$\{\{ secrets\.DRPC_ETHEREUM_RPC_2_JEFF \}\}/);
  assert.match(yaml, /ALCHEMY_BERACHAIN_RPC:/);
  assert.match(yaml, /generate_dolo_liquidity\.py --registry data\/dolo-liquidity-pools\.json --output data\/dolo-liquidity\.json/);
  assert.match(yaml, /--full-history/);
  assert.match(yaml, /validate_data\.py data\/dolo-liquidity\.json/);
  assert.match(yaml, /git add data\/dolo-liquidity\.json data\/dolo-liquidity-pools\.json/);
  assert.match(yaml, /for i in 1 2 3/);
  assert.match(yaml, /if \[ "\$pushed" != "true" \]/);
  assert.doesNotMatch(yaml, /if: always\(\)[\s\S]*?git add/);
});

test('Pages deploy waits for successful DOLO liquidity refreshes', () => {
  const yaml = fs.readFileSync(pagesPath, 'utf8');
  assert.match(yaml, /- Update DOLO Liquidity/);
});
