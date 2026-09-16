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
  assert.match(yaml, /permissions:[\s\S]*?contents: write[\s\S]*?actions: write/);
  assert.match(yaml, /full_history:/);
  assert.match(yaml, /type: boolean/);
  assert.match(yaml, /cancel-in-progress: true/);
  assert.match(yaml, /timeout-minutes: 55/);
  assert.match(yaml, /ref: master/);
  assert.match(yaml, /sparse-checkout:/);
  assert.match(yaml, /generate_dolo_liquidity\.py/);
  assert.match(yaml, /vedolo_vote_power\.py/);
  assert.match(yaml, /dolo_price\.json/);
  assert.match(yaml, /tests\/fixtures\/dolo-liquidity/);
  assert.match(yaml, /sparse-checkout-cone-mode: false/);
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
  assert.match(yaml, /name: Refresh DOLO flow LP attribution/);
  assert.match(yaml, /github\.event_name == 'workflow_dispatch'/);
  assert.match(yaml, /gh workflow run update-dolo-flows\.yml --ref master -f skip_holders=true/);
  const steps = yaml.split(/\n      - name: /);
  const publish = steps.find(step => step.startsWith('Commit and push validated data'));
  assert.doesNotMatch(publish, /always\(\)|continue-on-error/);
  assert.match(yaml, /actions\/cache\/restore@v4/);
  assert.match(yaml, /actions\/cache\/save@v4/);
  assert.match(yaml, /always\(\) && steps\.lp_scan_cache_restore\.outcome == 'success'/);
  assert.match(yaml, /LP_SCANNER_CACHE_DIR=\$RUNNER_TEMP\/dolo-lp-scanner-cache/);
});

test('Pages deploy waits for successful DOLO liquidity refreshes', () => {
  const yaml = fs.readFileSync(pagesPath, 'utf8');
  assert.match(yaml, /- Update DOLO Liquidity/);
});

test('generation deadline leaves setup and checkpoint-upload headroom inside the job budget', () => {
  const yaml = fs.readFileSync(workflowPath, 'utf8');
  const jobBudget = Number(yaml.match(/^    timeout-minutes: (\d+)$/m)?.[1]);
  const steps = yaml.split(/\n      - name: /);
  const generation = steps.find(step => step.startsWith('Generate DOLO liquidity\n'));
  const generationBudget = Number(generation?.match(/^        timeout-minutes: (\d+)$/m)?.[1]);
  const setupHeadroom = 5;
  const cacheUploadHeadroom = 5;
  assert.ok(Number.isInteger(generationBudget) && generationBudget > 0,
    'Generation needs its own positive deadline so a cold backfill cannot exhaust the job');
  assert.ok(generationBudget + setupHeadroom + cacheUploadHeadroom <= jobBudget,
    'Generation must leave at least five minutes each for setup and checkpoint upload');
  assert.doesNotMatch(generation, /continue-on-error/);
  const save = steps.find(step => step.startsWith('Save LP scanner checkpoints even on failed refresh\n'));
  assert.ok(steps.indexOf(save) > steps.indexOf(generation), 'Checkpoint saving follows generation');
  assert.match(save, /always\(\)/, 'Generation timeout must still attempt checkpoint saving');
});
