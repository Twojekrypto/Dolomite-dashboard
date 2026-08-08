const assert = require('node:assert/strict');
const test = require('node:test');

let summaryModule = {};
try {
  summaryModule = require('../odolo-exercise-summary.js');
} catch(error) {
  if(error.code !== 'MODULE_NOT_FOUND') throw error;
}
const { summarizeLatestOdoloExercises } = summaryModule;

test('summarizes volume and normalizes duplicate wallet casing', () => {
  assert.equal(typeof summarizeLatestOdoloExercises, 'function');
  const result = summarizeLatestOdoloExercises([
    {addr:'0xAbC', vedolo:100, usdc:4, lockDays:30},
    {addr:'0xabc', vedolo:300, usdc:18, lockDays:90},
  ]);

  assert.deepEqual(result, {
    exercises:2,
    uniqueWallets:1,
    vedoloReceived:400,
    usdcPaid:22,
    avgExercisePrice:0.055,
    avgLockDays:75,
  });
});

test('ignores malformed amounts and returns null averages without a valid denominator', () => {
  assert.equal(typeof summarizeLatestOdoloExercises, 'function');
  const result = summarizeLatestOdoloExercises([
    {addr:'0x1', vedolo:'bad', usdc:-5, lockDays:Infinity},
    {addr:'', vedolo:0, usdc:0, lockDays:0},
  ]);

  assert.equal(result.exercises, 2);
  assert.equal(result.uniqueWallets, 1);
  assert.equal(result.vedoloReceived, 0);
  assert.equal(result.usdcPaid, 0);
  assert.equal(result.avgExercisePrice, null);
  assert.equal(result.avgLockDays, null);
});
