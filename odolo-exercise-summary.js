(function(root){
  function nonNegative(value){
    const number = Number(value);
    return Number.isFinite(number) && number >= 0 ? number : 0;
  }

  function summarizeLatestOdoloExercises(rows){
    const source = Array.isArray(rows) ? rows : [];
    const wallets = new Set();
    let vedoloReceived = 0;
    let usdcPaid = 0;
    let lockWeightedTotal = 0;
    let lockWeight = 0;

    source.forEach(row => {
      const address = String(row?.addr || '').trim().toLowerCase();
      if(address) wallets.add(address);

      const vedolo = nonNegative(row?.vedolo);
      const usdc = nonNegative(row?.usdc);
      vedoloReceived += vedolo;
      usdcPaid += usdc;

      const lockDays = Number(row?.lockDays);
      if(vedolo > 0 && Number.isFinite(lockDays) && lockDays >= 0){
        lockWeightedTotal += vedolo * lockDays;
        lockWeight += vedolo;
      }
    });

    return {
      exercises:source.length,
      uniqueWallets:wallets.size,
      vedoloReceived,
      usdcPaid,
      avgExercisePrice:vedoloReceived > 0 ? usdcPaid / vedoloReceived : null,
      avgLockDays:lockWeight > 0 ? lockWeightedTotal / lockWeight : null,
    };
  }

  root.summarizeLatestOdoloExercises = summarizeLatestOdoloExercises;
  if(typeof module !== 'undefined' && module.exports){
    module.exports = {summarizeLatestOdoloExercises};
  }
})(typeof window !== 'undefined' ? window : globalThis);
