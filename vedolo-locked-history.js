(function(root, factory){
  const api = factory();
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root) root.VeDoloLockedHistory = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function(){
  "use strict";

  function activeLockedDoloTotal(holders, snapshotSec){
    if(!Array.isArray(holders) || !Number.isFinite(snapshotSec) || snapshotSec <= 0) return null;
    let total = 0;
    let positionCount = 0;
    holders.forEach(holder => {
      if(!Array.isArray(holder?.token_details)) return;
      holder.token_details.forEach(position => {
        const amount = Number(position?.dolo);
        const end = Number(position?.end);
        if(!Number.isFinite(amount) || amount < 0 || !Number.isFinite(end)) return;
        positionCount += 1;
        if(end > snapshotSec) total += amount;
      });
    });
    return positionCount ? total : null;
  }

  return {activeLockedDoloTotal};
});
