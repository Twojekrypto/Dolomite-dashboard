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

  const DAY_SECONDS = 86400;

  function finiteNumber(value, fallback=0){
    const number = Number(value);
    return Number.isFinite(number) ? number : fallback;
  }

  function tokenIdOf(row, key="tokenId"){
    const tokenId = Number(row && row[key]);
    return Number.isSafeInteger(tokenId) && tokenId > 0 ? tokenId : 0;
  }

  function applyLockedEvent(positions, event){
    const row = event.row;
    const tokenId = tokenIdOf(row);
    if(!tokenId) return;
    if(event.kind === "unlock"){
      if(!positions.has(tokenId)) throw new Error(`unlock source position #${tokenId} is missing`);
      positions.delete(tokenId);
      return;
    }

    const depositType = Number(row.depositType);
    const amount = finiteNumber(row.dolo);
    const locktime = finiteNumber(row.locktime);
    const current = positions.get(tokenId);
    if(depositType === 0 || depositType === 2){
      if(!current) throw new Error(`increase source position #${tokenId} is missing`);
      positions.set(tokenId, {
        amount:current.amount + Math.max(0, amount),
        end:locktime || current.end,
      });
      return;
    }
    if(depositType === 1){
      if(current) throw new Error(`create target position #${tokenId} already exists`);
      positions.set(tokenId, {amount:Math.max(0, amount), end:locktime});
      return;
    }
    if(depositType === 3){
      if(!current) throw new Error(`extend source position #${tokenId} is missing`);
      positions.set(tokenId, {amount:current.amount, end:locktime || current.end});
      return;
    }
    if(depositType === 4){
      const sourceTokenId = tokenIdOf(row, "sourceTokenId");
      const targetTokenId = tokenIdOf(row, "targetTokenId");
      if(!sourceTokenId || targetTokenId !== tokenId || sourceTokenId === targetTokenId){
        throw new Error(`merge transition is incomplete for position #${tokenId}`);
      }
      const source = positions.get(sourceTokenId);
      const target = positions.get(targetTokenId);
      if(!source) throw new Error(`merge source position #${sourceTokenId} is missing`);
      if(!target) throw new Error(`merge target position #${targetTokenId} is missing`);
      positions.delete(sourceTokenId);
      positions.set(targetTokenId, {
        amount:target.amount + source.amount,
        end:Math.max(target.end || 0, source.end || 0, locktime || 0),
      });
      return;
    }
    if(depositType === 5){
      const sourceTokenId = tokenIdOf(row, "sourceTokenId");
      const targetTokenId = tokenIdOf(row, "targetTokenId");
      if(!sourceTokenId || targetTokenId !== tokenId || sourceTokenId === targetTokenId){
        throw new Error(`split transition is incomplete for position #${tokenId}`);
      }
      const source = positions.get(sourceTokenId);
      if(!source) throw new Error(`split source position #${sourceTokenId} is missing`);
      if(positions.has(targetTokenId)) throw new Error(`split target position #${targetTokenId} already exists`);
      // Published display values use four decimals, so an on-chain dust split
      // can round to 0.0000 DOLO while still creating a valid target NFT.
      if(amount < 0 || source.amount + 1e-6 < amount){
        throw new Error(`split amount does not reconcile for position #${sourceTokenId}`);
      }
      positions.set(sourceTokenId, {amount:Math.max(0, source.amount - amount), end:source.end});
      positions.set(targetTokenId, {amount, end:locktime || source.end});
    }
  }

  function buildActiveLockedHistory(locks, unlocks, snapshotSec, currentLocked){
    const snapshot = finiteNumber(snapshotSec);
    if(snapshot <= 0) return [];
    const events = [];
    (Array.isArray(locks) ? locks : []).forEach(row => {
      const timestamp = finiteNumber(row?.timestamp);
      if(timestamp > 0 && timestamp <= snapshot) events.push({kind:"lock", row, timestamp});
    });
    (Array.isArray(unlocks) ? unlocks : []).forEach(row => {
      const timestamp = finiteNumber(row?.timestamp);
      if(timestamp > 0 && timestamp <= snapshot) events.push({kind:"unlock", row, timestamp});
    });
    events.sort((a,b) =>
      (a.timestamp - b.timestamp)
      || (finiteNumber(a.row?.block) - finiteNumber(b.row?.block))
      || (a.kind === "lock" ? -1 : 1)
    );
    if(!events.length) return [];

    const firstDay = Math.floor(events[0].timestamp / DAY_SECONDS) * DAY_SECONDS;
    const snapshotDay = Math.floor(snapshot / DAY_SECONDS) * DAY_SECONDS;
    const positions = new Map();
    const points = [];
    let eventIndex = 0;
    for(let day = firstDay; day <= snapshotDay; day += DAY_SECONDS){
      const cutoff = day === snapshotDay ? snapshot : day + DAY_SECONDS - 1;
      while(eventIndex < events.length && events[eventIndex].timestamp <= cutoff){
        applyLockedEvent(positions, events[eventIndex]);
        eventIndex += 1;
      }
      let active = 0;
      positions.forEach(position => {
        if(position.end > cutoff) active += position.amount;
      });
      points.push([day, active]);
    }

    const endpoint = Number(currentLocked);
    if(currentLocked !== null && currentLocked !== undefined && Number.isFinite(endpoint) && endpoint >= 0){
      points[points.length - 1][1] = endpoint;
    }
    return points;
  }

  return {activeLockedDoloTotal, buildActiveLockedHistory};
});
