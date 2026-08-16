(function(root, factory){
  const api = factory();
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root) root.VeDoloPositionActivity = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function(){
  "use strict";

  const DEPOSIT_KINDS = Object.freeze({
    0:"deposit",
    1:"create",
    2:"increase",
    3:"extend",
    4:"merge",
    5:"split",
  });

  function depositKind(row){
    const type = Number(row && row.depositType);
    return Object.prototype.hasOwnProperty.call(DEPOSIT_KINDS, type)
      ? DEPOSIT_KINDS[type]
      : "unknown";
  }

  function isExternalLock(row){
    const kind = depositKind(row);
    const amount = Number(row && row.dolo);
    return (kind === "deposit" || kind === "create" || kind === "increase")
      && Number.isFinite(amount)
      && amount > 0;
  }

  function buildActivityRows(locks, transfers){
    const depositRows = (Array.isArray(locks) ? locks : []).flatMap(lock => {
      const kind = depositKind(lock);
      if(kind !== "extend" && kind !== "merge" && kind !== "split") return [];
      const tokenId = Number(lock && lock.tokenId) || 0;
      return [{
        ...lock,
        kind,
        address:String(lock && (lock.beneficiaryAddress || lock.address) || "").toLowerCase(),
        sourceTokenId:Number(lock && lock.sourceTokenId) || (kind === "extend" ? tokenId : 0),
        targetTokenId:Number(lock && lock.targetTokenId) || tokenId,
        timestamp:Number(lock && lock.timestamp) || 0,
      }];
    });

    const transferRows = (Array.isArray(transfers) ? transfers : []).map(transfer => ({
      ...transfer,
      kind:"transfer",
      address:String(transfer && transfer.to || "").toLowerCase(),
      from:String(transfer && transfer.from || "").toLowerCase(),
      to:String(transfer && transfer.to || "").toLowerCase(),
      sourceTokenId:Number(transfer && transfer.tokenId) || 0,
      targetTokenId:Number(transfer && transfer.tokenId) || 0,
      timestamp:Number(transfer && transfer.timestamp) || 0,
      dolo:null,
      locktime:null,
    }));

    return depositRows.concat(transferRows).sort((a,b) =>
      (b.timestamp - a.timestamp)
      || ((Number(b.block) || 0) - (Number(a.block) || 0))
      || String(a.txHash || "").localeCompare(String(b.txHash || ""))
    );
  }

  return {depositKind, isExternalLock, buildActivityRows};
});
