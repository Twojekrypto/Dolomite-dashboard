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

  function normalizedAddress(value){
    return String(value || "").trim().toLowerCase();
  }

  function activityKind(row){
    if(row && row.kind) return String(row.kind);
    return row && (row.from || row.to) ? "transfer" : depositKind(row);
  }

  function activityTouchesAddress(row, address){
    const wallet = normalizedAddress(address);
    if(!wallet || !row) return false;
    if(activityKind(row) === "transfer"){
      return normalizedAddress(row.from) === wallet || normalizedAddress(row.to) === wallet;
    }
    return normalizedAddress(row.beneficiaryAddress || row.address) === wallet;
  }

  function timestampBound(value, fallback){
    const number = Number(value);
    return Number.isFinite(number) ? number : fallback;
  }

  function filterActivityRows(rows, options){
    const settings = options || {};
    const wallet = normalizedAddress(settings.address);
    const start = timestampBound(settings.startTimestamp ?? settings.start, -Infinity);
    const end = timestampBound(settings.endTimestamp ?? settings.end, Infinity);
    const selectedKinds = settings.kinds == null
      ? (settings.kind == null ? null : new Set([String(settings.kind)]))
      : new Set((Array.isArray(settings.kinds) ? settings.kinds : [settings.kinds]).map(String));
    return (Array.isArray(rows) ? rows : []).filter(row => {
      const timestamp = timestampBound(row && row.timestamp, 0);
      return (!wallet || activityTouchesAddress(row, wallet))
        && (!selectedKinds || selectedKinds.has(activityKind(row)))
        && timestamp >= start
        && timestamp <= end;
    });
  }

  function positionIdentity(row){
    const kind = activityKind(row);
    const source = row && row.sourceTokenId != null
      ? row.sourceTokenId
      : row && row.tokenId;
    const target = row && row.targetTokenId != null
      ? row.targetTokenId
      : row && row.tokenId;
    return {kind, source:String(source == null ? "" : source), target:String(target == null ? "" : target)};
  }

  function dedupeSemanticRows(rows){
    const seen = new Set();
    return rows.filter(row => {
      const hash = String(row && (row.txHash || row.hash) || "").trim().toLowerCase();
      if(!hash) return true;
      const identity = positionIdentity(row);
      const key = `${hash}:${identity.kind}:${identity.source}:${identity.target}`;
      if(seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  function positionActionLabel(kind){
    return {
      transfer:"Transfer veDOLO",
      merge:"Merge veDOLO positions",
      split:"Split veDOLO position",
      extend:"Extend veDOLO lock",
    }[kind] || "veDOLO position activity";
  }

  function positionTransitionLabel(kind, sourceTokenId, targetTokenId){
    const source = String(sourceTokenId || "");
    const target = String(targetTokenId || "");
    if((kind === "merge" || kind === "split") && source && target){
      return `Position #${source} -> #${target}`;
    }
    const position = target || source;
    return position ? `Position #${position}` : "veDOLO position";
  }

  function buildPortfolioActivityRows(locks, transfers, address, options){
    return dedupeSemanticRows(filterActivityRows(buildActivityRows(locks, transfers), {
      ...(options || {}),
      address,
    })).map(row => {
      const identity = positionIdentity(row);
      return {
        chain:"berachain",
        date:row.date || "",
        timestamp:timestampBound(row.timestamp, 0),
        blockNumber:String(row.blockNumber ?? row.block ?? ""),
        hash:String(row.txHash || row.hash || "").toLowerCase(),
        action:positionActionLabel(identity.kind),
        actionSub:"Position management event; no new DOLO locked.",
        route:identity.kind,
        from:normalizedAddress(row.from),
        to:normalizedAddress(row.to),
        sourceTokenId:identity.source,
        targetTokenId:identity.target,
        tokenId:identity.target,
        vedolo:null,
        lockDays:0,
        paid:0,
        pairedDolo:0,
        paidToken:"",
        price:null,
        principalDelta:0,
        isPositionManagement:true,
      };
    });
  }

  function buildHistoryActivityEvents(locks, transfers, address, options){
    return buildPortfolioActivityRows(locks, transfers, address, options).map(row => {
      const action = `vedolo${row.route.charAt(0).toUpperCase()}${row.route.slice(1)}`;
      return {
        chainKey:"berachain",
        txHash:row.hash,
        timestamp:row.timestamp,
        blockNumber:row.blockNumber,
        action,
        role:"neutral",
        serialId:`${action}:${row.sourceTokenId}:${row.targetTokenId}`,
        label:positionTransitionLabel(row.route, row.sourceTokenId, row.targetTokenId),
        asset:"veDOLO position",
        amount:"0",
        usd:0,
        taxCategory:"vedolo_position_management",
        reviewFlag:"not_applicable",
        reviewReason:"",
        sourceEntity:"vedoloFlowsRpcLogs",
        sourceLabel:"Berachain veDOLO RPC log history",
        principalDelta:0,
        isPositionManagement:true,
        route:row.route,
        from:row.from,
        to:row.to,
        sourceTokenId:row.sourceTokenId,
        targetTokenId:row.targetTokenId,
      };
    });
  }

  return {
    depositKind,
    isExternalLock,
    buildActivityRows,
    activityTouchesAddress,
    filterActivityRows,
    buildPortfolioActivityRows,
    buildHistoryActivityEvents,
  };
});
