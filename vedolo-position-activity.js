(function(root, factory){
  const api = factory();
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root){
    root.VeDoloPositionActivity = api;
    api.ensureSharedTableUx(root);
  }
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
  const ACTIVITY_KINDS = new Set(["create", "extend", "merge", "split"]);
  const ROUTE_LABELS = Object.freeze({
    all:"All Routes",
    create:"New Lock",
    extend:"Extend",
    merge:"Merge",
    split:"Split",
    transfer:"Transfer",
  });

  function ensureSharedTableUx(root){
    const doc = root && root.document;
    if(!doc) return;
    const version = "20260820-table-ux-v1";
    if(!doc.querySelector('link[href*="wallet-table-ux.css"]')){
      const link = doc.createElement("link");
      link.rel = "stylesheet";
      link.href = `wallet-table-ux.css?v=${version}`;
      link.dataset.doloTableUxVersion = version;
      doc.head.appendChild(link);
    }
    if(!doc.querySelector('script[src*="wallet-table-ux.js"]')){
      const script = doc.createElement("script");
      script.src = `wallet-table-ux.js?v=${version}`;
      script.async = false;
      script.dataset.doloTableUxVersion = version;
      doc.head.appendChild(script);
    }
  }

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
      if(!ACTIVITY_KINDS.has(kind)) return [];
      const tokenId = Number(lock && lock.tokenId) || 0;
      return [{
        ...lock,
        kind,
        isNewLock:kind === "create",
        address:String(lock && (lock.beneficiaryAddress || lock.address) || "").toLowerCase(),
        sourceTokenId:Number(lock && lock.sourceTokenId) || (kind === "extend" ? tokenId : 0),
        targetTokenId:Number(lock && lock.targetTokenId) || tokenId,
        timestamp:Number(lock && lock.timestamp) || 0,
      }];
    });

    const transferRows = (Array.isArray(transfers) ? transfers : []).map(transfer => ({
      ...transfer,
      kind:"transfer",
      isNewLock:false,
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

  function routeLabel(kind){
    return ROUTE_LABELS[String(kind || "")] || "Position Activity";
  }

  function positionActionLabel(kind){
    return {
      create:"New veDOLO lock",
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
    return position ? `${kind === "create" ? "New lock" : "Position"} #${position}` : "veDOLO position";
  }

  function routeIconSvg(kind, direction){
    const rawKey = String(kind || "");
    const key = ({odolo:"create", pair:"create", airdrop:"create", direct:"create"})[rawKey] || rawKey;
    const icons = {
      all:'<path d="M5 5h5a3 3 0 0 1 3 3v8a3 3 0 0 0 3 3h3"/><path d="m16 16 3 3-3 3"/><circle cx="5" cy="5" r="2"/>',
      create:'<rect x="5" y="10" width="14" height="11" rx="2"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/><path d="M12 13v5"/><path d="M9.5 15.5h5"/>',
      extend:'<circle cx="12" cy="12" r="8"/><path d="M12 8v5l3 2"/><path d="M18.5 5.5 20 4v4h-4"/>',
      merge:'<path d="M5 5h3a4 4 0 0 1 4 4v10"/><path d="M19 5h-3a4 4 0 0 0-4 4"/><path d="m9 16 3 3 3-3"/>',
      split:'<path d="M12 5v5a4 4 0 0 1-4 4H5"/><path d="M12 10a4 4 0 0 0 4 4h3"/><path d="m7 11-3 3 3 3"/><path d="m17 11 3 3-3 3"/>',
      transfer:'<path d="M5 8h13"/><path d="m15 5 3 3-3 3"/><path d="M19 16H6"/><path d="m9 13-3 3 3 3"/>',
    };
    let body = icons[key] || icons.all;
    if(key === "transfer" && direction === "in") body = '<path d="M19 12H6"/><path d="m9 9-3 3 3 3"/>';
    if(key === "transfer" && direction === "out") body = '<path d="M5 12h13"/><path d="m15 9 3 3-3 3"/>';
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${body}</svg>`;
  }

  function routeIconHtml(kind, active, className){
    const classes = [className || "route-icon", active ? "active" : ""].filter(Boolean).join(" ");
    return `<span class="${classes}" aria-hidden="true">${routeIconSvg(kind)}</span>`;
  }

  function transferDirectionIconHtml(direction, className){
    const classes = [className || "route-icon", `direction-${String(direction || "")}`].filter(Boolean).join(" ");
    return `<span class="${classes}" aria-hidden="true">${routeIconSvg("transfer", direction)}</span>`;
  }

  function buildPortfolioActivityRows(locks, transfers, address, options){
    return dedupeSemanticRows(filterActivityRows(buildActivityRows(locks, transfers), {
      ...(options || {}),
      address,
    })).filter(row => activityKind(row) !== "create").map(row => {
      const identity = positionIdentity(row);
      const isNewLock = false;
      const principal = 0;
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
        isNewLock:false,
        locktime:Number(row.locktime) || 0,
      };
    });
  }

  function buildHistoryActivityEvents(locks, transfers, address, options){
    return buildPortfolioActivityRows(locks, transfers, address, options)
      .map(row => {
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
    routeLabel,
    routeIconSvg,
    routeIconHtml,
    transferDirectionIconHtml,
    ensureSharedTableUx,
  };
});
