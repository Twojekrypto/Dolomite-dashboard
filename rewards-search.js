(function(root, factory){
  const api = factory();
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root) root.RewardsProgramSearch = api;
})(typeof window !== "undefined" ? window : globalThis, function(){
  function normalizeSearch(value){
    return String(value || "").trim().toLowerCase().replace(/\s+/g, " ");
  }

  function programSearchText(program, chainLabels = {}){
    const row = program || {};
    const visibleName = String(row.name || "").replace(/^lend\s+/i, "Supply ");
    return normalizeSearch([
      row.name,
      visibleName,
      row.provider,
      row.action,
      row.chain,
      chainLabels[row.chain],
      ...(Array.isArray(row.marketTokens) ? row.marketTokens : []),
      ...(Array.isArray(row.rewardTokens) ? row.rewardTokens : []),
      ...(Array.isArray(row.rewardTokenTotals) ? row.rewardTokenTotals.map(item => item?.symbol) : []),
    ].filter(Boolean).join(" "));
  }

  function filterPrograms(programs, status, query, chainLabels = {}){
    const rows = (Array.isArray(programs) ? programs : []).filter(program => (
      status === "LIVE" ? program?.status === "LIVE" : program?.status !== "LIVE"
    ));
    const terms = normalizeSearch(query).split(" ").filter(Boolean);
    if(!terms.length) return rows;
    return rows.filter(program => {
      const haystack = programSearchText(program, chainLabels);
      return terms.every(term => haystack.includes(term));
    });
  }

  return {normalizeSearch, programSearchText, filterPrograms};
});
