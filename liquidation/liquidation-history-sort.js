(function exposeLiquidationHistorySort(root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) {
    module.exports = api;
    return;
  }
  root.DolomiteLiquidationHistorySort = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function createLiquidationHistorySort() {
  function finiteNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : 0;
  }

  function liquidationTimestamp(value) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    const numeric = Number(value);
    if (String(value || '').trim() && Number.isFinite(numeric)) return numeric;
    const parsed = Date.parse(String(value || ''));
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function liquidationHistorySortValue(row, field) {
    switch (field) {
      case 'chain':
        return String(row?.chainLabel || row?.chain || '').trim().toLowerCase();
      case 'address':
        return String(row?.liquidatedAddress || '').trim().toLowerCase();
      case 'collateral':
        return finiteNumber(row?.collateralSeizedUSD);
      case 'debt':
        return finiteNumber(row?.debtRepaidUSD);
      case 'date':
      default:
        return liquidationTimestamp(row?.timestamp);
    }
  }

  function compareLiquidationHistoryValues(left, right) {
    if (typeof left === 'string' || typeof right === 'string') {
      return String(left).localeCompare(String(right), 'en', {
        numeric: true,
        sensitivity: 'base',
      });
    }
    return finiteNumber(left) - finiteNumber(right);
  }

  function sortLiquidationHistoryRows(rows, field = 'date', direction = 'desc') {
    const multiplier = direction === 'asc' ? 1 : -1;
    return (Array.isArray(rows) ? rows : []).slice().sort((left, right) => {
      const primary = compareLiquidationHistoryValues(
        liquidationHistorySortValue(left, field),
        liquidationHistorySortValue(right, field),
      );
      if (primary) return primary * multiplier;

      const dateTieBreak = liquidationTimestamp(right?.timestamp)
        - liquidationTimestamp(left?.timestamp);
      if (dateTieBreak) return dateTieBreak;

      return String(left?.txHash || '').localeCompare(String(right?.txHash || ''));
    });
  }

  return {
    liquidationHistorySortValue,
    sortLiquidationHistoryRows,
  };
});
