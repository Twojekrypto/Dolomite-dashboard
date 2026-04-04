const dolo_holderData = [
  { address: '0x123', chains: ['eth'] },
  { address: '0x456', chains: ['bera'] }
];
const DOLO_ADDR_LABELS = {
  '0x123': { label: 'Investor' },
  '0x456': { label: 'Team' }
};

const q = 'team';
const hideCex = false;
const showEth = true;
const showBera = true;

const dolo_holderFiltered = dolo_holderData.filter(h => {
    if (q) {
        const lowerAddr = h.address.toLowerCase();
        const info = DOLO_ADDR_LABELS[lowerAddr];
        const labelMatch = info && info.label.toLowerCase().includes(q);
        if (!lowerAddr.includes(q) && !labelMatch) return false;
    }
    if (hideCex) {
        const info = DOLO_ADDR_LABELS[h.address.toLowerCase()];
        if (info) return false; // filter all labeled addresses (CEX, LP, Protocol, CA)
        if (h.is_contract) return false; // filter unlabeled contracts too
    }
    const hasEth = h.chains && h.chains.includes('eth');
    const hasBera = h.chains && h.chains.includes('bera');
    if (!showEth && !showBera) return false;
    if (showEth && showBera) return true;
    if (showEth && !showBera) return hasEth;
    if (!showEth && showBera) return hasBera;
    return true;
});

console.log(dolo_holderFiltered);
