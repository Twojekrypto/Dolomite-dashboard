---
description: Verify UI/CSS changes using local server and browser tools
---

# UI Check — Verify Visual Changes

## When to Use
After any CSS/HTML changes to `index.html`, `liquidations.html`, or `excluded_addresses.html`.

## Steps

// turbo
1. Start local server: `cd ~/Desktop/Draft/"Dolomite website" && python3 -m http.server 8080 &`

2. Open in browser and navigate to the changed page:
   - Main dashboard: `http://localhost:8080/index.html`
   - Liquidations: `http://localhost:8080/liquidations.html`
   - Protocol addresses: `http://localhost:8080/excluded_addresses.html`

3. Wait for data to load (some pages fetch JSON, takes 3-8 seconds)

4. Verify CSS computed values via JavaScript console:
   ```js
   // Example: verify padding
   getComputedStyle(document.querySelector('.target')).paddingLeft
   // Example: verify alignment
   document.querySelector('.target').getBoundingClientRect().left
   ```

5. Take a screenshot for proof

// turbo
6. Kill local server: `pkill -f "python3 -m http.server 8080" || true`

## ⚠️ Rules
- **NEVER trust inline styles alone** — `!important` or higher specificity CSS can override them
- **After column changes** — audit ALL `nth-child` CSS selectors
- **Always use `getComputedStyle()`** to confirm actual rendered values
