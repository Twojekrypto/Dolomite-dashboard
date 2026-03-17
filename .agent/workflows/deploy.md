---
description: Commit, push, and verify deployment on GitHub Pages
---

# Deploy to GitHub Pages

## Prerequisites
- All changes tested locally via `python3 -m http.server`
- Browser verification completed (screenshot or JS console check)

## Steps

// turbo
1. Check git status: `cd ~/Desktop/Draft/"Dolomite website" && git status`

2. Stage and commit changes: `cd ~/Desktop/Draft/"Dolomite website" && git add -A && git commit -m "OPIS_ZMIANY"`

3. Push to both branches (REQUIRED — GH Pages serves from master):
   `cd ~/Desktop/Draft/"Dolomite website" && git push origin main && git push origin main:master`

// turbo
4. Wait 60 seconds for GitHub Actions deployment: `sleep 60`

5. Verify deployed changes in the browser:
   - Open `https://twojekrypto.github.io/dolomite-dashboard/?v=TIMESTAMP`
   - Add unique `?v=` parameter to bust GH Pages cache
   - Check the specific elements that were changed
