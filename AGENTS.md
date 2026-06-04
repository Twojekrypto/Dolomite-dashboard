# Agent Guidelines for Dolomite Dashboard

These instructions are for AI coding agents working in this repository. Keep them short in memory, but follow them carefully.

## Project Context

- This is a static Dolomite analytics dashboard served by GitHub Pages.
- The main UI is mostly in `index.html`; several preview pages mirror or slice the same UI.
- Python scripts build and refresh JSON data files consumed by the static pages.
- Before non-trivial work, read `PROJECT_STATE.md` and `lessons.md`.
- Treat `lessons.md` as accumulated production knowledge. If it conflicts with a generic best practice, prefer the local lesson.

## Core Working Principles

1. Think before coding.
   - State assumptions when a request is ambiguous.
   - Surface tradeoffs instead of silently picking a risky interpretation.
   - Ask before changing behavior that could affect deployed data or user-facing metrics.

2. Keep changes simple.
   - Write the minimum code that solves the current problem.
   - Do not add abstractions, configuration, or new dependencies unless the task needs them.
   - Prefer existing local patterns over new architecture.

3. Make surgical edits.
   - Touch only files directly related to the request.
   - Do not reformat large files, especially `index.html`, while making small fixes.
   - Do not clean up unrelated code. Mention unrelated problems instead.
   - Every changed line should trace back to the user's request.

4. Work toward verifiable success.
   - Define what "done" means before editing.
   - Run targeted checks after changes.
   - If a check cannot be run, explain why and name the residual risk.

## Dolomite-Specific Rules

- Always use `rg` for searches when available.
- Use `python3 -m http.server` for local UI testing because `file://` blocks `fetch()`.
- For CSS/UI changes, verify real browser-computed values with `getComputedStyle()` or bounding boxes, not only source inspection.
- After changing table columns, audit all related `nth-child` selectors.
- Be careful with flex toolbars and dropdown containers; tag-depth and `overflow` mistakes have broken layouts before.
- When adding or removing HTML IDs, search for all JavaScript references.
- Do not use `parseFloat` for wei arithmetic. Use precise integer parsing patterns already present in the codebase.
- Do not use bare `except: pass` in Python RPC/data code.
- If a Python script starts writing a new generated JSON file, update the relevant GitHub Actions workflow so the file is added and committed.
- Keep operational knobs in config files, but keep strict audit classification logic in code with tests.

## Verification Commands

Use the narrowest check that matches the change:

```bash
npm run check:earn-audit
python3 run_earn_audit_checks.py
python3 -m http.server
python3 -m py_compile path/to/script.py
node --check path/to/file.js
```

For static HTML/UI work, open the local server and inspect the exact page that changed.

## Git and Deployment

- The live GitHub Pages site is served from `master`, while normal work may happen on `main`.
- Do not assume pushing `main` updates production.
- Background agents can hit macOS Keychain issues when running `git push`; if deployment is needed, provide the user with explicit push commands instead of starting a credential prompt.
- Preserve unrelated local changes. If the worktree is dirty, only touch the files required for the task.

