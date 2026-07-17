# Local agent skills

This directory contains a deliberately small, pinned subset of [Anthropic's Agent Skills](https://github.com/anthropics/skills), copied from revision `9d2f1ae187231d8199c64b5b762e1bdf2244733d`. They are instructions and test helpers for an AI agent; they do not run in the dashboard or affect the deployed site by themselves.

## When to use them

- Before a visual redesign or a new dashboard component, ask: `Use frontend-design to propose and implement a design for [page/component]. Preserve the existing Graphite + Gold identity.`
- Before checking an interaction, layout, browser error, or mobile behavior, ask: `Use webapp-testing to verify [exact behavior] on [route]. Start the local static server, inspect the rendered page, and report screenshots/console errors.`

For this static site, tests should normally use `python3 -m http.server` from the repository root, rather than `file://`, because dashboard pages fetch JSON data. The helper in `webapp-testing/scripts/with_server.py` executes the supplied server command with a shell; use it only with a reviewed, literal local command.

## Browser test setup

The production dashboard does not need Playwright, so it is intentionally absent from `requirements.txt`. In Codex, you can simply make one of the requests above; the agent can use its browser tooling. For a manual local Playwright run without changing this repository's dependencies:

```bash
python3 -m venv /tmp/dolomite-playwright
source /tmp/dolomite-playwright/bin/activate
pip install playwright
python -m playwright install chromium
```

Then start the dashboard with `python3 -m http.server 8000` in the repository root and point a Playwright script at `http://localhost:8000`. Do not add Playwright to the production data-pipeline requirements just for ad-hoc UI checks.

The imported files retain their upstream Apache-2.0 license. To update either skill, review the upstream diff before replacing it and record the new revision here.
