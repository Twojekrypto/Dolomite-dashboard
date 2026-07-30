# Institutional Table and Market UX Design

## Goal

Make the veDOLO, Borrow, Supply Pool Health, and Supply Markets views read as
one Graphite + Gold product while preserving their existing data and routing.

## Chosen Direction

Use one continuous institutional surface:

- DOLO Holders is the reference for table headers, typography, rows, hover
  treatment, metadata, and footer/pagination surfaces.
- veDOLO summary metrics sit on the same quiet green-tinted surface as the
  `veDOLO Position Activity` header instead of changing to a separate flat
  panel.
- Supply Pool Health details expand edge-to-edge inside the table with one gold
  rail and structural dividers. There is no inset card, duplicate border, or
  decorative shadow.
- Supply Markets uses a compact market directory rather than a raw icon/address
  list. Each option shows a canonical icon, symbol, descriptive name, market ID,
  and shortened address; selection is communicated with a rail and text, not a
  checkbox.
- A single Supply-route icon presentation helper is consumed by the selector,
  Selected Market, and Supplier Leaderboard. Supply Pool Health uses the same
  official Dolomite media aliases for `dplvGLP` and `SolvBTC.BBN`.

This is preferred over a CSS-only patch because the visible icon mismatch is a
data-resolution problem, and preferred over a repository-wide component rewrite
because the latter would create unnecessary risk in the large static pages.

## Visual System

- Preserve the existing Graphite + Gold palette and Inter/JetBrains Mono
  typography.
- Keep color restrained: graphite surfaces, white hierarchy, gold for active
  controls and the expanded-row rail, semantic red/green only for data.
- Match DOLO Holders table title at 16px/600, metadata at 11px mono, headers at
  10px uppercase mono, and body data at 13px.
- Keep active-only sort arrows. Clicking filter controls inside a sortable
  header must not change sort order.
- Keep stable table geometry and existing pagination behavior.

## Behavior

### Liquidation History sorting

The default order remains Date descending. Chain and Address compare
case-insensitively; Date compares timestamps; Collateral and Debt compare USD
numbers. Equal primary values use Date descending and transaction hash as
deterministic tie-breakers. The currently sorted header exposes `aria-sort` and
the only visible arrow.

### Canonical icons

The exact market address wins over symbol aliases. `dplvGLP` resolves to the
official `plvGLP` artwork and `SolvBTC.BBN` to the official `solvBTCbbn`
artwork. The same presentation is applied to selector options, the selected
asset control, Selected Market, and Supplier Leaderboard.

### Responsive behavior

Desktop uses balanced three-column Supply Health detail content. Tablet stacks
the identity above metrics and then reduces detail columns. Mobile becomes a
single compact column. The Supply Markets directory stays within the viewport
and scrolls internally.

## Verification

- Contract tests cover Liquidation sorting, active-only sort headers, canonical
  icon aliases, the edge-to-edge Supply Health tray, aligned veDOLO columns, and
  route cache-version bumps.
- Browser checks compare computed backgrounds, fonts, padding, metadata bounds,
  sort interaction, expanded detail bounds, dropdown overflow, and rendered icon
  URLs on the local HTTP server.
- Relevant Node/Python suites run before commit; production workflows and the
  live GitHub Pages route are checked after pushing `master`.
