# oDOLO Claimer Label Parity Implementation Plan

**Goal:** Preserve and display every trusted shared wallet label in oDOLO Claimer Breakdown with clear type and provenance.

**Architecture:** Add a small browser adapter that normalizes the shared DOLO registry into oDOLO's existing visual categories while retaining original metadata. Keep table rendering and data arithmetic unchanged.

## Task 1: Lock behavior with tests

- Add a Node regression suite for normalization, metadata preservation, friendly provenance tooltips, and the two known ENS claimers.
- Run it before implementation and confirm it fails because the adapter does not exist.

## Task 2: Implement label parity

- Add the adapter and load it after `dolo-address-labels.js`.
- Replace the lossy inline type map with the adapter.
- Pass badge/source/confidence data into Claimer Breakdown rows.
- Render an escaped type badge and tooltip only for known labels.
- Cache-bust the oDOLO route.

## Task 3: Verify and deploy

- Run focused JavaScript/Python tests and repository validation.
- Verify known, potential, and unknown address states on desktop and mobile through a local HTTP server.
- Rebase onto current production, push to `master`, and confirm the GitHub Pages deployment succeeds.

