#!/usr/bin/env python3
"""Secret-leak guard for the Dolomite dashboard.

Scans the files that GitHub Pages publishes (all git-tracked files except a small
allowlist of meta directories) for API keys and credentials. Fails with a
non-zero exit code if any match is found, so CI can block the commit/PR before a
leaked key ever reaches the public site.

This was added after an Alchemy RPC key leaked into a committed JSON data file
(`odolo_contract_data.json`). The guard catches the same class of mistake:
provider RPC URLs that embed a key, plus common cloud/service tokens.

Usage:
    python3 scripts/check_secret_leak.py            # scan tracked files
    python3 scripts/check_secret_leak.py file1 ...  # scan specific files

Exit codes:
    0 = clean
    1 = secret(s) found
    2 = usage / environment error
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

# --- What to scan ----------------------------------------------------------

# Directories never published by GitHub Pages (see pages.yml rsync excludes),
# plus archived/vendored code. Paths are matched against the tracked path.
SKIP_DIR_PREFIXES = (
    ".git/",
    ".github/",
    "node_modules/",
    "_old/",
    "_site/",
)

# Binary / non-text extensions we never need to scan.
SKIP_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".svg",
    ".woff", ".woff2", ".ttf", ".otf", ".eot",
    ".pdf", ".zip", ".gz", ".tgz", ".mp4", ".mov", ".webm",
    ".pyc", ".so", ".dylib", ".wasm",
}

# Files that legitimately contain the *patterns* (this guard + its docs) and
# would otherwise self-trigger. Matched against the exact tracked path.
SELF_ALLOWLIST = {
    "scripts/check_secret_leak.py",
    "SECURITY_AUDIT.md",
}

# Skip individual files larger than this (bytes). Tracked data files are well
# under this; anything bigger is not a hand-edited text surface.
MAX_FILE_BYTES = 60 * 1024 * 1024

# --- What counts as a secret ----------------------------------------------

# Each entry: (human-readable name, compiled regex). Patterns are intentionally
# specific to avoid false positives on public, key-free endpoints.
PATTERNS = [
    ("Alchemy RPC key",
     re.compile(r"[a-z0-9-]*\.alchemy\.com/v2/[A-Za-z0-9_-]{20,}")),
    ("Infura project key",
     re.compile(r"infura\.io/v3/[A-Za-z0-9]{16,}")),
    ("QuickNode endpoint key",
     re.compile(r"[a-z0-9-]+\.quiknode\.pro/[A-Za-z0-9]{16,}")),
    ("dRPC dkey",
     re.compile(r"drpc\.org/[^\s\"']*[?&]dkey=[A-Za-z0-9_-]{16,}")),
    ("Ankr keyed endpoint",
     re.compile(r"rpc\.ankr\.com/[a-z0-9_]+/[A-Za-z0-9]{32,}")),
    ("GetBlock key",
     re.compile(r"[a-z0-9.]*getblock\.io/[A-Za-z0-9]{16,}")),
    ("Moralis / Chainstack keyed RPC",
     re.compile(r"(moralis|chainstack)\.[^\s\"']*/[A-Za-z0-9]{24,}")),
    ("Generic apikey/access_token in URL",
     re.compile(r"[?&](apikey|api_key|access_token|key)=[A-Za-z0-9_-]{16,}")),
    ("GitHub token",
     re.compile(r"gh[pousr]_[A-Za-z0-9]{30,}")),
    # Anchored on the real key prefix so version/cache-bust strings like
    # "sk-token-dropdown-2026..." can never match.
    ("Anthropic API key",
     re.compile(r"sk-ant-(?:api03|admin01)-[A-Za-z0-9_-]{50,}")),
    # Real OpenAI keys are an unbroken base62 run; require >=20 chars with no
    # hyphen so dictionary/versioned slugs (split by "-") are excluded.
    ("OpenAI API key",
     re.compile(r"sk-(?:proj-|svcacct-)?[A-Za-z0-9]{20,}")),
    ("AWS access key id",
     re.compile(r"AKIA[0-9A-Z]{16}")),
    ("Slack token",
     re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}")),
    ("Private key block",
     re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----")),
]

# One combined regex used as a fast pre-filter: if it doesn't match the whole
# file we skip the per-pattern pass entirely. Names are recovered only on a hit.
_COMBINED = re.compile("|".join(f"(?:{rx.pattern})" for _, rx in PATTERNS))


def tracked_files() -> list[str]:
    try:
        out = subprocess.run(
            ["git", "ls-files", "-z"],
            check=True, capture_output=True, text=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        print(f"error: cannot list tracked files: {exc}", file=sys.stderr)
        sys.exit(2)
    return [p for p in out.split("\0") if p]


def should_scan(path: str) -> bool:
    if path in SELF_ALLOWLIST:
        return False
    if any(path.startswith(p) for p in SKIP_DIR_PREFIXES):
        return False
    if Path(path).suffix.lower() in SKIP_EXTENSIONS:
        return False
    return True


def scan_file(path: str) -> list[tuple[int, str, str]]:
    """Return list of (line_no, secret_name, redacted_snippet)."""
    p = Path(path)
    try:
        if p.stat().st_size > MAX_FILE_BYTES:
            return []
    except OSError:
        return []

    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return []

    # Fast path: nothing looks like a secret anywhere in the file.
    if not _COMBINED.search(text):
        return []

    # Slow path only runs on the rare file that matched: find exact pattern,
    # line number and a redacted snippet.
    hits: list[tuple[int, str, str]] = []
    for name, rx in PATTERNS:
        for m in rx.finditer(text):
            lineno = text.count("\n", 0, m.start()) + 1
            hits.append((lineno, name, _redact(m.group(0))))
    return hits


def _redact(s: str) -> str:
    """Show enough to locate the leak without reprinting the full secret."""
    s = s.strip()
    if len(s) <= 16:
        return s[:6] + "…"
    return s[:12] + "…" + s[-4:]


def main(argv: list[str]) -> int:
    targets = argv[1:] if len(argv) > 1 else tracked_files()
    scanned = 0
    findings: list[tuple[str, int, str, str]] = []

    for path in targets:
        if not should_scan(path):
            continue
        scanned += 1
        for lineno, name, snippet in scan_file(path):
            findings.append((path, lineno, name, snippet))

    if findings:
        print("✗ Secret-leak guard FAILED — potential credential(s) found:\n")
        for path, lineno, name, snippet in findings:
            print(f"  {path}:{lineno}  [{name}]  {snippet}")
        print(
            "\nRemove the secret, rotate it if it was ever pushed, and use a "
            "GitHub Actions secret (${{ secrets.* }}) or a public key-free "
            "endpoint instead. Scanned",
            scanned, "files.",
        )
        return 1

    print(f"✓ Secret-leak guard passed — no credentials found ({scanned} files scanned).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
