#!/usr/bin/env python3
"""
Signature-driven supply-chain malware scanner.

Loads all *.yml from SIGNATURES_DIR, walks every commit in BASE_SHA..HEAD_SHA,
then does a final pass over the PR's working-tree state.  Findings are
deduplicated by (sig_id, file_path, pattern_id); commit-scoped findings take
priority so the exact offending commit is shown.

Environment variables:
  BASE_SHA        - merge-base SHA (set automatically in GitHub Actions)
  HEAD_SHA        - PR head SHA (set automatically in GitHub Actions)
  SIGNATURES_DIR  - path to directory containing *.yml signatures
                    (default: signatures/ next to this file)
  FAIL_SEVERITY   - minimum severity that exits 1 (default: MEDIUM)
                    CRITICAL | HIGH | MEDIUM | LOW | INFO
  GITHUB_OUTPUT   - path to GitHub Actions output file (set by the runner)

Exit codes:
  0 - no findings at or above FAIL_SEVERITY
  1 - one or more findings at or above FAIL_SEVERITY
  2 - configuration / usage error
"""

import fnmatch
import hashlib
import math
import os
import re
import subprocess
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml not installed.  Run: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEVERITY_RANK: dict[str, int] = {
    "CRITICAL": 4,
    "HIGH": 3,
    "MEDIUM": 2,
    "LOW": 1,
    "INFO": 0,
}

_SCRIPT_DIR = Path(__file__).parent
SIGNATURES_DIR = Path(os.environ.get("SIGNATURES_DIR", _SCRIPT_DIR / "signatures"))
FAIL_SEVERITY = os.environ.get("FAIL_SEVERITY", "MEDIUM").upper()
BASE_SHA = os.environ.get("BASE_SHA", "")
HEAD_SHA = os.environ.get("HEAD_SHA", "HEAD")
GITHUB_OUTPUT = os.environ.get("GITHUB_OUTPUT", "")

# ---------------------------------------------------------------------------
# Signature loading
# ---------------------------------------------------------------------------


def load_signatures() -> list[dict]:
    sigs: list[dict] = []
    for path in sorted(SIGNATURES_DIR.glob("*.yml")):
        with path.open(encoding="utf-8") as fh:
            sig = yaml.safe_load(fh)
        if not sig.get("enabled", True):
            print(f"  [skip] {sig.get('id', path.name)} (disabled)")
            continue
        sigs.append(sig)
        print(f"  [load] {sig['id']} — {sig['name']} ({sig.get('severity', 'MEDIUM')})")
    return sigs


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------


def shannon_entropy(s: str) -> float:
    """Shannon entropy in bits per character."""
    if not s:
        return 0.0
    freq: dict[str, int] = {}
    for ch in s:
        freq[ch] = freq.get(ch, 0) + 1
    n = len(s)
    return -sum((c / n) * math.log2(c / n) for c in freq.values())


def extract_strings_pure(data: bytes, min_len: int = 4) -> str:
    """Pure-Python fallback for the `strings` binary utility."""
    result: list[str] = []
    current: list[str] = []
    for byte in data:
        if 0x20 <= byte < 0x7F:
            current.append(chr(byte))
        else:
            if len(current) >= min_len:
                result.append("".join(current))
            current = []
    if len(current) >= min_len:
        result.append("".join(current))
    return "\n".join(result)


def extract_strings(data: bytes) -> str:
    """Extract printable strings from binary data, using `strings` if available."""
    try:
        proc = subprocess.run(
            ["strings", "-"],
            input=data,
            capture_output=True,
        )
        if proc.returncode == 0:
            return proc.stdout.decode("utf-8", errors="replace")
    except FileNotFoundError:
        pass
    return extract_strings_pure(data)


def file_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def match_pattern(pattern: dict, text: str, raw_bytes: bytes | None = None) -> bool:
    """Return True if *pattern* matches *text* (or *raw_bytes* for hash checks)."""
    ptype = pattern.get("type", "literal")
    ci = pattern.get("case_insensitive", False)

    if ptype == "literal":
        needle: str = pattern["match"]
        if ci:
            return needle.lower() in text.lower()
        return needle in text

    if ptype == "regex":
        flags = re.IGNORECASE if ci else 0
        return bool(re.search(pattern["match"], text, flags))

    if ptype == "entropy":
        threshold: float = pattern.get("threshold", 4.8)
        min_len: int = pattern.get("min_length", 60)
        for line in text.splitlines():
            stripped = line.strip()
            if len(stripped) >= min_len and shannon_entropy(stripped) >= threshold:
                return True
        return False

    if ptype == "hash":
        if raw_bytes is None:
            return False
        digest = file_sha256(raw_bytes)
        return digest in pattern.get("hashes", [])

    return False


# ---------------------------------------------------------------------------
# Glob matching
# ---------------------------------------------------------------------------


def matches_glob(file_path: str, globs: list[str]) -> bool:
    """True if *file_path* basename or full path matches any glob in *globs*."""
    name = Path(file_path).name
    for g in globs:
        if fnmatch.fnmatch(name, g) or fnmatch.fnmatch(file_path, g):
            return True
    return False


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------


def _git(*args: str) -> str:
    result = subprocess.run(["git", *args], capture_output=True, text=True)
    return result.stdout


def _git_bytes(*args: str) -> bytes:
    result = subprocess.run(["git", *args], capture_output=True)
    return result.stdout


def pr_commits() -> list[str]:
    """Return SHA list for commits in BASE_SHA..HEAD_SHA (oldest first)."""
    if not BASE_SHA:
        return []
    out = _git("log", "--format=%H", "--reverse", f"{BASE_SHA}..{HEAD_SHA}")
    return [line.strip() for line in out.splitlines() if line.strip()]


def commit_changed_files(commit: str) -> list[tuple[str, str]]:
    """Return [(status, path)] for files changed in *commit*."""
    out = _git("diff-tree", "--no-commit-id", "-r", "--name-status", commit)
    pairs: list[tuple[str, str]] = []
    for line in out.splitlines():
        parts = line.strip().split("\t", 1)
        if len(parts) == 2:
            pairs.append((parts[0], parts[1]))
    return pairs


def pr_all_files() -> list[str]:
    """Return all files touched anywhere in this PR (for final-state pass)."""
    if not BASE_SHA:
        return []
    out = _git("diff", "--name-only", BASE_SHA, HEAD_SHA)
    return [line.strip() for line in out.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Core scan logic
# ---------------------------------------------------------------------------

# A finding is a plain dict so it serialises to the output table easily.
Finding = dict  # keys: sig_id, sig_name, severity, file_path, pattern_id,
#                        pattern_description, commit


def _scan_content(
    sig: dict,
    target: dict,
    file_path: str,
    text: str,
    raw_bytes: bytes | None,
    commit: str,
    seen: set[tuple[str, str, str]],
) -> list[Finding]:
    findings: list[Finding] = []
    sig_id: str = sig["id"]
    severity: str = sig.get("severity", "MEDIUM")
    sig_name: str = sig["name"]

    for pattern in target.get("patterns", []):
        pat_id: str = pattern.get("id", str(pattern.get("match", "?"))[:20])
        dedup_key = (sig_id, file_path, pat_id)
        if dedup_key in seen:
            continue

        if match_pattern(pattern, text, raw_bytes):
            seen.add(dedup_key)
            findings.append(
                {
                    "sig_id": sig_id,
                    "sig_name": sig_name,
                    "severity": severity,
                    "file_path": file_path,
                    "pattern_id": pat_id,
                    "pattern_description": pattern.get("description", pat_id),
                    "commit": commit,
                }
            )

    return findings


def _scan_file(
    sigs: list[dict],
    file_path: str,
    commit: str,
    seen: set[tuple[str, str, str]],
    get_text,   # callable () -> str | None
    get_bytes,  # callable () -> bytes | None
) -> list[Finding]:
    findings: list[Finding] = []

    for sig in sigs:
        for target in sig.get("file_targets", []):
            if not matches_glob(file_path, target.get("glob", [])):
                continue

            ttype = target.get("type", "text")
            if ttype == "text":
                text = get_text()
                if text is None:
                    continue
                findings.extend(
                    _scan_content(sig, target, file_path, text, None, commit, seen)
                )
            elif ttype == "binary":
                data = get_bytes()
                if data is None:
                    continue
                text = extract_strings(data)
                findings.extend(
                    _scan_content(sig, target, file_path, text, data, commit, seen)
                )

    return findings


def run_scan(sigs: list[dict]) -> list[Finding]:
    all_findings: list[Finding] = []
    # dedup set — commit-scoped findings are added first so final-state
    # duplicates of the same (sig, file, pattern) are silently dropped.
    seen: set[tuple[str, str, str]] = set()

    # ------------------------------------------------------------------
    # Phase 1 — walk every commit introduced by this PR
    # ------------------------------------------------------------------
    commits = pr_commits()
    print(f"\nPhase 1 — scanning {len(commits)} commit(s)")

    for commit in commits:
        short = commit[:8]
        print(f"  {short}")
        for status, file_path in commit_changed_files(commit):
            if status.startswith("D"):
                continue  # deleted file — no content to scan

            # Use closures to defer git-show until a signature actually matches
            def _make_text(c: str, p: str):
                def _fn() -> str | None:
                    out = _git("show", f"{c}:{p}")
                    return out or None
                return _fn

            def _make_bytes(c: str, p: str):
                def _fn() -> bytes | None:
                    data = _git_bytes("show", f"{c}:{p}")
                    return data or None
                return _fn

            all_findings.extend(
                _scan_file(
                    sigs,
                    file_path,
                    short,
                    seen,
                    _make_text(commit, file_path),
                    _make_bytes(commit, file_path),
                )
            )

    # ------------------------------------------------------------------
    # Phase 2 — final working-tree state of all PR-touched files
    # Catches payloads introduced then squashed/amended out of history.
    # ------------------------------------------------------------------
    print("\nPhase 2 — scanning final file state")
    for file_path in pr_all_files():
        p = Path(file_path)
        if not p.is_file():
            continue

        def _make_text_wt(path: Path):
            def _fn() -> str | None:
                try:
                    return path.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    return None
            return _fn

        def _make_bytes_wt(path: Path):
            def _fn() -> bytes | None:
                try:
                    return path.read_bytes()
                except OSError:
                    return None
            return _fn

        all_findings.extend(
            _scan_file(
                sigs,
                file_path,
                "final",
                seen,
                _make_text_wt(p),
                _make_bytes_wt(p),
            )
        )

    return all_findings


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _severity_meets_threshold(sev: str, threshold: str) -> bool:
    return SEVERITY_RANK.get(sev, 0) >= SEVERITY_RANK.get(threshold, 0)


def _build_report_table(findings: list[Finding]) -> str:
    rows: list[str] = []
    for f in findings:
        rows.append(
            f"| `{f['severity']}` | `{f['commit']}:{f['file_path']}` "
            f"| {f['sig_name']} | {f['pattern_description']} |"
        )
    return "\n".join(rows)


def _write_output(key: str, value: str) -> None:
    if not GITHUB_OUTPUT:
        return
    with open(GITHUB_OUTPUT, "a", encoding="utf-8") as fh:
        if "\n" in value:
            delimiter = "__SCAN_EOF__"
            fh.write(f"{key}<<{delimiter}\n{value}\n{delimiter}\n")
        else:
            fh.write(f"{key}={value}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    print("=== Supply-Chain Malware Scanner ===")
    print(f"Signatures dir : {SIGNATURES_DIR}")
    print(f"Fail severity  : {FAIL_SEVERITY}")
    print(f"Base SHA       : {BASE_SHA or '(none — local run?)'}")
    print(f"Head SHA       : {HEAD_SHA}")

    if not SIGNATURES_DIR.exists():
        print(
            f"\nERROR: signatures directory not found: {SIGNATURES_DIR}",
            file=sys.stderr,
        )
        return 2

    print("\nLoading signatures…")
    sigs = load_signatures()

    if not sigs:
        print("No signatures loaded — nothing to scan.")
        _write_output("found", "false")
        _write_output("summary", "No signatures loaded.")
        return 0

    findings = run_scan(sigs)

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    if not findings:
        msg = "No malware indicators found."
        print(f"\n✅  {msg}")
        _write_output("found", "false")
        _write_output("report", "")
        _write_output("summary", msg)
        return 0

    print(f"\n{'=' * 60}")
    print(f"⚠  {len(findings)} finding(s):")
    for f in findings:
        marker = "🚨" if _severity_meets_threshold(f["severity"], FAIL_SEVERITY) else "⚠ "
        print(
            f"  {marker} [{f['severity']}] {f['commit']}:{f['file_path']}"
            f" — {f['pattern_description']} ({f['sig_name']})"
        )

    blocking = [f for f in findings if _severity_meets_threshold(f["severity"], FAIL_SEVERITY)]
    report = _build_report_table(findings)
    summary = (
        f"{len(findings)} finding(s); "
        f"{len(blocking)} at or above {FAIL_SEVERITY} severity."
    )

    _write_output("found", "true")
    _write_output("report", report)
    _write_output("summary", summary)

    if blocking:
        print(f"\n🚨  {len(blocking)} blocking finding(s) at or above FAIL_SEVERITY={FAIL_SEVERITY}")
        return 1

    print(f"\n⚠   All findings are below FAIL_SEVERITY={FAIL_SEVERITY} — not blocking merge.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
