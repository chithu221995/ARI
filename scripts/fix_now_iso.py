"""
Scan project python files under app/ and replace any variable assignment/annotation
that defines `now_iso` with `_now_iso_str`. Do NOT touch callable usages like
now_iso(...). Prints diffs and writes a .bak backup for each changed file.
"""
from __future__ import annotations
import re
from pathlib import Path
import difflib
import sys

ROOT = Path.cwd()
TARGET_DIRS = ["app/ingest", "app/api", "app/core", "app/summarize"]
PY_GLOBS = ["*.py"]

assign_re = re.compile(r'^(\s*)now_iso\s*=', re.MULTILINE)
annot_re = re.compile(r'^(\s*)now_iso\s*:', re.MULTILINE)
# non-call occurrences to report (not to auto-replace)
noncall_re = re.compile(r'\bnow_iso\b(?!\s*\()')

files = []
for d in TARGET_DIRS:
    p = ROOT / d
    if not p.exists():
        continue
    for py in p.rglob("*.py"):
        files.append(py)

if not files:
    print("No files found under", TARGET_DIRS)
    sys.exit(0)

changed = 0
for f in files:
    text = f.read_text(encoding="utf-8")
    new = text
    new = assign_re.sub(r'\1_now_iso_str =', new)
    new = annot_re.sub(r'\1_now_iso_str:', new)

    if new != text:
        # show diff
        diff = ''.join(difflib.unified_diff(
            text.splitlines(keepends=True),
            new.splitlines(keepends=True),
            fromfile=str(f),
            tofile=str(f) + " (modified)",
        ))
        print(diff)
        # backup and write
        bak = f.with_suffix(f.suffix + ".bak")
        bak.write_text(text, encoding="utf-8")
        f.write_text(new, encoding="utf-8")
        changed += 1

    # Report any non-call occurrences that may still shadow the callable
    for m in noncall_re.finditer(new):
        # ensure this occurrence is not part of an assignment we already changed
        start = m.start()
        line = new[:start].splitlines()[-1] if new[:start] else ""
        if "now_iso" in line and "=" in line and not line.strip().startswith("#"):
            # already handled by assignment replacement
            continue
        print(f"NOTE: non-call occurrence in {f}:{new[:start].count('\\n')+1}: '{line.strip()}'")

print(f"Files modified: {changed}")