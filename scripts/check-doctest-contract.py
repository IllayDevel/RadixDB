#!/usr/bin/env python3
from pathlib import Path
import re

root = Path(__file__).resolve().parent.parent
sources = list((root / "src").rglob("*.rs")) + list((root / "crates").rglob("*.rs"))
text_by_path = {path: path.read_text(encoding="utf-8") for path in sources}

ignored = sum(len(re.findall(r"```ignore\b", text)) for text in text_by_path.values())
compiled = sum(len(re.findall(r"```(?:no_run|rust)\b", text)) for text in text_by_path.values())
if ignored > 56:
    raise SystemExit(f"ignored doctest inventory grew from the reviewed ceiling: {ignored} > 56")
if compiled < 6:
    raise SystemExit(f"executable/no_run doctest owner inventory shrank: {compiled} < 6")

api_text = "\n".join(
    text_by_path[path]
    for path in text_by_path
    if path.is_relative_to(root / "src/api")
)
for stale in ('row.get("name")', "IsolationLevel::Snapshot)?"):
    if stale in api_text:
        raise SystemExit(f"stale public API snippet remains: {stale}")

print(f"doctest contract: {compiled} compiled/runnable, {ignored} explicitly illustrative")
