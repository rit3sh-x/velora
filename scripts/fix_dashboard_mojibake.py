"""Strip mojibake from Grafana dashboard JSON.

Replaces UTF-8 chars that got saved as cp1252 byte sequences.
Reverts to ASCII-safe equivalents so renders consistent regardless of locale.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

MOJIBAKE = [
    ("â€”", "-"),
    ("â€“", "-"),
    ("â†”", "<->"),
    ("â†’", "->"),
    ("â†‘", "<-"),
    ("Â±", "+/-"),
    ("Î”", "delta"),
]


def fix_str(s: str) -> str:
    out = s
    for moj, asc in MOJIBAKE:
        out = out.replace(moj, asc)
    return out


def walk(obj):
    if isinstance(obj, dict):
        return {k: walk(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [walk(v) for v in obj]
    if isinstance(obj, str):
        return fix_str(obj)
    return obj


def main():
    total = 0
    for path in [
        ROOT / "monitoring/grafana/dashboards/coin.json",
        ROOT / "monitoring/grafana/dashboards/main.json",
    ]:
        before = path.read_text(encoding="utf-8")
        d = json.loads(before)
        d2 = walk(d)
        after = json.dumps(d2, indent=2, ensure_ascii=False)
        n = sum(before.count(moj) for moj, _ in MOJIBAKE)
        total += n
        path.write_text(after, encoding="utf-8")
        print(f"{path.name}: {n} mojibake replaced")
    print(f"total: {total}")


if __name__ == "__main__":
    main()
