import os
from pathlib import Path

DIR = Path(__file__).parent


def pretty(cur):
    if not cur.description:
        return [], []
    cols = [d.name for d in cur.description]
    rows = cur.fetchall()
    widths = [min(30, max([len(str(c))] + [len(str(r[i])) for r in rows])) for i, c in enumerate(cols)]
    header = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
    sep = "-+-".join("-" * w for w in widths)
    print(header)
    print(sep)
    for r in rows:
        print(" | ".join(str(v)[:w].ljust(w) for v, w in zip(r, widths)))
    return cols, rows


