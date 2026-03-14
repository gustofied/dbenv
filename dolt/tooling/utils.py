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


def get_memory_mb():
    try:
        import httpx
        r = httpx.get("http://localhost:11228/metrics", timeout=2)
        for line in r.text.splitlines():
            if line.startswith("go_memstats_alloc_bytes "):
                return float(line.split()[-1]) / 1024 / 1024
    except Exception:
        pass
    return None


def get_disk_mb(data_name):
    total = 0
    data_dir = DIR / f"data/{data_name}"
    if data_dir.exists():
        for dirpath, _, filenames in os.walk(data_dir):
            for f in filenames:
                total += os.path.getsize(os.path.join(dirpath, f))
    return total / 1024 / 1024
