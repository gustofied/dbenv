# type: ignore
import json
import socket
import subprocess
import time
import psycopg as pg
from datetime import datetime
from pathlib import Path
import server
from lab import init_db, lab, epoch, DB
from utils import get_memory_mb, get_disk_mb

DIR = Path(__file__).parent
RESULTS_DIR = DIR / "results"
cfg = "lab.yaml"
VIEWER_PORT = 8090


def ensure_viewer():
    """Spawn lab_viewer if not already running on VIEWER_PORT."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", VIEWER_PORT)) == 0:
            print(f"  viewer already running on :{VIEWER_PORT}")
            return None
    print(f"  spawning viewer on :{VIEWER_PORT}")
    proc = subprocess.Popen(
        ["uv", "run", "uvicorn", "lab_viewer:app", "--port", str(VIEWER_PORT)],
        cwd=str(DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc

def summary(all_steps):
    print(f"\n{'='*65}")

    creates = [s["timing"]["create"] for s in all_steps]
    works = [s["timing"]["work"] for s in all_steps]
    deletes = [s["timing"]["delete"] for s in all_steps]
    totals = [s["timing"]["total"] for s in all_steps]

    def stats(label, vals):
        avg = sum(vals) / len(vals)
        mn, mx = min(vals), max(vals)
        first5 = sum(vals[:5]) / min(5, len(vals))
        last5 = sum(vals[-5:]) / min(5, len(vals))
        print(f"  {label:>8}: avg={avg:.3f}s  min={mn:.3f}s  max={mx:.3f}s  first5={first5:.3f}s  last5={last5:.3f}s")

    n = len(all_steps)
    rollouts_per_step = len(all_steps[0]["rollouts"]) if all_steps else 0
    print(f"  {n} steps x {rollouts_per_step} rollouts = {n * rollouts_per_step} branch cycles")
    stats("create", creates)
    stats("work", works)
    stats("delete", deletes)
    stats("total", totals)

    if len(totals) >= 5:
        drift = (sum(totals[-5:]) / 5) / (sum(totals[:5]) / 5)
        print(f"\n  drift (last5 / first5): {drift:.2f}x")
        if drift < 1.5:
            print("  no significant degradation")
        else:
            print(f"  {drift:.1f}x slowdown over {n} steps")

def gc():
    print(f"\n{'='*65}")
    disk_before = get_disk_mb("lab")
    mem_before = get_memory_mb()
    print(f"  before GC:  mem={mem_before:.0f} MB  disk={disk_before:.1f} MB" if mem_before else f"  before GC:  disk={disk_before:.1f} MB")

    print("  running dolt_gc()...")
    conn = pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}")
    conn.autocommit = True
    t_gc = time.time()
    conn.execute("SELECT dolt_gc()")
    t_gc = time.time() - t_gc
    conn.close()

    disk_after = get_disk_mb("lab")
    mem_after = get_memory_mb()
    print(f"  dolt_gc() took {t_gc:.2f}s")
    print(f"  after GC:   disk={disk_after:.1f} MB")
    if disk_before > 0:
        print(f"  reclaimed: {disk_before - disk_after:.1f} MB ({(1 - disk_after/disk_before)*100:.0f}%)")

    return {"gc_time": round(t_gc, 2), "disk_before": round(disk_before, 1), "disk_after": round(disk_after, 1)}

def run(num_steps=50, rollouts_per_world=10, num_rows=109):
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    results_file = RESULTS_DIR / f"run_{timestamp}.json"

    viewer = ensure_viewer()
    doltgres = server.fresh_start(cfg)
    init_db()
    admin, cur, worlds = lab(rollouts_per_world=rollouts_per_world)

    all_steps = epoch(cur, worlds, num_steps=num_steps, rollouts_per_world=rollouts_per_world, num_rows=num_rows)

    summary(all_steps)
    admin.close()
    gc_result = gc()

    # write results
    output = {
        "timestamp": timestamp,
        "config": {"num_steps": num_steps, "rollouts_per_world": rollouts_per_world, "num_rows": num_rows},
        "steps": all_steps,
        "gc": gc_result,
    }
    with open(results_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nresults written to {results_file}")

    server.terminate(doltgres)

if __name__ == "__main__":
    run()
