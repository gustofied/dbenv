import time
import json
import concurrent.futures
from pathlib import Path
import psycopg as pg
import server
from utils import pretty, get_memory_mb, get_disk_mb

DIR = Path(__file__).parent
RESULTS_FILE = DIR / "epoch_results.json"

NUM_EXAMPLES = 10
ROLLOUTS_PER_EXAMPLE = 4
ROWS_PER_WORKER = 10
NUM_STEPS = 100
TOTAL = NUM_EXAMPLES * ROLLOUTS_PER_EXAMPLE
DB = "epoch_loop"


def setup():
    # create database
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE {DB}")

    admin = pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}")
    admin.autocommit = True
    cur = admin.cursor()

    # create table
    cur.execute("""CREATE TABLE world (
        id int8 PRIMARY KEY,
        example_id int8 NOT NULL,
        rollout_id int8,
        label text,
        value int8
    )""")

    # create users
    for ex in range(NUM_EXAMPLES):
        for r in range(ROLLOUTS_PER_EXAMPLE):
            user = f"user_e{ex}_r{r}"
            cur.execute(f"CREATE ROLE {user} WITH LOGIN PASSWORD '{user}pass'")
            cur.execute(f"GRANT ALL ON DATABASE {DB} TO {user}")
            cur.execute(f"GRANT ALL ON TABLE world TO {user}")

    # commit base schema
    cur.execute("SELECT dolt_add('world')")
    cur.execute("SELECT dolt_commit('-m', 'base schema')")
    base_hash = cur.execute("SELECT commit_hash FROM dolt_log LIMIT 1").fetchone()[0]

    # create example seeds
    seed_hashes = []
    for ex in range(NUM_EXAMPLES):
        cur.execute(f"SELECT dolt_reset('--hard', '{base_hash}')")
        cur.execute(f"INSERT INTO world VALUES ({ex * 10000}, {ex}, NULL, 'seed', {ex})")
        cur.execute(f"SELECT dolt_commit('-Am', 'seed for example {ex}')")
        h = cur.execute("SELECT commit_hash FROM dolt_log LIMIT 1").fetchone()[0]
        seed_hashes.append(h)

    # reset main to base
    cur.execute(f"SELECT dolt_reset('--hard', '{base_hash}')")
    admin.close()

    return seed_hashes


def rollout_worker(args):
    step, ex, r, branch = args
    user = f"user_e{ex}_r{r}"
    pw = f"{user}pass"

    t0 = time.time()
    conn = pg.connect(f"host=127.0.0.1 user={user} password={pw} dbname={DB}/{branch}")
    conn.autocommit = True
    cur = conn.cursor()

    for j in range(ROWS_PER_WORKER):
        pid = step * 1000000 + ex * 10000 + r * 100 + j + 1
        cur.execute(f"INSERT INTO world VALUES ({pid}, {ex}, {r}, 'step{step}', {j})")

    cur.execute(f"SELECT dolt_commit('-Am', 'step{step} e{ex}_r{r}: {ROWS_PER_WORKER} rows')")
    commit_hash = cur.execute("SELECT commit_hash FROM dolt_log LIMIT 1").fetchone()[0]
    row_count = cur.execute("SELECT COUNT(*) FROM world").fetchone()[0]
    duration = time.time() - t0
    conn.close()

    return {
        "step": step,
        "example_id": ex,
        "rollout_id": r,
        "branch": branch,
        "user": user,
        "commit_hash": commit_hash,
        "rows": row_count,
        "duration_ms": round(duration * 1000),
    }


def run_epoch_loop(seed_hashes):
    print(f"\n{'step':>5} | {'create':>7} | {'work':>7} | {'delete':>7} | {'total':>7} | rows")
    print("-" * 65)

    all_results = []

    for step in range(NUM_STEPS):
        # create branches from seeds
        t_create = time.time()
        conn = pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}")
        conn.autocommit = True
        cur = conn.cursor()
        for ex in range(NUM_EXAMPLES):
            for r in range(ROLLOUTS_PER_EXAMPLE):
                branch = f"e{ex}_r{r}"
                try:
                    cur.execute(f"SELECT dolt_branch('-D', '{branch}')")
                except Exception:
                    pass
                cur.execute(f"SELECT dolt_branch('{branch}', '{seed_hashes[ex]}')")
        conn.close()
        t_create = time.time() - t_create

        # parallel rollout work
        tasks = []
        for ex in range(NUM_EXAMPLES):
            for r in range(ROLLOUTS_PER_EXAMPLE):
                tasks.append((step, ex, r, f"e{ex}_r{r}"))

        t_work = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=TOTAL) as pool:
            results = list(pool.map(rollout_worker, tasks))
        t_work = time.time() - t_work

        # delete branches
        t_delete = time.time()
        conn = pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}")
        conn.autocommit = True
        cur = conn.cursor()
        for ex in range(NUM_EXAMPLES):
            for r in range(ROLLOUTS_PER_EXAMPLE):
                cur.execute(f"SELECT dolt_branch('-D', 'e{ex}_r{r}')")
        conn.close()
        t_delete = time.time() - t_delete

        t_total = t_create + t_work + t_delete
        mem = get_memory_mb()
        disk = get_disk_mb("epoch_loop")

        # verify
        expected = ROWS_PER_WORKER + 1
        ok = all(r["rows"] == expected for r in results)
        status = f"{expected}" if ok else "MISMATCH"

        print(f"{step:5d} | {t_create:6.2f}s | {t_work:6.2f}s | {t_delete:6.2f}s | {t_total:6.2f}s | {status}")

        # store step results
        step_record = {
            "step": step,
            "timing": {
                "create": round(t_create, 3),
                "work": round(t_work, 3),
                "delete": round(t_delete, 3),
                "total": round(t_total, 3),
            },
            "mem_mb": round(mem, 1) if mem else None,
            "disk_mb": round(disk, 1),
            "rollouts": results,
        }
        all_results.append(step_record)

        # write after each step so dashboard can read live
        with open(RESULTS_FILE, "w") as f:
            json.dump(all_results, f, indent=2)

    return all_results


def summary(all_results):
    print(f"\n{'='*65}")

    totals = [r["timing"]["total"] for r in all_results]
    creates = [r["timing"]["create"] for r in all_results]
    works = [r["timing"]["work"] for r in all_results]
    deletes = [r["timing"]["delete"] for r in all_results]

    def stats(label, vals):
        avg = sum(vals) / len(vals)
        mn, mx = min(vals), max(vals)
        first5 = sum(vals[:5]) / min(5, len(vals))
        last5 = sum(vals[-5:]) / min(5, len(vals))
        print(f"  {label:>8}: avg={avg:.3f}s  min={mn:.3f}s  max={mx:.3f}s  first5={first5:.3f}s  last5={last5:.3f}s")

    print(f"  {NUM_STEPS} steps x {TOTAL} rollouts = {NUM_STEPS * TOTAL} branch cycles")
    print(f"  {NUM_STEPS * TOTAL * ROWS_PER_WORKER} total inserts + {NUM_STEPS * TOTAL} commits")
    stats("create", creates)
    stats("work", works)
    stats("delete", deletes)
    stats("total", totals)

    drift = (sum(totals[-5:]) / 5) / (sum(totals[:5]) / 5)
    print(f"\n  drift (last5 / first5): {drift:.2f}x")
    if drift < 1.5:
        print("  no significant degradation")
    else:
        print(f"  {drift:.1f}x slowdown over {NUM_STEPS} steps")

    # gc
    print(f"\n  running dolt_gc()...")
    disk_before = get_disk_mb("epoch_loop")
    conn = pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}")
    conn.autocommit = True
    t_gc = time.time()
    conn.execute("SELECT dolt_gc()")
    t_gc = time.time() - t_gc
    conn.close()
    disk_after = get_disk_mb("epoch_loop")
    print(f"  dolt_gc() took {t_gc:.2f}s")
    print(f"  disk: {disk_before:.1f} MB -> {disk_after:.1f} MB (reclaimed {disk_before - disk_after:.1f} MB)")


if __name__ == "__main__":
    doltgres = server.serve("epoch_loop.yaml")
    seed_hashes = setup()
    all_results = run_epoch_loop(seed_hashes)
    summary(all_results)
    server.terminate(doltgres)
