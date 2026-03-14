# type: ignore
import psycopg as pg
import time
import concurrent.futures

cfg= "lab.yaml"
DB = "tooling_lab"

def init_db():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
        print(cur.statusmessage)

def init_schema(cur, table):
    cur.execute(f"""CREATE TABLE {table} (
        id int8 PRIMARY KEY,
        label text,
        value int8,
        score float8,
        status text,
        metadata text
    )""")
    cur.execute(f"SELECT dolt_add('{table}')")
    h = cur.execute("SELECT dolt_commit('-m', 'base schema')").fetchone()[0][0]
    return h

def create_roles(cur, num_users):
    for i in range(num_users):
        user = f"user_{i}"
        cur.execute(f"CREATE ROLE {user} WITH LOGIN PASSWORD '{user}pass'")
        cur.execute(f"GRANT ALL ON DATABASE {DB} TO {user}")
        cur.execute(f"GRANT ALL ON SCHEMA public TO {user}")
        cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA public TO {user}")

def create_db_worlds(admin_cur, table, base_hash, num_worlds):
    admin_cur.execute(f"SELECT dolt_branch('worlds', '{base_hash}')")
    worlds_conn= pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}/worlds")
    worlds_conn.autocommit = True
    cur = worlds_conn.cursor()
    world_snapshots = {}
    for w in range(num_worlds):
        cur.execute(f"SELECT dolt_reset('--hard', '{base_hash}')")  
        cur.execute(f"INSERT INTO {table} VALUES ({w * 10000}, 'world_{w}', {w}, 0.0, 'seed', 'initial')")  
        h = cur.execute(f"SELECT dolt_commit('-Am', 'world {w}')").fetchone()[0][0]  
        world_snapshots[f"world_{w}"] = h
    worlds_conn.close()
    return world_snapshots


def rollout(world_name, world_hash, user, branch, world_idx, rollout_idx, num_rows=10):
    t0 = time.time()
    conn = pg.connect(f"host=127.0.0.1 user={user} password={user}pass dbname={DB}/{branch}")
    conn.autocommit = True
    cur = conn.cursor()
    for j in range(num_rows):
        row_id = world_idx * 100000 + rollout_idx * 1000 + j + 1
        cur.execute(f"INSERT INTO world VALUES ({row_id}, '{branch}_row{j}', {j}, {j * 0.1}, 'done', 'step_{j}')")
    cur.execute(f"SELECT dolt_commit('-Am', '{branch}: {num_rows} rows')")
    commit_hash = cur.fetchone()[0][0]
    conn.close()
    return {
        "world": world_name,
        "world_hash": world_hash,
        "branch": branch,
        "user": user,
        "commit_hash": commit_hash,
        "rows": num_rows,
        "duration_ms": round((time.time() - t0) * 1000),
    }

def batch(admin_cur, world_snapshots, rollouts_per_world=2, num_rows=10):
    # create all branches serially with admin
    tasks = []
    for w_idx, (w_name, w_hash) in enumerate(world_snapshots.items()):
        for r in range(rollouts_per_world):
            user = f"user_{r}"
            branch = f"{w_name}_r{r}"
            admin_cur.execute(f"SELECT dolt_branch('{branch}', '{w_hash}')")
            tasks.append((w_name, w_hash, user, branch, w_idx, r))

    # run workers in parallel, no shared cursor
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = [
            pool.submit(rollout, w_name, w_hash, user, branch, w_idx, r_idx, num_rows)
            for w_name, w_hash, user, branch, w_idx, r_idx in tasks
        ]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())
    return results

def verify(results, expected_rows, verbose=False):
    for r in results:
        conn = pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}/{r['branch']}")
        conn.autocommit = True
        cur = conn.cursor()
        count = cur.execute("SELECT COUNT(*) FROM world").fetchone()[0]
        conn.close()
        ok = count == expected_rows
        if verbose or not ok:
            status = "ok" if ok else f"MISMATCH (got {count})"
            print(f"  {r['branch']}: {status}")
        if not ok:
            return False
    return True

def epoch(admin_cur, world_snapshots, num_steps=10, rollouts_per_world=2, num_rows=10):
    print(f"\n{'step':>5} | {'create':>7} | {'work':>7} | {'delete':>7} | {'total':>7} | rollouts")
    print("-" * 65)

    all_steps = []
    for step in range(num_steps):
        # create branches
        t_create = time.time()
        tasks = []
        for w_idx, (w_name, w_hash) in enumerate(world_snapshots.items()):
            for r in range(rollouts_per_world):
                branch = f"s{step}_{w_name}_r{r}"
                admin_cur.execute(f"SELECT dolt_branch('{branch}', '{w_hash}')")
                tasks.append((w_name, w_hash, f"user_{r}", branch, w_idx, r))
        t_create = time.time() - t_create

        # parallel work
        t_work = time.time()
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as pool:
            futures = [
                pool.submit(rollout, w_name, w_hash, user, branch, w_idx, r_idx, num_rows)
                for w_name, w_hash, user, branch, w_idx, r_idx in tasks
            ]
            for f in concurrent.futures.as_completed(futures):
                r = f.result()
                r["step"] = step
                results.append(r)
        t_work = time.time() - t_work

        # verify before deleting
        # each branch has 1 seed row + num_rows inserted
        ok = verify(results, expected_rows=num_rows + 1)

        # delete branches
        t_delete = time.time()
        for t in tasks:
            admin_cur.execute(f"SELECT dolt_branch('-D', '{t[3]}')")
        t_delete = time.time() - t_delete

        t_total = t_create + t_work + t_delete
        status = "ok" if ok else "FAIL"
        print(f"{step:5d} | {t_create:6.2f}s | {t_work:6.2f}s | {t_delete:6.2f}s | {t_total:6.2f}s | {len(results)} {status}")

        all_steps.append({
            "step": step,
            "timing": {
                "create": round(t_create, 3),
                "work": round(t_work, 3),
                "delete": round(t_delete, 3),
                "total": round(t_total, 3),
            },
            "ok": ok,
            "rollouts": results,
        })

    return all_steps

def lab(num_worlds=3, rollouts_per_world=2):
    admin = pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}")
    admin.autocommit = True
    cur = admin.cursor()

    base_hash = init_schema(cur, table="world")
    print("base hash:", base_hash)

    create_roles(cur, num_users=rollouts_per_world)

    worlds = create_db_worlds(cur, table="world", base_hash=base_hash, num_worlds=num_worlds)
    print("worlds:", worlds)

    return admin, cur, worlds
