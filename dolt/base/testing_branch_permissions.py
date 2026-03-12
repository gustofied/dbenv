import os
import shutil
import subprocess
import time
from pathlib import Path
import psycopg as pg

def pretty(cur):
    if not cur.description:
        return
    cols = [d.name for d in cur.description]
    rows = cur.fetchall()
    widths = [min(30, max([len(str(c))] + [len(str(r[i])) for r in rows])) for i, c in enumerate(cols)]
    header = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
    sep = "-+-".join("-" * w for w in widths)
    print(header)
    print(sep)
    for r in rows:
        print(" | ".join(str(v)[:w].ljust(w) for v, w in zip(r, widths)))

DIR = Path(__file__).parent

def run_doltgres():
    os.chdir(DIR)
    subprocess.run(["pkill", "-xi", "doltgres"], stderr=subprocess.DEVNULL)
    time.sleep(1)
    Path("/tmp/.s.PGSQL.5432").unlink(missing_ok=True)
    # clean slate
    shutil.rmtree(DIR / "data/testing_branch_permissions", ignore_errors=True)
    shutil.rmtree(DIR / ".doltcfg/testing_branch_permissions", ignore_errors=True)
    subprocess.Popen(["doltgres", "--config", str(DIR / "testing_branch_permissions.yaml")])
    time.sleep(2)

def setup():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        cur = conn.cursor()
        cur.execute("CREATE DATABASE branch_test")

    admin = pg.connect("host=127.0.0.1 user=postgres password=password dbname=branch_test")
    admin.autocommit = True
    admin_cur = admin.cursor()

    # create roles
    admin_cur.execute("CREATE ROLE alice WITH LOGIN PASSWORD 'alice123'")
    admin_cur.execute("CREATE ROLE bob WITH LOGIN PASSWORD 'bob123'")

    # create a table and seed data
    admin_cur.execute("CREATE TABLE items(id int8 primary key, name text)")
    admin_cur.execute("INSERT INTO items VALUES (1, 'seed')")
    admin_cur.execute("GRANT ALL ON DATABASE branch_test TO alice, bob")
    admin_cur.execute("GRANT ALL ON TABLE items TO alice, bob")

    # commit seed
    admin_cur.execute("SELECT dolt_add('items')")
    admin_cur.execute("SELECT dolt_commit('-m', 'seed commit')")

    # create branches
    admin_cur.execute("SELECT dolt_branch('dev1', 'main')")
    admin_cur.execute("SELECT dolt_branch('dev2', 'main')")

    admin.close()

def testing():
    admin = pg.connect("host=127.0.0.1 user=postgres password=password dbname=branch_test")
    admin.autocommit = True
    admin_cur = admin.cursor()

    # see what's in the table before we touch it
    print("--- dolt_branch_control (before) ---")
    admin_cur.execute("SELECT * FROM dolt_branch_control")
    pretty(admin_cur)

    # clear all rows
    print("\n--- clearing all rows ---")
    admin_cur.execute("DELETE FROM dolt_branch_control")
    print(f"deleted: {admin_cur.statusmessage}")

    # verify empty
    admin_cur.execute("SELECT * FROM dolt_branch_control")
    rows = admin_cur.fetchall()
    print(f"rows after delete: {len(rows)}")

    # set branch permissions: only alice on dev1, only bob on dev2
    print("\n--- inserting permissions ---")
    admin_cur.execute("INSERT INTO dolt_branch_control VALUES ('%', 'dev1', 'alice', '%', 'write')")
    print("alice -> dev1")
    admin_cur.execute("INSERT INTO dolt_branch_control VALUES ('%', 'dev2', 'bob', '%', 'write')")
    print("bob -> dev2")

    # check what's in the table
    print("\n--- dolt_branch_control ---")
    try:
        admin_cur.execute("SELECT * FROM dolt_branch_control")
        pretty(admin_cur)
    except Exception as e:
        print(f"failed: {e}")

    # alice writes to dev1 (should work)
    print("\n--- alice writes to dev1 ---")
    alice = pg.connect("host=127.0.0.1 user=alice password=alice123 dbname=branch_test/dev1")
    alice.autocommit = True
    alice_cur = alice.cursor()
    try:
        alice_cur.execute("INSERT INTO items VALUES (2, 'alice on dev1')")
        print("success")
    except Exception as e:
        print(f"failed: {e}")

    # alice writes to dev2 (should fail if permissions work)
    print("\n--- alice writes to dev2 (should be denied) ---")
    alice2 = pg.connect("host=127.0.0.1 user=alice password=alice123 dbname=branch_test/dev2")
    alice2.autocommit = True
    alice2_cur = alice2.cursor()
    try:
        alice2_cur.execute("INSERT INTO items VALUES (3, 'alice on dev2')")
        print("success (permissions not enforced)")
    except Exception as e:
        print(f"denied: {e}")


    admin_cur.execute("SELECT * FROM dolt_log")
    pretty(admin_cur)

    # --- namespace control: who can create branches ---
    print("\n--- dolt_branch_namespace_control ---")

    # only alice can create branches starting with "alice-"
    admin_cur.execute("INSERT INTO dolt_branch_namespace_control VALUES ('%', 'alice-%', 'alice', '%')")
    print("added: alice can create alice-* branches")

    # alice creates a branch (should work)
    print("\n--- alice creates alice-experiment ---")
    alice_ns = pg.connect("host=127.0.0.1 user=alice password=alice123 dbname=branch_test")
    alice_ns.autocommit = True
    alice_ns_cur = alice_ns.cursor()
    try:
        alice_ns_cur.execute("SELECT dolt_branch('alice-experiment', 'main')")
        print("success")
    except Exception as e:
        print(f"failed: {e}")

    # bob tries to create an alice- branch (should fail)
    print("\n--- bob creates alice-sneaky (should be denied) ---")
    bob_ns = pg.connect("host=127.0.0.1 user=bob password=bob123 dbname=branch_test")
    bob_ns.autocommit = True
    bob_ns_cur = bob_ns.cursor()
    try:
        bob_ns_cur.execute("SELECT dolt_branch('alice-sneaky', 'main')")
        print("success (namespace not enforced)")
    except Exception as e:
        print(f"denied: {e}")

    # bob creates a non-alice branch (should work, no rule restricts it)
    print("\n--- bob creates bob-work (should work) ---")
    try:
        bob_ns_cur.execute("SELECT dolt_branch('bob-work', 'main')")
        print("success")
    except Exception as e:
        print(f"failed: {e}")

    alice_ns.close()
    bob_ns.close()
    alice.close()
    alice2.close()
    admin.close()


if __name__ == "__main__":
    run_doltgres()
    setup()
    testing()
