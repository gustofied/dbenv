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
    shutil.rmtree(DIR / "data/testing_access_management", ignore_errors=True)
    shutil.rmtree(DIR / ".doltcfg/testing_access_management", ignore_errors=True)
    subprocess.Popen(["doltgres", "--config", str(DIR / "testing_access_management.yaml")])
    time.sleep(2)

def setup():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        cur = conn.cursor()
        cur.execute("CREATE DATABASE access_test")

def testing():
    admin = pg.connect("host=127.0.0.1 user=postgres password=password dbname=access_test")
    admin.autocommit = True
    admin_cur = admin.cursor()

    # create roles
    admin_cur.execute("CREATE ROLE alice WITH LOGIN PASSWORD 'alice123'")
    admin_cur.execute("CREATE ROLE bob WITH LOGIN PASSWORD 'bob123'")

    # create a table
    admin_cur.execute("CREATE TABLE items(id int8 primary key, name text)")

    # grant database and table access
    admin_cur.execute("GRANT ALL ON DATABASE access_test TO alice, bob")
    admin_cur.execute("GRANT ALL ON TABLE items TO alice, bob")

    # connect as alice and bob
    alice = pg.connect("host=127.0.0.1 user=alice password=alice123 dbname=access_test")
    alice.autocommit = True
    alice_cur = alice.cursor()

    bob = pg.connect("host=127.0.0.1 user=bob password=bob123 dbname=access_test")
    bob.autocommit = True
    bob_cur = bob.cursor()

    # alice and bob write
    alice_cur.execute("INSERT INTO items VALUES (1, 'alice item')")
    bob_cur.execute("INSERT INTO items VALUES (2, 'bob item')")

    # check from admin
    admin_cur.execute("SELECT * FROM items")
    pretty(admin_cur)

    alice.close()
    bob.close()
    admin.close()


if __name__ == "__main__":
    run_doltgres()
    setup()
    testing()
