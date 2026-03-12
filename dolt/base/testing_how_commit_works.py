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
    shutil.rmtree(DIR / "data/testing_how_commit_works", ignore_errors=True)
    shutil.rmtree(DIR / ".doltcfg/testing_how_commit_works", ignore_errors=True)
    subprocess.Popen(["doltgres", "--config", str(DIR / "testing_how_commit_works.yaml")])
    time.sleep(2)

def setup():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        cur = conn.cursor()
        cur.execute("CREATE DATABASE testing_commit")

    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=testing_commit") as conn:
        cur = conn.cursor()
        
        # creating two users/roles
        cur.execute("CREATE ROLE alice WITH LOGIN PASSWORD 'alice123'")
        cur.execute("CREATE ROLE bob WITH LOGIN PASSWORD 'bob123'")
        cur.execute("GRANT ALL ON DATABASE testing_commit TO alice, bob")


def testing():
    admin = pg.connect("host=127.0.0.1 user=postgres password=password dbname=testing_commit")
    alice = pg.connect("host=127.0.0.1 user=alice password=alice123 dbname=testing_commit")
    bob = pg.connect("host=127.0.0.1 user=bob password=bob123 dbname=testing_commit")

    admin.autocommit = True
    alice.autocommit = True

    admin_cur = admin.cursor()
    alice_cur = alice.cursor()

    admin_cur.execute("CREATE TABLE commit_testing(id int8 primary key, name text, value int8)")
    admin_cur.execute("GRANT ALL ON TABLE commit_testing TO alice, bob")

    alice_cur.execute("INSERT INTO commit_testing VALUES (1, 'hello', 42)")
    admin.execute("INSERT INTO commit_testing VALUES (2, 'hello', 42)")



    admin_cur.execute("SELECT * from dolt_log")
    pretty(admin_cur)

    alice.close()
    bob.close()
    admin.close()


if __name__ == "__main__":
    run_doltgres()
    setup()
    testing()
