import os
import shutil
import subprocess
import time
from pathlib import Path
import psycopg as pg

def pretty_table(cur):
    if not cur.description:
        return
    cols = [d.name for d in cur.description]
    rows = cur.fetchall()
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows)) for i, c in enumerate(cols)]
    header = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
    sep = "-+-".join("-" * w for w in widths)
    print(header)
    print(sep)
    for r in rows:
        print(" | ".join(str(v).ljust(w) for v, w in zip(r, widths)))

DIR = Path(__file__).parent

def run_doltgres():
    # config paths (data_dir, cfg_dir) resolve relative to cwd,
    # so we chdir to the script's folder before starting the server.
    os.chdir(DIR)
    subprocess.run(["pkill", "-xi", "doltgres"], stderr=subprocess.DEVNULL)
    time.sleep(1)
    # remove stale unix socket so doltgres doesn't warn on restart
    # hacky, but I think having this reminds that unix socket could be used instead of tcp
    Path("/tmp/.s.PGSQL.5432").unlink(missing_ok=True)
    # clean slate: wipe data and config so every run starts fresh
    shutil.rmtree(DIR / "data/getting_started", ignore_errors=True)
    shutil.rmtree(DIR / ".doltcfg/getting_started", ignore_errors=True)
    subprocess.Popen(["doltgres", "--config", str(DIR / "getting_started.yaml")])
    time.sleep(2)


def talk_to_doltgres():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        # kinda explicit but who cares
        cur = conn.cursor()
        cur.execute("SELECT 1")
        print(cur.fetchone())
        cur.execute("CREATE DATABASE IF NOT EXISTS getting_started")

    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=getting_started") as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        print(cur.fetchone())

        cur.execute("""CREATE TABLE IF NOT EXISTS employees(
                id int8,
                last_name text,
                first_name text,
                primary key(id))
        """)
        print(cur.statusmessage)

        cur.execute("""CREATE TABLE IF NOT EXISTS teams(
                id int8,
                team_name text,
                primary key(id))
        """)
        print(cur.statusmessage) # nice ways to check things when return is not rows.

        cur.execute("""CREATE TABLE IF NOT EXISTS employees_teams(
                team_id int8,
                employee_id int8,
                primary key(team_id, employee_id),
                foreign key (team_id) references teams(id),
                foreign key (employee_id) references employees(id))
        """)
        print(cur.statusmessage)

        # a way of doing a check, lists relations
        cur.execute("""SELECT schemaname AS schema, tablename AS name, 'table' AS type, tableowner AS owner
                FROM pg_tables WHERE schemaname = 'public'""")
        pretty_table(cur)

        print("- - - -")

        cur.execute("select * from dolt_status;")
        pretty_table(cur)

        cur.execute("select dolt_add('teams', 'employees', 'employees_teams');")
        
        print("- - - -")

        cur.execute("select * from dolt_status;")
        pretty_table(cur)

        time.sleep(3)


        cur.execute("select dolt_commit('-m', 'Created initial schema');")
        pretty_table(cur)

        cur.execute("select * from dolt_log;")
        pretty_table(cur)


if __name__ == "__main__":
    run_doltgres()
    talk_to_doltgres()
