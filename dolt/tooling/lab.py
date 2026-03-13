from psycopg.connection import Connection
import server
import utils
import psycopg as pg
from utils import pretty
from pathlib import Path

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
        value int8
    )""")
    cur.execute(f"SELECT dolt_add('{table}')")
    h = cur.execute("SELECT dolt_commit('-m', 'base schema')").fetchone()[0][0]
    return h

def create_roles(cur, num_users):
    for i in range(num_users):
        user = f"user_{i}"
        cur.execute(f"CREATE ROLE {user} WITH LOGIN PASSWORD '{user}pass'")
        cur.execute(f"GRANT ALL ON DATABASE {DB} TO {user}") # could probs be part of the later rollout thingy granting accessing?

def create_db_worlds(admin_cur, table, base_hash, num_worlds):
    admin_cur.execute(f"SELECT dolt_branch('worlds', '{base_hash}')")
    worlds_conn= pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}/worlds")
    worlds_conn.autocommit = True
    cur = worlds_conn.cursor()
    world_snapshots = {}
    for w in range(num_worlds):
        cur.execute(f"SELECT dolt_reset('--hard', '{base_hash}')")  # ty:ignore[no-matching-overload]
        cur.execute(f"INSERT INTO {table} VALUES ({w * 10000}, 'world_{w}', {w})")  # ty:ignore[no-matching-overload]
        h = cur.execute(f"SELECT dolt_commit('-Am', 'world {w}')").fetchone()[0][0]  # ty:ignore[no-matching-overload]
        world_snapshots[f"world_{w}"] = h
    worlds_conn.close()
    return world_snapshots

def lab():
    admin = pg.connect(f"host=127.0.0.1 user=postgres password=password dbname={DB}")
    admin.autocommit = True
    cur = admin.cursor()

    base_hash = init_schema(cur, table="world")
    print("base hash:", base_hash)

    create_roles(cur, num_users=3)

    worlds = create_db_worlds(cur, table="world", base_hash=base_hash, num_worlds=3)
    print("worlds:", worlds)

    cur.execute("SELECT * FROM dolt_log")
    pretty(cur)
    admin.close()


if __name__ == "__main__":
    doltgres = server.fresh_start(cfg)
    init_db()
    lab()
    doltgres.wait()
