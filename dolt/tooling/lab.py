import server
import utils
import psycopg as pg
from utils import pretty
from pathlib import Path

# DIR = Path(__file__).parent.resolve()
cfg= "lab.yaml"
DB = "tooling_lab"

def init_db():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
        print(cur.statusmessage)

def lab():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=tooling_lab") as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM dolt_log")
        cols, rows = pretty(cur)
    return [dict(zip(cols, row)) for row in rows]

if __name__ == "__main__":
    doltgres = server.fresh_start(cfg)
    init_db()
    lab()
    doltgres.wait()
