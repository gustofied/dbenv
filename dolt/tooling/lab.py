import server 
import utils
import psycopg as pg
from utils import pretty
from pathlib import Path

# DIR = Path(__file__).parent.resolve()
cfg= "lab.yaml"
DB = "tooling_lab"

def setup():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE {DB}")
        print(cur.statusmessage)

def lab():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=tooling_lab") as conn:
        admin_cur = conn.cursor()
        admin_cur.execute("SELECT * FROM dolt_log();")
        pretty(admin_cur)

if __name__ == "__main__":
    doltgres = server.run(cfg)
    setup()
    lab()
    doltgres.wait()
