import server 
import utils
import psycopg as pg
from utils import pretty, get_memory_mb, get_disk_mb
from pathlib import Path


# DIR = Path(__file__).parent.resolve()
cfg= "lab.yaml"
DB = "tooling_lab"

def setup():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE {DB}")
        print(cur.statusmessage)

if __name__ == "__main__":
    doltgres = server.run(cfg)
    setup()
    doltgres.wait()