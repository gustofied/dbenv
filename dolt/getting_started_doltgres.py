import os
import subprocess
import time
from pathlib import Path
import psycopg as pg

DIR = Path(__file__).parent

def run_doltgres():
    # config.yaml paths (data_dir, cfg_dir) resolve relative to cwd,
    # so we chdir to the script's folder before starting the server.
    os.chdir(DIR)
    subprocess.run(["pkill", "-xi", "doltgres"], stderr=subprocess.DEVNULL)
    time.sleep(1)
    subprocess.Popen(["doltgres", "--config", str(DIR / "config.yaml")])
    time.sleep(2)


def talk_to_doltgres():
    with pg.connect("host=127.0.0.1 user=postgres password=password dbname=postgres") as conn:
        conn.execute("SELECT 1")

if __name__ == "__main__":
    run_doltgres()
    talk_to_doltgres()
