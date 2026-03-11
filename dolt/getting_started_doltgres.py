import os
import subprocess
import time
from pathlib import Path

DIR = Path(__file__).parent

def run_doltgres():
    # config.yaml paths (data_dir, cfg_dir) resolve relative to cwd,
    # so we chdir to the script's folder before starting the server.
    os.chdir(DIR)
    subprocess.Popen(["doltgres", "--config", str(DIR / "config.yaml")])
    time.sleep(2)

if __name__ == "__main__":
    run_doltgres()
