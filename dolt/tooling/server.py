import os
import shutil
import subprocess
import time
from pathlib import Path
import requests

DIR = Path(__file__).parent

def install():
    if shutil.which("doltgres") is not None:
        print("doltgres already installed!")
    else:
        r = requests.get('https://github.com/dolthub/doltgresql/releases/latest/download/install.sh')
        r.raise_for_status()
        subprocess.run(["sudo", "bash", "-c", r.text], check=True)


def run(config_name):
    os.chdir(DIR)
    subprocess.run(["pkill", "-xi", "doltgres"], stderr=subprocess.DEVNULL)
    time.sleep(1)
    Path("/tmp/.s.PGSQL.5432").unlink(missing_ok=True)
    # clean slate
    data_name = config_name.replace(".yaml", "")
    shutil.rmtree(DIR / f"data/{data_name}", ignore_errors=True)
    shutil.rmtree(DIR / f".doltcfg/{data_name}", ignore_errors=True)
    doltgres = subprocess.Popen(["doltgres", "--config", str(DIR / config_name)])
    time.sleep(2)
    return doltgres


def serve(config_name):
    install()
    doltgres = run(config_name)
    doltgres.wait()


def terminate(doltgres):
    doltgres.terminate()
    doltgres.wait()
