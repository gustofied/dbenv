import os
import subprocess
import time
from pathlib import Path

DIR = Path(__file__).parent

def kill_existing():
    subprocess.run(["pkill", "-xi", "doltgres"], stderr=subprocess.DEVNULL)
    time.sleep(1)
    Path("/tmp/.s.PGSQL.5432").unlink(missing_ok=True)

def main():
    os.chdir(DIR)
    kill_existing()
    proc = subprocess.Popen(["doltgres", "--config", str(DIR / "server.yaml")])
    time.sleep(2)

    print("server running on localhost:5432")
    print("ctrl+c to stop\n")

    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        print("\nserver stopped")

if __name__ == "__main__":
    main()
