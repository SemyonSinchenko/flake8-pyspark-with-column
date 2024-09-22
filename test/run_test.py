import subprocess
from pathlib import Path
from subprocess import PIPE

ERR1 = "PSPRK001 Usage of withColumn in a loop detected, use withColumns or select instead!"
ERR2 = "PSPRK002 Usage of withColumn in reduce detected, use withColumns or select instead!"


if __name__ == "__main__":
    file_to_check = Path(__file__).parent.joinpath("bad_pyspark_script.py")
    run_flake = subprocess.run(["flake8", str(file_to_check.absolute())], stdout=PIPE)
    assert ERR1 in run_flake.stdout.decode()
    assert ERR2 in run_flake.stdout.decode()
