"""Clean build outputs then build the wheel -- keeps the bundle's dist/*.whl glob unambiguous."""

import shutil
import subprocess
import sys

shutil.rmtree("dist", ignore_errors=True)
shutil.rmtree("build", ignore_errors=True)
sys.exit(subprocess.call([sys.executable, "-m", "build", "--wheel"]))
