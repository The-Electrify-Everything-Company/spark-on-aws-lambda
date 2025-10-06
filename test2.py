import subprocess
import sys
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

# Run spark-submit and capture full output
result = subprocess.run(
    ["spark-submit", "/var/task/test.py"],
    env=os.environ,
    check=True,
    stdout=sys.stdout,
    stderr=sys.stderr,
    text=True
)
