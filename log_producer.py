import random
from time import sleep
import sys

from gen_log import generate_log_line

while True:
    log = generate_log_line()
    sys.stdout.write(log + "\n")
    sys.stdout.flush()
    sleep_sec = random.randint(1, 10)
    sleep(sleep_sec)

