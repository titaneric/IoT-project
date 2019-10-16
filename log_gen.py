import time
import random
import os
from datetime import datetime, timedelta

import numpy as np
from faker import Faker


line = ("{receive_time},{type_},{source_ip},{from_port},{dest_ip},{to_port},"
        "{application},{action},{session_end},{byte_receive},{byte_send},"
        "{ip_protocol},{packet_receive},{packet_send},{start_time}")

def generate_log_line():
    fake = Faker()
    start_time = datetime.now()
    receive_time = start_time + timedelta(seconds=3)
    source_ip = fake.ipv4()
    dest_ip = fake.ipv4()
    to_port = 80
    from_port = 22
    type_="end"
    application = "dns"
    action = "allow"
    session_end = "aged-out"
    byte_receive = random.randrange(1, 1000, 1)
    byte_send = random.randrange(1, 1000, 1)
    ip_protocol = "udp"
    packet_receive = random.randrange(1, 1000, 1)
    packet_send = random.randrange(1, 1000, 1)


    log_line = line.format(**locals())

    return log_line


if __name__ == "__main__":
    for i in range(3):
        log = generate_log_line()
        print(log)