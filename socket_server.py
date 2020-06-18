from subprocess import Popen, PIPE
import socket
import sys
from os import kill
import signal


log_pipe = Popen(['python', 'log_producer.py'], shell=False, stdout=PIPE)
TCP_IP = '127.0.0.1'
TCP_PORT = 6666

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

try:
    while True:
        line = log_pipe.stdout.readline()
        if line:
            print("receive: ", line)
            s.send(line)
        else:
            break

    s.close()
except KeyboardInterrupt:
    kill(log_pipe.pid, signal.SIGTERM)
