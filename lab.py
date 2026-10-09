"""Test transport: run the real selector server in a separate local process.

Socket pairs replace TCP connection establishment only. No frame handling or
file behavior is stubbed. The command-line client can run in another process.
"""
from pathlib import Path
import os
import selectors
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
SERVER_BOOT = '''
import signal, socket, sys
from server import FileServer
app = FileServer(sys.argv[1], unknown=sys.argv[2] == 'yes', timeout=float(sys.argv[3]))
for descriptor in sys.argv[4:]:
    app.attach(socket.socket(fileno=int(descriptor)))
running = True
def stop(*args):
    global running
    running = False
signal.signal(signal.SIGTERM, stop)
try:
    while running:
        app.tick(0.01)
finally:
    app.close()
'''
CLIENT_BOOT = '''
import runpy, socket, sys
fd = int(sys.argv[1])
sys.argv = ['bcurl'] + sys.argv[2:]
connected = False
def connect(*args, **kwargs):
    global connected
    if connected:
        raise RuntimeError('client attempted a second connection')
    connected = True
    sock = socket.socket(fileno=fd)
    sock.settimeout(5)
    return sock
socket.create_connection = connect
runpy.run_path('bcurl', run_name='__main__')
'''


class LocalServer:
    def __init__(self, root, count=1, *, unknown=False, timeout=5):
        self.tcp = os.environ.get('N1_TEST_TRANSPORT') == 'tcp'
        if self.tcp:
            self.clients = []
            args = [sys.executable, 'bserve', str(root), '0', '--timeout', str(timeout)]
            if unknown:
                args.append('--unknown')
            self.process = subprocess.Popen(args, cwd=ROOT, stdout=subprocess.DEVNULL,
                                            stderr=subprocess.PIPE)
            with selectors.DefaultSelector() as ready:
                ready.register(self.process.stderr, selectors.EVENT_READ)
                if not ready.select(5):
                    self.close()
                    raise RuntimeError('TCP server startup timed out')
            line = self.process.stderr.readline().decode()
            if not line.startswith('N1 file server on '):
                self.close()
                raise RuntimeError('TCP startup failed: ' + line)
            self.port = int(line.rsplit(':', 1)[1])
            self.clients = [socket.create_connection(('127.0.0.1', self.port), 5)
                            for _ in range(count)]
            return
        self.clients, servers = [], []
        for _ in range(count):
            a, b = socket.socketpair()
            a.settimeout(5)
            b.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
            self.clients.append(a)
            servers.append(b)
        descriptors = tuple(s.fileno() for s in servers)
        self.process = subprocess.Popen(
            [sys.executable, '-c', SERVER_BOOT, str(root),
             'yes' if unknown else 'no', str(timeout)] + list(map(str, descriptors)),
            pass_fds=descriptors, cwd=ROOT, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE)
        for sock in servers:
            sock.close()

    def cli(self, *args, index=0):
        if self.tcp:
            args = [arg.replace('local:9000', '127.0.0.1:%d' % self.port) for arg in args]
            return subprocess.run([sys.executable, 'bcurl', *args], cwd=ROOT,
                                  capture_output=True, timeout=15)
        sock = self.clients[index]
        return subprocess.run([sys.executable, '-c', CLIENT_BOOT,
                               str(sock.fileno()), *args],
                              cwd=ROOT, pass_fds=(sock.fileno(),),
                              capture_output=True, timeout=15)

    def close(self):
        for sock in self.clients:
            sock.close()
        self.process.terminate()
        try:
            _, error = self.process.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate()
            raise AssertionError('server did not shut down')
        if self.process.returncode not in (0, -15):
            raise AssertionError('server process failed: ' + error.decode())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
