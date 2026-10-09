"""CLI/process integration plus adversarial wire and streaming tests."""
import concurrent.futures
import io
import os
from pathlib import Path
import random
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
import client
from lab import LocalServer
import wire


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name).resolve() / 'public'
        self.root.mkdir()
        self.home = b'<h1>Independent N1 fixture</h1>\n'
        (self.root / 'index.html').write_bytes(self.home)
        self.big = bytes(range(256)) * 8193
        (self.root / 'archive.bin').write_bytes(self.big)
        (self.root / 'space name.txt').write_bytes(b'names with spaces\n')
        (self.root / 'empty').touch()
        (self.root / 'folder').mkdir()
        (self.root / 'folder' / 'index.html').write_bytes(b'nested index')

    def test_cli_two_files_single_connection(self):
        with LocalServer(self.root) as app:
            result = app.cli('-v', 'local:9000/', 'local:9000/archive.bin')
        self.assertEqual(result.returncode, 0, result.stderr[-1000:])
        self.assertEqual(result.stdout, self.home + self.big)
        self.assertIn(b'TX REQUEST id=1', result.stderr)
        self.assertIn(b'TX REQUEST id=2', result.stderr)
        self.assertIn(b'4e 31', result.stderr)
        self.assertNotIn(b'Traceback', result.stderr)

    def test_cli_download_to_disk(self):
        target = self.root.parent / 'download.bin'
        with LocalServer(self.root) as app:
            result = app.cli('-o', str(target), 'local:9000/archive.bin')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, b'')
        self.assertEqual(target.read_bytes(), self.big)

    def test_cli_head_and_http_error_exit(self):
        with LocalServer(self.root) as app:
            result = app.cli('-I', 'local:9000/archive.bin', 'local:9000/missing')
        self.assertEqual(result.returncode, 22, result.stderr)
        self.assertIn(b':status: 200', result.stdout)
        self.assertIn(b':status: 404', result.stdout)
        self.assertIn(('content-length: %d' % len(self.big)).encode(), result.stdout)
        self.assertNotIn(b'file not found', result.stdout)

    def test_cli_unknown_frames_both_directions(self):
        with LocalServer(self.root, unknown=True) as app:
            result = app.cli('--unknown', '-v', 'local:9000/')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, self.home)
        self.assertIn(b'UNKNOWN(68)', result.stderr)
        self.assertIn(b'UNKNOWN(69)', result.stderr)

    def test_recoverable_errors_and_url_paths(self):
        with LocalServer(self.root) as app:
            fetch = client.Fetcher(app.clients[0], 'local')
            for path, want in [('/absent', 404), ('/../secret', 400),
                               ('/%2e%2e/secret', 400), ('/%00', 400),
                               ('/folder/', 200), ('/space%20name.txt', 200),
                               ('/index.html?version=1', 200), ('/empty', 200), ('/', 200)]:
                with self.subTest(path=path):
                    out = io.BytesIO()
                    self.assertEqual(fetch.fetch(path, out)[0], want)
                    if path == '/empty':
                        self.assertEqual(out.getvalue(), b'')

    def test_symlink_root_confinement(self):
        secret = self.root.parent / 'secret'
        secret.write_bytes(b'private file')
        (self.root / 'escape').symlink_to(secret)
        (self.root / 'folder' / 'index.html').unlink()
        (self.root / 'folder' / 'index.html').symlink_to(secret)
        with LocalServer(self.root) as app:
            fetch = client.Fetcher(app.clients[0], 'local')
            for path in ('/escape', '/folder/'):
                out = io.BytesIO()
                self.assertEqual(fetch.fetch(path, out)[0], 400)
                self.assertNotIn(b'private file', out.getvalue())

    def test_streaming_output_is_bounded(self):
        class Sink:
            size, largest = 0, 0
            def write(self, data):
                self.size += len(data)
                self.largest = max(self.largest, len(data))
        sink = Sink()
        with LocalServer(self.root) as app:
            self.assertEqual(client.Fetcher(app.clients[0], 'local').fetch('/archive.bin', sink)[0], 200)
        self.assertEqual(sink.size, len(self.big))
        self.assertLessEqual(sink.largest, 8192)

    def test_slow_reader_does_not_block_second_client(self):
        with LocalServer(self.root, count=2) as app:
            app.clients[0].sendall(self.request(1, '/archive.bin'))
            app.clients[1].settimeout(2)
            out = io.BytesIO()
            self.assertEqual(client.Fetcher(app.clients[1], 'local').fetch('/', out)[0], 200)
            self.assertEqual(out.getvalue(), self.home)

    def test_concurrent_clients(self):
        with LocalServer(self.root, count=16) as app:
            def download(sock):
                out = io.BytesIO()
                result = client.Fetcher(sock, 'local').fetch('/archive.bin', out)
                return result[0] == 200 and out.getvalue() == self.big
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                self.assertTrue(all(pool.map(download, app.clients)))

    @staticmethod
    def request(number, path='/', method='GET', flags=wire.FINISHED):
        return wire.Packet(wire.REQUEST, flags, number, wire.encode_fields([
            (':method', method), (':path', path), ('host', 'local')])).pack()

    def receive(self, sock, number):
        headers, body = None, bytearray()
        while True:
            packet = wire.read_packet(sock)
            if packet.kind not in wire.KNOWN:
                continue
            self.assertEqual(packet.request, number)
            if packet.kind == wire.RESPONSE:
                headers = wire.decode_fields(packet.body)
            else:
                self.assertEqual(packet.kind, wire.CONTENT)
                body += packet.body
            if packet.flags & wire.FINISHED:
                return int(headers[':status']), bytes(body)

    def test_malformed_requests_keep_connection(self):
        payloads = [b'', b'\0\x41', b'\0\x01\xff\0\0',
                    b'\0\x01\0\0\x03BAD\0\0',
                    b'\0\0trailing', b'\0\x01\x04\xff\xffx',
                    wire.encode_fields([(':method', 'GET'), (':path', '/')]),
                    wire.encode_fields([(':method', 'GET'), (':path', '/'), ('host', '')])]
        with LocalServer(self.root) as app:
            sock = app.clients[0]
            number = 1
            for payload in payloads:
                sock.sendall(wire.Packet(wire.REQUEST, 1, number, payload).pack())
                self.assertEqual(self.receive(sock, number)[0], 400)
                sock.sendall(self.request(number + 1))
                self.assertEqual(self.receive(sock, number + 1), (200, self.home))
                number += 2
            sock.sendall(self.request(number, method='POST'))
            self.assertEqual(self.receive(sock, number)[0], 405)
            sock.sendall(self.request(number + 1, flags=0))
            self.assertEqual(self.receive(sock, number + 1)[0], 400)

    def test_fragmentation_pipeline_and_unknown_skip(self):
        with LocalServer(self.root) as app:
            sock = app.clients[0]
            blob = wire.Packet(0xe1, 255, 0, b'\0' * 300).pack() + self.request(1)
            for byte in blob:
                sock.sendall(bytes([byte]))
            self.assertEqual(self.receive(sock, 1), (200, self.home))
            sock.sendall(self.request(2) + wire.Packet(0xe2, 1, 100, b'').pack() + self.request(3))
            self.assertEqual(self.receive(sock, 2), (200, self.home))
            self.assertEqual(self.receive(sock, 3), (200, self.home))

    def test_bad_envelopes_abort_immediately(self):
        for raw in (struct.pack('!2sBBII', b'N1', 16, 1, 1, wire.LIMIT + 1),
                    struct.pack('!2sBBII', b'XX', 16, 1, 1, 0), self.request(0)):
            with self.subTest(raw=raw), LocalServer(self.root) as app:
                sock = app.clients[0]
                sock.sendall(raw)
                answer = wire.read_packet(sock)
                self.assertEqual((answer.kind, answer.request), (wire.ABORT, 0))
                self.assertEqual(sock.recv(1), b'')

    def test_reused_id_aborts(self):
        with LocalServer(self.root) as app:
            sock = app.clients[0]
            sock.sendall(self.request(1))
            self.receive(sock, 1)
            sock.sendall(self.request(1))
            self.assertEqual(wire.read_packet(sock).kind, wire.ABORT)

    def test_idle_and_partial_eof_cleanup(self):
        with LocalServer(self.root, count=2, timeout=0.1) as app:
            app.clients[0].sendall(b'N1\x10')
            app.clients[0].shutdown(socket.SHUT_WR)
            self.assertEqual(app.clients[0].recv(1), b'')
            self.assertEqual(app.clients[1].recv(1), b'')

    def test_independent_golden_request(self):
        # Literal bytes written from the spec, without the project's encoder.
        payload = bytes.fromhex('0003 04 0003 474554 05 0001 2f 01 0001 78')
        raw = bytes.fromhex('4e31 10 01 00000001 00000010') + payload
        self.assertEqual(len(payload), 16)
        with LocalServer(self.root) as app:
            app.clients[0].sendall(raw)
            self.assertEqual(self.receive(app.clients[0], 1), (200, self.home))

    def test_client_rejects_bad_response(self):
        cases = [wire.Packet(wire.CONTENT, 1, 1, b'body before status'),
                 wire.Packet(wire.RESPONSE, 1, 2, wire.encode_fields([(':status', '200')])),
                 wire.Packet(wire.RESPONSE, 1, 1, wire.encode_fields([(':status', '+200')])),
                 wire.Packet(wire.RESPONSE, 1, 1, wire.encode_fields([(':status', '700')])),
                 wire.Packet(wire.RESPONSE, 1, 1, b'\0\x01\xff\0\0')]
        for packet in cases:
            with self.subTest(packet=packet):
                a, b = socket.socketpair()
                a.settimeout(2)
                try:
                    b.sendall(packet.pack())
                    with self.assertRaises(wire.WireError):
                        client.Fetcher(a, 'local').fetch('/', io.BytesIO())
                finally:
                    a.close()
                    b.close()

    def test_header_codec_random_input(self):
        rng = random.Random(17)
        for _ in range(5000):
            try:
                wire.decode_fields(rng.randbytes(rng.randrange(0, 100)))
            except wire.WireError:
                pass
        fields = [('x-project-note', b'literal value'), (':method', b'GET')]
        self.assertEqual(wire.decode_fields(wire.encode_fields(fields)), dict(fields))
        with self.assertRaises(wire.WireError):
            wire.encode_fields([('host', 'one'), ('host', 'two')])

    def test_cli_bad_arguments_do_not_connect(self):
        for args in (['one:9000/', 'two:9000/'], ['local:99999/'],
                     ['--timeout', 'nan', 'local:9000/']):
            result = subprocess.run([sys.executable, 'bcurl', *args], capture_output=True, timeout=3)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertNotIn(b'Traceback', result.stderr)


if __name__ == '__main__':
    if '--tcp' in sys.argv:
        sys.argv.remove('--tcp')
        try:
            with socket.socket() as probe:
                probe.bind(('127.0.0.1', 0))
        except OSError as exc:
            raise SystemExit('TCP tests blocked before startup: %s' % exc)
        os.environ['N1_TEST_TRANSPORT'] = 'tcp'
    unittest.main(verbosity=2)
