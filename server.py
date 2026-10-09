"""A single-threaded selector loop; file responses are produced incrementally."""
import argparse
from email.utils import formatdate
import math
import mimetypes
import os
from pathlib import Path
import selectors
import socket
import stat
import sys
import time
from urllib.parse import unquote
import wire

CHUNK = 8192


class BadRequest(Exception):
    def __init__(self, status, message):
        self.status, self.message = status, message


def locate(root, url):
    if not url.startswith('/'):
        raise BadRequest(400, 'path must start with /')
    path = url.split('?', 1)[0].split('#', 1)[0]
    try:
        path = unquote(path, errors='strict')
    except UnicodeDecodeError:
        raise BadRequest(400, 'invalid UTF-8 in path') from None
    if '\0' in path or '..' in path.split('/'):
        raise BadRequest(400, 'invalid path segment')
    target = root.joinpath(*[p for p in path.split('/') if p not in ('', '.')]).resolve()
    if target.is_dir():
        target = (target / 'index.html').resolve()
    if target != root and root not in target.parents:
        raise BadRequest(400, 'path leaves the document root')
    return target


def reply(root, packet, inject_unknown=False):
    """Yield one frame at a time; no whole-file response buffer."""
    file = None
    head = False
    extra = []
    try:
        try:
            if not packet.flags & wire.FINISHED:
                raise BadRequest(400, 'requests must set FINISHED')
            fields = wire.decode_fields(packet.body)
            head = fields.get(':method') == b'HEAD'
            if set(n for n in fields if n.startswith(':')) != {':method', ':path'}:
                raise BadRequest(400, 'request needs only :method and :path pseudo-fields')
            if not fields.get('host', b'').strip():
                raise BadRequest(400, 'missing host')
            method = fields[':method'].decode('ascii')
            path = fields[':path'].decode('utf-8')
            if method not in ('GET', 'HEAD'):
                extra = [('allow', 'GET, HEAD')]
                raise BadRequest(405, 'only GET and HEAD are implemented')
            target = locate(root, path)
            if not target.is_file():
                raise BadRequest(404, 'file not found')
            file = target.open('rb')
            metadata = os.fstat(file.fileno())
            if not stat.S_ISREG(metadata.st_mode):
                raise BadRequest(404, 'not a regular file')
            size = metadata.st_size
            kind = mimetypes.guess_type(str(target))[0] or 'application/octet-stream'
            status, body = 200, b''
            extra += [('last-modified', formatdate(metadata.st_mtime, usegmt=True))]
        except (wire.WireError, UnicodeError) as exc:
            status, body = 400, (str(exc) + '\n').encode()
            size, kind = len(body), 'text/plain; charset=utf-8'
        except (BadRequest, OSError, RuntimeError) as exc:
            if isinstance(exc, BadRequest):
                status, message = exc.status, exc.message
            elif isinstance(exc, PermissionError):
                status, message = 403, 'file is not readable'
            elif isinstance(exc, FileNotFoundError):
                status, message = 404, 'file not found'
            elif isinstance(exc, RuntimeError):
                status, message = 400, 'invalid symlink path'
            else:
                status, message = 500, 'file could not be read'
            body = (message + '\n').encode()
            size, kind = len(body), 'text/plain; charset=utf-8'
        if inject_unknown:
            yield wire.Packet(0x68, 0xa0, packet.request, b'extension example')
        fields = [(':status', str(status)), ('content-length', str(size)),
                  ('content-type', kind), ('server', 'framed-files/1'),
                  ('date', formatdate(usegmt=True))] + extra
        yield wire.Packet(wire.RESPONSE, wire.FINISHED if head else 0,
                          packet.request, wire.encode_fields(fields))
        if head:
            return
        if status == 200:
            remaining = size
            while remaining:
                chunk = file.read(min(CHUNK, remaining))
                if not chunk:
                    raise wire.WireError('file changed during transfer')
                remaining -= len(chunk)
                yield wire.Packet(wire.CONTENT, 0, packet.request, chunk)
        elif body:
            yield wire.Packet(wire.CONTENT, 0, packet.request, body)
        yield wire.Packet(wire.CONTENT, wire.FINISHED, packet.request)
    finally:
        if file is not None:
            file.close()


class Session:
    def __init__(self, sock):
        self.sock = sock
        self.incoming = bytearray()
        self.outgoing = memoryview(b'')
        self.source = None
        self.last_id = 0
        self.ending = False
        self.active = time.monotonic()


class FileServer:
    def __init__(self, root, *, timeout=30, unknown=False, verbose=False):
        self.root = Path(root).resolve()
        self.timeout, self.unknown, self.verbose = timeout, unknown, verbose
        self.selector = selectors.DefaultSelector()
        self.peers = {}
        self.listener = None

    def attach(self, sock):
        sock.setblocking(False)
        session = Session(sock)
        self.peers[sock] = session
        self.selector.register(sock, selectors.EVENT_READ, session)

    def listen(self, host, port):
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((host, port))
        self.listener.listen(64)
        self.listener.setblocking(False)
        self.selector.register(self.listener, selectors.EVENT_READ, None)
        return self.listener.getsockname()

    def drop(self, session):
        self.selector.unregister(session.sock)
        self.peers.pop(session.sock)
        if session.source is not None:
            session.source.close()
        session.sock.close()

    def abort(self, session, message):
        if session.source is not None:
            session.source.close()
            session.source = None
        packet = wire.Packet(wire.ABORT, wire.FINISHED, 0,
                             message.encode('utf-8')[:512])
        session.outgoing = memoryview(packet.pack())
        session.ending = True
        self.selector.modify(session.sock, selectors.EVENT_WRITE, session)

    def advance(self, session):
        while not session.outgoing:
            if session.ending:
                self.drop(session)
                return
            if session.source is not None:
                try:
                    packet = next(session.source)
                    if self.verbose:
                        print('TX ' + wire.describe(packet), file=sys.stderr)
                    session.outgoing = memoryview(packet.pack())
                    break
                except StopIteration:
                    session.source = None
            packet = wire.extract(session.incoming)
            if packet is None:
                break
            if self.verbose:
                print('RX ' + wire.describe(packet), file=sys.stderr)
            if packet.kind not in wire.KNOWN:
                continue
            if packet.kind != wire.REQUEST:
                raise wire.WireError('expected REQUEST packet')
            if not packet.request or packet.request <= session.last_id:
                raise wire.WireError('request ids must increase and be nonzero')
            session.last_id = packet.request
            session.source = reply(self.root, packet, self.unknown)
        mask = selectors.EVENT_WRITE if session.outgoing else selectors.EVENT_READ
        self.selector.modify(session.sock, mask, session)

    def tick(self, wait=0.05):
        for key, events in self.selector.select(wait):
            session = key.data
            if session is None:
                try:
                    sock, _ = self.listener.accept()
                    self.attach(sock)
                except (BlockingIOError, ConnectionAbortedError):
                    pass
                continue
            try:
                if events & selectors.EVENT_READ:
                    part = session.sock.recv(32768)
                    if not part:
                        self.drop(session)
                        continue
                    session.incoming.extend(part)
                if events & selectors.EVENT_WRITE:
                    sent = session.sock.send(session.outgoing)
                    session.outgoing = session.outgoing[sent:]
                session.active = time.monotonic()
                self.advance(session)
            except BlockingIOError:
                pass
            except (OSError, ConnectionError):
                self.drop(session)
            except wire.WireError as exc:
                self.abort(session, str(exc))
        now = time.monotonic()
        for session in list(self.peers.values()):
            if now - session.active > self.timeout:
                self.drop(session)

    def close(self):
        for session in list(self.peers.values()):
            self.drop(session)
        if self.listener:
            try:
                self.selector.unregister(self.listener)
            except KeyError:
                pass
            self.listener.close()
        self.selector.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('port', type=int, nargs='?', default=9000)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--timeout', type=float, default=30)
    parser.add_argument('--unknown', action='store_true')
    parser.add_argument('-v', action='store_true')
    args = parser.parse_args(argv)
    if not args.root.is_dir() or not 0 <= args.port <= 65535:
        parser.error('supply a directory and a port in 0..65535')
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error('timeout must be positive and finite')
    app = FileServer(args.root, timeout=args.timeout, unknown=args.unknown, verbose=args.v)
    try:
        host, port = app.listen(args.host, args.port)
        print('N1 file server on %s:%d' % (host, port), file=sys.stderr)
        while True:
            app.tick()
    except KeyboardInterrupt:
        return 0
    except OSError as exc:
        print('bserve: %s' % exc, file=sys.stderr)
        return 1
    finally:
        app.close()


if __name__ == '__main__':
    sys.exit(main())
