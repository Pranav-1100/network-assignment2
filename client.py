"""Blocking N1 client that streams file bytes directly to an output sink."""
import argparse
import math
import socket
import sys
from urllib.parse import urlsplit
import wire


class Fetcher:
    def __init__(self, sock, host, trace=None):
        self.sock, self.host, self.trace = sock, host, trace
        self.next_id = 1

    def send(self, packet):
        if self.trace:
            self.trace('TX', packet)
        self.sock.sendall(packet.pack())

    def fetch(self, path, output, *, head=False, unknown=False):
        request = self.next_id
        if request > 0xffffffff:
            raise wire.WireError('request ids exhausted; start a new invocation')
        self.next_id += 1
        if unknown:
            self.send(wire.Packet(0x69, 0xff, 0, b'optional client extension'))
        fields = [('host', self.host), (':path', path),
                  ('accept', '*/*'), (':method', 'HEAD' if head else 'GET'),
                  ('user-agent', 'n1-fetch/1')]
        self.send(wire.Packet(wire.REQUEST, wire.FINISHED, request, wire.encode_fields(fields)))
        headers = None
        while True:
            packet = wire.read_packet(self.sock)
            if self.trace:
                self.trace('RX', packet)
            if packet.kind not in wire.KNOWN:
                continue
            if packet.kind == wire.ABORT:
                if packet.request != 0:
                    raise wire.WireError('ABORT must use request id zero')
                raise wire.WireError('server aborted: ' + packet.body.decode('utf-8', 'replace'))
            if packet.request != request:
                raise wire.WireError('response carries the wrong request id')
            if packet.kind == wire.RESPONSE:
                if headers is not None:
                    raise wire.WireError('response headers repeated')
                headers = wire.decode_fields(packet.body)
                if {n for n in headers if n.startswith(':')} != {':status'}:
                    raise wire.WireError('response requires only :status pseudo-field')
                code = headers[':status']
                if len(code) != 3 or not code.isdigit() or not 200 <= int(code) <= 599:
                    raise wire.WireError('invalid status')
                if head and not packet.flags & wire.FINISHED:
                    raise wire.WireError('HEAD response must end at its headers')
            elif packet.kind == wire.CONTENT:
                if headers is None or head:
                    raise wire.WireError('unexpected CONTENT')
                output.write(packet.body)
            else:
                raise wire.WireError('unexpected frame type from server')
            if packet.flags & wire.FINISHED:
                return int(headers[':status']), headers


def address(raw):
    parsed = urlsplit(raw if '://' in raw else 'n1://' + raw)
    if parsed.scheme != 'n1' or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('use n1://host:port/path or host:port/path')
    port = parsed.port if parsed.port is not None else 9000
    if not 1 <= port <= 65535:
        raise ValueError('port must be 1..65535')
    target = parsed.path or '/'
    if parsed.query:
        target += '?' + parsed.query
    return parsed.hostname, port, target


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('urls', nargs='+')
    parser.add_argument('-v', action='store_true', help='hexdump all frames to stderr')
    parser.add_argument('-I', '--head', action='store_true')
    parser.add_argument('-o', '--output')
    parser.add_argument('--unknown', action='store_true')
    parser.add_argument('--timeout', type=float, default=10)
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error('timeout must be positive and finite')
    try:
        targets = [address(url) for url in args.urls]
        host, port, _ = targets[0]
        if any((h, p) != (host, port) for h, p, _ in targets):
            raise ValueError('all URLs must use one host and port')
    except ValueError as exc:
        print('bcurl: %s' % exc, file=sys.stderr)
        return 2
    try:
        sock = socket.create_connection((host, port), args.timeout)
    except OSError as exc:
        print('bcurl: connection failed: %s' % exc, file=sys.stderr)
        return 7
    output = None
    def trace(direction, packet):
        print(direction + ' ' + wire.describe(packet), file=sys.stderr)
    try:
        with sock:
            output = open(args.output, 'wb') if args.output else sys.stdout.buffer
            authority = ('[%s]:%d' if ':' in host else '%s:%d') % (host, port)
            fetcher = Fetcher(sock, authority, trace if args.v else None)
            exit_code = 0
            for _, _, path in targets:
                status, headers = fetcher.fetch(path, output, head=args.head, unknown=args.unknown)
                if args.head:
                    for name, value in headers.items():
                        output.write(name.encode() + b': ' + value + b'\n')
                if status >= 400:
                    exit_code = 22
            output.flush()
            return exit_code
    except (OSError, wire.WireError) as exc:
        print('bcurl: %s' % exc, file=sys.stderr)
        return 1
    finally:
        if args.output and output is not None:
            output.close()


if __name__ == '__main__':
    sys.exit(main())
