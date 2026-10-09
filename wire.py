"""N1 wire vocabulary: twelve-byte envelopes and counted header records."""
from dataclasses import dataclass
import re
import struct

ENVELOPE = struct.Struct('!2sBBII')
SIGNATURE = b'N1'
REQUEST, RESPONSE, CONTENT, ABORT = 0x10, 0x20, 0x21, 0x7e
FINISHED = 1
LIMIT = 262144
KNOWN = {REQUEST, RESPONSE, CONTENT, ABORT}
LABELS = {REQUEST: 'REQUEST', RESPONSE: 'RESPONSE', CONTENT: 'CONTENT', ABORT: 'ABORT'}
NAMES = ('host', 'user-agent', 'accept', ':method', ':path', ':status',
         'content-type', 'content-length', 'server', 'last-modified')
NAME_CODE = {name: index + 1 for index, name in enumerate(NAMES)}
TOKEN = re.compile(r':?[a-z0-9!#$%&\'*+.^_`|~-]+\Z')


class WireError(Exception):
    pass


@dataclass(frozen=True)
class Packet:
    kind: int
    flags: int
    request: int
    body: bytes = b''

    def pack(self):
        if not 0 <= self.request <= 0xffffffff or len(self.body) > LIMIT:
            raise WireError('request id or payload exceeds its limit')
        return ENVELOPE.pack(SIGNATURE, self.kind, self.flags, self.request,
                             len(self.body)) + self.body


def inspect_envelope(raw):
    if len(raw) != ENVELOPE.size:
        raise WireError('incomplete envelope')
    magic, kind, flags, request, length = ENVELOPE.unpack(raw)
    if magic != SIGNATURE:
        raise WireError('expected N1 signature')
    if length > LIMIT:
        raise WireError('payload exceeds 256 KiB')
    return kind, flags, request, length


def extract(buffer):
    """Consume one complete envelope, leaving subsequent bytes untouched."""
    if len(buffer) < ENVELOPE.size:
        return None
    kind, flags, request, length = inspect_envelope(buffer[:ENVELOPE.size])
    end = ENVELOPE.size + length
    if len(buffer) < end:
        return None
    packet = Packet(kind, flags, request, bytes(buffer[ENVELOPE.size:end]))
    del buffer[:end]
    return packet


def encode_fields(fields):
    records = list(fields)
    if len(records) > 64:
        raise WireError('at most 64 fields are allowed')
    seen = set()
    output = bytearray(struct.pack('!H', len(records)))
    for name, value in records:
        if not TOKEN.fullmatch(name) or name in seen:
            raise WireError('invalid or repeated field name')
        seen.add(name)
        raw = value.encode('utf-8') if isinstance(value, str) else value
        if len(raw) > 65535:
            raise WireError('field value too long')
        code = NAME_CODE.get(name, 0)
        output.append(code)
        if not code:
            name_bytes = name.encode('ascii')
            if len(name_bytes) > 255:
                raise WireError('field name too long')
            output += struct.pack('!H', len(name_bytes)) + name_bytes
        output += struct.pack('!H', len(raw)) + raw
    if len(output) > LIMIT:
        raise WireError('header block too long')
    return bytes(output)


def decode_fields(raw):
    cursor = 0

    def take(n):
        nonlocal cursor
        if cursor + n > len(raw):
            raise WireError('truncated header record')
        part = raw[cursor:cursor + n]
        cursor += n
        return part

    def short():
        return int.from_bytes(take(2), 'big')

    count = short()
    if count > 64:
        raise WireError('too many header records')
    result = {}
    for _ in range(count):
        code = take(1)[0]
        if code == 0:
            length = short()
            if not 1 <= length <= 255:
                raise WireError('literal name length must be 1..255')
            try:
                name = take(length).decode('ascii')
            except UnicodeDecodeError:
                raise WireError('literal name is not ASCII') from None
            if not TOKEN.fullmatch(name):
                raise WireError('invalid literal name')
        elif code <= len(NAMES):
            name = NAMES[code - 1]
        else:
            raise WireError('unassigned field code')
        if name in result:
            raise WireError('repeated field name')
        result[name] = take(short())
    if cursor != len(raw):
        raise WireError('bytes remain after the declared header records')
    return result


def read_packet(sock):
    def exact(n):
        result = bytearray()
        while len(result) < n:
            part = sock.recv(n - len(result))
            if not part:
                raise WireError('peer closed before the response finished')
            result.extend(part)
        return bytes(result)
    kind, flags, request, length = inspect_envelope(exact(ENVELOPE.size))
    return Packet(kind, flags, request, exact(length))


def describe(packet):
    raw = packet.pack()
    lines = ['%s id=%d flags=0x%02x payload=%d' %
             (LABELS.get(packet.kind, 'UNKNOWN(%02x)' % packet.kind),
              packet.request, packet.flags, len(packet.body))]
    for offset in range(0, len(raw), 16):
        part = raw[offset:offset + 16]
        lines.append('%04x  %-47s  %s' %
                     (offset, part.hex(' '), ''.join(chr(b) if 32 <= b < 127 else '.' for b in part)))
    return '\n'.join(lines)
