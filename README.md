# Framed Files: N1

A binary, HTTP-like file protocol for the Network Architecture course
project: a server (`bserve`), a client (`bcurl`) and the two-page spec that
connects them. Python 3.9+ and its standard library; no dependencies.

N1 has a 12-byte, version-tagged envelope, separate request and response
packet types, sequential 32-bit request ids, and counted header records with
ten static name codes. File bytes are streamed in 8 KiB chunks. The server
is a single selector event loop, so many connections share one thread.

## Run

From this folder, start the server:

```sh
./bserve ./www 9000
```

Then use another terminal:

```sh
./bcurl -v localhost:9000/index.html
./bcurl localhost:9000/index.html localhost:9000/notes.txt
./bcurl -I localhost:9000/index.html
./bcurl -o /tmp/n1-notes.txt localhost:9000/notes.txt
```

One invocation uses one connection. `-v` prints every frame as a hex dump to
stderr; response bodies go to stdout or `-o`. `-I` requests metadata only.
Exit codes: 0 success, 22 a 4xx/5xx response, 7 connection failed,
2 invalid arguments, 1 transport/protocol/output failure. Partial files may
remain if a download fails; check the exit code.

To demonstrate extension handling, run `bserve` with `--unknown`, then run
`bcurl --unknown -v localhost:9000/`. Both receivers skip the extra packets.

## Hand-in materials

- `SPEC.md`: normative protocol, with widths, header table and error rules.
- `SPEC.html`: two A4 print sections; open in a browser and print/save as PDF
  with browser headers/footers disabled. Rebuild with `python3 print_spec.py`.
- `bserve`, `bcurl`, `server.py`, `client.py`, `wire.py`: runnable programs.
- `docs/exchange.md`: complete recorded request and response with annotations.

`www/` contains the demonstration document root. `capture.py` records real
protocol traffic, `lab.py` supplies the socket-pair test transport,
and `test_end_to_end.py` exercises the implementation.

## Tests

```sh
python3 test_end_to_end.py
python3 test_end_to_end.py --tcp
```

The default suite starts the production server in a separate process and
connects it with local stream socket pairs. CLI tests launch the actual
`bcurl` entry point in another process; only its connection creation is
substituted. No frame, filesystem, client or server behavior is mocked.
The `--tcp` variant starts the real TCP listener and the normal CLI without
that substitution. It checks permission to bind before starting the suite.

Both variants pass all **18 end-to-end tests**, including a 2,097,408-byte
binary download, file output, HEAD and error exits, one connection for
multiple files, both-direction unknown packets, 16 concurrent clients,
slow-reader isolation, malformed-request recovery, exact-length framing,
fragmentation/pipelining, symlink confinement, client validation, a manually
encoded request, and 5,000 random decoder inputs. See `docs/test-results.txt`.

## Regenerate the exchange

With the TCP server running (`./bserve ./www 9000`):

```sh
python3 capture.py --port 9000
```

Or use a separate server process over a permitted local socket pair:

```sh
python3 capture.py --local
```

The capture states which transport was used, includes every frame byte,
and labels each envelope field and header-record byte group by offset.

## Limits

The receive limit is 256 KiB per frame; at most 64 header records per block.
The server pauses reading a connection while writing its current response,
so a slow reader cannot accumulate an unbounded output queue. One slow
network peer does not block the others, although filesystem reads are
synchronous and slow storage can delay the event loop. Serve a trusted,
stable root. There is no TLS, request body, multiplexing, authentication,
flow-control protocol or automatic reconnect.
