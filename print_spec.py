"""Produce a two-page print layout from SPEC.md using only the standard library."""
from html import escape
from pathlib import Path

root = Path(__file__).resolve().parent
pages = (root / 'SPEC.md').read_text().split('---PAGE---')
assert len(pages) == 2
style = '''
@page { size: A4; margin: 14mm; }
body { margin: 0; color: #15252d; background: #e7ecee; }
section { box-sizing: border-box; width: 182mm; margin: 14mm auto;
          padding: 0; background: white; break-after: page; }
section:last-child { break-after: auto; }
pre { white-space: pre-wrap; font: 9pt/1.22 "Courier New", monospace; }
footer { font: 9pt sans-serif; text-align: right; border-top: 1px solid #89a; padding-top: 4mm; }
@media print { body { background: white; } section { margin: 0; width: auto; } }
'''
blocks = []
for number, page in enumerate(pages, 1):
    text = page.strip()
    assert len(text.splitlines()) <= 66, 'page too long'
    blocks.append('<section><pre>' + escape(text) + '</pre><footer>N1 / %d of 2</footer></section>' % number)
(root / 'SPEC.html').write_text('<!doctype html><html lang="en"><meta charset="utf-8">'
                              '<title>N1 protocol - two-page specification</title><style>'
                              + style + '</style>' + ''.join(blocks) + '</html>')
print('Wrote SPEC.html: two A4 print sections')
