"""
sql_errors: show where a Spark SQL query failed.

    from sql_errors import query_context, failure_block

query_context(sql, msg)
    When Spark's error says `line N pos M` (or `line N, pos M`), returns the query
    lines around line N, with an arrow on line N. Returns [] when there is no line.

failure_block(title, where, msg, sql=None, not_run=None)
    The full '####' block every tool prints when it stops: what failed, where
    (a dict of label -> value, printed in order), the error, the query lines, and
    what did not run.

Used by: python/sql_pipeline/rerun_table.py, python/monitoring/*.py
On the server, copy this file next to the tool that imports it.
"""
import re

_LINE = re.compile(r'line (\d+),? pos (\d+)')


def query_context(sql, msg, around=2, width=200):
    m = _LINE.search(msg or '')
    if not m or not sql:
        return []
    ln, lines = int(m.group(1)), sql.splitlines()
    if not 1 <= ln <= len(lines):
        return []
    out = [f"  query near line {ln} (after sql_var replacement):"]
    for k in range(max(1, ln - around), min(len(lines), ln + around) + 1):
        out.append(f"    {'>' if k == ln else ' '} {k:>4} | {lines[k - 1][:width]}")
    return out


def failure_block(title, where, msg, sql=None, not_run=None, err_type=None):
    msg = (msg or '').strip()
    width = max([len(k) for k in where] + [5])
    out = ['', '#' * 90, f"FAILED at {title}"]
    for k, v in where.items():
        out.append(f"  {k.ljust(width)}: {v}")
    first = msg.splitlines()[0][:500] if msg else (err_type or 'unknown error')
    out.append(f"  {'error'.ljust(width)}: {first}")
    for l in msg.splitlines()[1:8]:
        out.append(' ' * (width + 4) + l[:300])
    out += query_context(sql, msg)
    if not_run:
        out.append(f"  not run: {', '.join(not_run)}")
    out.append('#' * 90)
    return out
