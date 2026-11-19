"""Tests for change_monitor with a fake Spark (no cluster needed).
Run:  python python/monitoring/tests/test_change_monitor.py
"""
import os, sys, json, re, tempfile
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
import pandas as pd
from fixtures import (NOW, bnpl_daily, FakeSparkCounts, date_fm, replace_sql_var, APP_SOURCE, browser)
from change_monitor import run_change_monitor, EXAMPLE_SOURCES, PLACEHOLDERS

TMP = tempfile.mkdtemp(prefix='change_monitor_test_')
R = []
def ok(name, cond, extra=''):
    R.append(('PASS' if cond else 'FAIL', name, extra))

DAYS, DATA = bnpl_daily()
SOURCES = [dict(EXAMPLE_SOURCES[0], cols=['transaction_source', 'product_source', 'transaction_status']), APP_SOURCE]

def run(sources=SOURCES, fail_on=None, **kw):
    sp = FakeSparkCounts(DAYS, DATA, fail_on=fail_on)
    buf = []
    out = kw.pop('out_path', os.path.join(TMP, 'report.html'))
    r = run_change_monitor(sources, out, spark=sp, date_fm=date_fm, replace_sql_var=replace_sql_var,
                           now=NOW, log=buf.append, **kw)
    return r, '\n'.join(buf), sp

# ---------------- dry run ----------------
r, log, sp = run(dry_run=True)
ok('dry run: ok and nothing sent to Spark', r['ok'] and r.get('dry_run') and not sp.queries)
ok('dry run: 3 grans x (3 + 2) columns = 15 queries', len(r['queries']) == 15, len(r['queries']))
ok('dry run: every placeholder replaced', all(not any(p in q for p in PLACEHOLDERS) for *_, q in r['queries']))
ok('dry run: aliased column a.channel -> label channel, column kept',
   any(c == 'channel' and "cast(a.channel as string)" in q and "'channel' as column_name" in q for _, _, c, q in r['queries']))
ok('dry run: queries printed', log.count('--- query ') == 15 and 'nothing was run' in log)
ok('dry run: no file written', not os.path.exists(os.path.join(TMP, 'report.html')))

# ---------------- full run ----------------
r, log, sp = run()
res = r['res']
ok('run: ok and file written', r['ok'] and os.path.exists(r['out_path']))
ok('run: prints each step', '[1/3] pull counts' in log and '[2/3] score' in log and '[3/3] write report' in log
   and ' 15/15 ' in log)
top = res[(res.direction == 'spike') & (res.date_name == 'daily') & (res.column_name == 'transaction_status')].iloc[0]
ok('spike: failed is the top daily spike of transaction_status, latest period',
   (top.column_name, top.column_value, top.lag) == ('transaction_status', 'failed', 1), (top.column_value, top.lag))
qr = res[(res.column_value == 'qr_merchant') & (res.date_name == 'daily')]
ok('spike: value new 11 days ago is a spike (has a baseline, so not NEW)', (qr.direction == 'spike').any() and (qr.flag == '').all())
nw = res[(res.column_value == 'paylater_link') & (res.date_name == 'daily')]
ok('spike: value new inside the last 5 days is flagged NEW', (nw.flag == 'NEW').any() and (nw[nw.flag == 'NEW'].direction == 'spike').all())
pos = res[(res.column_value == 'pos') & (res.date_name == 'weekly') & (res.lag == 1)]
ok('drop: pos falls -> drop', (pos.direction == 'drop').all() and len(pos) == 1)
tk = res[(res.source == 'bnpl_app') & (res.column_value == 'tiktok_shop') & (res.direction == 'spike')]
ok('second source scored (tiktok_shop spike)', len(tk) > 0)
ok('order: spikes before drops before the rest, lag ascending within spikes',
   list(res.dir_rank) == sorted(res.dir_rank) and res[res.direction == 'spike'].lag.is_monotonic_increasing)
daily_periods = set(res[res.date_name == 'daily'].period)
ok('incomplete period dropped: today is not scored', '2026-10-26' not in daily_periods and '2026-10-25' in daily_periods)
wk = set(res[res.date_name == 'weekly'].period)
ok('incomplete week dropped (week of today)', 'wk 2026-10-26' not in wk and 'wk 2026-10-19' in wk)
ok('5 recent periods per value', res.groupby('key').size().max() == 5)

html = open(r['out_path'], encoding='utf-8').read()
D = json.loads(re.search(r"const D = (\{.*?\});\nconst M", html, re.S).group(1))
ok('html: data embedded, both sources listed', [s['name'] for s in D['meta']['sources']] == ['bnpl_txn', 'bnpl_app'])
ok('html: every row has a chart series', all(row['key'] in D['series'] for row in D['rows']))
ok('html: offline (no http/https references)', not re.search(r'(src|href)=["\']https?:', html) and 'fetch(' not in html)

# ---------------- failures ----------------
r, log, _ = run(fail_on='product_source')
ok('stop_on_error: stops and reports', not r['ok'] and r['failed_step'] == 2 and r['failed_query'][2] == 'product_source')
ok('stop_on_error: one line with query, source / granularity / column and error',
   'FAILED at query 2/15: bnpl_txn / daily / product_source: ' in log and 'UNRESOLVED_COLUMN' in log, log[-400:])
ok('stop_on_error: lists what did not run', 'not run: bnpl_txn/daily/transaction_status' in log)
r, log, _ = run(fail_on='product_source', stop_on_error=False, out_path=os.path.join(TMP, 'r2.html'))
ok('stop_on_error=False: report still written, failures listed', r['ok'] and len(r['failed']) == 3
   and 'product_source' in open(r['out_path'], encoding='utf-8').read().split('"failed":')[1][:400])

# ---------------- bad input ----------------
def raises(fn, text):
    try:
        fn()
    except Exception as e:
        return text in str(e)
    return False
ok('unknown setting -> clear error', raises(lambda: run(z_threshold=3), 'Unknown setting(s): z_threshold'))
ok('missing placeholder -> clear error', raises(lambda: run([dict(SOURCES[0], sql='select 1')]), 'does not contain'))
ok('duplicate source name -> clear error', raises(lambda: run([SOURCES[0], SOURCES[0]]), 'used twice'))
ok('no ts_col -> clear error', raises(lambda: run([{k: v for k, v in SOURCES[0].items() if k != 'ts_col'}]), 'ts_col'))
ok('empty sources -> clear error', raises(lambda: run([]), 'sources is empty'))

class BadCols(FakeSparkCounts):
    def sql(self, q):
        return type('X', (), {'toPandas': lambda s: pd.DataFrame({'a': [1]})})()
try:
    run_change_monitor(SOURCES, os.path.join(TMP, 'x.html'), spark=BadCols(DAYS, DATA), date_fm=date_fm,
                       replace_sql_var=replace_sql_var, now=NOW, log=lambda m: None)
    ok('wrong output columns -> clear error', False)
except RuntimeError as e:
    ok('wrong output columns -> clear error', 'returned no' in str(e) and 'cnt_all' in str(e))

r, log, _ = run(min_base=400, out_path=os.path.join(TMP, 'r3.html'))
ok('not enough history -> skipped with a reason, nothing scored', not r['ok'] and 'complete periods (need' in log)

r, log, _ = run(grans=['weekly'], z_th=3.0, out_path=os.path.join(TMP, 'r4.html'))
ok('settings: grans and z_th respected', r['ok'] and set(r['res'].date_name) == {'weekly'}
   and (r['res'][r['res'].direction == 'spike'].z_max > 3).all())

# ---------------- browser (desktop + phone) ----------------
pw = browser()
if pw is None:
    R.append(('SKIP', 'browser checks', 'playwright not installed'))
else:
    with pw() as p:
        b = p.chromium.launch()
        for w in (1280, 360):
            pg = b.new_page(viewport={'width': w, 'height': 900})
            errs = []
            pg.on('pageerror', lambda e: errs.append(str(e)))
            pg.goto('file://' + os.path.join(TMP, 'report.html'))
            pg.locator('.cv', has_text='failed').first.click()
            pg.wait_for_timeout(200)
            n_charts = pg.locator('#mB .card svg').count()
            if w == 360:
                ok('browser 360px: detail charts open at the newest periods',
                   pg.evaluate("[...document.querySelectorAll('#mB .ch')].every(c => c.scrollLeft > 0)"))
            pg.keyboard.press('Escape')
            pg.click('button[data-t=all]')
            pg.select_option('#fSrc', 'bnpl_app')
            n_rows = pg.locator('#tAll tbody tr').count()
            wide = pg.evaluate('document.documentElement.scrollWidth > window.innerWidth')
            pg.screenshot(path=os.path.join(TMP, f'change_{w}.png'), full_page=False)
            ok(f'browser {w}px: no JS errors, 3 charts on click, source filter works, no sideways scroll',
               not errs and n_charts == 3 and n_rows > 0 and not wide, (errs[:1], n_charts, n_rows, wide))
        b.close()

# ---------------- summary ----------------
for s, n, x in R:
    if s != 'PASS':
        print(f'{s}: {n}  {x}')
fails = sum(1 for s, *_ in R if s == 'FAIL')
print(f"{len(R)} checks {fails} failures")
print(f"(files in {TMP})")
sys.exit(1 if fails else 0)
