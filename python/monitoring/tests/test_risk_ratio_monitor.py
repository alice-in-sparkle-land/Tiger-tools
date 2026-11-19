"""Tests for risk_ratio_monitor.
Run:  python python/monitoring/tests/test_risk_ratio_monitor.py

Part A (always): the pandas steps (scoring, drill-down, top clients, explore, HTML) on aggregates
                 computed here in plain pandas from fake loans.
Part B (when PySpark + Java are installed): the full run on a local Spark, checked against Part A,
                 plus failures, dry run and the report in a headless browser.
"""
import os, sys, json, re, tempfile, shutil
from itertools import combinations
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
import numpy as np
import pandas as pd
from fixtures import TODAY_RISK, loans, RISK_SOURCE, browser
import risk_ratio_monitor as rm

TMP = tempfile.mkdtemp(prefix='risk_monitor_test_')
R = []
def ok(name, cond, extra=''):
    R.append(('PASS' if cond else 'FAIL', name, extra))

NOW = TODAY_RISK + pd.Timedelta(hours=9)
CFG = rm._settings({})
L = loans()

# ----------------------------------------------------------------------------
# plain-pandas versions of the Spark aggregations
# ----------------------------------------------------------------------------
def trunc(s, g):
    s = pd.to_datetime(s)
    if g == 'weekly':
        return s.dt.normalize() - pd.to_timedelta(s.dt.dayofweek, 'D')
    if g == 'monthly':
        return s.dt.to_period('M').dt.to_timestamp()
    return s.dt.normalize()

def prep(g, cfg=CFG):
    df = L[pd.to_datetime(L.disbursement_date) >= rm.lookback_start(g, NOW, cfg)].copy()
    df['date_value'] = trunc(df.disbursement_date, g)
    df['_o'] = df.f_pd30_obs; df['_d'] = df.f_pd30 * df._o; df['_a'] = df.loan_amount * df._o
    df['_co'] = df.client_id.where(df._o == 1); df['_cd'] = df.client_id.where((df._o == 1) & (df.f_pd30 == 1))
    return df

def agg(df, keys):
    return df.groupby(keys).agg(n_loan=('_o', 'size'), obs_n=('_o', 'sum'), def_n=('_d', 'sum'), obs_amt=('_a', 'sum'),
                                obs_cl=('_co', 'nunique'), def_cl=('_cd', 'nunique')).reset_index()

def pd_segments(g):
    df, out = prep(g), []
    for c in RISK_SOURCE['seg_cols']:
        a = agg(df, ['date_value', c]).rename(columns={c: 'column_value'})
        a['column_name'] = c; out.append(a)
    o = pd.concat(out, ignore_index=True); o['source'], o['date_name'] = 'pd30', g
    return o

def pd_pairs(g):
    df, out = prep(g), []
    for c1, c2 in combinations(RISK_SOURCE['seg_cols'], 2):
        a = agg(df, ['date_value', c1, c2]).drop(columns='n_loan').rename(columns={c1: 'v1', c2: 'v2'})
        a['c1'], a['c2'] = c1, c2; out.append(a[a.obs_n > 0])
    o = pd.concat(out, ignore_index=True); o['source'], o['date_name'] = 'pd30', g
    return o

def pd_clients(g, rec_p, top=10):
    df = prep(g); df = df[df.date_value.isin(rec_p) & (df._o == 1)]
    out = []
    for c in RISK_SOURCE['seg_cols']:
        a = df.groupby([c, 'client_id']).agg(obs_n=('_o', 'size'), def_n=('_d', 'sum'), obs_amt=('_a', 'sum')).reset_index()
        for i, p in enumerate(rec_p):
            lag = len(rec_p) - i
            a = a.merge(df[df.date_value == p].groupby([c, 'client_id'])['_d'].sum().rename(f'd{lag}').reset_index(),
                        on=[c, 'client_id'], how='left')
        a = a.fillna(0).sort_values([c, 'def_n', 'obs_n', 'obs_amt'], ascending=[True, False, False, False])
        a['rk'] = a.groupby(c).cumcount() + 1
        a = a[a.rk <= top].rename(columns={c: 'column_value', 'client_id': 'client'}); a['column_name'] = c
        out.append(a)
    o = pd.concat(out, ignore_index=True); o['source'], o['date_name'] = 'pd30', g
    return o

def pd_cube(g, cols, vals, min_loans):
    df = prep(g); ks = [f'k{i}' for i in range(len(cols))]
    for k, c in zip(ks, cols):
        df[k] = df[c].astype(str).where(df[c].astype(str).isin(vals[c]), '(other)')
    out = []
    for r in range(len(ks) + 1):
        for s in combinations(ks, r):
            a = agg(df, ['date_value'] + list(s))
            for k in ks:
                if k not in s: a[k] = '__ALL__'
            out.append(a)
    cube = pd.concat(out, ignore_index=True)
    cube['_n'] = cube.groupby(ks)['n_loan'].transform('sum')
    return cube[cube._n >= min_loans].drop(columns='_n'), ks

# ----------------------------------------------------------------------------
# Part A: pandas steps
# ----------------------------------------------------------------------------
segs = pd.concat([pd_segments(g) for g in CFG['grans']], ignore_index=True)
log = []
res, series, periods = rm.score(segs, CFG, NOW, log.append)
ok('score: runs and returns rows', res is not None and len(res) > 0)

wk = segs[(segs.date_name == 'weekly') & (segs.column_name == 'a')].groupby('date_value')[['n_loan', 'obs_n']].sum()
share = (wk.obs_n / wk.n_loan)
wk_last = periods[('pd30', 'weekly', 'a')][0][-1]
later = share[(share.index > wk_last) & (share.index < rm.current_period_start('weekly', NOW))]
ok('maturity: last scored week is >= 80% observable, every later week is below',
   share[wk_last] >= CFG['min_mature'] and len(later) > 0 and (later < CFG['min_mature']).all(), (wk_last, share[wk_last]))
ok('maturity: incomplete current week/month never scored',
   not any(p in set(res.period) for p in ['wk 2026-11-16', '2026-11']))
sp = res[res.direction == 'spike']
ok('spike: travel (b) flagged in the latest matured week',
   ((sp.column_value == 'travel') & (sp.date_name == 'weekly') & (sp.lag <= 2)).any())
ok('spike: web (a) flagged (driven by web x fashion)', ((sp.column_value == 'web')).any())
ok('drop: north (c) flagged as a drop',
   ((res.column_value == 'north') & (res.direction == 'drop')).any())
rare = res[res.column_value == 'rare']
ok('thin rule: a tiny segment is either skipped or has thin rows with no z',
   rare.empty or (rare[~rare.eligible.astype(bool)].z_ratio.isna().all()))
ok('thin rule: thin periods are never flagged', (res[~res.eligible.astype(bool)].direction == '').all())
ok('order: spikes, then drops, then the rest; lag ascending in spikes',
   list(res.dir_rank) == sorted(res.dir_rank) and res[res.direction == 'spike'].lag.is_monotonic_increasing)
ok('series: every scored key has a chart series with gaps as None',
   all(k in series for k in res.key) and any(None in s['r'] for s in series.values()))

pairs = pd.concat([pd_pairs(g) for g in CFG['grans']], ignore_index=True)
drill = rm.build_drill(pairs, series, periods, CFG)
dw = drill.get('pd30|weekly|a|web', {})
ok('drill: a=web has splits by b and c', set(dw) == {'b', 'c'}, list(dw))
kids_b = dw.get('b', {}).get('kids', [])
best = max(kids_b, key=lambda k: max([z for z in k['zr'] if z is not None] or [-9])) if kids_b else {}
ok('drill: inside a=web, fashion or travel has the highest z', best.get('v') in ('fashion', 'travel'), best.get('v'))
ok('drill: tiny sub-segment "rare" hidden', all(k['v'] != 'rare' for k in kids_b) and dw['b']['hidden'] >= 1)
ok('drill: at most drill_top_k kids', all(len(b['kids']) <= CFG['drill_top_k'] for v in drill.values() for b in v.values()))

rec_w = periods[('pd30', 'weekly', 'a')][0][-CFG['n_recent']:]
rec_m = periods[('pd30', 'monthly', 'a')][0][-CFG['n_recent']:]
cl = pd.concat([pd_clients('weekly', rec_w), pd_clients('monthly', rec_m)], ignore_index=True)
clients = rm.pack_clients(cl, res, CFG)
pc = clients.get('pd30|weekly|a|partner')
ok('clients: partner top client is 777', pc is not None and pc['top'][0]['c'] == '777', pc and pc['top'][0]['c'])
ok('clients: partner is concentrated and the ratio falls without the top 5',
   pc is not None and pc['top5_sd'] >= CFG['conc_th'] and pc['ex5'] < pc['ratio'], pc and (pc['top5_sd'], pc['ex5'], pc['ratio']))
tv = clients.get('pd30|weekly|b|travel')
ok('clients: travel is not concentrated', tv is not None and tv['top5_sd'] < CFG['conc_th'])
ok('clients: per-period defaults add up to each client total',
   all(abs(sum(t['dl']) - t['d']) < 1e-9 for c_ in clients.values() for t in c_['top']))
masked = rm.pack_clients(cl, res, dict(CFG, mask_client=True))
ok('mask_client: ids hashed, same client same code',
   masked['pd30|weekly|a|partner']['top'][0]['c'].startswith('c_')
   and masked['pd30|weekly|a|partner']['top'][0]['c'] == masked['pd30|monthly|a|partner']['top'][0]['c'])

cols = ['a', 'b', 'c']
vals = {c: L[c].astype(str).value_counts().index[:CFG['explore_max_val']].tolist() for c in cols}
explore = {'pd30': {'cols': cols, 'grans': {}}}
for g in CFG['grans']:
    cube, ks = pd_cube(g, cols, vals, CFG['explore_min_loans'])
    explore['pd30']['grans'][g] = rm.pack_explore(cube, ks, cols, vals, g, CFG, NOW, CFG['explore_min_loans'])
G = explore['pd30']['grans']['weekly']
ai, bi = G['vals'][0].index('web'), G['vals'][1].index('fashion')
pi = G['p'].index('wk 2026-08-31')
cell = {c_[0]: c_ for c_ in G['cells'][f'{ai},{bi},-1']}[pi]
raw = L[(L.a == 'web') & (L.b == 'fashion') & (L.f_pd30_obs == 1)]
raw = raw[trunc(raw.disbursement_date, 'weekly') == '2026-08-31']
ok('explore: a cell matches the raw loans exactly (obs, defaults, amount, distinct clients)',
   cell[2:] == [len(raw), int(raw.f_pd30.sum()), round(raw.loan_amount.sum()), raw.client_id.nunique(),
                raw[raw.f_pd30 == 1].client_id.nunique()], (cell, len(raw)))
ok('explore: "All x All x All" exists and newest weeks are marked immature',
   '-1,-1,-1' in G['cells'] and G['mat'][-1] is False and G['mat'][0] is True)
ok('explore: incomplete current week excluded', 'wk 2026-11-16' not in G['p'])

html = rm.render_html(res, series, drill, clients, explore, [RISK_SOURCE], CFG, NOW)
D = json.loads(re.search(r"const D = (\{.*?\});\nconst M", html, re.S).group(1))
ok('html: data embedded with drill, clients and explore', D['drill'] and D['clients'] and D['explore'])
ok('html: offline (no http/https references)', not re.search(r'(src|href)=["\']https?:', html) and 'fetch(' not in html)
open(os.path.join(TMP, 'pandas_report.html'), 'w', encoding='utf-8').write(html)

def raises(fn, text):
    try:
        fn()
    except Exception as e:
        return text in str(e)
    return False
ok('bad input: unknown setting', raises(lambda: rm._settings({'zth': 3}), 'Unknown setting(s): zth'))
ok('bad input: missing def_col', raises(lambda: rm._check_sources([{k: v for k, v in dict(RISK_SOURCE, data='select 1').items() if k != 'def_col'}]), "no 'def_col'"))
ok('bad input: two seg_cols with the same short name',
   raises(lambda: rm._check_sources([dict(RISK_SOURCE, data='select 1', seg_cols=['x.a', 'y.a'])]), 'same name'))
log = []
r0, _, _ = rm.score(segs, dict(CFG, min_base=500), NOW, log.append)
ok('not enough history: nothing scored, reason printed', r0 is None and 'matured periods (need' in '\n'.join(log))

# ----------------------------------------------------------------------------
# Part B: full run on a local Spark
# ----------------------------------------------------------------------------
def local_spark():
    if not shutil.which('java'):
        return None, 'java not installed'
    try:
        from pyspark.sql import SparkSession
    except Exception:
        return None, 'pyspark not installed'
    os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
    s = (SparkSession.builder.master('local[2]').appName('risk_monitor_test').config('spark.ui.enabled', 'false')
         .config('spark.sql.shuffle.partitions', '8').config('spark.sql.session.timeZone', 'Asia/Ho_Chi_Minh').getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    return s, ''

spark, why = local_spark()
if spark is None:
    R.append(('SKIP', 'Part B (local Spark)', why))
else:
    spark.createDataFrame(L).createOrReplaceTempView('loans_tbl')
    SRC = dict(RISK_SOURCE, data="select *\nfrom loans_tbl\nwhere loan_amount > var_min_amt")
    SV = [{'from': 'var_min_amt', 'to': '0'}]
    rep = lambda q, v: rm_replace(q, v)
    def rm_replace(q, v):
        for it in v:
            q = q.replace(it['from'], str(it['to']))
        return q

    buf = []
    r = rm.run_risk_ratio_monitor([SRC], os.path.join(TMP, 'x.html'), spark=spark, sql_var=SV,
                                  replace_sql_var=rm_replace, dry_run=True, now=NOW, log=buf.append)
    out = '\n'.join(buf)
    ok('dry run: plan printed, sql_var applied, nothing written',
       r['ok'] and 'loan_amount > 0' in out and 'explore cube pd30/monthly' in out and not os.path.exists(os.path.join(TMP, 'x.html')))

    buf = []
    out_path = os.path.join(TMP, 'spark_report.html')
    r = rm.run_risk_ratio_monitor([SRC], out_path, spark=spark, sql_var=SV, replace_sql_var=rm_replace,
                                  now=NOW, log=buf.append)
    out = '\n'.join(buf)
    ok('spark run: ok, report written, every step printed',
       r['ok'] and os.path.exists(out_path) and '[4/4] write report' in out and f"{len(r.get('failed', []))}" == '0', out[-600:])
    rs = r['res']
    if rs is not None:
        cols_cmp = ['key', 'period', 'obs_n', 'def_n', 'obs_cl', 'def_cl', 'eligible', 'direction']
        a_ = res[cols_cmp].sort_values(['key', 'period']).reset_index(drop=True)
        b_ = rs[cols_cmp].sort_values(['key', 'period']).reset_index(drop=True)
        same = a_.shape == b_.shape and (a_.astype(str).values == b_.astype(str).values).all()
        ok('spark run: identical to the plain-pandas aggregation (counts, clients, flags)', same, (a_.shape, b_.shape))
        Ds = json.loads(re.search(r"const D = (\{.*?\});\nconst M", open(out_path, encoding='utf-8').read(), re.S).group(1))
        ok('spark run: drill-down identical', Ds['drill'] == json.loads(json.dumps(drill, default=str)))
        ok('spark run: top clients identical', Ds['clients'] == json.loads(json.dumps(clients, default=str)))
        ok('spark run: explore cells identical',
           Ds['explore']['pd30']['grans']['weekly']['cells'] == json.loads(json.dumps(G['cells'])))

    # failure in the data SQL: stops at step 1
    buf = []
    bad = dict(RISK_SOURCE, data="select *\nfrom loans_tbl\nwhere no_such_column = 1")
    r = rm.run_risk_ratio_monitor([bad], os.path.join(TMP, 'bad.html'), spark=spark, now=NOW, log=buf.append)
    out = '\n'.join(buf)
    ok('failure: stops at step 1, one line with step, source, granularity and error',
       not r['ok'] and r['failed_step'] == 1 and 'FAILED at step 1/' in out and 'source pd30 · weekly: ' in out
       and 'no_such_column' in out, out[-400:])
    ok('failure: lists the steps that did not run', 'not run: pairs pd30/weekly' in out)

    # a non-essential step failing with stop_on_error=False: report still written, failure listed
    orig = rm.spark_cube
    def boom(*a, **k):
        raise RuntimeError('simulated cube failure')
    rm.spark_cube = boom
    buf = []
    r = rm.run_risk_ratio_monitor([SRC], os.path.join(TMP, 'skip.html'), spark=spark, sql_var=SV,
                                  replace_sql_var=rm_replace, stop_on_error=False, now=NOW, log=buf.append)
    rm.spark_cube = orig
    ok('stop_on_error=False: explore failure skipped, report written without Explore',
       r['ok'] and len(r['failed']) == 2 and '"explore": {}' in open(os.path.join(TMP, 'skip.html'), encoding='utf-8').read())
    rm.spark_cube = boom
    buf = []
    r = rm.run_risk_ratio_monitor([SRC], os.path.join(TMP, 'stop.html'), spark=spark, sql_var=SV,
                                  replace_sql_var=rm_replace, now=NOW, log=buf.append)
    rm.spark_cube = orig
    ok('stop_on_error=True: the same failure stops the run', not r['ok'] and 'simulated cube failure' in '\n'.join(buf))

    # explore cap: tiny max rows forces the minimum up
    buf = []
    r = rm.run_risk_ratio_monitor([SRC], os.path.join(TMP, 'cap.html'), spark=spark, sql_var=SV,
                                  replace_sql_var=rm_replace, now=NOW, log=buf.append, explore_max_rows=3000, drill=False)
    out = '\n'.join(buf)
    ok('explore cap: minimum doubles until the cube fits', r['ok'] and 'doubling the minimum' in out)
    spark.stop()

# ----------------------------------------------------------------------------
# browser (desktop + phone)
# ----------------------------------------------------------------------------
pw = browser()
rep_path = os.path.join(TMP, 'spark_report.html') if os.path.exists(os.path.join(TMP, 'spark_report.html')) \
    else os.path.join(TMP, 'pandas_report.html')
if pw is None:
    R.append(('SKIP', 'browser checks', 'playwright not installed'))
else:
    with pw() as p:
        b = p.chromium.launch()
        for w in (1280, 360):
            pg = b.new_page(viewport={'width': w, 'height': 900})
            errs = []
            pg.on('pageerror', lambda e: errs.append(str(e)))
            pg.goto('file://' + rep_path)
            pg.locator('#top .cv', has_text='web').first.click()
            pg.wait_for_timeout(250)
            n_drill = pg.locator('.dcard').count()
            n_client = pg.locator('.dcard-c').count()
            if w == 360:
                ok('browser 360px: detail charts open at the newest periods',
                   pg.evaluate("[...document.querySelectorAll('#mB .ch')].every(c => c.scrollLeft > 0)"))
                pg.screenshot(path=os.path.join(TMP, 'risk_modal_360.png'))
            pg.click('#mEx')
            pg.wait_for_timeout(200)
            exs = pg.inner_text('#exS')
            wide = pg.evaluate('document.documentElement.scrollWidth > window.innerWidth')
            pg.screenshot(path=os.path.join(TMP, f'risk_{w}.png'))
            if w == 1280:
                pg.goto('file://' + rep_path)
                pg.evaluate("try{localStorage.removeItem('rrm_min_amt_mil')}catch(e){}")
                pg.reload()
                pg.click('button[data-t=all]')
                n0 = pg.locator('#tAll tbody tr').count()
                amts = pg.evaluate("D.rows.map(r=>r.obs_amt||0).sort((a,b)=>a-b)")
                cut = amts[len(amts) // 2] / 1e6                       # median row amount, in mil
                pg.fill('#fAmt', f'{cut:.3f}')
                pg.wait_for_timeout(150)
                n1 = pg.locator('#tAll tbody tr').count()
                expect = pg.evaluate(f"D.rows.filter(r=>(r.obs_amt||0) >= {cut:.3f}*1e6).length")
                note = pg.inner_text('#amtNote')
                pg.reload(); pg.wait_for_timeout(150)
                kept = pg.input_value('#fAmt'); pg.click('button[data-t=all]'); n2 = pg.locator('#tAll tbody tr').count()
                pg.click('#fAmtX'); pg.wait_for_timeout(100); n3 = pg.locator('#tAll tbody tr').count()
                ok('amount filter: hides rows under the minimum (All tab), note says how many are shown',
                   n1 == expect and 0 < n1 < n0 and 'showing' in note, (n0, n1, expect, note))
                ok('amount filter: remembered after reload, clear restores all rows',
                   kept == f'{cut:.3f}' and n2 == n1 and n3 == n0 and pg.inner_text('#amtNote') == '', (kept, n2, n3))
                pg.fill('#fAmt', '999999'); pg.wait_for_timeout(100)
                ok('amount filter: very high minimum -> Top tab shows no spikes, no errors',
                   pg.evaluate("SPIKES.length") == 0 and not errs)
                pg.click('#fAmtX')
                # Explore split: top 5 by loans + Others; Others = sample minus the top lines (loans)
                pg.click('#exTab'); pg.click('#exReset'); pg.wait_for_timeout(100)
                pg.select_option('#exSp', 'b'); pg.wait_for_timeout(150)
                sp = pg.evaluate("""(() => { const s = exSeries(), L = s.lines;
                  return {v: L.map(l=>l.v), nl: L.map(l=>l.nl),
                    sums: s.rows.every((r,i)=>L.reduce((a,l)=>a+l.rows[i].nl,0)===r.nl && L.reduce((a,l)=>a+l.rows[i].d,0)===r.d
                                              && L.reduce((a,l)=>a+l.rows[i].o,0)===r.o),
                    othClients: L.filter(l=>l.others).every(l=>l.rows.every(r=>r.rc==null)),
                    leg: document.querySelectorAll('#exL .sw').length, dis: document.querySelector('#exF select[data-ci="1"]').disabled}; })()""")
                ok('explore split: values ranked by loans, small ones in Others, lines add back to the sample',
                   sp['v'] == ['electronics', 'fashion', 'grocery', 'travel', 'Others'] and sp['sums']
                   and sp['nl'][:4] == sorted(sp['nl'][:4], reverse=True) and sp['dis'] and sp['leg'] == 6, sp)
                ok('explore split: Others has no client ratio; client mode hides the Others line',
                   sp['othClients'] and (pg.click('#exM button[data-m=rc]') or True) and
                   pg.wait_for_timeout(100) is None and pg.locator('#exL .sw').count() == 5 and 'Others not shown' in pg.inner_text('#exS'))
                pg.select_option('#exSp', 'c'); pg.select_option('#exF select[data-ci="0"]', label='web'); pg.wait_for_timeout(150)
                sc = pg.evaluate("exSeries().lines.map(l=>[l.v, l.nl])")
                web = L[(L.a == 'web') & (pd.to_datetime(L.disbursement_date) >= rm.lookback_start('weekly', NOW, CFG))
                        & (pd.to_datetime(L.disbursement_date) < rm.current_period_start('weekly', NOW))]
                ok('explore split: respects the other filters (c inside a = web), 5 values -> no Others',
                   [v for v, _ in sc] == web.c.value_counts().index.tolist()[:5] and 'Others' not in [v for v, _ in sc], sc)
                pg.click('#exD summary'); pg.wait_for_timeout(100)
                ok('explore split: data table has one row per period per line',
                   pg.locator('#exT tbody tr').count() == pg.evaluate("exG().p.length") * (1 + len(sc)) and not errs, errs[:1])
                pg.click('#exReset')
            if w == 360:
                pg.click('#exTab'); pg.select_option('#exSp', 'b'); pg.wait_for_timeout(150)
                ok('browser 360px: Explore split, no sideways scroll, no errors',
                   not errs and not pg.evaluate('document.documentElement.scrollWidth > window.innerWidth'))
                pg.screenshot(path=os.path.join(TMP, 'risk_split_360.png'), full_page=True)
            ok(f'browser {w}px: no JS errors, drill + clients on click, Explore link, no sideways scroll',
               not errs and n_drill == 2 and n_client == 1 and 'a = web' in exs and not wide,
               (errs[:1], n_drill, n_client, exs, wide))
        b.close()

for s, n, x in R:
    if s != 'PASS':
        print(f'{s}: {n}  {x}')
fails = sum(1 for s, *_ in R if s == 'FAIL')
print(f"{len(R)} checks {fails} failures")
print(f"(files in {TMP})")
sys.exit(1 if fails else 0)
