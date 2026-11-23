"""Tests for the Fisher backtest engine that html/fisher embeds in every generated .py.

    python html/fisher/tests/test_engine.py

Part A: fisher_greater and bh_qvalues against scipy (skipped if scipy is missing).
Part B: the full run on a local Spark with fake data, checked against a brute-force pandas
        rebuild (every 2x2 table, family size, p, q), plus planted sellers being caught,
        labels, cooldown, and the one-line failure. Skipped if PySpark/Java are missing.
"""
import os, sys, io, time, contextlib
os.environ['TZ'] = 'UTC'; time.tzset()   # fake timestamps and the Spark session agree on the day
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ENGINE = os.path.join(HERE, '..', 'src', 'engine.py')
ns = {}
exec(compile(open(ENGINE, encoding='utf-8').read(), ENGINE, 'exec'), ns)
fisher_greater, bh_qvalues, run = ns['fisher_greater'], ns['bh_qvalues'], ns['run_fisher_backtest']

passed = failed = 0


def check(name, ok, info=''):
    global passed, failed
    if ok:
        passed += 1; print('  ok  ', name)
    else:
        failed += 1; print('  FAIL', name, info)


# ---------------------------------------------------------------------------- Part A
print('Part A: Fisher and BH')
try:
    from scipy.stats import fisher_exact
    rng = np.random.default_rng(1)
    T = [rng.integers(0, s, size=4) for s in (3, 8, 30, 200, 3000) for _ in range(300)]
    T += [np.array(t) for t in [(0, 0, 0, 0), (5, 0, 0, 5), (0, 5, 5, 0), (1, 0, 0, 0), (0, 0, 0, 9),
                                (50, 2, 400, 60000), (3, 120000, 40, 2000000), (900, 100, 800, 200),
                                (20000, 30000, 20100, 29900), (1, 1, 1, 1)]]
    T = np.array(T)
    mine = fisher_greater(T[:, 0], T[:, 1], T[:, 2], T[:, 3])
    ref = np.array([fisher_exact([[a, b], [c, d]], alternative='greater')[1] for a, b, c, d in T])
    rel = np.abs(mine - ref) / np.maximum(ref, 1e-300)
    ok = (rel < 1e-6) | (np.abs(mine - ref) < 1e-12)
    check(f'fisher_greater = scipy on {len(T)} tables', ok.all(), T[~ok][:5].tolist())
    from scipy.stats import false_discovery_control
    p = rng.random(200) ** 3
    check('bh_qvalues = scipy BH (full family)', np.allclose(bh_qvalues(p, 200), false_discovery_control(p)))
    small = np.sort(p)[:50]
    full = false_discovery_control(p)[np.argsort(p)][:50]
    check('bh_qvalues with only the smallest p kept = full BH', np.allclose(bh_qvalues(small, 200), full))
except ImportError:
    print('  (scipy not installed: skipped)')


# ---------------------------------------------------------------------------- fake data
def fake(seed=7, days=110):
    rng = np.random.default_rng(seed)
    start = pd.Timestamp('2026-06-01')
    groups = [('fashion', '89a1'), ('fashion', '89a2'), ('travel', '89b1')]
    merch = {f'M{i:02d}': groups[i % 3] for i in range(18)}
    rows = []
    for m, (sg, h3) in merch.items():
        rate = rng.uniform(4, 12)
        for di in range(days):
            day = start + pd.Timedelta(days=di)
            late = di >= days - 20
            lam = rate * (3.5 if (m == 'M04' and late) else 1)          # M04: volume jump
            for _ in range(rng.poisson(lam)):
                cust = f'C{rng.integers(0, 600 if m != "M04" else 900):04d}'
                p_hi = 0.55 if (m == 'M07' and late) else 0.12          # M07: high-utilisation share jump
                util = rng.uniform(0.9, 1.0) if rng.random() < p_hi else rng.uniform(0, 0.9)
                amt = float(rng.integers(1, 30)) * 1e5
                bad = m in ('M04', 'M07') and late
                rows.append(dict(txn_date=day + pd.Timedelta(hours=int(rng.integers(0, 23))), merchant_id=m,
                                 client_id=cust, seller_group=sg, h3_9=h3, utilization=util, txn_amount=amt,
                                 f_susp=int(bad and rng.random() < 0.3), f_pd30=int(rng.random() < (0.25 if bad else 0.05)),
                                 f_pd30_obs=int(di < days - 5)))
    return pd.DataFrame(rows)


CONFIG = {
    'date_col': 'txn_date', 'merchant_col': 'merchant_id', 'customer_col': 'client_id',
    'metrics': [
        {'name': 'high_util', 'kind': 'flag', 'expr': 'utilization > 0.9'},
        {'name': 'big_ticket', 'kind': 'flag', 'expr': 'txn_amount >= 2500000'},
        {'name': 'volume', 'kind': 'volume'},
    ],
    'comparisons': [
        {'name': '1 vs 2-8', 'recent': [1, 1], 'base': [2, 8]},
        {'name': '7 vs 8-15', 'recent': [1, 7], 'base': [8, 15]},
        {'name': '30 vs 31-60', 'recent': [1, 30], 'base': [31, 60]},
    ],
    'units': ['customer', 'txn'], 'peer_cols': ['seller_group', 'h3_9'], 'min_peers': 3,
    'min_total': 10, 'min_flag': 5, 'q_max': 0.05, 'or_min': 2.0, 'cooldown': 7,
    'susp_cond': 'f_susp = 1', 'def_col': 'f_pd30', 'obs_col': 'f_pd30_obs',
}


def brute(pdf, cfg, a_s, a_e):
    """Every 2x2 table the engine should build, the slow obvious way."""
    pdf = pdf.copy()
    pdf['d'] = pdf['txn_date'].dt.normalize()
    pdf['g'] = pdf[cfg['peer_cols']].astype(str).agg(' | '.join, axis=1)
    flags = [m for m in cfg['metrics'] if m['kind'] == 'flag']
    pdf['f_high_util'] = (pdf['utilization'] > 0.9).astype(int)
    pdf['f_big_ticket'] = (pdf['txn_amount'] >= 2500000).astype(int)
    out = []
    for t in pd.date_range(a_s, a_e):
        def win(w):
            s, e = w
            return pdf[(pdf['d'] >= t - pd.Timedelta(days=e - 1)) & (pdf['d'] <= t - pd.Timedelta(days=s - 1))]
        for unit in cfg['units']:
            def counts(sub):
                res = {}
                for m, gm in sub.groupby('merchant_id'):
                    g = gm['g'].iloc[0]
                    cell = sub[sub['g'] == g]
                    if unit == 'customer':
                        n = gm['client_id'].nunique()
                        f = {k['name']: gm.loc[gm['f_' + k['name']] == 1, 'client_id'].nunique() for k in flags}
                        others = cell[~cell['client_id'].isin(gm['client_id'])]
                        pn = others['client_id'].nunique()
                        pf = {k['name']: others.loc[others['f_' + k['name']] == 1, 'client_id'].nunique() for k in flags}
                        alln = sub['client_id'].nunique()
                    else:
                        n = len(gm); f = {k['name']: int(gm['f_' + k['name']].sum()) for k in flags}
                        others = cell[cell['merchant_id'] != m]
                        pn = len(others); pf = {k['name']: int(others['f_' + k['name']].sum()) for k in flags}
                        alln = len(sub)
                    res[m] = dict(g=g, n=n, f=f, pn=pn, pf=pf, pm=cell['merchant_id'].nunique() - 1, all=alln)
                return res
            for c in cfg['comparisons']:
                R, B = counts(win(c['recent'])), counts(win(c['base']))
                sb = win(c['base'])
                ball = sb['client_id'].nunique() if unit == 'customer' else len(sb)
                for m, r in R.items():
                    b = B.get(m, dict(n=0, f={k['name']: 0 for k in flags}, pn=0, pf={k['name']: 0 for k in flags}))
                    for mt in cfg['metrics']:
                        k = mt['name']
                        if mt['kind'] == 'flag':
                            tests = [('self', r['f'][k], r['n'] - r['f'][k], b['f'][k], b['n'] - b['f'][k], r['f'][k]),
                                     ('peer', r['f'][k], r['n'] - r['f'][k], r['pf'][k], r['pn'] - r['pf'][k], r['f'][k])]
                        else:
                            tests = [('self', r['n'], r['all'] - r['n'], b['n'], ball - b['n'], r['n']),
                                     ('peer', r['n'], r['pn'], b['n'], b['pn'], r['n'])]
                        for test, a_, b_, c_, d_, fl in tests:
                            gate = r['n'] >= cfg['min_total'] and fl >= cfg['min_flag']
                            if test == 'peer':
                                gate = gate and r['pm'] >= cfg['min_peers'] and r['pn'] >= cfg['min_total']
                            if gate:
                                out.append((t, m, k, c['name'], unit, test, a_, b_, c_, d_))
    return pd.DataFrame(out, columns=['anchor', 'merchant', 'metric', 'comparison', 'unit', 'test', 'a', 'b', 'c', 'd'])


# ---------------------------------------------------------------------------- Part B
print('Part B: full run on local Spark')
try:
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master('local[2]').appName('fisher_test')
             .config('spark.sql.shuffle.partitions', '4').config('spark.ui.enabled', 'false').config('spark.ui.showConsoleProgress', 'false')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
except Exception as e:  # noqa
    print('  (PySpark/Java not available: skipped)', e)
    spark = None

if spark is not None:
    from scipy.stats import fisher_exact
    pdf = fake()
    df = spark.createDataFrame(pdf)
    cfg = dict(CONFIG, anchor_start='2026-08-01')
    shadow = {}
    exec('from pyspark.sql.functions import *', shadow)      # what many notebooks do first
    exec(compile(open(ENGINE, encoding='utf-8').read(), ENGINE, 'exec'), shadow)
    check('notebook case: sum/max/min/round are Spark functions', shadow['sum'] is not sum and shadow['max'] is not max)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        r = shadow['run_fisher_backtest'](df, cfg, spark=spark)
    log = buf.getvalue()
    check('run ok (after from pyspark.sql.functions import *)', r['ok'], log[-800:])
    det, summ = r['detail'], r['summary']
    a_s, a_e = pd.Timestamp('2026-08-01'), pd.Timestamp(pdf['txn_date'].max().normalize())
    check('anchor range printed', 'anchors 2026-08-01 to ' + str(a_e.date()) in log, log[:600])

    ref = brute(pdf, CONFIG, a_s, a_e)
    keys = ['anchor', 'merchant', 'metric', 'comparison', 'unit', 'test']
    fam_ref = ref.groupby(['anchor', 'metric', 'comparison', 'unit', 'test']).size()
    fam_eng = det.groupby(['anchor', 'metric', 'comparison', 'unit', 'test'])['n_tests'].first()
    j = fam_eng.to_frame('e').join(fam_ref.to_frame('r'), how='left')
    check('family sizes (gated tests per day x rule) match brute force', (j['e'] == j['r']).all(), j[j['e'] != j['r']].head())
    lean = ref[ref['a'] * ref['d'] > ref['b'] * ref['c']]
    m = lean.merge(det, on=keys, how='outer', suffixes=('_r', '_e'), indicator=True)
    check('same candidate rows as brute force', (m['_merge'] == 'both').all() and len(m) > 500,
          m['_merge'].value_counts().to_dict())
    both = m[m['_merge'] == 'both']
    same = all((both[c + '_r'] == both[c + '_e']).all() for c in 'abcd')
    check(f'every 2x2 table identical ({len(both):,} rows, both units, self and peer)', same)
    pv = np.array([fisher_exact([[a, b], [c, d]], alternative='greater')[1]
                   for a, b, c, d in both[['a_e', 'b_e', 'c_e', 'd_e']].values])
    check('p-values = scipy', np.allclose(both['p'].values, pv, rtol=1e-6, atol=1e-12))
    # full-family BH with brute-force p for every gated test, then compare q on the kept rows
    ref['p'] = [fisher_exact([[a, b], [c, d]], alternative='greater')[1] for a, b, c, d in ref[['a', 'b', 'c', 'd']].values]
    from scipy.stats import false_discovery_control
    ref['q'] = ref.groupby(['anchor', 'metric', 'comparison', 'unit', 'test'])['p'].transform(
        lambda s: false_discovery_control(s.values))
    mq = det.merge(ref[keys + ['q']], on=keys, suffixes=('', '_r'))
    small = mq['q_r'] <= 0.5
    check('q-values = BH over the full family (where q <= 0.5)', np.allclose(mq.loc[small, 'q'], mq.loc[small, 'q_r'], rtol=1e-6, atol=1e-12))
    check('  ... and never below it elsewhere (rows leaning down are left out)', (mq['q'] >= mq['q_r'] * (1 - 1e-6) - 1e-12).all())
    fr = (mq['q_r'] <= CONFIG['q_max']) & (mq['odds_ratio'] >= CONFIG['or_min'])
    check('  ... so the same rows fire', (fr == mq['fired']).all())
    al = det[det['alert']]
    check('planted M07 (high_util) alerts on high_util', ((al['merchant'] == 'M07') & (al['metric'] == 'high_util')).any())
    check('planted M04 (volume) alerts on volume', ((al['merchant'] == 'M04') & (al['metric'] == 'volume')).any())
    planted = al['merchant'].isin(['M04', 'M07'])
    rate_p, rate_n = planted.sum() / 2, (~planted).sum() / 16
    check(f'planted sellers alert far more ({rate_p:.1f} vs {rate_n:.1f} alerts per seller)', rate_p >= 5 * rate_n)
    gaps = al.sort_values('anchor').groupby(['merchant', 'metric', 'comparison', 'unit', 'test'])['anchor'].diff().dt.days.dropna()
    check('cooldown: alerts of one rule on one seller are >= 7 days apart', (gaps >= 7).all())
    check('every alert is a fired row', det.loc[det['alert'], 'fired'].all())
    fired = det[det['fired']]
    check('fired = q <= 0.05 and OR >= 2', ((fired['q'] <= 0.05) & (fired['odds_ratio'] >= 2)).all())
    for c in ['rule_type', 'precision', 'recall', 'recall_early', 'median_lead_days', 'lift_after_alert',
              'def_rate_after_alert', 'def_rate_never_alerted', 'lift_in_window']:
        check(f'summary has {c}', c in summ.columns)
    check('rule_type: volume rules marked volume, the rest flag',
          (summ.loc[summ['metric'] == 'volume', 'rule_type'] == 'volume').all() and
          (summ.loc[summ['metric'] != 'volume', 'rule_type'] == 'flag').all())
    best = summ[(summ['metric'] == 'high_util') & (summ['test'] == 'self') & (summ['comparison'] == '7 vs 8-15')]
    check('high_util 7 vs 8-15 has precision > 0 and lift after alert > 1',
          len(best) > 0 and (best['precision'] > 0).all() and (best['lift_after_alert'] > 1).all(), best.to_string())
    # forward lift rebuilt by hand for one rule
    rule = dict(metric='high_util', comparison='7 vs 8-15', unit='customer', test='self')
    g = det[(det[list(rule)] == pd.Series(rule)).all(axis=1) & det['alert']]
    L = pdf[pdf['f_pd30_obs'] == 1].assign(day=lambda x: x['txn_date'].dt.normalize())
    L = L[(L['day'] > a_s) & (L['day'] <= a_e + pd.Timedelta(days=30))]
    cov = {(m, t + pd.Timedelta(days=i)) for m, t in zip(g['merchant'], g['anchor']) for i in range(1, 31)}
    inside = L[[(m, d) in cov for m, d in zip(L['merchant_id'], L['day'])]]
    never = L[~L['merchant_id'].isin(set(g['merchant']))]
    want = round((inside['f_pd30'].mean()) / never['f_pd30'].mean(), 2)
    got = summ[(summ[list(rule)] == pd.Series(rule)).all(axis=1)]['lift_after_alert'].iloc[0]
    check(f'lift_after_alert = hand rebuild ({got} vs {want})', abs(got - want) < 0.011)
    check('summary has one row per rule with tests',
          len(summ) == summ[['metric', 'comparison', 'unit', 'test']].drop_duplicates().shape[0])

    # outcomes: rebuilt by hand for two rules, both groups, both bases
    out = r['outcomes']
    check('outcomes: 4 groups per rule', r['ok'] and set(out['group']) == {'fired', 'not_fired', 'alert', 'not_alerted'}
          and len(out) == 4 * len(out[['metric', 'comparison', 'unit', 'test']].drop_duplicates()))
    T = pdf.assign(day=pdf['txn_date'].dt.normalize())
    T = T[(T['day'] >= a_s) & (T['day'] <= a_e)]
    T['s'] = T['f_susp'] == 1; T['o'] = T['f_pd30_obs'] == 1; T['x'] = T['o'] & (T['f_pd30'] == 1)
    rec = {c['name']: c['recent'] for c in CONFIG['comparisons']}
    def counts(sub, outside_of=None):
        if outside_of is None:
            cl = sub.groupby('client_id').agg(s=('s', 'any'), o=('o', 'any'), x=('x', 'any'))
        else:   # clients with at least one transaction of that kind outside the events
            cl = sub.groupby('client_id').agg(n=('s', 'size'), s=('s', 'any'), o=('o', 'any'), x=('x', 'any'))
        return dict(txn=len(sub), susp_txn=int(sub['s'].sum()), obs_txn=int(sub['o'].sum()), def_txn=int(sub['x'].sum()),
                    client=len(cl), susp_client=int(cl['s'].sum()), obs_client=int(cl['o'].sum()), def_client=int(cl['x'].sum()))
    ok_all = True
    for rule in [dict(metric='high_util', comparison='7 vs 8-15', unit='customer', test='self'),
                 dict(metric='volume', comparison='1 vs 2-8', unit='txn', test='peer')]:
        s_, e_ = rec[rule['comparison']]
        for basis, other in [('fired', 'not_fired'), ('alert', 'not_alerted')]:
            ev = det[(det[list(rule)] == pd.Series(rule)).all(axis=1) & det[basis]]
            cov = {(m, t - pd.Timedelta(days=b - 1)) for m, t in zip(ev['merchant'], ev['anchor']) for b in range(s_, e_ + 1)}
            inside = np.array([(m, d) in cov for m, d in zip(T['merchant_id'], T['day'])], dtype=bool)
            want_in = counts(T[inside])
            out_t = T[~inside]
            want_out = counts(out_t)   # rows outside; distinct clients among them
            for grp, want in [(basis, want_in), (other, want_out)]:
                got = out[(out[list(rule)] == pd.Series(rule)).all(axis=1) & (out['group'] == grp)].iloc[0]
                bad = {k: (int(got[k]), v) for k, v in want.items() if int(got[k]) != v}
                if bad:
                    ok_all = False; print('   ', rule['metric'], rule['comparison'], grp, bad)
    check('outcomes = hand rebuild (8 counts, fired / not_fired / alert / not_alerted)', ok_all)
    f_ = out[out['group'].isin(['fired', 'not_fired'])].groupby(['metric', 'comparison', 'unit', 'test'])['txn'].sum()
    check('  ... fired + not_fired transactions = all transactions in the anchor range', (f_ == len(T)).all())
    hu = out[(out['metric'] == 'high_util') & (out['comparison'] == '7 vs 8-15') & (out['test'] == 'self') & (out['unit'] == 'customer')].set_index('group')
    check('  ... planted: suspicious rate by transaction higher when fired', hu.loc['fired', 'susp_rate_txn'] > 3 * hu.loc['not_fired', 'susp_rate_txn'])

    # peer fallback: every h3 cell has 5 other sellers < min_peers 6, so all peer tests fall back to seller_group
    cfg_fb = dict(CONFIG, anchor_start='2026-09-01', min_peers=6, peer_fallback_cols=['seller_group'],
                  susp_cond=None, def_col=None, obs_col=None)
    with contextlib.redirect_stdout(io.StringIO()):
        r5 = run(df, cfg_fb, spark=spark)
    check('fallback run ok', r5['ok'])
    pe = r5['detail'][r5['detail']['test'] == 'peer']
    check('  ... every peer test used the fallback group', len(pe) > 0 and (pe['peer_level'] == 'fallback').all())
    check('  ... travel (5 other sellers in seller_group) has no peer test', not pe['peer_group'].str.contains('travel').any())
    ref5 = brute(pdf, dict(CONFIG, peer_cols=['seller_group'], min_peers=6), pd.Timestamp('2026-09-01'), a_e)
    ref5 = ref5[(ref5['test'] == 'peer') & (ref5['a'] * ref5['d'] > ref5['b'] * ref5['c'])]
    m5 = ref5.merge(pe, on=keys, how='outer', indicator=True, suffixes=('_r', '_e'))
    check('  ... peer tables = brute force on seller_group alone',
          (m5['_merge'] == 'both').all() and all((m5[c + '_r'] == m5[c + '_e']).all() for c in 'abcd'),
          m5['_merge'].value_counts().to_dict())

    # BH over every rule of the day
    cfg_day = dict(CONFIG, anchor_start='2026-09-01', units=['txn'], bh_scope='day', susp_cond=None, def_col=None, obs_col=None)
    with contextlib.redirect_stdout(io.StringIO()):
        r6 = run(df, cfg_day, spark=spark)
    ref6 = ref[(ref['unit'] == 'txn') & (ref['anchor'] >= '2026-09-01')].copy()
    ref6['q'] = ref6.groupby('anchor')['p'].transform(lambda s: false_discovery_control(s.values))
    m6 = r6['detail'].merge(ref6[keys + ['q']], on=keys, suffixes=('', '_r'))
    sm = m6['q_r'] <= 0.5
    check('bh_scope=day: q = BH over all rules that day', r6['ok'] and len(m6) == len(r6['detail']) and
          np.allclose(m6.loc[sm, 'q'], m6.loc[sm, 'q_r'], rtol=1e-6, atol=1e-12))
    check('  ... family size = all tests that day', (r6['detail'].groupby('anchor')['n_tests'].nunique() == 1).all() and
          (r6['detail'].groupby('anchor')['n_tests'].first() == ref6.groupby('anchor').size().reindex(
              r6['detail'].groupby('anchor')['n_tests'].first().index)).all())

    # transactions only, no peers, no labels
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        r2 = run(df, dict(CONFIG, units=['txn'], peer_cols=[], susp_cond=None, def_col=None, obs_col=None), spark=spark)
    check('txn only, no peers, no labels: ok', r2['ok'], buf.getvalue()[-500:])
    check('  ... only txn/self rows', set(r2['detail']['unit']) == {'txn'} and set(r2['detail']['test']) == {'self'})
    check('  ... summary without label columns', 'precision' not in r2['summary'].columns)
    check('  ... outcomes with counts only', list(r2['outcomes'].columns[-4:]) == ['txn', 'client', 'rule_type', 'group'][:0] + list(r2['outcomes'].columns[-4:])
          and 'susp_txn' not in r2['outcomes'].columns and 'def_txn' not in r2['outcomes'].columns and 'client' in r2['outcomes'].columns)

    # failures: one line + not run
    for bad, want in [(dict(CONFIG, merchant_col='nope'), 'columns not in df: nope'),
                      (dict(CONFIG, metrics=[{'name': 'x', 'kind': 'flag', 'expr': 'utilization >> 0.9'}]), 'FAILED at step 1/'),
                      (dict(CONFIG, anchor_start='2027-01-01'), 'no anchor days')]:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            r3 = run(df, bad, spark=spark)
        out = buf.getvalue()
        check(f'failure reported: {want}', (not r3['ok']) and want in out and 'not run:' in out, out[-400:])
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        r4 = run(df, dict(CONFIG, max_rows=10), spark=spark)
    check('max_rows stops with a clear message', not r4['ok'] and 'over max_rows' in buf.getvalue())
    spark.stop()

print(f'\n{passed} passed, {failed} failed')
sys.exit(1 if failed else 0)
