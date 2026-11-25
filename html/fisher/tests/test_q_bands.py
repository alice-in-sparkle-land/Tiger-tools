"""Test for extras/fisher_q_bands_cell.py: runs the backtest on fake data on a local Spark
(after `from pyspark.sql.functions import *`, like the notebook), then the cell, and checks
every band against a transaction-by-transaction pandas rebuild.

    python html/fisher/tests/test_q_bands.py
"""
import os, sys, io, time, contextlib
os.environ['TZ'] = 'UTC'; time.tzset()
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
T_ = os.path.join(HERE, 'test_engine.py')
src = open(T_).read(); ns = {'__file__': T_}
exec(src[:src.index('# ---------------------------------------------------------------------------- Part B')], ns)
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master('local[2]').config('spark.sql.shuffle.partitions', '4')
         .config('spark.ui.showConsoleProgress', 'false').config('spark.sql.session.timeZone', 'UTC').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
pdf = ns['fake'](); df = spark.createDataFrame(pdf)
CONFIG = dict(ns['CONFIG'], anchor_start='2026-08-01')
g = {'spark': spark, 'df': df, 'CONFIG': CONFIG}
exec('from pyspark.sql.functions import *', g)                     # notebook case
exec(open(os.path.join(HERE, '..', 'src', 'engine.py')).read(), g)
with contextlib.redirect_stdout(io.StringIO()):
    r = g['run_fisher_backtest'](df, CONFIG, spark=spark)
g['detail'] = r['detail']
exec(open(os.path.join(HERE, '..', 'extras', 'fisher_q_bands_cell.py')).read(), g)
qb, det = g['q_bands'], r['detail']
ok = True
bands = [0, 0.001, 0.01, 0.05, 0.2, 1.0]
labs = [f'q {bands[i]}-{bands[i+1]}' for i in range(5)] + ['not in detail']
a_s, a_e = det['anchor'].min(), det['anchor'].max()
T = pdf.assign(day=pdf['txn_date'].dt.normalize()); T = T[(T['day'] >= a_s) & (T['day'] <= a_e)].copy()
T['s'] = T['f_susp'] == 1; T['o'] = T['f_pd30_obs'] == 1; T['x'] = T['o'] & (T['f_pd30'] == 1)
rec = {c['name']: c['recent'] for c in CONFIG['comparisons']}
for rule in [dict(metric='high_util', comparison='7 vs 8-15', unit='customer', test='self'),
             dict(metric='volume', comparison='30 vs 31-60', unit='txn', test='peer:h3_9'),
             dict(metric='money_surge', comparison='7 vs 8-15', unit='amount', test='self'),
             dict(metric='big_ticket', comparison='1 vs 2-8', unit='txn', test='self')]:
    d = det[(det[list(rule)] == pd.Series(rule)).all(axis=1)]
    s_, e_ = rec[rule['comparison']]
    best = {}
    for m, t, q in zip(d['merchant'], d['anchor'], d['q']):
        for b in range(s_, e_ + 1):
            k = (m, t - pd.Timedelta(days=b - 1)); best[k] = min(best.get(k, 9), q)
    qq = np.array([best.get((m, dd), np.nan) for m, dd in zip(T['merchant_id'], T['day'])])
    T['band'] = np.where(np.isnan(qq), 5, pd.cut(pd.Series(qq), bands, labels=False, include_lowest=True).fillna(5).astype(int))
    for b, lab in enumerate(labs):
        sub = T[T['band'] == b]; cl = sub.groupby('client_id')[['s', 'o', 'x']].any()
        want = dict(txn=len(sub), susp_txn=int(sub.s.sum()), obs_txn=int(sub.o.sum()), def_txn=int(sub.x.sum()),
                    client=len(cl), susp_client=int(cl.s.sum()), obs_client=int(cl.o.sum()), def_client=int(cl.x.sum()))
        got = qb[(qb[list(rule)] == pd.Series(rule)).all(axis=1) & (qb['band'] == lab)].iloc[0]
        bad = {k: (int(got[k]), v) for k, v in want.items() if int(got[k]) != v}
        if bad: ok = False; print('MISMATCH', rule['metric'], rule['comparison'], lab, bad)
        if b < 5:
            n = ((pd.cut(d['q'], bands, labels=False, include_lowest=True) == b)).sum()
            if int(got['seller_days']) != n: ok = False; print('seller_days', lab, got['seller_days'], n)
tot = qb.groupby(['metric', 'comparison', 'unit', 'test'])['txn'].sum()
checks = [('q bands = brute force (4 rules incl. money surge, 6 bands, 8 counts and seller_days)', ok),
          ('bands add up to all transactions, every rule', bool((tot == len(T)).all())),
          ('6 bands per rule', len(qb) == 6 * len(tot))]
for name, c in checks:
    print('  ok  ' if c else '  FAIL', name)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 30)
print(qb[(qb.metric == 'high_util') & (qb.comparison == '7 vs 8-15') & (qb.unit == 'customer') & (qb.test == 'self')].to_string())
spark.stop()
n_fail = sum(not c for _, c in checks)
print(f'\n{len(checks) - n_fail} passed, {n_fail} failed')
sys.exit(1 if n_fail else 0)
