# ============================================================================
# Fisher backtest: outcomes by q band (run AFTER the generated backtest file,
# in the same notebook: it uses df, spark, CONFIG and detail from that run)
#
# For every rule, each transaction in the anchor range gets the SMALLEST q of
# the seller-days whose recent window covers it. Transactions no tested
# seller-day covers go in the band "not in detail" (not tested, or the share
# went down, so q is about 1). Then, per rule and band:
#   txn, client, susp_txn, susp_client, obs_txn, obs_client, def_txn, def_client
#   susp_rate_txn/client = susp / total, def_rate_txn/client = def / obs
# A client counts in every band where they have a transaction.
# ============================================================================
import builtins as _bi
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

_min, _max = _bi.min, _bi.max      # safe after `from pyspark.sql.functions import *`

BANDS = [0, 0.001, 0.01, 0.05, 0.2, 1.0]          # q cut-offs: edit if you like
REST = 'not in detail'


def q_band_outcomes(df, detail, config, spark, bands=BANDS):
    cfg = config
    keys = ['metric', 'comparison', 'unit', 'test']
    labels = [f'q {bands[i]}-{bands[i + 1]}' for i in range(len(bands) - 1)]
    rec = {c['name']: tuple(c['recent']) for c in cfg['comparisons']}
    has_s = bool(cfg.get('susp_cond'))
    has_d = bool(cfg.get('def_col') and cfg.get('obs_col'))

    det = detail[detail['unit'] != 'ratio'][keys + ['merchant', 'anchor', 'q', 'fired', 'alert']].copy()   # ratio z rows have no q
    det['anchor'] = pd.to_datetime(det['anchor'])
    a_s, a_e = det['anchor'].min(), det['anchor'].max()
    rules = det[keys].drop_duplicates().reset_index(drop=True)
    rules['rid'] = np.arange(len(rules))
    det = det.merge(rules, on=keys)
    det['band'] = pd.cut(det['q'], bands, labels=False, include_lowest=True).astype(int)
    det['ws'] = det['comparison'].map(lambda c: rec[c][0])
    det['we'] = det['comparison'].map(lambda c: rec[c][1])
    print(f'[1/3] {len(det):,} tested seller-days, {len(rules)} rules, anchors {a_s.date()} to {a_e.date()}')

    # seller-day -> its recent window -> per (rule, seller, day) the smallest band
    D = spark.createDataFrame(pd.DataFrame({
        'rid': det['rid'].astype(int), '_m': det['merchant'].astype(str),
        '_t': det['anchor'].dt.strftime('%Y-%m-%d'), 'band': det['band'].astype(int),
        'ws': det['ws'].astype(int), 'we': det['we'].astype(int)}))
    lo = F.greatest(F.expr('date_sub(to_date(_t), cast(we - 1 as int))'), F.lit(str(a_s.date())).cast('date'))
    hi = F.expr('date_sub(to_date(_t), cast(ws - 1 as int))')
    cov = (D.withColumn('_lo', lo).withColumn('_hi', hi).where(F.col('_lo') <= F.col('_hi'))
           .withColumn('_d', F.explode(F.sequence('_lo', '_hi')))
           .groupBy('rid', '_m', '_d').agg(F.min('band').alias('band')))

    d = F.to_date(F.col(cfg['date_col']))
    one = lambda c: F.when(c, 1).otherwise(0)
    T = df.where(d.between(str(a_s.date()), str(a_e.date()))).select(
        F.col(cfg['merchant_col']).cast('string').alias('_m'), d.alias('_d'),
        F.col(cfg['customer_col']).cast('string').alias('_c'),
        (one(F.expr(cfg['susp_cond'])) if has_s else F.lit(0)).alias('_s'),
        (one(F.col(cfg['obs_col']).cast('int') == 1) if has_d else F.lit(0)).alias('_o'),
        (one((F.col(cfg['obs_col']).cast('int') == 1) & (F.col(cfg['def_col']).cast('int') == 1))
         if has_d else F.lit(0)).alias('_x')).persist()
    tx = lambda: [F.count(F.lit(1)).alias('txn'), F.sum('_s').alias('susp_txn'),
                  F.sum('_o').alias('obs_txn'), F.sum('_x').alias('def_txn')]
    cl = lambda c: [F.count(F.lit(1)).alias('client'), F.sum(one(F.col(c + 's') > 0)).alias('susp_client'),
                    F.sum(one(F.col(c + 'o') > 0)).alias('obs_client'), F.sum(one(F.col(c + 'x') > 0)).alias('def_client')]
    print('[2/3] joining transactions to the covered seller-days ...')
    tot = T.agg(*tx()).toPandas().iloc[0]
    per_c = T.groupBy('_c').agg(F.count(F.lit(1)).alias('n'), F.sum('_s').alias('ns'),
                                F.sum('_o').alias('no'), F.sum('_x').alias('nx'))
    tot_c = per_c.agg(*cl('n')).toPandas().iloc[0]
    J = T.join(cov, ['_m', '_d']).persist()                    # one row per transaction x rule covering it
    in_t = J.groupBy('rid', 'band').agg(*tx()).toPandas()
    jc = J.groupBy('rid', 'band', '_c').agg(F.count(F.lit(1)).alias('k'), F.sum('_s').alias('ks'),
                                            F.sum('_o').alias('ko'), F.sum('_x').alias('kx'))
    in_c = jc.groupBy('rid', 'band').agg(*cl('k')).toPandas()
    # rest: clients with at least one transaction (of that kind) not covered by the rule
    jr = J.groupBy('rid', '_c').agg(F.count(F.lit(1)).alias('k'), F.sum('_s').alias('ks'),
                                    F.sum('_o').alias('ko'), F.sum('_x').alias('kx'))
    full = jr.join(per_c, '_c').groupBy('rid').agg(
        F.sum(one(F.col('k') == F.col('n'))).alias('client'),
        F.sum(one((F.col('ns') > 0) & (F.col('ks') == F.col('ns')))).alias('susp_client'),
        F.sum(one((F.col('no') > 0) & (F.col('ko') == F.col('no')))).alias('obs_client'),
        F.sum(one((F.col('nx') > 0) & (F.col('kx') == F.col('nx')))).alias('def_client')).toPandas()
    J.unpersist(); T.unpersist()

    print('[3/3] building the table ...')
    tcols, ccols = ['txn', 'susp_txn', 'obs_txn', 'def_txn'], ['client', 'susp_client', 'obs_client', 'def_client']
    ev = det.groupby(['rid', 'band']).agg(seller_days=('q', 'size'), fired_days=('fired', 'sum'),
                                          alert_days=('alert', 'sum')).reset_index()
    rows = []
    for _, ru in rules.iterrows():
        rid = ru['rid']
        it, ic = in_t[in_t['rid'] == rid], in_c[in_c['rid'] == rid]
        for b, lab in enumerate(labels):
            r = {k: ru[k] for k in keys}; r['band'] = lab
            e = ev[(ev['rid'] == rid) & (ev['band'] == b)]
            r.update(seller_days=int(e['seller_days'].sum()), fired_days=int(e['fired_days'].sum()),
                     alert_days=int(e['alert_days'].sum()))
            t_, c_ = it[it['band'] == b], ic[ic['band'] == b]
            r.update({k: int(t_[k].sum()) for k in tcols}); r.update({k: int(c_[k].sum()) for k in ccols})
            rows.append(r)
        r = {k: ru[k] for k in keys}; r['band'] = REST
        r.update(seller_days=0, fired_days=0, alert_days=0)
        r.update({k: int(tot[k] or 0) - int(it[k].sum()) for k in tcols})
        f_ = full[full['rid'] == rid]
        r.update({k: int(tot_c[k] or 0) - (int(f_[k].iloc[0]) if len(f_) else 0) for k in ccols})
        rows.append(r)
    o = pd.DataFrame(rows)
    dv = lambda a, b: (o[a] / o[b].where(o[b] > 0)).round(4)
    o['susp_rate_txn'], o['susp_rate_client'] = dv('susp_txn', 'txn'), dv('susp_client', 'client')
    o['def_rate_txn'], o['def_rate_client'] = dv('def_txn', 'obs_txn'), dv('def_client', 'obs_client')
    if not has_s:
        o = o.drop(columns=['susp_txn', 'susp_client', 'susp_rate_txn', 'susp_rate_client'])
    if not has_d:
        o = o.drop(columns=['obs_txn', 'obs_client', 'def_txn', 'def_client', 'def_rate_txn', 'def_rate_client'])
    print(f'      done: {len(o):,} rows ({len(rules)} rules x {len(labels) + 1} bands)')
    return o


q_bands = q_band_outcomes(df, detail, CONFIG, spark)

# Look at one rule: do the rates rise as q gets smaller?
# q_bands[(q_bands.metric == 'high_util') & (q_bands.comparison == '7 vs 8-15')
#         & (q_bands.unit == 'customer') & (q_bands.test == 'self')]
q_bands
