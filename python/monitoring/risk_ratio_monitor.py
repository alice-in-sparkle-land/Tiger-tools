"""
risk_ratio_monitor: spot recent changes in a default ratio (e.g. fpd30) by segment, as one HTML report.

    from risk_ratio_monitor import run_risk_ratio_monitor, EXAMPLE_SOURCES
    run_risk_ratio_monitor(EXAMPLE_SOURCES, '/dbfs/FileStore/risk_ratio_monitor.html', dry_run=True)
    r = run_risk_ratio_monitor(EXAMPLE_SOURCES, '/dbfs/FileStore/risk_ratio_monitor.html', sql_var=sql_var)

    ratio        = sum(def) / sum(obs)                                            (observed loans)
    ratio_client = count(distinct client with def=1) / count(distinct client with obs=1)

What it does
  1. Aggregates the loan-level data in Spark, one pass per granularity (segment columns are
     unpivoted, so all columns share one groupBy).
  2. Per segment value: z-score of the last n_recent MATURED periods (>= min_mature of the loans
     observable) against all matured periods before them, on both ratios. A period only counts
     when obs >= min_obs or observed amount > min_amt; values that never pass are skipped.
  3. Drill-down data: each scored value split by every other segment column, and its top clients.
  4. Explore data: every filter combination (value or All per column) with exact distinct clients
     (GROUPING SETS), small combinations dropped so the file stays small.
  5. Writes one offline HTML file. Prints every step; stops at the first failed step and says where.
"""
import os, re, json, time, hashlib
from itertools import combinations
import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# failure printout (kept in this file: no extra module to copy to the server)
# ----------------------------------------------------------------------------
def _query_context(sql, msg, around=2, width=200):
    """The query lines around Spark's 'line N pos M', arrow on line N; [] when there is no line."""
    m = re.search(r'line (\d+),? pos (\d+)', msg or '')
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
    """The '####' block printed when a run stops: what failed, where, the error, the query lines, what did not run."""
    msg = (msg or '').strip()
    width = max([len(k) for k in where] + [5])
    out = ['', '#' * 90, f"FAILED at {title}"]
    for k, v in where.items():
        out.append(f"  {k.ljust(width)}: {v}")
    first = msg.splitlines()[0][:500] if msg else (err_type or 'unknown error')
    out.append(f"  {'error'.ljust(width)}: {first}")
    for l in msg.splitlines()[1:8]:
        out.append(' ' * (width + 4) + l[:300])
    out += _query_context(sql, msg)
    if not_run:
        out.append(f"  not run: {', '.join(not_run)}")
    out.append('#' * 90)
    return out

# ----------------------------------------------------------------------------
# settings
# ----------------------------------------------------------------------------
DEFAULTS = {
    'grans':             ['weekly', 'monthly'],          # 'daily' also works
    'tz':                'Asia/Ho_Chi_Minh',
    'trunc':             {'daily': 'day', 'weekly': 'week', 'monthly': 'month'},
    'lookback':          {'daily': 6, 'weekly': 24, 'monthly': 36},   # months of history
    'min_obs':           4,          # a period counts if obs >= min_obs ...
    'min_amt':           5_000_000,  # ... or observed amount > min_amt
    'min_mature':        0.8,        # a period is scored when >= 80% of its loans are observable
    'n_recent':          5,
    'z_th':              2.0,
    'min_base':          6,          # usable baseline periods needed to score a value
    'top_n':             10,
    'spark_n':           30,
    'recency_w':         {1: 1.0, 2: 0.8, 3: 0.6, 4: 0.45, 5: 0.3},
    'detail_n':          {'daily': 120, 'weekly': 104, 'monthly': 60},
    # drill-down
    'drill':             True,
    'drill_top_k':       6,          # sub-segments shown per split, by volume
    'drill_min_elig':    0.6,        # a sub-segment must be usable in >= 60% of shown periods ...
    'drill_min_recent':  3,          # ... and in >= 3 of the last n_recent
    'top_clients':       10,
    'conc_th':           0.4,        # 'concentrated' when the top 5 clients hold >= 40% of defaults
    'mask_client':       False,      # True: client ids shown as a short hash
    # explore tab
    'explore':           True,
    'explore_cols':      None,       # None = all seg_cols; or a shorter list
    'explore_max_val':   10,         # values per column kept by volume; the rest -> '(other)'
    'explore_min_loans': 200,        # drop combinations with fewer loans over the window
    'explore_max_rows':  400_000,    # cap; the minimum doubles until the cube fits
    'title':             'Risk Ratio Monitor',
}

EXAMPLE_SOURCES = [
    {
        'name':       'pd30',                     # short id, no '|'
        'label':      'PD30',
        'data':       "select * from your_schema.your_loan_table",   # SQL string or a Spark DataFrame
        'date_col':   'disbursement_date',        # period date of the loan, local time
        'def_col':    'f_pd30',                   # default flag 0/1
        'obs_col':    'f_pd30_obs',               # observable flag 0/1
        'client_col': 'client_id',
        'amt_col':    'loan_amount',              # None = only the obs-count rule
        'seg_cols':   ['a', 'b', 'c'],
    },
]

_METS = ['obs_n', 'def_n', 'obs_amt', 'obs_cl', 'def_cl']


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def _settings(settings):
    unknown = set(settings) - set(DEFAULTS)
    if unknown:
        raise ValueError(f"Unknown setting(s): {', '.join(sorted(unknown))}. Known: {', '.join(DEFAULTS)}")
    cfg = {k: (v.copy() if isinstance(v, (dict, list)) else v) for k, v in DEFAULTS.items()}
    cfg.update(settings)
    return cfg


def _check_sources(sources):
    if not sources:
        raise ValueError('sources is empty.')
    names = set()
    for i, s in enumerate(sources):
        for k in ('name', 'data', 'date_col', 'def_col', 'obs_col', 'client_col', 'seg_cols'):
            if s.get(k) is None or (isinstance(s.get(k), (str, list)) and not s.get(k)):
                raise ValueError(f"sources[{i}] has no '{k}'.")
        if '|' in s['name']:
            raise ValueError(f"source name '{s['name']}' contains '|'.")
        if s['name'] in names:
            raise ValueError(f"source name '{s['name']}' is used twice.")
        names.add(s['name'])
        labels = [c.split('.')[-1] for c in s['seg_cols']]
        if len(set(labels)) != len(labels):
            raise ValueError(f"source '{s['name']}': two seg_cols end with the same name: {labels}")


def _now(cfg, now):
    t = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz=cfg['tz'])
    if t.tzinfo is not None:
        t = t.tz_convert(cfg['tz']).tz_localize(None)
    return t


def current_period_start(g, now):
    d = now.normalize()
    if g == 'daily':
        return d
    if g == 'weekly':
        return d - pd.Timedelta(days=d.weekday())
    return d.replace(day=1)


def lookback_start(g, now, cfg):
    """First moment inside the history window. A period that starts earlier is partial and dropped."""
    return now.normalize() - pd.DateOffset(months=cfg['lookback'][g])


def fmt_period(p, g):
    return p.strftime('%Y-%m') if g == 'monthly' else (
        'wk ' + p.strftime('%Y-%m-%d') if g == 'weekly' else p.strftime('%Y-%m-%d'))


def _nz(x, nd=5):
    return None if x is None or pd.isna(x) else round(float(x), nd)


def _ts(s):
    """Any datetime-ish Series -> naive pd.Timestamp values (works on old and new pandas)."""
    return pd.to_datetime(s).map(pd.Timestamp)


# ----------------------------------------------------------------------------
# 1. Spark steps (each returns a pandas DataFrame)
# ----------------------------------------------------------------------------
def _F():
    from pyspark.sql import functions as F
    return F


def _base(sdf, src, g, cfg, now):
    F = _F()
    lb = lookback_start(g, now, cfg).strftime('%Y-%m-%d')
    a = src.get('amt_col')
    sdf = sdf.where(F.col(src['date_col']) >= F.lit(lb))
    cols = [F.date_trunc(cfg['trunc'][g], F.col(src['date_col'])).alias('date_value'),
            F.col(src['def_col']).alias('_d'), F.col(src['obs_col']).alias('_o'),
            F.col(src['client_col']).alias('_c'), (F.col(a) if a else F.lit(0)).alias('_a')]
    return sdf, cols


def _metrics(F, with_loans=True):
    obs1 = F.col('_o') == 1
    m = [F.sum(F.when(obs1, 1).otherwise(0)).alias('obs_n'),
         F.sum(F.when(obs1, F.col('_d')).otherwise(0)).alias('def_n'),        # defaults on observed loans only
         F.sum(F.when(obs1, F.col('_a')).otherwise(0)).alias('obs_amt'),
         F.countDistinct(F.when(obs1, F.col('_c'))).alias('obs_cl'),
         F.countDistinct(F.when(obs1 & (F.col('_d') == 1), F.col('_c'))).alias('def_cl')]
    return ([F.count(F.lit(1)).alias('n_loan')] if with_loans else []) + m


def spark_segments(sdf, src, g, cfg, now):
    """One row per period x segment column x value."""
    F = _F()
    sdf, cols = _base(sdf, src, g, cfg, now)
    seg = F.explode(F.array(*[
        F.struct(F.lit(s.split('.')[-1]).alias('column_name'),
                 F.coalesce(F.col(s).cast('string'), F.lit('(null)')).alias('column_value'))
        for s in src['seg_cols']]))
    long = sdf.select(*cols, seg.alias('_s'))
    pdf = (long.groupBy('date_value', F.col('_s.column_name').alias('column_name'),
                        F.col('_s.column_value').alias('column_value'))
           .agg(*_metrics(F)).toPandas())
    pdf['source'], pdf['date_name'] = src['name'], g
    return pdf


def spark_pairs(sdf, src, g, cfg, now):
    """Two-way cuts (a x b, a x c, b x c) for the drill-down; both directions are built in pandas."""
    F = _F()
    cs = src['seg_cols']
    sdf, cols = _base(sdf, src, g, cfg, now)
    val = lambda s: F.coalesce(F.col(s).cast('string'), F.lit('(null)'))
    pair = F.explode(F.array(*[
        F.struct(F.lit(cs[i].split('.')[-1]).alias('c1'), val(cs[i]).alias('v1'),
                 F.lit(cs[j].split('.')[-1]).alias('c2'), val(cs[j]).alias('v2'))
        for i in range(len(cs)) for j in range(i + 1, len(cs))]))
    long = sdf.select(*cols, pair.alias('_p'))
    pdf = (long.groupBy('date_value', '_p.c1', '_p.v1', '_p.c2', '_p.v2').agg(*_metrics(F, with_loans=False))
           .where(F.col('obs_n') > 0).toPandas())
    pdf['source'], pdf['date_name'] = src['name'], g
    return pdf


def spark_clients(sdf, src, g, cfg, rec_p):
    """Top clients per segment value over the recent periods (observed loans only)."""
    F = _F()
    from pyspark.sql import Window
    dv = F.date_trunc(cfg['trunc'][g], F.col(src['date_col']))
    rec_lit = [F.lit(p.strftime('%Y-%m-%d %H:%M:%S')).cast('timestamp') for p in rec_p]
    a = src.get('amt_col')
    seg = F.explode(F.array(*[
        F.struct(F.lit(s.split('.')[-1]).alias('column_name'),
                 F.coalesce(F.col(s).cast('string'), F.lit('(null)')).alias('column_value'))
        for s in src['seg_cols']]))
    long = (sdf.where(F.col(src['date_col']) >= F.lit(rec_p[0].strftime('%Y-%m-%d')))
            .select(dv.alias('date_value'), F.col(src['def_col']).alias('_d'), F.col(src['obs_col']).alias('_o'),
                    F.col(src['client_col']).cast('string').alias('client'), (F.col(a) if a else F.lit(0)).alias('_a'),
                    seg.alias('_s'))
            .where(F.col('date_value').isin(*rec_lit) & (F.col('_o') == 1)))
    lag_cols = [F.sum(F.when(F.col('date_value') == rec_lit[i], F.col('_d')).otherwise(0)).alias(f'd{len(rec_p) - i}')
                for i in range(len(rec_p))]                                   # d1 = most recent period
    agg = (long.groupBy(F.col('_s.column_name').alias('column_name'), F.col('_s.column_value').alias('column_value'), 'client')
           .agg(F.count(F.lit(1)).alias('obs_n'), F.sum('_d').alias('def_n'), F.sum('_a').alias('obs_amt'), *lag_cols))
    w = Window.partitionBy('column_name', 'column_value').orderBy(F.desc('def_n'), F.desc('obs_n'), F.desc('obs_amt'))
    pdf = agg.withColumn('rk', F.row_number().over(w)).where(F.col('rk') <= cfg['top_clients']).toPandas()
    pdf['source'], pdf['date_name'] = src['name'], g
    return pdf


def spark_top_values(sdf, src, cfg):
    F = _F()
    cols = [c for c in (cfg['explore_cols'] or src['seg_cols']) if c in src['seg_cols']]
    vals = {}
    for col in cols:
        top = (sdf.select(F.coalesce(F.col(col).cast('string'), F.lit('(null)')).alias('v'))
               .groupBy('v').count().orderBy(F.desc('count')).limit(cfg['explore_max_val']).toPandas())
        vals[col] = top['v'].astype(str).tolist()
    return cols, vals


def spark_cube(spark, sdf, src, g, cfg, now, cols, vals, log=print):
    """GROUPING SETS over every subset of the explore columns: exact distinct clients for any filter."""
    F = _F()
    from pyspark.sql import Window
    sdf, base_cols = _base(sdf, src, g, cfg, now)
    ks = [f'k{i}' for i in range(len(cols))]
    sel = list(base_cols)
    for k, col in zip(ks, cols):
        v = F.coalesce(F.col(col).cast('string'), F.lit('(null)'))
        sel.append(F.when(v.isin(*vals[col]), v).otherwise(F.lit('(other)')).alias(k))
    view = f"_explore_{src['name']}_{g}"
    sdf.select(*sel).createOrReplaceTempView(view)
    sets = ', '.join('(' + ', '.join(['date_value'] + list(s)) + ')' for r in range(len(ks) + 1) for s in combinations(ks, r))
    q = f"""select date_value, {', '.join(f"case when grouping({k}) = 1 then '__ALL__' else {k} end as {k}" for k in ks)}
     , count(1)                                                as n_loan
     , sum(case when _o = 1 then 1 else 0 end)                 as obs_n
     , sum(case when _o = 1 then _d else 0 end)                as def_n
     , sum(case when _o = 1 then _a else 0 end)                as obs_amt
     , count(distinct case when _o = 1 then _c end)            as obs_cl
     , count(distinct case when _o = 1 and _d = 1 then _c end) as def_cl
from {view}
group by date_value, {', '.join(ks)} grouping sets ({sets})"""
    cube = spark.sql(q)
    cube = cube.withColumn('_combo_n', F.sum('n_loan').over(Window.partitionBy(*ks))).cache()
    min_loans = cfg['explore_min_loans']
    try:
        while True:
            kept = cube.where(F.col('_combo_n') >= min_loans)
            n_rows = kept.count()
            if n_rows <= cfg['explore_max_rows']:
                break
            log(f"        {n_rows:,} rows with >= {min_loans:,} loans: over explore_max_rows, doubling the minimum")
            min_loans *= 2
        log(f"        {n_rows:,} rows kept (combinations with >= {min_loans:,} loans)")
        pdf = kept.drop('_combo_n').toPandas()
    finally:
        cube.unpersist()
    return pdf, ks, min_loans


# ----------------------------------------------------------------------------
# 2. pandas steps (no Spark: testable anywhere)
# ----------------------------------------------------------------------------
def _clean(df):
    df = df.copy()
    df['date_value'] = _ts(df['date_value'])
    for c in [c for c in ['n_loan'] + _METS if c in df.columns]:
        df[c] = df[c].astype(float)
    return df


def score_block(d, src, g, col, cfg, now, log=print):
    d = d[(d['date_value'] < current_period_start(g, now)) & (d['date_value'] >= lookback_start(g, now, cfg))]
    if d.empty:
        return None, {}, None
    tot = d.groupby('date_value')[['n_loan', 'obs_n']].sum()
    periods = sorted(tot.index[(tot['obs_n'] / tot['n_loan'].replace(0, np.nan)) >= cfg['min_mature']])
    n_recent, min_base = cfg['n_recent'], cfg['min_base']
    if len(periods) < n_recent + min_base:
        log(f"      skip {src}/{g}/{col}: only {len(periods)} matured periods (need {n_recent + min_base})")
        return None, {}, None

    pv = {m: d.pivot_table(index='column_value', columns='date_value', values=m, aggfunc='sum')
              .reindex(columns=periods).fillna(0) for m in _METS}
    elig = (pv['obs_n'] >= cfg['min_obs']) | (pv['obs_amt'] > cfg['min_amt'])
    base_p, rec_p = periods[:-n_recent], periods[-n_recent:]
    keep_v = elig[rec_p].any(axis=1) & (elig[base_p].sum(axis=1) >= min_base)
    if not keep_v.any():
        return None, {}, None
    pv = {m: t[keep_v] for m, t in pv.items()}
    elig = elig[keep_v]

    ratio = (pv['def_n'] / pv['obs_n'].replace(0, np.nan)).where(elig)
    ratio_cl = (pv['def_cl'] / pv['obs_cl'].replace(0, np.nan)).where(elig)
    mu_r, sd_r = ratio[base_p].mean(axis=1), ratio[base_p].std(axis=1)
    mu_c, sd_c = ratio_cl[base_p].mean(axis=1), ratio_cl[base_p].std(axis=1)

    out = []
    for lag, p in enumerate(reversed(rec_p), start=1):
        out.append(pd.DataFrame({
            'source': src, 'date_name': g, 'column_name': col, 'column_value': ratio.index,
            'period': fmt_period(p, g), 'lag': lag, 'eligible': elig[p].values,
            'obs_n': pv['obs_n'][p].values, 'def_n': pv['def_n'][p].values, 'obs_amt': pv['obs_amt'][p].values,
            'obs_cl': pv['obs_cl'][p].values, 'def_cl': pv['def_cl'][p].values,
            'ratio': ratio[p].values, 'base_ratio': mu_r.values,
            'z_ratio': ((ratio[p] - mu_r) / sd_r.replace(0, np.nan)).values,
            'ratio_cl': ratio_cl[p].values, 'base_ratio_cl': mu_c.values,
            'z_cl': ((ratio_cl[p] - mu_c) / sd_c.replace(0, np.nan)).values,
        }))

    keep_p = periods[-cfg['detail_n'][g]:]
    series = {}
    for v in ratio.index:
        series[f'{src}|{g}|{col}|{v}'] = {
            'p':  [fmt_period(p, g) for p in keep_p],
            'r':  [_nz(x) for x in ratio.loc[v, keep_p]],         # None = thin period (gap)
            'rc': [_nz(x) for x in ratio_cl.loc[v, keep_p]],
            'o':  [float(x) for x in pv['obs_n'].loc[v, keep_p]],
            'd':  [float(x) for x in pv['def_n'].loc[v, keep_p]],
            'a':  [float(x) for x in pv['obs_amt'].loc[v, keep_p]],
            'mr': _nz(mu_r[v]), 'sr': _nz(sd_r[v]), 'mc': _nz(mu_c[v]), 'sc': _nz(sd_c[v]),
        }
    return pd.concat(out, ignore_index=True), series, (periods, keep_p)


def score(raw, cfg, now, log=print):
    """raw: one row per source x gran x column x value x period -> (res, series, periods)."""
    raw = _clean(raw)
    raw['column_value'] = raw['column_value'].astype(str)
    parts, series, periods = [], {}, {}
    for (src, g, col), d in raw.groupby(['source', 'date_name', 'column_name']):
        r, s, per = score_block(d, src, g, col, cfg, now, log)
        if r is not None:
            parts.append(r)
            series.update(s)
            periods[(src, g, col)] = per
    if not parts:
        return None, {}, {}
    res = pd.concat(parts, ignore_index=True)
    res['flag'] = ''
    res.loc[~res['eligible'].astype(bool), ['z_ratio', 'z_cl']] = np.nan       # thin periods: shown, never flagged
    res['z_max'] = res[['z_ratio', 'z_cl']].max(axis=1)
    res['z_min'] = res[['z_ratio', 'z_cl']].min(axis=1)
    is_spike = res['z_max'] > cfg['z_th']                                    # ratio up = risk up
    is_drop = (res['z_min'] < -cfg['z_th']) & ~is_spike
    res['direction'] = np.select([is_spike, is_drop], ['spike', 'drop'], '')
    w = res['lag'].map(cfg['recency_w']).fillna(min(cfg['recency_w'].values()))
    mag = np.where(is_spike, res['z_max'], np.where(is_drop, -res['z_min'], res[['z_ratio', 'z_cl']].abs().max(axis=1)))
    res['score'] = (pd.Series(mag, index=res.index).fillna(0) * w).round(2)
    res['dir_rank'] = res['direction'].map({'spike': 0, 'drop': 1, '': 2})
    res = res.sort_values(['dir_rank', 'lag', 'score'], ascending=[True, True, False]).reset_index(drop=True)
    res['ord'] = res.index
    res['key'] = res['source'] + '|' + res['date_name'] + '|' + res['column_name'] + '|' + res['column_value'].astype(str)
    return res, series, periods


def build_drill(pairs, series, periods, cfg):
    """pairs: c1, v1, c2, v2 + metrics per period -> {parent key: {child column: {kids, hidden, hidden_sh}}}."""
    if pairs is None or len(pairs) == 0:
        return {}
    pr = _clean(pairs)
    fwd = pr.rename(columns={'c1': 'pcol', 'v1': 'pval', 'c2': 'ccol', 'v2': 'cval'})
    rev = pr.rename(columns={'c2': 'pcol', 'v2': 'pval', 'c1': 'ccol', 'v1': 'cval'})
    pr = pd.concat([fwd, rev], ignore_index=True)
    pr['pval'], pr['cval'] = pr['pval'].astype(str), pr['cval'].astype(str)
    n_recent = cfg['n_recent']
    drill = {}
    for (src, g, pcol, ccol), d in pr.groupby(['source', 'date_name', 'pcol', 'ccol']):
        if (src, g, pcol) not in periods:
            continue
        per, keep_p = periods[(src, g, pcol)]                   # same matured periods as the parent chart
        base_p, rec_p = per[:-n_recent], per[-n_recent:]
        d = d[d['date_value'].isin(per)]
        for pval, dd in d.groupby('pval'):
            key = f'{src}|{g}|{pcol}|{pval}'
            if key not in series:
                continue
            pv = {m: dd.pivot_table(index='cval', columns='date_value', values=m, aggfunc='sum')
                      .reindex(columns=per).fillna(0) for m in _METS}
            elig = (pv['obs_n'] >= cfg['min_obs']) | (pv['obs_amt'] > cfg['min_amt'])
            ok = (elig[keep_p].mean(axis=1) >= cfg['drill_min_elig']) & (elig[rec_p].sum(axis=1) >= cfg['drill_min_recent'])
            vol = pv['obs_n'][keep_p].sum(axis=1)
            kids = list(vol[ok].sort_values(ascending=False).index[:cfg['drill_top_k']])
            rec_obs = pv['obs_n'][rec_p].sum(axis=1)
            tot_rec = rec_obs.sum() or np.nan
            ratio = (pv['def_n'] / pv['obs_n'].replace(0, np.nan)).where(elig)
            ratio_cl = (pv['def_cl'] / pv['obs_cl'].replace(0, np.nan)).where(elig)
            enough = ratio[base_p].notna().sum(axis=1) >= 3
            mu_r, sd_r = ratio[base_p].mean(axis=1), ratio[base_p].std(axis=1).replace(0, np.nan)
            mu_c, sd_c = ratio_cl[base_p].mean(axis=1), ratio_cl[base_p].std(axis=1).replace(0, np.nan)
            rp = list(reversed(rec_p))                          # lag 1..n
            out = []
            for k in kids:
                zr = ((ratio.loc[k, rp] - mu_r[k]) / sd_r[k]) if enough[k] else pd.Series(np.nan, index=rp)
                zc = ((ratio_cl.loc[k, rp] - mu_c[k]) / sd_c[k]) if enough[k] else pd.Series(np.nan, index=rp)
                out.append({
                    'v': k, 'sh': _nz(rec_obs[k] / tot_rec, 4), 'on': float(rec_obs[k]),
                    'r': [_nz(x) for x in ratio.loc[k, keep_p]], 'rc': [_nz(x) for x in ratio_cl.loc[k, keep_p]],
                    'o': [float(x) for x in pv['obs_n'].loc[k, keep_p]],
                    'mr': _nz(mu_r[k]), 'mc': _nz(mu_c[k]),
                    'zr': [_nz(x, 2) for x in zr], 'zc': [_nz(x, 2) for x in zc],
                    'lr': [_nz(x) for x in ratio.loc[k, rp]], 'lc': [_nz(x) for x in ratio_cl.loc[k, rp]],
                })
            hid = [v for v in vol.index if v not in set(kids)]
            drill.setdefault(key, {})[ccol] = {
                'kids': out, 'hidden': len(hid),
                'hidden_sh': _nz(rec_obs[hid].sum() / tot_rec, 4) if hid else 0,
            }
    return drill


def pack_clients(cl, res, cfg):
    """cl: top-client rows (column_name, column_value, client, obs_n, def_n, obs_amt, d1..dn, rk, source, date_name)."""
    if cl is None or len(cl) == 0:
        return {}
    tot = res.groupby('key')[['obs_n', 'def_n']].sum()                         # segment totals, recent periods
    cl = cl.copy()
    cl['key'] = cl['source'] + '|' + cl['date_name'] + '|' + cl['column_name'] + '|' + cl['column_value'].astype(str)
    cl = cl[cl['key'].isin(tot.index)]
    if cfg['mask_client']:
        cl['client'] = cl['client'].map(lambda x: 'c_' + hashlib.sha1(str(x).encode()).hexdigest()[:8])
    out = {}
    for key, dd in cl.sort_values('rk').groupby('key'):
        to, td = float(tot.loc[key, 'obs_n']), float(tot.loc[key, 'def_n'])
        t5 = dd.head(5)
        o5, d5 = to - t5['obs_n'].sum(), td - t5['def_n'].sum()
        lags = sorted([c for c in dd.columns if c.startswith('d') and c[1:].isdigit()], key=lambda c: int(c[1:]))
        out[key] = {
            'tot_o': to, 'tot_d': td, 'ratio': _nz(td / to) if to else None,
            'ex5': _nz(d5 / o5) if o5 > 0 else None,
            'top1_sd': _nz(dd['def_n'].iloc[0] / td, 4) if td else None,
            'top5_sd': _nz(t5['def_n'].sum() / td, 4) if td else None,
            'top': [{'c': str(r.client), 'o': float(r.obs_n), 'd': float(r.def_n), 'a': float(r.obs_amt),
                     'dl': [float(getattr(r, l)) for l in lags]} for r in dd.itertuples()],
        }
    return out


def pack_explore(cube, ks, cols, vals, g, cfg, now, min_loans):
    """Cube rows (k0..kn = value or '__ALL__') -> compact JSON for the Explore tab."""
    cube = _clean(cube)
    cube = cube[(cube['date_value'] < current_period_start(g, now)) & (cube['date_value'] >= lookback_start(g, now, cfg))]
    periods = sorted(set(cube['date_value']))
    pidx = {p: i for i, p in enumerate(periods)}
    vlist = []
    for k, col in zip(ks, cols):
        present = set(cube[k].astype(str).unique()) - {'__ALL__'}
        vlist.append([v for v in vals[col] if v in present] + (['(other)'] if '(other)' in present else []))
    vix = [{v: i for i, v in enumerate(vl)} for vl in vlist]
    tot = cube[(cube[ks] == '__ALL__').all(axis=1)].set_index('date_value')
    mature = [bool(tot.loc[p, 'obs_n'] / tot.loc[p, 'n_loan'] >= cfg['min_mature']) if p in tot.index else False
              for p in periods]
    cells = {}
    for r in cube.itertuples(index=False):
        ck = ','.join(str(-1 if getattr(r, k) == '__ALL__' else vix[i][str(getattr(r, k))]) for i, k in enumerate(ks))
        cells.setdefault(ck, []).append([pidx[pd.Timestamp(r.date_value)], int(r.n_loan), int(r.obs_n), int(r.def_n),
                                         round(float(r.obs_amt)), int(r.obs_cl), int(r.def_cl)])
    for v in cells.values():
        v.sort()                                         # by period: same output whatever order Spark returns
    return {'p': [fmt_period(pd.Timestamp(p), g) for p in periods], 'mat': mature,
            'vals': vlist, 'cells': cells, 'min_loans': min_loans}


def render_html(res, series, drill, clients, explore, sources, cfg, now, failed=()):
    keep = ['ord', 'key', 'source', 'date_name', 'column_name', 'column_value', 'period', 'lag', 'eligible',
            'obs_n', 'def_n', 'obs_amt', 'obs_cl', 'def_cl', 'ratio', 'base_ratio', 'z_ratio',
            'ratio_cl', 'base_ratio_cl', 'z_cl', 'flag', 'direction', 'score']
    out = res[keep].copy()
    out['eligible'] = out['eligible'].astype(bool)
    for c in ['z_ratio', 'z_cl', 'score']:
        out[c] = out[c].round(2)
    for c in ['ratio', 'base_ratio', 'ratio_cl', 'base_ratio_cl']:
        out[c] = out[c].round(5)
    out = out.replace([np.inf, -np.inf], np.nan).astype(object).where(out.notna(), None)
    keys = set(out['key'])
    payload = {
        'rows': out.to_dict(orient='records'),
        'series': {k: v for k, v in series.items() if k in keys},
        'drill': drill, 'clients': clients, 'explore': explore,
        'meta': {'z': cfg['z_th'], 'n_recent': cfg['n_recent'], 'top_n': cfg['top_n'], 'grans': cfg['grans'],
                 'title': cfg['title'],
                 'sources': [{'name': s['name'], 'label': s.get('label', s['name']),
                              'cols': [c.split('.')[-1] for c in s['seg_cols']]} for s in sources],
                 'drill_recent': cfg['drill_min_recent'], 'drill_elig': cfg['drill_min_elig'],
                 'drill_k': cfg['drill_top_k'], 'conc_th': cfg['conc_th'],
                 'failed': list(failed), 'min_obs': cfg['min_obs'], 'min_amt': cfg['min_amt'],
                 'min_mature': cfg['min_mature'], 'generated': now.strftime('%Y-%m-%d %H:%M'),
                 'spark_n': cfg['spark_n']},
    }
    data_json = json.dumps(payload, default=str).replace('</', '<\\/')
    return _HTML.replace('__DATA__', data_json)


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def run_risk_ratio_monitor(sources, out_path='risk_ratio_monitor.html', spark=None, sql_var=None,
                           replace_sql_var=None, dry_run=False, stop_on_error=True, now=None, log=print, **settings):
    """
    sources          list of source dicts (see EXAMPLE_SOURCES); 'data' is a SQL string or a Spark DataFrame
    out_path         where to write the HTML
    sql_var          optional: replaced in SQL-string 'data' (same as run_sql_folder)
    dry_run          True = print the plan and the data SQL (after sql_var), run nothing
    stop_on_error    True = stop at the first failed step; False = skip it and list it in the report
                     (a failed main aggregation always stops: nothing to score without it)
    spark, replace_sql_var   default to sachGiaoKhoa; pass fakes or a local Spark for testing
    now              'today' for the incomplete-period cut and the history window
    **settings       any key of DEFAULTS, e.g. grans=['weekly'], explore_cols=['a', 'b']
    Returns a dict: ok, out_path, res (scored DataFrame), failed, and where it stopped if it did.
    """
    t0 = time.time()
    cfg = _settings(settings)
    _check_sources(sources)
    if spark is None or (sql_var and replace_sql_var is None):
        import sachGiaoKhoa as sgk
        spark = spark if spark is not None else sgk.SPARK
        replace_sql_var = replace_sql_var or sgk.replace_sql_var
    now = _now(cfg, now)
    result = {'ok': False, 'out_path': out_path, 'res': None, 'failed': [], 'title': cfg['title'],
              'z_th': cfg['z_th'], 'kind': 'risk',
              'source_labels': {s['name']: s.get('label', s['name']) for s in sources}}

    def data_sql(src):
        q = src['data']
        return replace_sql_var(q, sql_var) if (sql_var and isinstance(q, str)) else q

    # the plan: every Spark step, numbered
    plan = []
    for src in sources:
        for g in cfg['grans']:
            plan.append(('segments', src, g))
            if cfg['drill'] and len(src['seg_cols']) > 1:
                plan.append(('pairs', src, g))
    after = []
    for src in sources:
        for g in cfg['grans']:
            if cfg['drill']:
                after.append(('clients', src, g))
        if cfg['explore']:
            after.append(('top values', src, None))
            for g in cfg['grans']:
                after.append(('explore cube', src, g))
    n_steps = len(plan) + len(after)
    label = lambda kind, src, g: f"{kind} {src['name']}" + (f"/{g}" if g else '')
    log(f"risk_ratio_monitor: {len(sources)} source(s), granularities {', '.join(cfg['grans'])}, {n_steps} Spark steps")

    if dry_run:
        for src in sources:
            q = data_sql(src)
            log(f"\n--- data for {src['name']} ---")
            log(q.rstrip() if isinstance(q, str) else f"(a Spark DataFrame: {type(q).__name__})")
            log(f"    date {src['date_col']}, def {src['def_col']}, obs {src['obs_col']}, client {src['client_col']}, "
                f"amount {src.get('amt_col')}, segments {', '.join(src['seg_cols'])}")
        log('\nPlan:')
        for i, s in enumerate(plan + after, 1):
            log(f"  {i:>3}. {label(*s)}")
        log(f"\nDry run: nothing was run. The report would be written to {out_path}")
        result.update(ok=True, dry_run=True, plan=[label(*s) for s in plan + after])
        return result

    sdfs = {}
    def sdf_of(src):
        if src['name'] not in sdfs:
            q = data_sql(src)
            sdfs[src['name']] = spark.sql(q) if isinstance(q, str) else q
        return sdfs[src['name']]

    def fail(i, kind, src, g, e, essential):
        msg = str(e).strip()
        q = data_sql(src)
        if stop_on_error or essential:
            rest = [label(*s) for s in (plan + after)[i:i + 10]]
            for l in failure_block(f"step {i}/{n_steps}: {label(kind, src, g)}",
                                   {'source': src['name'], 'step': kind, 'granularity': g or '-', 'engine': 'Spark'},
                                   msg, sql=q if isinstance(q, str) else None, err_type=type(e).__name__,
                                   not_run=rest + (['...'] if n_steps - i > 10 else [])):
                log(l)
            result.update(failed_step=i, failed_at=label(kind, src, g), error=msg)
            return True
        result['failed'].append(f"{label(kind, src, g)}: {msg.splitlines()[0][:200] if msg else type(e).__name__}")
        log(f"  {i:>3}/{n_steps} {label(kind, src, g)}: FAILED, skipped ({result['failed'][-1]})")
        return False

    # ---- 1. main aggregation (+ pairs for the drill-down) ----
    log('\n[1/4] aggregate in Spark')
    seg_frames, pair_frames = [], []
    for i, (kind, src, g) in enumerate(plan, 1):
        st = time.time()
        try:
            pdf = spark_segments(sdf_of(src), src, g, cfg, now) if kind == 'segments' else spark_pairs(sdf_of(src), src, g, cfg, now)
        except Exception as e:
            if fail(i, kind, src, g, e, essential=(kind == 'segments')):
                return result
            continue
        (seg_frames if kind == 'segments' else pair_frames).append(pdf)
        log(f"  {i:>3}/{n_steps} {label(kind, src, g)}: {len(pdf):,} rows ({time.time() - st:.1f}s)")

    # ---- 2. score ----
    log('\n[2/4] score')
    res, series, periods = score(pd.concat(seg_frames, ignore_index=True), cfg, now, log)
    if res is None:
        log('Nothing could be scored (not enough matured periods, or no value passes the obs / amount rule).')
        result['error'] = 'nothing scored'
        return result
    c = res['direction'].replace('', 'none').value_counts()
    log(f"  {len(res):,} rows scored: {c.get('spike', 0)} spikes, {c.get('drop', 0)} drops")
    drill = build_drill(pd.concat(pair_frames, ignore_index=True) if pair_frames else None, series, periods, cfg)
    if cfg['drill']:
        log(f"  drill-downs: {len(drill):,} values")

    # ---- 3. clients + explore ----
    log('\n[3/4] top clients and explore data')
    client_frames, explore, tops = [], {}, {}
    for j, (kind, src, g) in enumerate(after, len(plan) + 1):
        st = time.time()
        try:
            if kind == 'clients':
                per = [v for (s_, g_, _), v in periods.items() if s_ == src['name'] and g_ == g]
                if not per:
                    log(f"  {j:>3}/{n_steps} {label(kind, src, g)}: nothing scored, skipped")
                    continue
                pdf = spark_clients(sdf_of(src), src, g, cfg, per[0][0][-cfg['n_recent']:])
                client_frames.append(pdf)
                log(f"  {j:>3}/{n_steps} {label(kind, src, g)}: {len(pdf):,} rows ({time.time() - st:.1f}s)")
            elif kind == 'top values':
                cols, vals = spark_top_values(sdf_of(src), src, cfg)
                tops[src['name']] = (cols, vals)
                n_cells = int(np.prod([len(vals[c_]) + 2 for c_ in cols]))
                explore[src['name']] = {'cols': [c_.split('.')[-1] for c_ in cols], 'grans': {}}
                log(f"  {j:>3}/{n_steps} {label(kind, src, g)}: {', '.join(f'{c_} {len(vals[c_])}' for c_ in cols)} values; "
                    f"up to {n_cells:,} combinations per period ({time.time() - st:.1f}s)")
            else:
                if src['name'] not in tops:
                    continue
                cols, vals = tops[src['name']]
                log(f"  {j:>3}/{n_steps} {label(kind, src, g)}:")
                cube, ks, min_loans = spark_cube(spark, sdf_of(src), src, g, cfg, now, cols, vals, log)
                explore[src['name']]['grans'][g] = pack_explore(cube, ks, cols, vals, g, cfg, now, min_loans)
                log(f"        done ({time.time() - st:.1f}s)")
        except Exception as e:
            if fail(j, kind, src, g, e, essential=False):
                return result
    clients = pack_clients(pd.concat(client_frames, ignore_index=True) if client_frames else None, res, cfg)
    explore = {k: v for k, v in explore.items() if v['grans']}

    # ---- 4. HTML ----
    log('\n[4/4] write report')
    html = render_html(res, series, drill, clients, explore, sources, cfg, now, result['failed'])
    d = os.path.dirname(out_path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    log(f"  written: {out_path} ({len(html.encode('utf-8')) / 1e6:.1f} MB)")
    log(f"\nDone in {time.time() - t0:.0f}s." + (f" {len(result['failed'])} step(s) failed and were skipped."
                                                if result['failed'] else ''))
    result.update(ok=True, res=res)
    return result


# ----------------------------------------------------------------------------
# report template (data goes in at __DATA__)
# ----------------------------------------------------------------------------
_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Risk Ratio Monitor</title>
<style>
:root{--bg:#f7f7f5;--card:#fff;--fg:#1d1d1b;--mut:#6b6b66;--bd:#e3e2dd;--up:#c2410c;--upbg:#fff1e6;
--dn:#1d4ed8;--dnbg:#e8efff;--rec:rgba(194,65,12,.08);--ln:#57534e}
@media (prefers-color-scheme:dark){:root{--bg:#161615;--card:#1f1f1d;--fg:#ecebe6;--mut:#9b9a94;--bd:#33322f;
--up:#fb923c;--upbg:#3a2414;--dn:#93b4ff;--dnbg:#1a2340;--rec:rgba(251,146,60,.12);--ln:#c9c7c0}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);
font:13px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}
header{padding:20px 20px 0}h1{margin:0;font-size:20px}.sub{color:var(--mut);margin-top:4px}
.tabs{display:flex;gap:4px;padding:16px 20px 0;border-bottom:1px solid var(--bd)}
.tab{border:0;background:none;color:var(--mut);padding:8px 14px;font:inherit;font-weight:600;cursor:pointer;
border-bottom:2px solid transparent}.tab.on{color:var(--fg);border-color:var(--up)}
main{padding:20px}.pane{display:none}.pane.on{display:block}
.card{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:14px 16px;margin-bottom:16px}
h2{font-size:15px;margin:0 0 10px}h3{font-size:13px;margin:14px 0 6px;color:var(--mut);text-transform:uppercase;letter-spacing:.04em}
.badge{display:inline-block;min-width:20px;padding:1px 7px;border-radius:10px;background:var(--upbg);color:var(--up);
font-weight:700;font-size:11px;margin-left:6px;text-transform:none}
.wrap{overflow-x:auto}table{border-collapse:collapse;width:100%;white-space:nowrap}
th,td{padding:5px 8px;border-bottom:1px solid var(--bd);text-align:left;vertical-align:middle}
th{font-size:11px;color:var(--mut);font-weight:600;position:sticky;top:0;background:var(--card)}
th[data-k]{cursor:pointer}th[data-k]:hover{color:var(--fg)}
.num{text-align:right;font-variant-numeric:tabular-nums}.val{font-weight:600;max-width:260px;overflow:hidden;text-overflow:ellipsis}
td.up{color:var(--up);background:var(--upbg);font-weight:700}td.down{color:var(--dn);background:var(--dnbg);font-weight:700}
.tag{font-size:10px;padding:0 5px;border-radius:4px;background:var(--up);color:#fff;margin-left:4px}
.empty{color:var(--mut);font-style:italic;padding:4px 0}
details{margin-top:6px}summary{cursor:pointer;color:var(--mut)}
svg .rec{fill:var(--rec)}svg .ln{fill:none;stroke:var(--ln);stroke-width:1.3}svg .dot{fill:var(--up)}
.mx td,.mx th{text-align:center}.mx td.hit{background:var(--upbg);color:var(--up);font-weight:700}
.filters{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:12px}
.filters label{display:flex;flex-direction:column;font-size:11px;color:var(--mut);gap:3px}
select,input{font:inherit;padding:5px 8px;border:1px solid var(--bd);border-radius:6px;background:var(--card);color:var(--fg)}
.count{color:var(--mut);margin-bottom:6px}
.cv{cursor:pointer;color:inherit;text-decoration:underline dotted;text-underline-offset:3px}
.cv:hover{color:var(--up)}
.ov{position:fixed;inset:0;background:rgba(0,0,0,.45);display:flex;justify-content:center;align-items:flex-start;
padding:24px 12px;overflow-y:auto;z-index:50}.ov[hidden]{display:none}
.modal{background:var(--bg);border:1px solid var(--bd);border-radius:12px;width:min(980px,100%);padding:16px}
.mh{display:flex;gap:12px;align-items:flex-start;margin-bottom:12px}.mh>div:first-child{flex:1}
.mt{font-size:17px;font-weight:700}.mt span{color:var(--mut);font-weight:500}
.seg{display:flex;border:1px solid var(--bd);border-radius:7px;overflow:hidden}
.seg button{border:0;background:var(--card);color:var(--mut);padding:6px 12px;font:inherit;font-weight:600;cursor:pointer}
.seg button.on{background:var(--up);color:#fff}
.x{border:0;background:none;color:var(--mut);font-size:24px;line-height:1;cursor:pointer;padding:0 4px}
.ch{position:relative}.ch svg{width:100%;height:auto;display:block}
.ch .ax{font-size:10px;fill:var(--mut)}.ch .grid{stroke:var(--bd)}
.ch .base{stroke:var(--mut);stroke-dasharray:4 3}.ch .hi{stroke:var(--up);stroke-dasharray:4 3;opacity:.8}
.ch .lo{stroke:var(--dn);stroke-dasharray:4 3;opacity:.8}.ch .bl{font-size:10px}
.ch .ser{fill:none;stroke:var(--ln);stroke-width:1.6}.ch .ser-pt,svg .ln-pt{fill:var(--ln)}.ch .rec{fill:var(--rec)}
.ch .mk{stroke:var(--card);stroke-width:1.5}.ch .mk.up{fill:var(--up)}.ch .mk.down{fill:var(--dn)}.ch .mk.n{fill:var(--mut)}
.ch .zl{font-size:10px;font-weight:700}.ch .zl.up{fill:var(--up)}.ch .zl.down{fill:var(--dn)}
.ch .hv{stroke:var(--mut);stroke-width:1;opacity:0}
.tip{position:absolute;pointer-events:none;background:var(--card);border:1px solid var(--bd);border-radius:6px;
padding:5px 8px;font-size:11px;white-space:nowrap;box-shadow:0 2px 8px rgba(0,0,0,.15);display:none}
.strip{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:6px;margin-top:8px}
.chip{border:1px solid var(--bd);border-radius:7px;padding:6px 8px;background:var(--card);font-size:11px}
.chip b{display:block;font-size:12px}.chip.up{border-color:var(--up);background:var(--upbg)}
.chip.down{border-color:var(--dn);background:var(--dnbg)}.chip .zz{font-weight:700}
.chip.up .zz{color:var(--up)}.chip.down .zz{color:var(--dn)}
.leg{display:flex;flex-wrap:wrap;gap:12px;color:var(--mut);font-size:11px;margin-top:2px}
.leg i{display:inline-block;width:16px;height:0;border-top:2px dashed;vertical-align:middle;margin-right:4px}
@media (max-width:640px){.strip{grid-template-columns:repeat(2,minmax(0,1fr))}}
.srcbar{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:16px}
.sc{border:1px solid var(--bd);background:var(--card);color:var(--fg);border-radius:18px;padding:6px 12px;font:inherit;
font-weight:600;cursor:pointer}.sc.on{background:var(--fg);color:var(--bg);border-color:var(--fg)}
.sc .badge{margin-left:6px}.sc.on .badge{background:var(--up);color:#fff}
.lnk{border:0;background:none;color:inherit;font:inherit;font-weight:700;cursor:pointer;padding:0;text-decoration:underline dotted;text-underline-offset:3px}
.lnk:hover{color:var(--up)}.mx .grp th{background:var(--bg);text-align:left}.mx td.tot{font-weight:700}
.srcl{display:inline-block;font-size:11px;padding:1px 6px;border-radius:4px;border:1px solid var(--bd);color:var(--mut)}
.warn{background:var(--upbg);border-color:var(--up);color:var(--up)}.warn div{font-size:11px;font-family:monospace}
h2 .srcl{margin-left:8px;vertical-align:middle;font-weight:500}
.amtf{display:flex;flex-wrap:wrap;align-items:center;gap:6px 8px;margin-top:10px;font-size:12px}
.amtf label{font-weight:600}.amtf input{width:90px;font:inherit;padding:4px 8px;border:1px solid var(--bd);border-radius:6px;background:var(--card);color:var(--fg)}
.amtf .sub{margin:0}
/* drill-down: categorical slots 1-6 of the validated reference palette */
:root{--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--s4:#eda100;--s5:#e87ba4;--s6:#008300;--s7:#6250d6;--s8:#e34948}
@media (prefers-color-scheme:dark){:root{--s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;--s5:#d55181;--s6:#0ca30c;--s7:#9085e9;--s8:#e66767}}
.dh{display:flex;gap:12px;align-items:center;justify-content:space-between;margin:22px 0 10px;flex-wrap:wrap}
.dh h2{margin:0;font-size:16px}.dh .sub{margin:2px 0 0}
.sw{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:6px;vertical-align:-1px}
.swl{display:inline-block;width:16px;height:0;border-top:2px dashed var(--mut);margin-right:6px;vertical-align:3px}
.dleg{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:12px;margin:2px 0 6px}
.dleg span{white-space:nowrap}
.ch .par-pt{fill:var(--mut)}.ch .par{fill:none;stroke:var(--mut);stroke-width:2;stroke-dasharray:5 4}
.ch .kid{fill:none;stroke-width:2;stroke-linejoin:round}
.ch .kid.dim{opacity:.15}
.ch .kmk{stroke:var(--card);stroke-width:2}
.ch .klb{font-size:10px;font-weight:700;fill:var(--fg)}
.dt td,.dt th{padding:4px 8px}.dt tr.hot td{background:var(--upbg)}
.dt tr:hover td{background:var(--rec)}
.tip .ti{display:flex;justify-content:space-between;gap:14px}
.cs{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin:4px 0 10px}
.cs>div{border:1px solid var(--bd);border-radius:8px;padding:8px 10px}.cs b{display:block;font-size:17px}
.cs span{color:var(--mut);font-size:11px}.cs .hotc{border-color:var(--up);background:var(--upbg)}.cs .hotc b{color:var(--up)}
.bar{display:inline-block;width:80px;height:8px;border-radius:4px;background:var(--bd);vertical-align:middle;overflow:hidden}
.bar i{display:block;height:100%;background:var(--fg);opacity:.65;border-radius:0 4px 4px 0}
.lagd{display:inline-flex;gap:3px}.lagd i{font-style:normal;min-width:24px;text-align:center;border-radius:4px;padding:0 3px;
background:var(--bg);color:var(--mut);font-size:11px;font-variant-numeric:tabular-nums}
.lagd i.on{background:var(--upbg);color:var(--up);font-weight:700}
@media (max-width:640px){.cs{grid-template-columns:repeat(2,minmax(0,1fr))}}
/* explore */
#exF select{min-width:120px}.exs{color:var(--mut);margin:-4px 0 12px}.exs b{color:var(--fg)}
#exF .seg{align-self:flex-end}
.ch .bar0{fill:var(--mut);opacity:.38}.ch .bar0.im{opacity:.16}
.ch .imm{fill:var(--bd);opacity:.45}.ch .lr{fill:none;stroke:var(--s1);stroke-width:2;stroke-linejoin:round}
.ch .lc{fill:none;stroke:var(--s2);stroke-width:2;stroke-linejoin:round}.ch .lr-pt{fill:var(--s1)}.ch .lc-pt{fill:var(--s2)}
.ch .dl{font-size:11px;font-weight:600;fill:var(--fg)}.ch .ptl{font-size:11px;fill:var(--mut);font-weight:600}
.dt .xlink{opacity:.0;margin-left:6px;font-size:11px}.dt tr:hover .xlink{opacity:1}
#exD{margin-top:10px}#exD summary{font-size:12px}

/* phone: charts keep a readable size and scroll inside their box; notes wrap */
@media (max-width:640px){.ch{overflow-x:auto;-webkit-overflow-scrolling:touch}.ch svg{min-width:600px}.dleg span,.leg span{white-space:normal}
.mh{flex-wrap:wrap}.mh>div:first-child{order:0;flex:1 1 calc(100% - 48px)}.mh .x{order:1}.mh .seg,.mh #mEx{order:2}}
</style></head><body>
<header><h1 id="ttl"></h1>
<div class="sub" id="sub"></div>
<div class="amtf"><label for="fAmt">Min observed amount</label><input id="fAmt" type="number" min="0" step="any" inputmode="decimal" placeholder="0"><span>mil</span><button class="lnk" id="fAmtX" type="button" hidden>clear</button><span class="sub" id="amtNote"></span></div></header>
<nav class="tabs"><button class="tab on" data-t="top">Top changes</button><button class="tab" data-t="all">All changes</button><button class="tab" data-t="ex" id="exTab">Explore</button></nav>
<main>
<section class="pane on" id="top"></section>
<section class="pane" id="all">
 <div class="card">
  <div class="filters">
   <label>Source<select id="fSrc"><option value="">All</option></select></label>
   <label>Granularity<select id="fG"><option value="">All</option></select></label>
   <label>Column<select id="fC"><option value="">All</option></select></label>
   <label>Direction<select id="fD"><option value="">All</option><option>spike</option><option>drop</option><option value="none">none</option></select></label>
   <label>Lag<select id="fL"><option value="">All</option></select></label>
   <label>Min |z|<input id="fZ" type="number" step="0.5" value="0" style="width:80px"></label>
   <label>Search value<input id="fS" type="text" placeholder="contains..."></label>
  </div>
  <div class="count" id="cnt"></div>
  <div class="wrap"><table id="tAll"></table></div>
 </div>
</section>
<section class="pane" id="ex">
 <div class="card">
  <div class="filters" id="exF"></div>
  <div class="exs" id="exS"></div>
  <div class="cs" id="exK"></div>
  <div class="dleg" id="exL"></div>
  <div class="ch" id="exC"></div>
  <details id="exD"><summary>Data table · <button class="lnk" id="exCsv" type="button">download CSV</button></summary><div class="wrap" id="exT"></div></details>
 </div>
</section>
</main>
<div class="ov" id="ov" hidden><div class="modal" role="dialog" aria-modal="true">
 <div class="mh"><div><div class="mt" id="mT"></div><div class="sub" id="mS"></div></div>
  <div class="seg" id="mSeg"><button data-m="r" class="on">Ratio</button><button data-m="rc">Client ratio</button></div>
  <button class="lnk" id="mEx" type="button" style="white-space:nowrap;align-self:center">Explore this sample →</button>
  <button class="x" id="mX" aria-label="Close">×</button></div>
 <div id="mB"></div>
</div></div>
<script>
const D = __DATA__;
const M = D.meta;
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fN = v => v==null ? '–' : Math.round(v).toLocaleString();
const fP = v => v==null ? '–' : (v*100).toFixed(1)+'%';
const fP2 = v => v==null ? '–' : (v*100).toFixed(2)+'%';
const fZ = (v,flag) => flag==='NEW' ? 'new' : (v==null ? '–' : v.toFixed(1));
const zc = v => v==null ? '' : v>M.z ? 'up' : v<-M.z ? 'down' : '';

const fA = v => v==null ? '–' : v>=1e9 ? (v/1e9).toFixed(1)+'B' : v>=1e6 ? (v/1e6).toFixed(1)+'M' : v>=1e3 ? (v/1e3).toFixed(0)+'k' : Math.round(v);
// polyline segments that break on null (= period not eligible)
function segs(vals, x, y, cls){
  let out='', cur=[];
  const flush=()=>{ if(cur.length>1) out+=`<polyline class="${cls}" points="${cur.join(' ')}"/>`; else if(cur.length===1){const [a,b]=cur[0].split(','); out+=`<circle cx="${a}" cy="${b}" r="1.6" class="${cls}-pt"/>`;} cur=[]; };
  vals.forEach((v,i)=>{ if(v==null) flush(); else cur.push(x(i).toFixed(1)+','+y(v).toFixed(1)); }); flush();
  return out;
}
function spark(key, period){
  const S = D.series[key]; if(!S) return '';
  const s = {p:S.p.slice(-M.spark_n), c:S.r.slice(-M.spark_n)};
  const W=120,H=28,n=s.c.length,mx=Math.max(...s.c.filter(v=>v!=null),1e-9);
  const x=i=> n>1 ? 2+i*(W-4)/(n-1) : W/2, y=v=> H-3-(v/mx)*(H-6);
  const rs=Math.max(0,n-M.n_recent), i=s.p.indexOf(period);
  return `<svg width="${W}" height="${H}"><rect class="rec" x="${x(rs)-3}" y="0" width="${W-x(rs)+3}" height="${H}"/>`+
    segs(s.c, x, y, 'ln')+(i>=0&&s.c[i]!=null?`<circle class="dot" cx="${x(i)}" cy="${y(s.c[i])}" r="2.6"/>`:'')+`</svg>`;
}

const SRC = Object.fromEntries(M.sources.map(s=>[s.name, s]));
const sLbl = n => (SRC[n] && SRC[n].label) || n;
const COLS_BASE = [['column_value','Value'],['period','Period'],['lag','Lag'],['obs_n','Obs'],['def_n','Def'],['obs_amt','Obs amt'],
  ['ratio','Ratio'],['base_ratio','Base ratio'],['z_ratio','z ratio'],['ratio_cl','Client ratio'],['base_ratio_cl','Base client'],
  ['z_cl','z client'],['score','Score'],[null,'Trend']];
const NUMK = ['lag','obs_n','def_n','obs_amt','ratio','base_ratio','z_ratio','ratio_cl','base_ratio_cl','z_cl','score'];
const PRE = {source:['source','Source'], date_name:['date_name','Gran.'], column_name:['column_name','Column'], direction:['direction','Dir.']};
const PRE_ALL = ['source','date_name','column_name','direction'];

function head(cols, sortable){
  return '<thead><tr>'+cols.map(([k,l])=>`<th ${sortable&&k?`data-k="${k}"`:''} class="${NUMK.includes(k)?'num':''}">${l}</th>`).join('')+'</tr></thead>';
}
function preCell(r, k){
  if(k==='source') return `<td><span class="srcl">${esc(sLbl(r.source))}</span></td>`;
  if(k==='direction') return `<td class="${r.direction==='spike'?'up':r.direction==='drop'?'down':''}">${r.direction||''}</td>`;
  return `<td>${esc(r[k])}</td>`;
}
function row(r, pre){
  const thin = r.eligible ? '' : ` title="below ${M.min_obs} obs and ≤ ${fA(M.min_amt)} amount: not scored" style="opacity:.55"`;
  return `<tr${thin}>${(pre||[]).map(k=>preCell(r,k)).join('')}<td class="val" title="Open weekly / monthly charts"><span class="cv" data-src="${esc(r.source)}" data-col="${esc(r.column_name)}" data-cv="${esc(r.column_value)}">${esc(r.column_value)}</span>${r.eligible?'':'<span class="tag" style="background:var(--mut)">thin</span>'}</td>`+
    `<td>${r.period}</td><td class="num">${r.lag}</td><td class="num">${fN(r.obs_n)}</td><td class="num">${fN(r.def_n)}</td><td class="num">${fA(r.obs_amt)}</td>`+
    `<td class="num">${fP2(r.ratio)}</td><td class="num">${fP2(r.base_ratio)}</td><td class="num ${zc(r.z_ratio)}">${fZ(r.z_ratio)}</td>`+
    `<td class="num">${fP2(r.ratio_cl)}</td><td class="num">${fP2(r.base_ratio_cl)}</td><td class="num ${zc(r.z_cl)}">${fZ(r.z_cl)}</td>`+
    `<td class="num">${r.score??'–'}</td><td>${spark(r.key,r.period)}</td></tr>`;
}
const table = (rows, pre) => { const p = pre||[];
  return `<div class="wrap"><table>${head(p.map(k=>PRE[k]).concat(COLS_BASE))}<tbody>${rows.map(r=>row(r,p)).join('')}</tbody></table></div>`; };

// ---------- Top tab ----------
let TOPSRC = '__all';
// observed-amount filter for Top / All changes (a row = one value in one period; its own observed amount)
let MINAMT = 0, SPIKES = [];
const amtOk = r => !MINAMT || (r.obs_amt || 0) >= MINAMT;
function renderTop(){
  const RW = D.rows.filter(amtOk);
  SPIKES = RW.filter(r=>r.direction==='spike');
  const el = document.getElementById('top');
  const srcs = TOPSRC==='__all' ? M.sources : [SRC[TOPSRC]];
  let h = '';
  if(M.failed && M.failed.length) h += `<div class="card warn"><b>${M.failed.length} queries failed — missing from this report</b>`+
    M.failed.map(f=>`<div>${esc(f)}</div>`).join('')+'</div>';
  // source chips
  h += '<div class="srcbar">' + [{name:'__all',label:'All sources'}].concat(M.sources).map(s=>{
    const n = s.name==='__all' ? SPIKES.length : SPIKES.filter(r=>r.source===s.name).length;
    return `<button class="sc ${TOPSRC===s.name?'on':''}" data-src="${esc(s.name)}">${esc(s.label)}<span class="badge">${n}</span></button>`;
  }).join('') + '</div>';
  // matrix: source / column x granularity
  h += '<div class="card"><h2>Spike count · column × granularity</h2><div class="wrap"><table class="mx"><thead><tr><th></th>'+
    M.grans.map(g=>`<th>${g}</th>`).join('')+'<th>total</th></tr></thead><tbody>';
  srcs.forEach(s=>{
    const ss = SPIKES.filter(r=>r.source===s.name);
    h += `<tr class="grp"><th colspan="${M.grans.length+2}">${TOPSRC==='__all'?`<button class="lnk" data-src="${esc(s.name)}">${esc(s.label)}</button>`:esc(s.label)}`+
      ` <span style="color:var(--mut);font-weight:500">· ${ss.length} spikes</span></th></tr>`;
    s.cols.forEach(c=>{ const cs = ss.filter(r=>r.column_name===c);
      h += `<tr><th style="text-align:left;padding-left:18px">${esc(c)}</th>`+M.grans.map(g=>{
        const n=cs.filter(r=>r.date_name===g).length; return `<td class="${n?'hit':''}">${n}</td>`;}).join('')+
        `<td class="tot ${cs.length?'hit':''}">${cs.length}</td></tr>`; });
  });
  h += '</tbody></table></div></div>';

  if(TOPSRC==='__all'){
    const n = M.top_n*2, drops = RW.filter(r=>r.direction==='drop');
    h += `<div class="card"><h2>Top spikes across all sources</h2>`+
      (SPIKES.length ? table(SPIKES.slice(0,n), ['source','column_name','date_name']) +
        (SPIKES.length>n ? `<details><summary>${SPIKES.length-n} more spikes</summary>${table(SPIKES.slice(n), ['source','column_name','date_name'])}</details>` : '')
      : '<div class="empty">no spikes</div>') +
      (drops.length ? `<details><summary>${drops.length} drops</summary>${table(drops, ['source','column_name','date_name'])}</details>` : '') +
      `<div class="sub" style="margin-top:10px">Pick a source above for per-column detail.</div></div>`;
    el.innerHTML = h; return;
  }

  const S = SRC[TOPSRC];
  S.cols.forEach(c=>{
    h += `<div class="card"><h2>${esc(c)}<span class="srcl">${esc(S.label)}</span></h2>`;
    M.grans.forEach(g=>{
      const blk = RW.filter(r=>r.source===S.name&&r.column_name===c&&r.date_name===g);
      const sp = blk.filter(r=>r.direction==='spike');
      const dr = blk.filter(r=>r.direction==='drop');
      h += `<h3>${g}<span class="badge">${sp.length}</span></h3>`;
      if(!blk.length) h += '<div class="empty">not scored (not enough history)</div>';
      else if(!sp.length) h += '<div class="empty">no spikes</div>';
      else h += table(sp.slice(0, M.top_n)) + (sp.length>M.top_n?`<details><summary>${sp.length-M.top_n} more spikes</summary>${table(sp.slice(M.top_n))}</details>`:'');
      if(dr.length) h += `<details><summary>${dr.length} drops</summary>${table(dr)}</details>`;
    });
    h += '</div>';
  });
  el.innerHTML = h;
}

// ---------- All tab ----------
let sortK='ord', sortD=1;
function fill(id, vals){ const s=document.getElementById(id); vals.forEach(v=>s.insertAdjacentHTML('beforeend',`<option>${esc(v)}</option>`)); }
function renderAll(){
  const sr=fSrc.value,g=fG.value,c=fC.value,d=fD.value,l=fL.value,z=parseFloat(fZ.value)||0,q=fS.value.trim().toLowerCase();
  let rows = D.rows.filter(r=>amtOk(r)&&(!sr||r.source===sr)&&(!g||r.date_name===g)&&(!c||r.column_name===c)&&
    (!d||(d==='none'?!r.direction:r.direction===d))&&(!l||String(r.lag)===l)&&
    (Math.max(Math.abs(r.z_ratio??0),Math.abs(r.z_cl??0))>=z)&&
    (!q||String(r.column_value).toLowerCase().includes(q)));
  rows.sort((a,b)=>{const x=a[sortK],y=b[sortK]; if(x==null)return 1; if(y==null)return -1; return (x>y?1:x<y?-1:0)*sortD;});
  document.getElementById('cnt').textContent = `${rows.length.toLocaleString()} rows`;
  tAll.innerHTML = head(PRE_ALL.map(k=>PRE[k]).concat(COLS_BASE),true)+'<tbody>'+rows.map(r=>row(r,PRE_ALL)).join('')+'</tbody>';
  tAll.querySelectorAll('th[data-k]').forEach(th=>{
    if(th.dataset.k===sortK) th.textContent += sortD>0?' ▲':' ▼';
    th.onclick=()=>{ const k=th.dataset.k; if(k===sortK) sortD=-sortD; else {sortK=k; sortD=['source','column_value','period','date_name','column_name','direction','lag'].includes(k)?1:-1;} renderAll(); };
  });
}

// ---------- Detail modal: 3 charts per value ----------
const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const pDate = p => p.replace('wk ','');
function tickLbl(p, g){
  const d = pDate(p).split('-');
  if(g==='monthly') return `${MON[+d[1]-1]} '${d[0].slice(2)}`;
  if(g==='weekly')  return `${+d[2]} ${MON[+d[1]-1]} '${d[0].slice(2)}`;
  return `${+d[2]} ${MON[+d[1]-1]}`;
}
function fullLbl(p, g){
  const d = pDate(p).split('-');
  if(g==='monthly') return `${MON[+d[1]-1]} ${d[0]}`;
  const s = `${+d[2]} ${MON[+d[1]-1]} ${d[0]}`;
  return g==='weekly' ? `Week of ${s}` : s;
}
const ROWIDX = {};
D.rows.forEach(r => { (ROWIDX[r.key] = ROWIDX[r.key] || {})[r.period] = r; });
let MODAL = null, METRIC = 'r';

function detailChart(g, src, col, val){
  const key = `${src}|${g}|${col}|${val}`, s = D.series[key], rr = ROWIDX[key] || {};
  const title = `<h3 style="margin-top:4px">${g}</h3>`;
  if(!s) return `<div class="card">${title}<div class="empty">Not scored for this granularity (not enough history, or no data).</div></div>`;
  const isR = METRIC==='r', vals = isR ? s.r : s.rc;
  const mu = isR ? s.mr : s.mc, sd = isR ? s.sr : s.sc, zk = isR ? 'z_ratio' : 'z_cl';
  const fmtY = v => (v*100).toFixed(v===0?0:v<0.01?2:1)+'%';
  const W=900,H=250,L=52,R=60,T=26,B=30,n=vals.length;
  const hi = (mu!=null&&sd!=null) ? mu + M.z*sd : null, lo = (mu!=null&&sd!=null) ? mu - M.z*sd : null;
  let ymax = Math.max(...vals.filter(v=>v!=null), hi||0, mu||0) * 1.12 || 1;
  { const st = ymax/4, mg = Math.pow(10, Math.floor(Math.log10(st))), f = st/mg;
    ymax = (f<=1?1:f<=2?2:f<=2.5?2.5:f<=5?5:10) * mg * 4; }
  const x = i => L + (n>1 ? i*(W-L-R)/(n-1) : (W-L-R)/2), y = v => T + (1 - v/ymax)*(H-T-B);
  let h = `<svg viewBox="0 0 ${W} ${H}" data-key="${esc(key)}">`;
  for(let k=0;k<=4;k++){ const v=ymax*k/4; h += `<line class="grid" x1="${L}" x2="${W-R}" y1="${y(v)}" y2="${y(v)}"/><text class="ax" x="${L-6}" y="${y(v)+3}" text-anchor="end">${fmtY(v)}</text>`; }
  const rs = Math.max(0, n - M.n_recent);
  h += `<rect class="rec" x="${x(rs)-6}" y="${T-14}" width="${W-R-x(rs)+12}" height="${H-B-T+14}"/>`;
  h += `<text class="ax" x="${(x(rs)+W-R)/2}" y="${T-4}" text-anchor="middle">last ${M.n_recent}</text>`;
  const nt = Math.min(7, n), step = Math.max(1, Math.round((n-1)/(nt-1||1)));
  for(let i=0;i<n;i+=step){ if(n-1-i < step*0.6 && i!==n-1) continue; h += `<text class="ax" x="${x(i)}" y="${H-B+16}" text-anchor="middle">${tickLbl(s.p[i],g)}</text>`; }
  h += `<text class="ax" x="${x(n-1)}" y="${H-B+16}" text-anchor="middle">${tickLbl(s.p[n-1],g)}</text>`;
  if(mu!=null){
    h += `<line class="base" x1="${L}" x2="${x(rs)}" y1="${y(mu)}" y2="${y(mu)}"/><text class="bl ax" x="${W-R+4}" y="${y(mu)+3}">mean</text>`;
    if(hi!=null && hi<=ymax) h += `<line class="hi" x1="${L}" x2="${W-R}" y1="${y(hi)}" y2="${y(hi)}"/><text class="bl ax" x="${W-R+4}" y="${y(hi)+3}" style="fill:var(--up)">+${M.z}σ</text>`;
    if(lo!=null && lo>0) h += `<line class="lo" x1="${L}" x2="${W-R}" y1="${y(lo)}" y2="${y(lo)}"/><text class="bl ax" x="${W-R+4}" y="${y(lo)+3}" style="fill:var(--dn)">−${M.z}σ</text>`;
  }
  h += segs(vals, x, y, 'ser');
  let lastLbl = Infinity;                       // label newest first; skip labels closer than 46px
  for(let i=n-1;i>=rs;i--){
    const r = rr[s.p[i]], v = vals[i]; if(v==null) continue;
    const z = r ? r[zk] : null, cls = z==null ? 'n' : z>M.z ? 'up' : z<-M.z ? 'down' : 'n';
    h += `<circle class="mk ${cls}" cx="${x(i)}" cy="${y(v)}" r="${cls==='n'?3.5:5}"/>`;
    if(cls!=='n' && lastLbl - x(i) >= 46){
      h += `<text class="zl ${cls}" x="${x(i)}" y="${y(v) + (cls==='down'?16:-9)}" text-anchor="middle">z ${z.toFixed(1)}</text>`;
      lastLbl = x(i);
    }
  }
  h += `<line class="hv" x1="0" x2="0" y1="${T}" y2="${H-B}"/><rect x="${L}" y="${T}" width="${W-L-R}" height="${H-T-B}" fill="transparent" class="hit"/></svg>`;

  let strip = '<div class="strip">';
  for(let i=n-1;i>=rs;i--){
    const r = rr[s.p[i]]; if(!r) continue;
    const z = r[zk], cls = z==null ? '' : z>M.z ? 'up' : z<-M.z ? 'down' : '';
    strip += `<div class="chip ${cls}"><b>${fullLbl(s.p[i],g)}</b>${fP2(isR?r.ratio:r.ratio_cl)} <span style="color:var(--mut)">vs ${fP2(isR?r.base_ratio:r.base_ratio_cl)}</span><br>`+
      `<span class="zz">${r.eligible ? 'z '+(z==null?'–':z.toFixed(1)) : 'thin – not scored'}</span> · ${isR ? fN(r.def_n)+'/'+fN(r.obs_n)+' loans' : fN(r.def_cl)+'/'+fN(r.obs_cl)+' clients'}</div>`;
  }
  strip += '</div>';
  const rng = `${fullLbl(s.p[0],g)} → ${fullLbl(s.p[n-1],g)}`;
  return `<div class="card">${title}<div class="sub" style="margin:-4px 0 6px">${rng} · ${n} matured periods · gaps = thin periods (obs &lt; ${M.min_obs} and amount ≤ ${fA(M.min_amt)})</div>`+
    `<div class="ch">${h}<div class="tip"></div></div>${strip}</div>`;
}

function bindHover(card){
  const svg = card.querySelector('svg'); if(!svg) return;
  const key = svg.dataset.key, s = D.series[key], g = key.split('|')[1], rr = ROWIDX[key] || {};
  const tip = card.querySelector('.tip'), hv = svg.querySelector('.hv'), hit = svg.querySelector('.hit');
  const vb = svg.viewBox.baseVal, L=+hit.getAttribute('x'), Wp=+hit.getAttribute('width'), n=s.p.length;
  const move = e => {
    const b = svg.getBoundingClientRect(), sx = (e.clientX - b.left) * vb.width / b.width;
    const i = Math.max(0, Math.min(n-1, Math.round((sx - L) / (n>1 ? Wp/(n-1) : 1))));
    const px = L + (n>1 ? i*Wp/(n-1) : Wp/2);
    hv.setAttribute('x1',px); hv.setAttribute('x2',px); hv.style.opacity=.6;
    const r = rr[s.p[i]];
    tip.innerHTML = `<b>${fullLbl(s.p[i],g)}</b><br>Ratio ${fP2(s.r[i])} · Client ratio ${fP2(s.rc[i])}`+
      `<br>Def ${fN(s.d[i])} / Obs ${fN(s.o[i])} · Obs amt ${fA(s.a[i])}`+
      (s.r[i]==null ? '<br><i>thin period – not scored</i>' : '') +
      (r ? `<br>z ratio ${fZ(r.z_ratio)} · z client ${fZ(r.z_cl)}` : '');
    tip.style.display='block';
    const left = px * b.width / vb.width;
    tip.style.left = Math.min(left + 10, b.width - tip.offsetWidth - 4) + 'px'; tip.style.top = '6px';
  };
  hit.addEventListener('pointermove', move); hit.addEventListener('pointerdown', move);
  hit.addEventListener('pointerleave', ()=>{ tip.style.display='none'; hv.style.opacity=0; });
}

// ---------- Drill-down: the clicked sample split by every other segment column ----------
let DG = null;                                    // drill granularity
// colour follows the sub-segment (entity), stable across weekly/monthly and Ratio/Client ratio
const KCOL = {};
function colorMap(src, col, val, ccol){
  const id = `${src}|${col}|${val}|${ccol}`;
  if(KCOL[id]) return KCOL[id];
  const m = {}; let n = 0;
  M.grans.forEach(g=>{ const b = (D.drill[`${src}|${g}|${col}|${val}`]||{})[ccol];
    (b ? b.kids : []).forEach(k=>{ if(!(k.v in m) && n < 8) m[k.v] = n++; }); });
  return KCOL[id] = m;
}
let CM = {};                                      // colour map of the split being drawn
const KC = j => { const i = CM.__kids ? CM[CM.__kids[j]] : j; return i==null ? 'var(--mut)' : `var(--s${i+1})`; };
function drillChart(key, ccol, blk, g){
  const ps = D.series[key], isR = METRIC==='r', n = ps.p.length;
  const par = isR ? ps.r : ps.rc;
  const kv = blk.kids.map(k => isR ? k.r : k.rc);
  const all = par.concat(...kv).filter(v=>v!=null);
  const W=900,H=260,L=52,R=36,T=26,B=30;
  let ymax = (Math.max(...all, 1e-4)) * 1.08;
  { const st = ymax/4, mg = Math.pow(10, Math.floor(Math.log10(st))), f = st/mg;
    ymax = (f<=1?1:f<=2?2:f<=2.5?2.5:f<=5?5:10) * mg * 4; }
  const x = i => L + (n>1 ? i*(W-L-R)/(n-1) : (W-L-R)/2), y = v => T + (1 - v/ymax)*(H-T-B);
  const fmtY = v => (v*100).toFixed(v===0?0:v<0.01?2:1)+'%';
  let h = `<svg viewBox="0 0 ${W} ${H}" data-key="${esc(key)}" data-ccol="${esc(ccol)}">`;
  for(let k=0;k<=4;k++){ const v=ymax*k/4; h += `<line class="grid" x1="${L}" x2="${W-R}" y1="${y(v)}" y2="${y(v)}"/><text class="ax" x="${L-6}" y="${y(v)+3}" text-anchor="end">${fmtY(v)}</text>`; }
  const rs = Math.max(0, n - M.n_recent);
  h += `<rect class="rec" x="${x(rs)-6}" y="${T-14}" width="${W-R-x(rs)+12}" height="${H-B-T+14}"/>`;
  h += `<text class="ax" x="${(x(rs)+W-R)/2}" y="${T-4}" text-anchor="middle">last ${M.n_recent}</text>`;
  const nt = Math.min(7, n), step = Math.max(1, Math.round((n-1)/(nt-1||1)));
  for(let i=0;i<n;i+=step){ if(n-1-i < step*0.6 && i!==n-1) continue; h += `<text class="ax" x="${x(i)}" y="${H-B+16}" text-anchor="middle">${tickLbl(ps.p[i],g)}</text>`; }
  h += `<text class="ax" x="${x(n-1)}" y="${H-B+16}" text-anchor="middle">${tickLbl(ps.p[n-1],g)}</text>`;
  h += `<g>${segs(par, x, y, 'par')}</g>`;
  kv.forEach((vals,j)=>{ h += `<g class="kg" data-j="${j}" style="stroke:${KC(j)};fill:${KC(j)}">${segs(vals, x, y, 'kid')}</g>`; });
  // flag markers: sub-segment points in the last N with z > threshold
  let lbls = [];
  blk.kids.forEach((k,j)=>{
    const zs = isR ? k.zr : k.zc;
    zs.forEach((z,lag0)=>{ const i = n-1-lag0, v = kv[j][i];
      if(z!=null && z>M.z && v!=null){
        h += `<circle class="kmk" cx="${x(i)}" cy="${y(v)}" r="5" style="fill:${KC(j)}"/>`;
        lbls.push({x:x(i), y:y(v), t:'z '+z.toFixed(1)});
      }});
  });
  lbls.sort((a,b)=>a.y-b.y).forEach((l,i,arr)=>{                 // skip labels that would collide
    if(arr.slice(0,i).some(o=>o.shown && Math.abs(o.x-l.x)<44 && Math.abs(o.y-l.y)<16)) return;
    l.shown = true; h += `<text class="klb" x="${l.x}" y="${l.y-9}" text-anchor="middle">${l.t}</text>`;
  });
  h += `<line class="hv" x1="0" x2="0" y1="${T}" y2="${H-B}"/><rect x="${L}" y="${T}" width="${W-L-R}" height="${H-T-B}" fill="transparent" class="hit"/></svg>`;
  return h;
}

function drillTable(blk){
  const isR = METRIC==='r';
  const rows = blk.kids.map((k,j)=>{
    const zs = isR ? k.zr : k.zc, ls = isR ? k.lr : k.lc;
    let mz = null, ml = null; zs.forEach((z,i)=>{ if(z!=null && (mz==null || z>mz)){ mz=z; ml=i+1; } });
    return {k, j, z1: zs[0], l1: ls[0], mz, ml};
  }).sort((a,b)=>(b.mz??-99)-(a.mz??-99));
  return `<div class="wrap"><table class="dt"><thead><tr><th>Sub-segment</th><th class="num">Obs (last ${M.n_recent})</th><th class="num">Share of sample</th>`+
    `<th class="num">${isR?'Ratio':'Client ratio'} lag 1</th><th class="num">Baseline</th><th class="num">z lag 1</th><th class="num">Max z (last ${M.n_recent})</th></tr></thead><tbody>`+
    rows.map(o=>`<tr class="${o.mz!=null&&o.mz>M.z?'hot':''}" data-j="${o.j}"><td class="val"><span class="sw" style="background:${KC(o.j)}"></span>${esc(o.k.v)}`+
      `<button class="lnk xlink" data-xv="${esc(o.k.v)}" title="Open this sub-sample in Explore">explore →</button></td>`+
      `<td class="num">${fN(o.k.on)}</td><td class="num">${fP(o.k.sh)}</td><td class="num">${fP2(o.l1)}</td><td class="num">${fP2(isR?o.k.mr:o.k.mc)}</td>`+
      `<td class="num ${zc(o.z1)}">${fZ(o.z1)}</td><td class="num ${zc(o.mz)}">${o.mz==null?'–':o.mz.toFixed(1)+' <span style="color:var(--mut);font-weight:400">lag '+o.ml+'</span>'}</td></tr>`).join('')+
    `</tbody></table></div>`;
}

function clientCard(key, g){
  const C = (D.clients||{})[key]; if(!C) return '';
  const ps = D.series[key], per = ps.p.slice(-M.n_recent);
  const isR = METRIC==='r';
  const conc = C.top5_sd!=null && C.top5_sd >= M.conc_th;
  let h = `<div class="card dcard-c"><h3 style="margin-top:4px">top clients · last ${M.n_recent} ${g.replace('ly','')}s (${fullLbl(per[0],g)} → ${fullLbl(per[per.length-1],g)})</h3>`;
  h += `<div class="cs"><div><b>${fP(C.top1_sd)}</b><span>of defaults from the top client</span></div>`+
    `<div class="${conc?'hotc':''}"><b>${fP(C.top5_sd)}</b><span>of defaults from the top 5 clients</span></div>`+
    `<div><b>${fP2(C.ratio)}</b><span>loan ratio, whole segment</span></div>`+
    `<div class="${conc?'hotc':''}"><b>${fP2(C.ex5)}</b><span>loan ratio without the top 5</span></div></div>`;
  if(conc) h += `<div class="sub" style="margin:-2px 0 8px;color:var(--up)">Concentrated: ${fP(C.top5_sd)} of the defaults come from 5 clients, so check them before reading this as a segment-wide shift.</div>`;
  const mx = Math.max(...C.top.map(t=>t.d), 1);
  h += `<div class="wrap"><table class="dt"><thead><tr><th class="num">#</th><th>Client</th><th class="num">Obs loans</th><th class="num">Defaults</th>`+
    `<th>Share of defaults</th><th class="num">Share of obs</th><th class="num">Obs amount</th>`+
    `<th>Defaults per ${g.replace('ly','')} <span style="font-weight:400">(oldest → newest)</span></th></tr></thead><tbody>`+
    C.top.map((t,i)=>{
      const sd = C.tot_d ? t.d/C.tot_d : null, so = C.tot_o ? t.o/C.tot_o : null;
      const dl = t.dl.slice().reverse();                                   // lag5 ... lag1
      return `<tr><td class="num">${i+1}</td><td class="val">${esc(t.c)}</td><td class="num">${fN(t.o)}</td><td class="num">${fN(t.d)}</td>`+
        `<td><span class="bar"><i style="width:${sd==null?0:Math.max(2,sd*100)}%"></i></span> ${fP(sd)}</td>`+
        `<td class="num">${fP(so)}</td><td class="num">${fA(t.a)}</td>`+
        `<td><span class="lagd">${dl.map(v=>`<i class="${v>0?'on':''}" title="${fN(v)}">${fN(v)}</i>`).join('')}</span></td></tr>`;
    }).join('') + `</tbody></table></div>`;
  h += `<div class="sub" style="margin-top:6px">Observed loans only · ranked by defaults, then observed loans · ${fN(C.tot_d)} defaults / ${fN(C.tot_o)} obs in the segment over these ${M.n_recent} periods.</div></div>`;
  return h;
}

function drillSection(src, col, val){
  const S = SRC[src], others = S.cols.filter(c=>c!==col);
  const grans = M.grans.filter(g => D.drill[`${src}|${g}|${col}|${val}`] || (D.clients||{})[`${src}|${g}|${col}|${val}`]);
  if(!grans.length) return '';
  if(!grans.includes(DG)){
    const hit = D.rows.find(r=>r.source===src && r.column_name===col && r.column_value===val && r.direction==='spike');
    DG = hit && grans.includes(hit.date_name) ? hit.date_name : grans[0];
  }
  const key = `${src}|${DG}|${col}|${val}`, dr = D.drill[key] || {};
  let h = `<div class="dh"><div><h2>Inside ${esc(col)} = ${esc(val)}</h2>`+
    `<div class="sub">${others.length ? `Same sample split by ${others.map(esc).join(', then ')} · ${METRIC==='r'?'loan ratio':'client ratio'} · top ${M.drill_k} sub-segments by volume, small ones hidden · then ` : ''}the clients with the biggest share</div></div>`+
    `<div class="seg" id="dSeg">${grans.map(g=>`<button data-g="${g}" class="${g===DG?'on':''}">${g}</button>`).join('')}</div></div>`;
  others.forEach(c=>{
    const blk = dr[c];
    h += `<div class="card dcard"><h3 style="margin-top:4px">split by ${esc(c)}</h3>`;
    if(!blk || !blk.kids.length){ h += `<div class="empty">No ${esc(c)} sub-segment is big enough to show cleanly.</div></div>`; return; }
    CM = Object.assign({}, colorMap(src, col, val, c), {__kids: blk.kids.map(k=>k.v)});
    h += `<div class="dleg"><span><i class="swl"></i>${esc(col)} = ${esc(val)} (whole sample)</span>`+
      blk.kids.map((k,j)=>`<span data-j="${j}"><i class="sw" style="background:${KC(j)}"></i>${esc(k.v)}</span>`).join('')+'</div>';
    h += `<div class="ch">${drillChart(key, c, blk, DG)}<div class="tip"></div></div>${drillTable(blk)}`;
    if(blk.hidden) h += `<div class="sub" style="margin-top:6px">${blk.hidden} small sub-segment${blk.hidden>1?'s':''} hidden (${fP(blk.hidden_sh)} of recent obs): `+
      `fewer than ${M.drill_recent} usable periods in the last ${M.n_recent}, under ${Math.round(M.drill_elig*100)}% usable overall, or outside the top ${M.drill_k}.</div>`;
    h += '</div>';
  });
  return h + clientCard(key, DG);
}

function bindDrill(card){
  const svg = card.querySelector('svg'); if(!svg) return;
  const key = svg.dataset.key, ccol = svg.dataset.ccol, g = key.split('|')[1];
  const ps = D.series[key], blk = D.drill[key][ccol], n = ps.p.length, isR = METRIC==='r';
  const [src_, , col_, ...vparts] = key.split('|');
  const cmap = Object.assign({}, colorMap(src_, col_, vparts.join('|'), ccol), {__kids: blk.kids.map(k=>k.v)});
  const KC = j => { const i = cmap[cmap.__kids[j]]; return i==null ? 'var(--mut)' : `var(--s${i+1})`; };
  const tip = card.querySelector('.tip'), hv = svg.querySelector('.hv'), hit = svg.querySelector('.hit');
  const vb = svg.viewBox.baseVal, L=+hit.getAttribute('x'), Wp=+hit.getAttribute('width');
  const kgs = [...svg.querySelectorAll('.kg')];
  const focus = j => kgs.forEach(gr=>gr.querySelectorAll('.kid').forEach(p=>p.classList.toggle('dim', j!=null && +gr.dataset.j!==j)));
  card.querySelectorAll('[data-j]').forEach(el=>{ if(el.closest('svg')) return;
    el.addEventListener('pointerenter', ()=>focus(+el.dataset.j)); el.addEventListener('pointerleave', ()=>focus(null)); });
  const move = e => {
    const b = svg.getBoundingClientRect(), sx = (e.clientX - b.left) * vb.width / b.width;
    const i = Math.max(0, Math.min(n-1, Math.round((sx - L) / (n>1 ? Wp/(n-1) : 1))));
    const px = L + (n>1 ? i*Wp/(n-1) : Wp/2);
    hv.setAttribute('x1',px); hv.setAttribute('x2',px); hv.style.opacity=.6;
    const lag = n-1-i;
    const items = blk.kids.map((k,j)=>({j, v:k.v, r:(isR?k.r:k.rc)[i], o:k.o[i], z: lag < M.n_recent ? (isR?k.zr:k.zc)[lag] : null}))
      .sort((a,b)=>(b.r??-1)-(a.r??-1));
    tip.innerHTML = `<b>${fullLbl(ps.p[i],g)}</b><div class="ti"><span><i class="swl"></i>whole sample</span><b>${fP2((isR?ps.r:ps.rc)[i])}</b></div>`+
      items.map(o=>`<div class="ti"><span><i class="sw" style="background:${KC(o.j)}"></i>${esc(o.v)} <span style="color:var(--mut)">${fN(o.o)} obs</span></span>`+
        `<b>${o.r==null?'thin':fP2(o.r)}${o.z!=null?` <span style="color:${o.z>M.z?'var(--up)':'var(--mut)'}">z ${o.z.toFixed(1)}</span>`:''}</b></div>`).join('');
    tip.style.display='block';
    const left = px * b.width / vb.width;
    tip.style.left = (left + 12 + tip.offsetWidth > b.width ? left - tip.offsetWidth - 12 : left + 12) + 'px'; tip.style.top = '6px';
  };
  hit.addEventListener('pointermove', move); hit.addEventListener('pointerdown', move);
  hit.addEventListener('pointerleave', ()=>{ tip.style.display='none'; hv.style.opacity=0; });
}

function renderModal(){
  const {src, col, val} = MODAL;
  document.getElementById('mT').innerHTML = `<span class="srcl" style="font-size:12px">${esc(sLbl(src))}</span> <span>${esc(col)} =</span> ${esc(val)}`;
  const hits = D.rows.filter(r=>r.source===src && r.column_name===col && r.column_value===val && r.direction);
  document.getElementById('mS').textContent = hits.length
    ? 'Flagged: ' + hits.map(r=>`${r.date_name} ${r.period} (${r.direction})`).join(' · ')
    : 'No flags in the last ' + M.n_recent + ' periods';
  const mB = document.getElementById('mB');
  mB.innerHTML = M.grans.map(g=>detailChart(g, src, col, val)).join('') +
    `<div class="leg"><span><i style="border-color:var(--mut)"></i>baseline mean</span><span><i style="border-color:var(--up)"></i>+${M.z}σ (spike line)</span>`+
    `<span><i style="border-color:var(--dn)"></i>−${M.z}σ (drop line)</span><span>shaded = last ${M.n_recent} scored periods · hover for exact values</span></div>` +
    drillSection(src, col, val);
  mB.querySelectorAll('.card:not(.dcard)').forEach(bindHover);
  mB.querySelectorAll('.dcard').forEach(bindDrill);
  requestAnimationFrame(() => mB.querySelectorAll('.ch').forEach(c => c.scrollLeft = c.scrollWidth));   // phones: open at the newest periods
  mB.querySelectorAll('#dSeg button').forEach(b => b.onclick = () => { DG = b.dataset.g; renderModal(); });
  document.querySelectorAll('#mSeg button').forEach(b=>b.classList.toggle('on', b.dataset.m===METRIC));
}
function openDetail(src, col, val){ MODAL = {src, col, val}; DG = null; renderModal(); document.getElementById('ov').hidden = false; document.body.style.overflow='hidden'; }
function closeDetail(){ document.getElementById('ov').hidden = true; document.body.style.overflow=''; MODAL = null; }
document.addEventListener('click', e => {
  const a = e.target.closest('.cv'); if(a) return openDetail(a.dataset.src, a.dataset.col, a.dataset.cv);
  const b = e.target.closest('#top [data-src]'); if(b){ TOPSRC = b.dataset.src; renderTop(); }
});
document.getElementById('mX').onclick = closeDetail;
document.getElementById('ov').addEventListener('click', e => { if(e.target.id==='ov') closeDetail(); });
document.addEventListener('keydown', e => { if(e.key==='Escape' && MODAL) closeDetail(); });
document.querySelectorAll('#mSeg button').forEach(b => b.onclick = () => { METRIC = b.dataset.m; renderModal(); });

// ---------- Explore tab: any sample -> trend of loan ratio, client ratio, observed amount ----------
const EXD = D.explore || {};
const EX = {src: Object.keys(EXD)[0], g: null, sel: {}, range: 'all'};
function exG(){ const S = EXD[EX.src]; return S && S.grans[EX.g]; }
function exControls(){
  const S = EXD[EX.src]; if(!S){ document.getElementById('exF').innerHTML = '<div class="empty">Explore data not built (EXPLORE = False).</div>'; return; }
  const gs = Object.keys(S.grans); if(!gs.includes(EX.g)) EX.g = gs[0];
  const G = exG();
  let h = '';
  if(Object.keys(EXD).length > 1) h += `<label>Source<select id="exSrc">${Object.keys(EXD).map(n=>`<option value="${esc(n)}" ${n===EX.src?'selected':''}>${esc(sLbl(n))}</option>`).join('')}</select></label>`;
  S.cols.forEach((c,i)=>{
    h += `<label>${esc(c)}<select data-ci="${i}"><option value="-1">All</option>`+
      G.vals[i].map((v,vi)=>`<option value="${vi}" ${EX.sel[c]===v?'selected':''}>${esc(v)}</option>`).join('')+`</select></label>`;
  });
  h += `<label>Show<select id="exR">${[['all','All periods'],['52','Last 52'],['26','Last 26'],['12','Last 12']].map(([v,l])=>`<option value="${v}" ${EX.range===v?'selected':''}>${l}</option>`).join('')}</select></label>`;
  h += `<div class="seg" id="exSeg">${gs.map(g=>`<button data-g="${g}" class="${g===EX.g?'on':''}">${g}</button>`).join('')}</div>`;
  h += `<button class="lnk" id="exReset" type="button" style="align-self:flex-end;margin-bottom:6px">reset</button>`;
  document.getElementById('exF').innerHTML = h;
  const f = document.getElementById('exF');
  f.querySelectorAll('select[data-ci]').forEach(s=>s.onchange=()=>{ const c = S.cols[+s.dataset.ci];
    if(s.value==='-1') delete EX.sel[c]; else EX.sel[c] = G.vals[+s.dataset.ci][+s.value]; exRender(); });
  const sr = document.getElementById('exSrc'); if(sr) sr.onchange = ()=>{ EX.src = sr.value; EX.sel = {}; exControls(); exRender(); };
  document.getElementById('exR').onchange = e => { EX.range = e.target.value; exRender(); };
  f.querySelectorAll('#exSeg button').forEach(b=>b.onclick=()=>{ EX.g = b.dataset.g; exControls(); exRender(); });
  document.getElementById('exReset').onclick = ()=>{ EX.sel = {}; EX.range = 'all'; exControls(); exRender(); };
}
function exKey(){
  const S = EXD[EX.src], G = exG();
  return S.cols.map((c,i)=>{ if(!(c in EX.sel)) return -1; const vi = G.vals[i].indexOf(EX.sel[c]); return vi<0 ? -1 : vi; }).join(',');
}
function exKeyMissing(){ return !exG().cells[exKey()]; }
function exSeries(){
  const S = EXD[EX.src], G = exG(), n = G.p.length;
  const missing = [];
  const key = S.cols.map((c,i)=>{ if(!(c in EX.sel)) return -1; const vi = G.vals[i].indexOf(EX.sel[c]); if(vi<0) missing.push(c); return vi<0 ? -1 : vi; }).join(',');
  const rows = Array.from({length:n}, (_,i)=>({i, p:G.p[i], mat:G.mat[i], nl:0, o:0, d:0, a:0, oc:0, dc:0}));
  (G.cells[key]||[]).forEach(([pi,nl,o,d,a,oc,dc])=>Object.assign(rows[pi], {nl,o,d,a,oc,dc}));
  rows.forEach(r=>{ r.thin = !(r.o >= M.min_obs || r.a > M.min_amt);
    r.r = r.o>0 && !r.thin ? r.d/r.o : null; r.rc = r.oc>0 && !r.thin ? r.dc/r.oc : null; });
  const k = EX.range==='all' ? n : Math.min(n, +EX.range);
  return {rows: rows.slice(n-k), missing};
}
function exChart(rows, g){
  const n = rows.length, W=900, L=58, R=70, T=14, H1=230, GAP=26, H2=110, B=26, H=T+H1+GAP+H2+B;
  const bw = (W-L-R)/Math.max(n,1), x = i => L + bw*(i+.5);
  const vr = rows.flatMap(r=>[r.r, r.rc]).filter(v=>v!=null);
  const nice = m => { const st=m/4, mg=Math.pow(10,Math.floor(Math.log10(st||1e-9))), f=st/mg; return (f<=1?1:f<=2?2:f<=2.5?2.5:f<=5?5:10)*mg*4; };
  const ymax = nice(Math.max(...vr, 1e-4)*1.08), amax = nice(Math.max(...rows.map(r=>r.a), 1)*1.05);
  const y = v => T + (1 - v/ymax)*H1, ya = v => T+H1+GAP + (1 - v/amax)*H2, yb = T+H1+GAP+H2;
  const fmtY = v => (v*100).toFixed(v===0?0:v<0.01?2:1)+'%';
  let h = `<svg viewBox="0 0 ${W} ${H}" id="exSvg">`;
  // immature periods
  const im = rows.map((r,i)=>r.mat?null:i).filter(v=>v!=null);
  if(im.length){ const a0 = Math.min(...im), a1 = Math.max(...im);
    h += `<rect class="imm" x="${L+bw*a0}" y="${T}" width="${bw*(a1-a0+1)}" height="${yb-T}"/><text class="ax" x="${L+bw*(a0+a1+1)/2}" y="${T+11}" text-anchor="middle">not yet mature</text>`; }
  for(let k=0;k<=4;k++){ const v=ymax*k/4; h += `<line class="grid" x1="${L}" x2="${W-R}" y1="${y(v)}" y2="${y(v)}"/><text class="ax" x="${L-6}" y="${y(v)+3}" text-anchor="end">${fmtY(v)}</text>`; }
  for(let k=0;k<=2;k++){ const v=amax*k/2; h += `<line class="grid" x1="${L}" x2="${W-R}" y1="${ya(v)}" y2="${ya(v)}"/><text class="ax" x="${L-6}" y="${ya(v)+3}" text-anchor="end">${fA(v)}</text>`; }
  h += `<text class="ptl" x="${L}" y="${T+H1+GAP-8}">Observed amount</text>`;
  // bars (2px gap, rounded data end)
  const gw = Math.max(1, bw-2);
  rows.forEach((r,i)=>{ if(r.a<=0) return; const top = ya(r.a), hh = yb-top;
    h += `<path class="bar0 ${r.mat?'':'im'}" d="M${x(i)-gw/2},${yb} V${top+Math.min(2,hh)} q0,-2 2,-2 H${x(i)+gw/2-2} q2,0 2,2 V${yb} Z"/>`; });
  // lines (gaps where the period is thin)
  const solid = k => rows.map(r=>r.mat ? r[k] : null);
  const dash  = k => rows.map((r,i)=>(!r.mat || (rows[i+1] && !rows[i+1].mat)) ? r[k] : null);   // overlap one point to join
  h += segs(solid('r'), x, y, 'lr') + segs(solid('rc'), x, y, 'lc');
  h += `<g style="stroke-dasharray:4 4;opacity:.6">${segs(dash('r'), x, y, 'lr') + segs(dash('rc'), x, y, 'lc')}</g>`;
  // direct labels at the last point of each line
  const last = k => { for(let i=n-1;i>=0;i--) if(rows[i][k]!=null && rows[i].mat) return i; return -1; };
  const lr = last('r'), lc = last('rc');
  const ly = []; if(lr>=0) ly.push({k:'Loan', i:lr, v:rows[lr].r}); if(lc>=0) ly.push({k:'Client', i:lc, v:rows[lc].rc});
  ly.sort((a,b)=>y(a.v)-y(b.v)); let prev = -99;
  ly.forEach(o=>{ let yy = Math.max(y(o.v)+4, prev+13); prev = yy; h += `<text class="dl" x="${x(o.i)+8}" y="${yy}">${o.k} ${fmtY(o.v)}</text>`; });
  // x ticks
  const nt = Math.min(8, n), step = Math.max(1, Math.round((n-1)/(nt-1||1)));
  for(let i=0;i<n;i+=step){ if(n-1-i < step*0.6 && i!==n-1) continue; h += `<text class="ax" x="${x(i)}" y="${H-8}" text-anchor="middle">${tickLbl(rows[i].p,g)}</text>`; }
  if(n>1) h += `<text class="ax" x="${x(n-1)}" y="${H-8}" text-anchor="middle">${tickLbl(rows[n-1].p,g)}</text>`;
  h += `<line class="hv" x1="0" x2="0" y1="${T}" y2="${yb}"/><rect class="hit" x="${L}" y="${T}" width="${W-L-R}" height="${yb-T}" fill="transparent"/></svg><div class="tip"></div>`;
  return h;
}
function exRender(){
  const S = EXD[EX.src]; if(!S || !exG()) return;
  const {rows, missing} = exSeries(), g = EX.g;
  const desc = S.cols.filter(c=>c in EX.sel).map(c=>`${esc(c)} = <b>${esc(EX.sel[c])}</b>`).join(' · ') || '<b>whole portfolio</b>';
  document.getElementById('exS').innerHTML = `${esc(sLbl(EX.src))} · ${desc}`+
    (missing.length ? ` <span style="color:var(--up)">· ${missing.map(esc).join(', ')} value not in the explore list (small values fold into (other)) — showing All for it</span>` : '')+
    (exKeyMissing() ? ` <span style="color:var(--up)">· this combination has fewer than ${fN(exG().min_loans)} loans in the window, so it isn't stored — widen a filter</span>` : '');
  // tiles: latest matured, non-thin period; compare with the average of the shown matured periods
  const ok = rows.filter(r=>r.mat && r.r!=null), L1 = ok[ok.length-1];
  const avg = k => { const v = ok.slice(0,-1).map(r=>r[k]).filter(v=>v!=null); return v.length ? v.reduce((a,b)=>a+b,0)/v.length : null; };
  const tile = (v, lab, sub) => `<div><b>${v}</b><span>${lab}${sub?`<br>${sub}`:''}</span></div>`;
  document.getElementById('exK').innerHTML = L1 ?
    tile(fP2(L1.r), `fpd30 by loans · ${fullLbl(L1.p,g)}`, `avg of earlier shown: ${fP2(avg('r'))}`)+
    tile(fP2(L1.rc), `fpd30 by clients · ${fullLbl(L1.p,g)}`, `avg of earlier shown: ${fP2(avg('rc'))}`)+
    tile(`${fN(L1.d)} / ${fN(L1.o)}`, 'defaults / observed loans', `${fN(L1.dc)} / ${fN(L1.oc)} clients`)+
    tile(fA(L1.a), 'observed amount', `${fN(L1.nl)} loans disbursed`)
    : '<div class="empty" style="grid-column:1/-1">No matured period passes the obs / amount rule for this sample.</div>';
  document.getElementById('exL').innerHTML = `<span><i class="sw" style="background:var(--s1)"></i>fpd30 by loans (def / obs)</span>`+
    `<span><i class="sw" style="background:var(--s2)"></i>fpd30 by clients (distinct)</span>`+
    `<span><i class="sw" style="background:var(--mut);opacity:.5"></i>observed amount (lower panel)</span>`+
    `<span style="color:var(--mut)">dashed = not yet mature · gaps = thin periods (obs &lt; ${M.min_obs} and amount ≤ ${fA(M.min_amt)})</span>`;
  const C = document.getElementById('exC'); C.innerHTML = exChart(rows, g); requestAnimationFrame(() => { C.scrollLeft = C.scrollWidth; });
  // hover: one crosshair across both panels
  const svg = C.querySelector('svg'), tip = C.querySelector('.tip'), hv = svg.querySelector('.hv'), hit = svg.querySelector('.hit');
  const vb = svg.viewBox.baseVal, x0 = +hit.getAttribute('x'), wp = +hit.getAttribute('width'), n = rows.length, bw = wp/Math.max(n,1);
  const move = e => { const b = svg.getBoundingClientRect(), sx = (e.clientX-b.left)*vb.width/b.width;
    const i = Math.max(0, Math.min(n-1, Math.floor((sx-x0)/bw))), px = x0 + bw*(i+.5), r = rows[i];
    hv.setAttribute('x1',px); hv.setAttribute('x2',px); hv.style.opacity=.6;
    tip.innerHTML = `<b>${fullLbl(r.p,g)}</b>${r.mat?'':' <span style="color:var(--mut)">(not yet mature)</span>'}`+
      `<div class="ti"><span><i class="sw" style="background:var(--s1)"></i>fpd30 loans</span><b>${r.r==null?'thin':fP2(r.r)}</b></div>`+
      `<div class="ti"><span><i class="sw" style="background:var(--s2)"></i>fpd30 clients</span><b>${r.rc==null?'thin':fP2(r.rc)}</b></div>`+
      `<div class="ti"><span>defaults / obs loans</span><b>${fN(r.d)} / ${fN(r.o)}</b></div>`+
      `<div class="ti"><span>defaults / obs clients</span><b>${fN(r.dc)} / ${fN(r.oc)}</b></div>`+
      `<div class="ti"><span><i class="sw" style="background:var(--mut);opacity:.5"></i>observed amount</span><b>${fA(r.a)}</b></div>`;
    tip.style.display='block'; const left = px*b.width/vb.width;
    tip.style.left = (left + 14 + tip.offsetWidth > b.width ? left - tip.offsetWidth - 14 : left + 14) + 'px'; tip.style.top = '8px'; };
  hit.addEventListener('pointermove', move); hit.addEventListener('pointerdown', move);
  hit.addEventListener('pointerleave', ()=>{ tip.style.display='none'; hv.style.opacity=0; });
  // data table + CSV
  const hd = ['Period','Mature','Loans','Obs loans','Defaults','fpd30 loans','Obs clients','Def clients','fpd30 clients','Obs amount'];
  document.getElementById('exT').innerHTML = `<table class="dt"><thead><tr>${hd.map((t,i)=>`<th class="${i>1?'num':''}">${t}</th>`).join('')}</tr></thead><tbody>`+
    rows.slice().reverse().map(r=>`<tr><td>${fullLbl(r.p,g)}</td><td>${r.mat?'yes':'no'}</td><td class="num">${fN(r.nl)}</td><td class="num">${fN(r.o)}</td><td class="num">${fN(r.d)}</td>`+
      `<td class="num">${r.r==null?'thin':fP2(r.r)}</td><td class="num">${fN(r.oc)}</td><td class="num">${fN(r.dc)}</td><td class="num">${r.rc==null?'thin':fP2(r.rc)}</td><td class="num">${fN(r.a)}</td></tr>`).join('')+'</tbody></table>';
  document.getElementById('exCsv').onclick = e => { e.preventDefault(); e.stopPropagation();
    const q = v => `"${String(v).replace(/"/g,'""')}"`;
    const meta = S.cols.map(c=>`${c}=${EX.sel[c]??'All'}`).join('; ');
    const lines = [['source','sample','granularity','period','mature','loans','obs_loans','defaults','fpd30_loans','obs_clients','def_clients','fpd30_clients','obs_amount'].join(',')]
      .concat(rows.map(r=>[EX.src, meta, g, r.p, r.mat, r.nl, r.o, r.d, r.r??'', r.oc, r.dc, r.rc??'', r.a].map(q).join(',')));
    const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([lines.join('\n')], {type:'text/csv'}));
    a.download = `explore_${EX.src}_${g}.csv`; document.body.appendChild(a); a.click(); a.remove(); };
}
function openExplore(src, sel, g){
  if(!EXD[src]) return;
  EX.src = src; EX.sel = Object.fromEntries(Object.entries(sel).filter(([c])=>EXD[src].cols.includes(c)));
  if(g && EXD[src].grans[g]) EX.g = g;
  if(MODAL) closeDetail();
  exControls(); exRender(); document.getElementById('exTab').click(); window.scrollTo(0,0);
}
document.getElementById('mEx').onclick = () => { const {src,col,val} = MODAL; openExplore(src, {[col]: val}, DG); };
document.addEventListener('click', e => { const b = e.target.closest('.xlink'); if(!b || !MODAL) return;
  const card = b.closest('.dcard'), ccol = card.querySelector('svg').dataset.ccol, {src,col,val} = MODAL;
  openExplore(src, {[col]: val, [ccol]: b.dataset.xv}, DG); });
document.getElementById('exTab').addEventListener('click', () => requestAnimationFrame(() => { const C = document.getElementById('exC'); C.scrollLeft = C.scrollWidth; }));
if(!Object.keys(EXD).length) document.getElementById('exTab').style.display = 'none';
else { exControls(); exRender(); }

document.title = M.title; document.getElementById('ttl').textContent = M.title;
document.getElementById('sub').textContent =
  `Generated ${M.generated} · ${M.sources.length} source${M.sources.length>1?'s':''} · last ${M.n_recent} matured periods (≥ ${Math.round(M.min_mature*100)}% observable) vs. all prior history · `+
  `spike = z > ${M.z} on ratio or client ratio · a period counts only if obs ≥ ${M.min_obs} or observed amount > ${fA(M.min_amt)} · sorted by recency, then z`;
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('.tab,.pane').forEach(e=>e.classList.remove('on'));
  b.classList.add('on'); document.getElementById(b.dataset.t).classList.add('on'); });
M.sources.forEach(s=>fSrc.insertAdjacentHTML('beforeend',`<option value="${esc(s.name)}">${esc(s.label)}</option>`));
fill('fG',M.grans); fill('fC',[...new Set(M.sources.flatMap(s=>s.cols))]); fill('fL',[...Array(M.n_recent)].map((_,i)=>i+1));
['fSrc','fG','fC','fD','fL','fZ','fS'].forEach(id=>document.getElementById(id).addEventListener('input',renderAll));
// min observed amount box: live filter, remembered in this browser
function setMinAmt(mil, save){
  const v = parseFloat(mil);
  MINAMT = isFinite(v) && v > 0 ? v * 1e6 : 0;
  const flagged = D.rows.filter(r=>r.direction), kept = flagged.filter(amtOk);
  document.getElementById('amtNote').textContent = MINAMT
    ? `showing ${kept.length} of ${flagged.length} flagged rows (periods with observed amount ≥ ${fA(MINAMT)})` : '';
  document.getElementById('fAmtX').hidden = !MINAMT;
  if(save){ try{ localStorage.setItem('rrm_min_amt_mil', MINAMT ? String(v) : ''); }catch(e){} }
  renderTop(); renderAll();
}
{ const inp = document.getElementById('fAmt'); let saved = '';
  try{ saved = localStorage.getItem('rrm_min_amt_mil') || ''; }catch(e){}
  inp.value = saved;
  inp.addEventListener('input', () => setMinAmt(inp.value, true));
  document.getElementById('fAmtX').onclick = () => { inp.value = ''; setMinAmt('', true); };
  setMinAmt(saved, false); }
</script></body></html>
"""

