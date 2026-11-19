"""
change_monitor: spot recent changes in category columns (counts and shares), as one HTML report.

    from change_monitor import run_change_monitor, EXAMPLE_SOURCES
    run_change_monitor(EXAMPLE_SOURCES, '/dbfs/FileStore/change_monitor.html', dry_run=True)  # print queries
    r = run_change_monitor(EXAMPLE_SOURCES, '/dbfs/FileStore/change_monitor.html')          # run

What it does
  1. For every source x granularity (daily / weekly / monthly) x category column it runs the
     source's SQL template on Spark (placeholders replaced with replace_sql_var).
  2. Per value: z-score of the last n_recent complete periods against all periods before them,
     on the count and on the share of the column total. Spikes first, most recent first.
  3. Writes one offline HTML file: source chips, "Top changes" and "All changes" tabs, and a
     daily / weekly / monthly chart for any value you click.
  4. Prints every step. Stops at the first failed query and says where (stop_on_error=False
     keeps going and lists the failures at the top of the report).

The SQL template of a source must output date_name, column_name, date_value, column_value,
cnt_all, group by 1,2,3,4 and use these placeholders:
  var_date_name, column_name_var, date_var, column_var, timeframe_var
"""
import os, json, time
import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# settings
# ----------------------------------------------------------------------------
DEFAULTS = {
    'grans':     ['daily', 'weekly', 'monthly'],
    'tz':        'Asia/Ho_Chi_Minh',
    'lookback':  {'daily': 30, 'weekly': 90, 'monthly': 240},   # months, passed to date_fm(n, 'month')
    'trunc':     {'daily': 'day', 'weekly': 'week', 'monthly': 'month'},
    'n_recent':  5,        # recent complete periods that are scored
    'z_th':      2.0,      # |z| threshold
    'min_base':  8,        # baseline periods needed to score a column
    'min_cnt':   20,       # a spike needs cnt >= this; a drop needs baseline >= this
    'top_n':     10,       # rows per column x granularity on the Top tab
    'spark_n':   30,       # periods in the sparkline
    'detail_n':  {'daily': 120, 'weekly': 104, 'monthly': 240},  # periods in the click-through charts
    'recency_w': {1: 1.0, 2: 0.8, 3: 0.6, 4: 0.45, 5: 0.3},
    'title':     'BNPL Change Monitor',
}

EXAMPLE_SOURCES = [
    {
        'name':   'bnpl_txn',                       # short id, no '|'
        'label':  'BNPL transactions',              # shown in the report
        'ts_col': 'dtime_create_transaction',       # UTC timestamp; or 'ts_expr': full expression
        'cols':   ['transaction_source', 'product_source', 'transaction_status', 'name_wc_result'],
        # 'lookback': {'daily': 12},                # optional override, months
        'sql': """select
   'var_date_name'                                  as date_name
 , 'column_name_var'                                as column_name
 , date_var                                         as date_value
 , coalesce(cast(column_var as string), '(null)')   as column_value
 , count(1)                                         as cnt_all
from af_month.rel_af_fact_bnpl_transaction
where month_transaction_partition >= timeframe_var
group by 1, 2, 3, 4
""",
    },
]

PLACEHOLDERS = ['var_date_name', 'column_name_var', 'date_var', 'column_var', 'timeframe_var']


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
        for k in ('name', 'sql', 'cols'):
            if not s.get(k):
                raise ValueError(f"sources[{i}] has no '{k}'.")
        if not (s.get('ts_col') or s.get('ts_expr')):
            raise ValueError(f"sources[{i}] ({s['name']}) needs 'ts_col' or 'ts_expr'.")
        if '|' in s['name']:
            raise ValueError(f"source name '{s['name']}' contains '|'.")
        if s['name'] in names:
            raise ValueError(f"source name '{s['name']}' is used twice.")
        names.add(s['name'])
        missing = [p for p in PLACEHOLDERS if p not in s['sql']]
        if missing:
            raise ValueError(f"source '{s['name']}': the SQL does not contain {', '.join(missing)}.")


def _deps(spark, date_fm, replace_sql_var):
    if spark is None or date_fm is None or replace_sql_var is None:
        import sachGiaoKhoa as sgk
        spark = spark if spark is not None else sgk.SPARK
        date_fm = date_fm or getattr(sgk, 'date_fm', None)
        replace_sql_var = replace_sql_var or sgk.replace_sql_var
        if date_fm is None:
            raise RuntimeError("date_fm not found in sachGiaoKhoa: pass date_fm=... yourself.")
    return spark, date_fm, replace_sql_var


def _now(cfg, now):
    t = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz=cfg['tz'])
    if t.tzinfo is not None:
        t = t.tz_convert(cfg['tz']).tz_localize(None)
    return t


def current_period_start(g, now):
    d = now.normalize()
    if g == 'daily':
        return d
    if g == 'weekly':                      # Spark date_trunc('week') = Monday
        return d - pd.Timedelta(days=d.weekday())
    return d.replace(day=1)


def fmt_period(p, g):
    return p.strftime('%Y-%m') if g == 'monthly' else (
        'wk ' + p.strftime('%Y-%m-%d') if g == 'weekly' else p.strftime('%Y-%m-%d'))


def _nz(x, nd=5):
    return None if x is None or pd.isna(x) else round(float(x), nd)


# ----------------------------------------------------------------------------
# 1. queries
# ----------------------------------------------------------------------------
def build_queries(sources, cfg, date_fm, replace_sql_var):
    """[(source, gran, column label, sql)] in run order."""
    out = []
    for src in sources:
        ts = src.get('ts_expr') or f"from_utc_timestamp(cast({src['ts_col']} as timestamp), '{cfg['tz']}')"
        lookback = dict(cfg['lookback'], **src.get('lookback', {}))
        for g in cfg['grans']:
            date_var = f"date_trunc('{cfg['trunc'][g]}', {ts})"
            timeframe_var = date_fm(lookback[g], 'month')
            for col in src['cols']:
                label = col.split('.')[-1]                 # 'a.channel' -> 'channel'
                sql_var = [                                # order matters: longer names first
                    {"from": 'column_name_var', "to": label},
                    {"from": 'var_date_name',   "to": g},
                    {"from": 'timeframe_var',   "to": timeframe_var},
                    {"from": 'column_var',      "to": col},
                    {"from": 'date_var',        "to": date_var},
                ]
                out.append((src['name'], g, label, replace_sql_var(src['sql'], sql_var)))
    return out


# ----------------------------------------------------------------------------
# 2. scoring (pandas only)
# ----------------------------------------------------------------------------
def score_block(d, src, g, col, cfg, now, log=print):
    d = d[d['date_value'] < current_period_start(g, now)]           # drop the incomplete period
    if d.empty:
        return None, {}
    piv = d.pivot_table(index='column_value', columns='date_value', values='cnt_all', aggfunc='sum', fill_value=0)
    piv = piv.reindex(columns=sorted(piv.columns))
    periods = list(piv.columns)
    n_recent = cfg['n_recent']
    if len(periods) < n_recent + cfg['min_base']:
        log(f"      skip {src}/{g}/{col}: only {len(periods)} complete periods (need {n_recent + cfg['min_base']})")
        return None, {}

    share = piv.div(piv.sum(axis=0).replace(0, np.nan), axis=1)
    base_p, rec_p = periods[:-n_recent], periods[-n_recent:]
    mu_c, sd_c = piv[base_p].mean(axis=1), piv[base_p].std(axis=1)
    mu_s, sd_s = share[base_p].mean(axis=1), share[base_p].std(axis=1)

    out = []
    for lag, p in enumerate(reversed(rec_p), start=1):
        x_c, x_s = piv[p], share[p]
        out.append(pd.DataFrame({
            'source': src, 'date_name': g, 'column_name': col, 'column_value': piv.index,
            'period': fmt_period(p, g), 'lag': lag,
            'cnt': x_c.values, 'base_cnt': mu_c.values,
            'z_cnt': ((x_c - mu_c) / sd_c.replace(0, np.nan)).values,
            'share': x_s.values, 'base_share': mu_s.values,
            'z_share': ((x_s - mu_s) / sd_s.replace(0, np.nan)).values,
        }))

    keep_p = periods[-cfg['detail_n'][g]:]
    series = {}
    for v in piv.index:
        series[f'{src}|{g}|{col}|{v}'] = {
            'p':  [fmt_period(p, g) for p in keep_p],
            'c':  [float(x) for x in piv.loc[v, keep_p]],
            's':  [_nz(x) for x in share.loc[v, keep_p]],
            'mc': _nz(mu_c[v], 2), 'sc': _nz(sd_c[v], 2),
            'ms': _nz(mu_s[v]),    'ss': _nz(sd_s[v]),
        }
    return pd.concat(out, ignore_index=True), series


def score(raw, cfg, now, log=print):
    """raw: source, date_name, column_name, date_value, column_value, cnt_all -> (res, series)."""
    raw = raw.copy()
    raw['date_value'] = pd.to_datetime(raw['date_value'])
    raw['cnt_all'] = raw['cnt_all'].astype(float)
    raw['column_value'] = raw['column_value'].astype(str)
    parts, series = [], {}
    for (src, g, col), d in raw.groupby(['source', 'date_name', 'column_name']):
        r, s = score_block(d, src, g, col, cfg, now, log)
        if r is not None:
            parts.append(r)
            series.update(s)
    if not parts:
        return None, {}
    res = pd.concat(parts, ignore_index=True)

    z_th, min_cnt = cfg['z_th'], cfg['min_cnt']
    res['flag'] = np.where((res['base_cnt'] == 0) & (res['cnt'] >= min_cnt), 'NEW', '')
    res['z_max'] = res[['z_cnt', 'z_share']].max(axis=1)
    res['z_min'] = res[['z_cnt', 'z_share']].min(axis=1)
    res.loc[res['flag'] == 'NEW', 'z_max'] = 10.0               # new categories rank as strong spikes
    is_spike = ((res['z_max'] > z_th) & (res['cnt'] >= min_cnt)) | (res['flag'] == 'NEW')
    is_drop = (res['z_min'] < -z_th) & (res['base_cnt'] >= min_cnt) & ~is_spike
    res['direction'] = np.select([is_spike, is_drop], ['spike', 'drop'], '')

    w = res['lag'].map(cfg['recency_w']).fillna(min(cfg['recency_w'].values()))
    mag = np.where(is_spike, res['z_max'], np.where(is_drop, -res['z_min'], res[['z_cnt', 'z_share']].abs().max(axis=1)))
    res['score'] = (pd.Series(mag, index=res.index).fillna(0) * w).round(2)
    res['dir_rank'] = res['direction'].map({'spike': 0, 'drop': 1, '': 2})
    res = res.sort_values(['dir_rank', 'lag', 'score'], ascending=[True, True, False]).reset_index(drop=True)
    res['ord'] = res.index
    res['key'] = res['source'] + '|' + res['date_name'] + '|' + res['column_name'] + '|' + res['column_value'].astype(str)
    return res, series


# ----------------------------------------------------------------------------
# 3. HTML
# ----------------------------------------------------------------------------
def render_html(res, series, sources, cfg, now, failed=()):
    keep = ['ord', 'key', 'source', 'date_name', 'column_name', 'column_value', 'period', 'lag', 'cnt', 'base_cnt',
            'z_cnt', 'share', 'base_share', 'z_share', 'flag', 'direction', 'score']
    out = res[keep].copy()
    for c in ['cnt', 'base_cnt', 'z_cnt', 'z_share', 'score']:
        out[c] = out[c].round(2)
    for c in ['share', 'base_share']:
        out[c] = out[c].round(5)
    out = out.replace([np.inf, -np.inf], np.nan).astype(object).where(out.notna(), None)
    keys = set(out['key'])
    payload = {
        'rows': out.to_dict(orient='records'),
        'series': {k: v for k, v in series.items() if k in keys},
        'meta': {'z': cfg['z_th'], 'n_recent': cfg['n_recent'], 'top_n': cfg['top_n'], 'grans': cfg['grans'],
                 'title': cfg['title'],
                 'sources': [{'name': s['name'], 'label': s.get('label', s['name']),
                              'cols': [c.split('.')[-1] for c in s['cols']]} for s in sources],
                 'failed': list(failed), 'generated': now.strftime('%Y-%m-%d %H:%M'),
                 'min_cnt': cfg['min_cnt'], 'spark_n': cfg['spark_n']},
    }
    data_json = json.dumps(payload, default=str).replace('</', '<\\/')
    return _HTML.replace('__DATA__', data_json)


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def run_change_monitor(sources, out_path='change_monitor.html', spark=None, date_fm=None, replace_sql_var=None,
                       dry_run=False, stop_on_error=True, now=None, log=print, **settings):
    """
    sources          list of source dicts (see EXAMPLE_SOURCES)
    out_path         where to write the HTML (e.g. /dbfs/FileStore/change_monitor.html)
    dry_run          True = print the queries (placeholders replaced) and run nothing
    stop_on_error    True = stop at the first failed query; False = skip it and list it in the report
    spark, date_fm, replace_sql_var   default to sachGiaoKhoa; pass fakes for testing
    now              'today' for the incomplete-period cut (default: now in settings['tz'])
    **settings       any key of DEFAULTS, e.g. z_th=2.5, grans=['weekly', 'monthly']
    Returns a dict: ok, out_path, res (scored DataFrame), failed, and where it stopped if it did.
    """
    t0 = time.time()
    cfg = _settings(settings)
    _check_sources(sources)
    spark, date_fm, replace_sql_var = _deps(spark, date_fm, replace_sql_var)
    now = _now(cfg, now)
    result = {'ok': False, 'out_path': out_path, 'res': None, 'failed': [], 'title': cfg['title'],
              'z_th': cfg['z_th'], 'kind': 'change',
              'source_labels': {s['name']: s.get('label', s['name']) for s in sources}}

    queries = build_queries(sources, cfg, date_fm, replace_sql_var)
    log(f"change_monitor: {len(sources)} source(s), {len(queries)} queries, granularities {', '.join(cfg['grans'])}")

    if dry_run:
        for i, (s, g, c, q) in enumerate(queries, 1):
            log(f"\n--- query {i}/{len(queries)}: {s} / {g} / {c} ---\n{q.rstrip()}")
        log(f"\nDry run: nothing was run. The report would be written to {out_path}")
        result.update(ok=True, dry_run=True, queries=queries)
        return result

    # ---- 1. pull ----
    log('\n[1/3] pull counts')
    frames = []
    for i, (s, g, c, q) in enumerate(queries, 1):
        st = time.time()
        try:
            df = spark.sql(q).toPandas()
        except Exception as e:
            msg = str(e).strip()
            if stop_on_error:
                first = msg.splitlines()[0][:500] if msg else type(e).__name__
                log(f"\nFAILED at query {i}/{len(queries)}: {s} / {g} / {c}: {first}")
                rest = [f"{a}/{b}/{x}" for a, b, x, _ in queries[i:i + 12]] + (['...'] if len(queries) - i > 12 else [])
                log(f"  not run: {', '.join(rest)}")
                result.update(failed_step=i, failed_query=(s, g, c), error=msg, query=q)
                return result
            result['failed'].append(f"{s}/{g}/{c}: {msg.splitlines()[0][:200] if msg else type(e).__name__}")
            log(f"  {i:>3}/{len(queries)} {s}/{g}/{c}: FAILED, skipped ({result['failed'][-1]})")
            continue
        missing = {'date_value', 'column_value', 'cnt_all'} - set(df.columns)
        if missing:
            raise RuntimeError(f"query {s}/{g}/{c} returned no {', '.join(sorted(missing))} column(s). "
                               f"Columns: {', '.join(df.columns)}")
        df['source'], df['date_name'], df['column_name'] = s, g, c
        frames.append(df)
        log(f"  {i:>3}/{len(queries)} {s}/{g}/{c}: {len(df):,} rows ({time.time() - st:.1f}s)")
    if not frames:
        log('No query returned data: nothing to report.')
        result['error'] = 'no data'
        return result

    # ---- 2. score ----
    log('\n[2/3] score')
    res, series = score(pd.concat(frames, ignore_index=True), cfg, now, log)
    if res is None:
        log('Nothing could be scored (not enough complete periods anywhere).')
        result['error'] = 'nothing scored'
        return result
    counts = res['direction'].replace('', 'none').value_counts()
    log(f"  {len(res):,} rows scored: {counts.get('spike', 0)} spikes, {counts.get('drop', 0)} drops")

    # ---- 3. HTML ----
    log('\n[3/3] write report')
    html = render_html(res, series, sources, cfg, now, result['failed'])
    d = os.path.dirname(out_path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    log(f"  written: {out_path} ({len(html.encode('utf-8')) / 1e6:.1f} MB)")
    log(f"\nDone in {time.time() - t0:.0f}s." + (f" {len(result['failed'])} query(ies) failed and were skipped."
                                                if result['failed'] else ''))
    result.update(ok=True, res=res)
    return result


# ----------------------------------------------------------------------------
# report template (data goes in at __DATA__)
# ----------------------------------------------------------------------------
_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BNPL Change Monitor</title>
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
.ch .ser{fill:none;stroke:var(--ln);stroke-width:1.6}.ch .rec{fill:var(--rec)}
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

/* phone: charts keep a readable size and scroll inside their box; notes wrap */
@media (max-width:640px){.ch{overflow-x:auto;-webkit-overflow-scrolling:touch}.ch svg{min-width:600px}.dleg span,.leg span{white-space:normal}
.mh{flex-wrap:wrap}.mh>div:first-child{order:0;flex:1 1 calc(100% - 48px)}.mh .x{order:1}.mh .seg,.mh #mEx{order:2}}
</style></head><body>
<header><h1 id="ttl"></h1>
<div class="sub" id="sub"></div></header>
<nav class="tabs"><button class="tab on" data-t="top">Top changes</button><button class="tab" data-t="all">All changes</button></nav>
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
</main>
<div class="ov" id="ov" hidden><div class="modal" role="dialog" aria-modal="true">
 <div class="mh"><div><div class="mt" id="mT"></div><div class="sub" id="mS"></div></div>
  <div class="seg" id="mSeg"><button data-m="c" class="on">Count</button><button data-m="s">Share</button></div>
  <button class="x" id="mX" aria-label="Close">×</button></div>
 <div id="mB"></div>
</div></div>
<script>
const D = __DATA__;
const M = D.meta;
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fN = v => v==null ? '–' : Math.round(v).toLocaleString();
const fP = v => v==null ? '–' : (v*100).toFixed(1)+'%';
const fZ = (v,flag) => flag==='NEW' ? 'new' : (v==null ? '–' : v.toFixed(1));
const zc = v => v==null ? '' : v>M.z ? 'up' : v<-M.z ? 'down' : '';

function spark(key, period){
  const S = D.series[key]; if(!S) return '';
  const s = {p:S.p.slice(-M.spark_n), c:S.c.slice(-M.spark_n)};
  const W=120,H=28,n=s.c.length,mx=Math.max(...s.c,1);
  const x=i=> n>1 ? 2+i*(W-4)/(n-1) : W/2, y=v=> H-3-(v/mx)*(H-6);
  const rs=Math.max(0,n-M.n_recent), i=s.p.indexOf(period);
  const pts=s.c.map((v,j)=>x(j).toFixed(1)+','+y(v).toFixed(1)).join(' ');
  return `<svg width="${W}" height="${H}"><rect class="rec" x="${x(rs)-3}" y="0" width="${W-x(rs)+3}" height="${H}"/>`+
    `<polyline class="ln" points="${pts}"/>`+(i>=0?`<circle class="dot" cx="${x(i)}" cy="${y(s.c[i])}" r="2.6"/>`:'')+`</svg>`;
}

const SRC = Object.fromEntries(M.sources.map(s=>[s.name, s]));
const sLbl = n => (SRC[n] && SRC[n].label) || n;
const COLS_BASE = [['column_value','Value'],['period','Period'],['lag','Lag'],['cnt','Count'],['base_cnt','Baseline'],
  ['z_cnt','z count'],['share','Share'],['base_share','Base share'],['z_share','z share'],['score','Score'],[null,'Trend']];
const PRE = {source:['source','Source'], date_name:['date_name','Gran.'], column_name:['column_name','Column'], direction:['direction','Dir.']};
const PRE_ALL = ['source','date_name','column_name','direction'];

function head(cols, sortable){
  return '<thead><tr>'+cols.map(([k,l])=>`<th ${sortable&&k?`data-k="${k}"`:''} class="${['lag','cnt','base_cnt','z_cnt','share','base_share','z_share','score'].includes(k)?'num':''}">${l}</th>`).join('')+'</tr></thead>';
}
function preCell(r, k){
  if(k==='source') return `<td><span class="srcl">${esc(sLbl(r.source))}</span></td>`;
  if(k==='direction') return `<td class="${r.direction==='spike'?'up':r.direction==='drop'?'down':''}">${r.direction||''}</td>`;
  return `<td>${esc(r[k])}</td>`;
}
function row(r, pre){
  return `<tr>${(pre||[]).map(k=>preCell(r,k)).join('')}<td class="val" title="Open daily / weekly / monthly charts"><span class="cv" data-src="${esc(r.source)}" data-col="${esc(r.column_name)}" data-cv="${esc(r.column_value)}">${esc(r.column_value)}</span>${r.flag?`<span class="tag">${r.flag}</span>`:''}</td>`+
    `<td>${r.period}</td><td class="num">${r.lag}</td><td class="num">${fN(r.cnt)}</td><td class="num">${fN(r.base_cnt)}</td>`+
    `<td class="num ${zc(r.z_cnt)}">${fZ(r.z_cnt,r.flag)}</td><td class="num">${fP(r.share)}</td><td class="num">${fP(r.base_share)}</td>`+
    `<td class="num ${zc(r.z_share)}">${fZ(r.z_share,r.flag)}</td><td class="num">${r.score??'–'}</td><td>${spark(r.key,r.period)}</td></tr>`;
}
const table = (rows, pre) => { const p = pre||[];
  return `<div class="wrap"><table>${head(p.map(k=>PRE[k]).concat(COLS_BASE))}<tbody>${rows.map(r=>row(r,p)).join('')}</tbody></table></div>`; };

// ---------- Top tab ----------
let TOPSRC = '__all';
const SPIKES = D.rows.filter(r=>r.direction==='spike');
function renderTop(){
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
    const n = M.top_n*2, drops = D.rows.filter(r=>r.direction==='drop');
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
      const blk = D.rows.filter(r=>r.source===S.name&&r.column_name===c&&r.date_name===g);
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
  let rows = D.rows.filter(r=>(!sr||r.source===sr)&&(!g||r.date_name===g)&&(!c||r.column_name===c)&&
    (!d||(d==='none'?!r.direction:r.direction===d))&&(!l||String(r.lag)===l)&&
    (r.flag==='NEW'||Math.max(Math.abs(r.z_cnt??0),Math.abs(r.z_share??0))>=z)&&
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
let MODAL = null, METRIC = 'c';

function detailChart(g, src, col, val){
  const key = `${src}|${g}|${col}|${val}`, s = D.series[key], rr = ROWIDX[key] || {};
  const title = `<h3 style="margin-top:4px">${g}</h3>`;
  if(!s) return `<div class="card">${title}<div class="empty">Not scored for this granularity (not enough history, or no data).</div></div>`;
  const isC = METRIC==='c', vals = isC ? s.c : s.s;
  const mu = isC ? s.mc : s.ms, sd = isC ? s.sc : s.ss, zk = isC ? 'z_cnt' : 'z_share';
  const fmtY = v => isC ? (v>=1e6 ? (v/1e6).toFixed(1)+'M' : v>=1e3 ? (v/1e3).toFixed(v>=1e4?0:1)+'k' : Math.round(v)) : (v*100).toFixed(v<0.1?1:0)+'%';
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
  const pts = vals.map((v,i)=> v==null ? null : `${x(i).toFixed(1)},${y(v).toFixed(1)}`).filter(Boolean).join(' ');
  h += `<polyline class="ser" points="${pts}"/>`;
  let lastLbl = Infinity;                       // label newest first; skip labels closer than 46px
  for(let i=n-1;i>=rs;i--){
    const r = rr[s.p[i]], v = vals[i]; if(v==null) continue;
    const z = r ? r[zk] : null, cls = r&&r.flag==='NEW' ? 'up' : z==null ? 'n' : z>M.z ? 'up' : z<-M.z ? 'down' : 'n';
    h += `<circle class="mk ${cls}" cx="${x(i)}" cy="${y(v)}" r="${cls==='n'?3.5:5}"/>`;
    if(cls!=='n' && lastLbl - x(i) >= 46){
      h += `<text class="zl ${cls}" x="${x(i)}" y="${y(v) + (cls==='down'?16:-9)}" text-anchor="middle">${r.flag==='NEW'?'NEW':'z '+z.toFixed(1)}</text>`;
      lastLbl = x(i);
    }
  }
  h += `<line class="hv" x1="0" x2="0" y1="${T}" y2="${H-B}"/><rect x="${L}" y="${T}" width="${W-L-R}" height="${H-T-B}" fill="transparent" class="hit"/></svg>`;

  let strip = '<div class="strip">';
  for(let i=n-1;i>=rs;i--){
    const r = rr[s.p[i]]; if(!r) continue;
    const z = r[zk], cls = r.flag==='NEW' ? 'up' : z==null ? '' : z>M.z ? 'up' : z<-M.z ? 'down' : '';
    strip += `<div class="chip ${cls}"><b>${fullLbl(s.p[i],g)}</b>${isC?fN(r.cnt):fP(r.share)} <span style="color:var(--mut)">vs ${isC?fN(r.base_cnt):fP(r.base_share)}</span><br>`+
      `<span class="zz">${r.flag==='NEW'?'NEW':'z '+(z==null?'–':z.toFixed(1))}</span> · lag ${r.lag}</div>`;
  }
  strip += '</div>';
  const rng = `${fullLbl(s.p[0],g)} → ${fullLbl(s.p[n-1],g)}`;
  return `<div class="card">${title}<div class="sub" style="margin:-4px 0 6px">${rng} · ${n} periods · current incomplete ${g==='daily'?'day':g.replace('ly','')} excluded</div>`+
    `<div class="ch">${h}<div class="tip"></div></div>${strip}</div>`;
}

function bindHover(card){
  const svg = card.querySelector('svg'); if(!svg) return;
  const key = svg.dataset.key, s = D.series[key], g = key.split('|')[1], rr = ROWIDX[key] || {};
  const tip = card.querySelector('.tip'), hv = svg.querySelector('.hv'), hit = svg.querySelector('.hit');
  const vb = svg.viewBox.baseVal, L=+hit.getAttribute('x'), Wp=+hit.getAttribute('width'), n=s.c.length;
  const move = e => {
    const b = svg.getBoundingClientRect(), sx = (e.clientX - b.left) * vb.width / b.width;
    const i = Math.max(0, Math.min(n-1, Math.round((sx - L) / (n>1 ? Wp/(n-1) : 1))));
    const px = L + (n>1 ? i*Wp/(n-1) : Wp/2);
    hv.setAttribute('x1',px); hv.setAttribute('x2',px); hv.style.opacity=.6;
    const r = rr[s.p[i]];
    tip.innerHTML = `<b>${fullLbl(s.p[i],g)}</b><br>Count ${fN(s.c[i])} · Share ${fP(s.s[i])}`+
      (r ? `<br>z count ${fZ(r.z_cnt,r.flag)} · z share ${fZ(r.z_share,r.flag)}` : '');
    tip.style.display='block';
    const left = px * b.width / vb.width;
    tip.style.left = Math.min(left + 10, b.width - tip.offsetWidth - 4) + 'px'; tip.style.top = '6px';
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
    `<span><i style="border-color:var(--dn)"></i>−${M.z}σ (drop line)</span><span>shaded = last ${M.n_recent} scored periods · hover for exact values</span></div>`;
  mB.querySelectorAll('.card').forEach(bindHover);
  requestAnimationFrame(() => mB.querySelectorAll('.ch').forEach(c => c.scrollLeft = c.scrollWidth));   // phones: open at the newest periods
  document.querySelectorAll('#mSeg button').forEach(b=>b.classList.toggle('on', b.dataset.m===METRIC));
}
function openDetail(src, col, val){ MODAL = {src, col, val}; renderModal(); document.getElementById('ov').hidden = false; document.body.style.overflow='hidden'; }
function closeDetail(){ document.getElementById('ov').hidden = true; document.body.style.overflow=''; MODAL = null; }
document.addEventListener('click', e => {
  const a = e.target.closest('.cv'); if(a) return openDetail(a.dataset.src, a.dataset.col, a.dataset.cv);
  const b = e.target.closest('#top [data-src]'); if(b){ TOPSRC = b.dataset.src; renderTop(); }
});
document.getElementById('mX').onclick = closeDetail;
document.getElementById('ov').addEventListener('click', e => { if(e.target.id==='ov') closeDetail(); });
document.addEventListener('keydown', e => { if(e.key==='Escape' && MODAL) closeDetail(); });
document.querySelectorAll('#mSeg button').forEach(b => b.onclick = () => { METRIC = b.dataset.m; renderModal(); });

document.title = M.title; document.getElementById('ttl').textContent = M.title;
document.getElementById('sub').textContent =
  `Generated ${M.generated} · ${M.sources.length} source${M.sources.length>1?'s':''} · last ${M.n_recent} complete periods vs. all prior history · spike = z > ${M.z} on count or share (count ≥ ${M.min_cnt}) · sorted by recency, then z`;
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('.tab,.pane').forEach(e=>e.classList.remove('on'));
  b.classList.add('on'); document.getElementById(b.dataset.t).classList.add('on'); });
M.sources.forEach(s=>fSrc.insertAdjacentHTML('beforeend',`<option value="${esc(s.name)}">${esc(s.label)}</option>`));
fill('fG',M.grans); fill('fC',[...new Set(M.sources.flatMap(s=>s.cols))]); fill('fL',[...Array(M.n_recent)].map((_,i)=>i+1));
['fSrc','fG','fC','fD','fL','fZ','fS'].forEach(id=>document.getElementById(id).addEventListener('input',renderAll));
renderTop(); renderAll();
</script></body></html>
"""

