"""Shared test data and fakes for the monitoring tests (no cluster needed)."""
import re
import numpy as np
import pandas as pd

NOW = pd.Timestamp('2026-10-26 10:00')          # a Monday: the current week starts today


# ----------------------------------------------------------------------------
# change_monitor: daily counts per (source, column, value) with planted changes
# ----------------------------------------------------------------------------
def bnpl_daily(seed=42):
    rng = np.random.default_rng(seed)
    days = pd.date_range('2023-10-01', NOW.normalize(), freq='D')     # includes today (incomplete)
    n = len(days)
    trend = 1 + np.arange(n) / n * 0.8
    dow = np.array([1.0, 1.0, 1.0, 1.05, 1.15, 1.3, 1.2])[days.dayofweek]
    lam = lambda b: b * trend * dow
    d = lambda s: days.get_loc(pd.Timestamp(s))
    data = {}
    add = lambda src, col, v, arr: data.setdefault((src, col), {}).__setitem__(v, arr)
    cats = {
        'transaction_source': {'app': 900, 'web': 350, 'pos': 200, 'partner_api': 60},
        'product_source': {'electronics': 500, 'fashion': 400, 'travel': 150, 'grocery': 250, 'gaming': 80},
        'transaction_status': {'success': 1200, 'failed': 90, 'pending': 40, 'cancelled': 60, 'refunded': 25},
    }
    for col, vals in cats.items():
        for v, b in vals.items():
            L = lam(b).copy()
            if col == 'transaction_status' and v == 'failed':
                L[d('2026-10-24'):] *= 3.2                       # spike, last 2 days
            if col == 'transaction_source' and v == 'pos':
                L[d('2026-10-12'):] *= 0.45                      # drop
            if col == 'product_source' and v == 'travel':
                L[d('2026-09-01'):] *= 1.9                       # monthly-visible
            add('bnpl_txn', col, v, rng.poisson(L))
    x = np.zeros(n)
    x[d('2026-10-15'):] = rng.poisson(120, n - d('2026-10-15'))
    add('bnpl_txn', 'transaction_source', 'qr_merchant', x)        # new 11 days ago -> spike
    x = np.zeros(n)
    x[d('2026-10-23'):] = rng.poisson(80, n - d('2026-10-23'))
    add('bnpl_txn', 'transaction_source', 'paylater_link', x)      # new inside the last 5 days -> NEW
    for col, vals in {'channel': {'app': 2500, 'web': 900, 'tiktok_shop': 150},
                      'decision': {'approved': 2400, 'rejected': 1100}}.items():
        for v, b in vals.items():
            L = lam(b).copy()
            if v == 'tiktok_shop':
                L[d('2026-10-22'):] *= 3.5
            add('bnpl_app', col, v, rng.poisson(L))
    return days, data


class FakeResult:
    def __init__(self, df):
        self.df = df

    def toPandas(self):
        return self.df


class FakeSparkCounts:
    """Answers the change_monitor template queries from bnpl_daily()."""
    def __init__(self, days, data, fail_on=None):
        self.days, self.data, self.fail_on, self.queries = days, data, fail_on, []

    def sql(self, q):
        self.queries.append(q)
        if self.fail_on and self.fail_on in q:
            raise Exception("[UNRESOLVED_COLUMN] A column `%s` cannot be resolved.; line 4 pos 22\n"
                            "== SQL ==\nselect ..." % self.fail_on)
        g = re.search(r"'(daily|weekly|monthly)'\s+as date_name", q).group(1)
        col = re.search(r"'(\w+)'\s+as column_name", q).group(1)
        src = 'bnpl_app' if 'bnpl_application' in q else 'bnpl_txn'
        rows = []
        for v, arr in self.data.get((src, col), {}).items():
            s = pd.Series(arr, index=self.days)
            if g == 'daily':
                s = s[s.index >= '2026-04-26']
            elif g == 'weekly':
                s = s.groupby(s.index - pd.to_timedelta(s.index.dayofweek, 'D')).sum()
            else:
                s = s.groupby(s.index.to_period('M').to_timestamp()).sum()
            rows += [(g, col, dt, v, int(c)) for dt, c in s.items() if c > 0]
        return FakeResult(pd.DataFrame(rows, columns=['date_name', 'column_name', 'date_value', 'column_value', 'cnt_all']))


def date_fm(n, unit):
    return "'2020-01'"


def replace_sql_var(sql, sql_var):                 # same behaviour as sachGiaoKhoa: plain str.replace, in order
    for item in sql_var:
        sql = sql.replace(item['from'], str(item['to']))
    return sql


APP_SOURCE = {
    'name': 'bnpl_app', 'label': 'BNPL applications', 'ts_col': 'a.dtime_create_application',
    'cols': ['a.channel', 'a.decision'],
    'sql': """select 'var_date_name' as date_name, 'column_name_var' as column_name, date_var as date_value
 , coalesce(cast(column_var as string), '(null)') as column_value, count(1) as cnt_all
from af_month.rel_af_fact_bnpl_application a
where a.month_partition >= timeframe_var
group by 1, 2, 3, 4""",
}


# ----------------------------------------------------------------------------
# risk_ratio_monitor: loan-level data
# ----------------------------------------------------------------------------
TODAY_RISK = pd.Timestamp('2026-11-16')


def loans(n=120_000, seed=7, extra_col=False):
    rng = np.random.default_rng(seed)
    disb = TODAY_RISK - pd.to_timedelta(rng.integers(0, 730, n), 'D')
    a = rng.choice(['app', 'web', 'partner'], n, p=[.6, .3, .1])
    b = rng.choice(['electronics', 'fashion', 'travel', 'grocery', 'rare'], n, p=[.35, .3, .12, .229, .001])
    c = rng.choice(['north', 'south', 'central', 'highland', 'mekong'], n, p=[.3, .3, .2, .08, .12])
    client = rng.integers(0, 50_000, n)
    amt = np.round(rng.lognormal(np.log(3e6), .6, n), -3)
    p = np.full(n, .04)
    recent = disb >= pd.Timestamp('2026-08-17')
    p[(b == 'travel') & recent] *= 3.0
    p[(a == 'web') & (b == 'fashion') & recent] *= 3.2
    p[(c == 'north') & (disb >= pd.Timestamp('2026-07-01'))] *= .45
    idx = np.where((a == 'partner') & recent & (disb <= pd.Timestamp('2026-09-12')))[0][:60]
    client[idx] = 777
    p[idx] = .95
    f_pd30 = (rng.random(n) < p).astype(int)
    age = (TODAY_RISK - disb).days
    f_obs = (age >= 60).astype(int)
    mid = (age >= 45) & (age < 60)
    f_obs[mid] = (rng.random(mid.sum()) < .5)
    f_pd30[f_obs == 0] = 0
    df = pd.DataFrame({'disbursement_date': disb.date, 'a': a, 'b': b, 'c': c, 'client_id': client,
                       'loan_amount': amt, 'f_pd30': f_pd30, 'f_pd30_obs': f_obs})
    if extra_col:
        df['d'] = rng.choice([f'd{i:02d}' for i in range(30)], n)
    return df


RISK_SOURCE = {
    'name': 'pd30', 'label': 'PD30', 'date_col': 'disbursement_date', 'def_col': 'f_pd30',
    'obs_col': 'f_pd30_obs', 'client_col': 'client_id', 'amt_col': 'loan_amount', 'seg_cols': ['a', 'b', 'c'],
}


# ----------------------------------------------------------------------------
# headless browser checks (skipped when playwright is not installed)
# ----------------------------------------------------------------------------
def browser():
    try:
        from playwright.sync_api import sync_playwright
        return sync_playwright
    except Exception:
        return None
