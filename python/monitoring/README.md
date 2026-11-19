# monitoring

Python tools that run on the server and produce **one offline HTML report** that flags recent changes. The report can then be emailed.

| File | What it does |
|---|---|
| `change_monitor.py` | Category columns (status, source, product, …): **counts and shares** by day, week and month. |
| `risk_ratio_monitor.py` | A default ratio (e.g. fpd30) by segment, **by loans and by distinct clients**, by week and month. Adds a drill-down, top clients and an Explore tab. |
| `send_report_email.py` | Emails either report: a summary table in the body, and the report zipped as an attachment. |

**On the server**, copy these files next to `sachGiaoKhoa.py`, together with [`../common/sql_errors.py`](../common/). Spark, `replace_sql_var` and `date_fm` come from `sachGiaoKhoa` unless you pass them in.

## How a value gets flagged (both monitors)

- Each value is scored on the **last 5 complete periods** (`n_recent`). The current day, week or month is never scored, because it isn't finished.
- **Baseline:** all periods before those 5. `z = (value − baseline mean) / baseline std`.
- **Spike:** `z > 2` (`z_th`) on either measure. **Drop:** `z < −2`.
- **Sorted** by spikes first, then the most recent period, then a score of z × a recency weight (lag 1 = 1.0 … lag 5 = 0.3).

## change_monitor.py

```python
from change_monitor import run_change_monitor, EXAMPLE_SOURCES
out = '/dbfs/FileStore/change_monitor.html'

run_change_monitor(EXAMPLE_SOURCES, out, dry_run=True)    # print every query, run nothing
r = run_change_monitor(EXAMPLE_SOURCES, out)              # run
r = run_change_monitor(EXAMPLE_SOURCES, out, grans=['weekly', 'monthly'], z_th=2.5)
```

**A source** is one SQL template with its own table, joins, filters and columns. Every source appears as a chip in the report.

| Key | Meaning |
|---|---|
| `name`, `label` | A short id (no `\|`) and the name shown in the report. |
| `sql` | Must output `date_name, column_name, date_value, column_value, cnt_all`, `group by 1,2,3,4`, and use the placeholders `var_date_name`, `column_name_var`, `date_var`, `column_var`, `timeframe_var`. |
| `ts_col` or `ts_expr` | The UTC timestamp column (converted to `Asia/Ho_Chi_Minh`), or a full expression. |
| `cols` | Category columns. An alias such as `a.channel` is shown as `channel`. |
| `lookback` | Optional, months per granularity, e.g. `{'daily': 12}`. |

Measures: **count** and **share of the column total**. A spike needs `cnt ≥ min_cnt`. A value that only appeared inside the last 5 periods is flagged **NEW**.

## risk_ratio_monitor.py

```python
from risk_ratio_monitor import run_risk_ratio_monitor
SOURCES = [{
    'name': 'pd30', 'label': 'PD30',
    'data': "select * from af_mon.your_loan_table where month_partition >= 'var_month'",  # or a Spark DataFrame
    'date_col': 'disbursement_date', 'def_col': 'f_pd30', 'obs_col': 'f_pd30_obs',
    'client_col': 'client_id', 'amt_col': 'loan_amount', 'seg_cols': ['a', 'b', 'c'],
}]
run_risk_ratio_monitor(SOURCES, out, sql_var=sql_var, dry_run=True)   # print the plan and the SQL
r = run_risk_ratio_monitor(SOURCES, out, sql_var=sql_var)
```

- `ratio = sum(def) / sum(obs)` and `ratio_client = distinct clients with def=1 / distinct clients with obs=1`. Defaults are counted on observed loans only.
- **Matured periods only.** A period is scored when at least 80% of its loans are observable (`min_mature`). Newer periods are left out, because they would look like false drops.
- **Thin periods.** A period counts only when `obs ≥ 4` or `observed amount > 5M` (`min_obs`, `min_amt`). Thin periods show as gaps and are never flagged. Values that never pass are skipped.
- **Spark work:** one pass per granularity for all segment columns together (they are unpivoted), one for the column pairs, one for the top clients, and one GROUPING SETS cube for Explore.

**Min observed amount (mil)**, at the top of the report, hides rows whose period has less observed amount than you type. It applies to Top changes and All changes, including the spike counts and the source chips. It filters live, with no re-run, and is remembered in your browser. **clear** shows everything again. The drill-down and Explore are not filtered.

**In the report**, click a value to see:

- its weekly and monthly charts, with the baseline and ±2σ lines;
- **the same sample split by every other segment column**: one line per sub-segment, and a table ranking which one drives the change. Small sub-segments are hidden.
- **the top clients**: share of defaults and of observations, the ratio without the top 5, and a warning when the top 5 hold at least 40% of defaults (`conc_th`).

**The Explore tab** lets you pick any sample, one value or All per column. It shows fpd30 by loans and by clients as lines, and observed amount as grey bars in a panel below on the same time axis. It also has tiles, a data table and a CSV download. Distinct clients are exact for every filter, because the cube is computed with GROUPING SETS and not added up from smaller groups. So it's **one value or All per column**, not several values at once.

**Keeping the file small:** each column keeps its top `explore_max_val` (10) values, and the rest go into `(other)`. Combinations with fewer than `explore_min_loans` (200) loans are dropped in Spark. If the cube is still over `explore_max_rows` (400,000) rows, the minimum doubles until it fits. To keep more detail, filter on fewer columns: `explore_cols=['a', 'b']`.

## Options (both monitors)

| Option | Default | Meaning |
|---|---|---|
| `dry_run` | `False` | Print the queries or plan and run nothing. |
| `stop_on_error` | `True` | Stop at the first failed step and print where. `False` skips it and lists it at the top of the report (in the risk monitor, a failed main aggregation always stops). |
| `spark`, `replace_sql_var`, `date_fm` | from `sachGiaoKhoa` | Pass fakes or a local Spark for testing. |
| `now` | now, in `tz` | "Today" for the incomplete-period cut. |
| `grans`, `n_recent`, `z_th`, `min_base`, `lookback`, `title`, … | see `DEFAULTS` in each file | Any other key of `DEFAULTS`. An unknown key is an error. |

Risk monitor only: `min_obs`, `min_amt`, `min_mature`, `drill`, `drill_top_k`, `drill_min_elig`, `drill_min_recent`, `top_clients`, `conc_th`, `mask_client` (shows client ids as a short hash), `explore`, `explore_cols`, `explore_max_val`, `explore_min_loans`, `explore_max_rows`.

Both return a dict: `ok`, `out_path`, `res` (the scored table) and `failed`, plus `failed_step` and `error` when the run stopped.

## Example output

```
change_monitor: 1 source(s), 6 queries, granularities daily, weekly, monthly

[1/3] pull counts
    1/6 bnpl_txn/daily/transaction_source: 752 rows (0.0s)
    ...
[2/3] score
  155 rows scored: 47 spikes, 10 drops

[3/3] write report
  written: /dbfs/FileStore/change_monitor.html (0.2 MB)
```

When a query fails:

```
##########################################################################################
FAILED at query 2/6
  source     : bnpl_txn
  granularity: daily
  column     : transaction_status
  engine     : Spark SQL
  error      : [UNRESOLVED_COLUMN] A column `transaction_status` cannot be resolved.; line 4 pos 22
  query near line 4 (after sql_var replacement):
         3 |  , 'transaction_status'                                as column_name
    >    4 |  , date_trunc('day', from_utc_timestamp(cast(dtime_create_transaction as timestamp), ...
         5 |  , coalesce(cast(transaction_status as string), '(null)')   as column_value
  not run: bnpl_txn/weekly/transaction_source, bnpl_txn/weekly/transaction_status, ...
##########################################################################################
```

## send_report_email.py

```python
from send_report_email import send_report
send_report(r, to=['team@company.com'], cc=['lead@company.com'], smtp_host='smtp.company.com',
            smtp_user='reports@company.com', dry_run=True)    # build it, save a body preview, send nothing
send_report(r, to=['team@company.com'], smtp_host='smtp.company.com', smtp_user='reports@company.com',
            smtp_password=dbutils.secrets.get('scope', 'smtp'))    # or set the SMTP_PASS environment variable
```

- **The body** is a plain table of the top 15 spikes. It has no scripts, so it shows properly in Outlook and Gmail.
- **The report** is attached zipped (`attach_as='zip'`, the default), because mail gateways often remove `.html` attachments. It is read as bytes, so nothing gets garbled. Recipients download it, unzip it and open the file in Chrome or Edge. The email preview can't run the charts.
- **Port 587** uses STARTTLS and **port 465** uses SSL. A failure prints the step (connect, starttls, login or send) and the server. Refused recipients are listed.
- Passwords never go in code.

## Tests

```bash
python python/monitoring/tests/test_change_monitor.py       # 36 checks, fake Spark
python python/monitoring/tests/test_risk_ratio_monitor.py   # 46 checks; Part B needs pyspark + java
python python/monitoring/tests/test_send_report_email.py    # 20 checks, fake SMTP
```

They also pass on Python 3.9, pandas 1.5 and PySpark 3.5.

- **change_monitor** is tested with a fake Spark: planted spikes, drops and NEW values, the incomplete-period cut, a stop with the query line, `stop_on_error=False`, bad input and settings.
- **risk_ratio_monitor:**
  - **Part A** runs the scoring, drill-down, top clients and Explore on aggregates rebuilt in plain pandas.
  - **Part B** runs the full pipeline on a local Spark and checks it gives **identical** counts, distinct clients, flags, drill-downs, top clients and Explore cells.
  - It also checks a stop in the data SQL (with the query line), a skipped step, and the Explore size cap.
- **send_report_email** is tested with a fake SMTP: the zip round-trip, the html attachment, SSL vs STARTTLS, the password from the environment, a dry run, login and STARTTLS failures, and refused recipients.
- With Playwright installed, the reports are also opened in Chromium at 1280px and 360px. There must be no JS errors and no sideways scrolling, the click-through and Explore links and the amount filter must work, and on a phone the charts must open on the newest periods.
