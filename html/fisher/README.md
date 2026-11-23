# Fisher Backtest Builder

Open `fisher_backtest_builder.html` in Chrome, on a computer or a phone. Declare your columns, metrics and comparisons, and it writes **one `.py` file** that backtests Fisher-test triggers for sellers receiving suspiciously more BNPL money.

The page works offline and nothing is uploaded. Your settings are saved in the browser, with Export and Import settings (Merge or Replace).

## Using the file

Paste the downloaded `.py` into a notebook cell where `spark` and `df` already exist, or run `exec(open('/path/fisher_backtest_YYYYMMDD.py').read())`. `df` is the **transaction-level** Spark DataFrame, one row per transaction; don't aggregate it first. The file ends with:

```python
r = run_fisher_backtest(df, CONFIG, spark=spark)
detail, summary, outcomes = r['detail'], r['summary'], r['outcomes']
```

It prints every step. At the first failure it prints one line, `FAILED at step i/n: <step>: <error>`, then `not run: ...`, and `r['ok']` is False.

The file imports only pandas, numpy and PySpark. The Fisher test is written inside it, so it doesn't need `scipy`. It works after `from pyspark.sql.functions import *`, which replaces Python's `sum`, `max`, `min` and `round` with Spark's: the engine keeps its own copies of the Python ones.

## What it tests

**Rolling backtest.** Every day from the first day where every window fits inside the data, up to the last day in `df`, plays "today" (the anchor). Windows count back from the anchor: day 1 is the anchor itself, so "7 vs 8–15" means the last 7 days against the 8 days before them. Each anchor uses only data up to that day.

For every seller × anchor × **metric** × **comparison** × **count unit** × **test**, it builds a 2×2 table and runs a one-sided Fisher exact test (only increases count):

| Metric | Self test (recent vs base window) | Peer test (seller vs its peers, same recent window) |
|---|---|---|
| **Flag share** (any SQL condition, e.g. `utilization > 0.9`) | flagged / not flagged, recent vs base | seller flagged / not flagged vs peers flagged / not flagged |
| **Amount flag** (e.g. `txn_amount >= 5000000`) | same as flag share | same as flag share |
| **Volume** | seller vs all other sellers, recent vs base | seller vs its peers, recent vs base |

- **Count unit.** `customer` counts each person once per window (a customer is flagged if they had a flagged transaction there). `txn` counts transactions. Repeated transfers from one customer are not independent, so `customer` is the safer test. `txn` also catches a few customers paying many times. Run both and compare them in the summary.
- **Peers** are the other sellers with the same values in the peer columns (e.g. `seller_group`, `h3_9`). The seller itself is always left out. Counting by customers, the seller's own customers are also left out of the peer row, so the two rows never share a person. A peer test runs only with at least `min_peers` other sellers. With **fallback group columns** (e.g. `seller_group` alone), a seller whose peer group is too small that day is compared with the coarser group instead; `detail.peer_level` says `group` or `fallback`, and `peer_sellers` how many other sellers were in it. Peer columns should describe the seller (one value per seller).
- **Gate.** A test runs only when the seller has at least `min_total` in the recent window and at least `min_flag` flagged (for volume, its own count). Peers also need `min_total`.
- **Fires** when the Benjamini–Hochberg q-value is ≤ `q_max` **and** the odds ratio (0.5 added to every cell) is ≥ `or_min`. The odds ratio gate stops very large sellers firing on tiny changes that still give a tiny p.
- **BH scope** (`bh_scope`). `rule` (default): over all sellers tested that day for that rule, so each rule keeps its own 5% false-discovery rate (right when you pick one rule to go live). `day`: over every rule and seller that day, so the rate holds across all rules together (right when several rules go live at once). Rows leaning down (a·d ≤ b·c) are not brought into pandas, but they still count in the family size. Their p-values are large, so q is exact wherever it matters and can only be slightly higher elsewhere, never lower (tested against BH over every row).
- **Alert** is a fired day at least `cooldown` days after the same rule last alerted the same seller.

**Volume rules answer a different question** from flag rules: whether the seller's *share* of transactions (or customers) rose, which cancels platform growth and seasonality, not whether a fraud signal rose. The summary marks them `rule_type = volume`; compare them with each other rather than with flag rules.

Recent and base windows of the same seller share customers, so the self test is not strictly independent. Judge the rules by the backtest numbers, not by p alone. With short windows (e.g. 1 day) few sellers pass the gate on a given day, so BH has little to correct. The summary shows whether such rules are noisy.

## Output

`detail` (pandas) has one row per tested seller × day × rule that leans upward: `anchor, merchant, peer_group, metric, comparison, unit, test, a, b, c, d, share_recent, share_ref, odds_ratio, or_lo, or_hi, p, q, n_tests` (the BH family size), `peer_sellers, peer_level, fired, alert`.

`summary` (pandas) has one row per rule (metric × comparison × unit × test):

| Column | Meaning |
|---|---|
| `tests`, `fired`, `alerts`, `alerts_per_day`, `sellers_alerted` | Workload the rule would create |
| `precision` | Alerted sellers that carry a suspicious label (at any date) |
| `susp_in_range`, `recall` | Sellers first flagged suspicious inside the anchor range, and the share of them the rule alerted |
| `recall_early`, `median_lead_days` | Share alerted on or before the flag date, and the median days from first alert to flag date (positive = earlier) |
| `rule_type` | `flag` (flag share or amount flag) or `volume` |
| `obs_after_alert`, `def_rate_after_alert`, `def_rate_never_alerted`, `lift_after_alert` | **Forward:** matured loans in the `lift_days` (default 30) after each alert, against loans of sellers this rule never alerted, in the same period. This is the one to judge a rule by: would acting on the alert have avoided bad loans? |
| `obs_in_window`, `def_rate_in_window`, `def_rate_outside`, `lift_in_window` | **Looking back:** matured loans inside the recent window of fired days vs all other matured loans in the anchor range. A seller that fires often covers much of its own history, so this mostly says "the alerted sellers have bad loans" |

`outcomes` (pandas) shows what each rule caught. Per rule there are four rows (`group`):

- `fired`: the seller's transactions inside the **recent window** of every fired day. For a 7 vs 8–15 rule firing on 10 Nov, that's 4–10 Nov.
- `not_fired`: every other transaction in the anchor range.
- `alert` / `not_alerted`: the same, using alerts only.

| Column | By transaction | By client (distinct) |
|---|---|---|
| Total | `txn` | `client` |
| Suspicious | `susp_txn` | `susp_client` (has a suspicious transaction in the group) |
| Observed (matured) | `obs_txn` | `obs_client` |
| Default (observed and defaulted) | `def_txn` | `def_client` |
| Rates | `susp_rate_txn` = susp_txn / txn, `def_rate_txn` = def_txn / obs_txn | `susp_rate_client`, `def_rate_client` the same by client |

- Overlapping windows are merged, so a transaction counts once per rule, and distinct clients are exact.
- A client counts in `not_fired` if they have at least one transaction outside the fired windows, so one client can be in both rows.
- Only transactions inside the anchor range count; a window starting before it is cut at its first day.
- Without labels, the suspicious or default columns are left out.

A seller is suspicious from the date of its first transaction matching the suspicious condition, or from the flagged-on date column when given. Labels are optional: without them the summary shows the workload columns only.

Cooldown changes only the alert count (workload). Precision, recall and lead time use each seller's first alert, which is always its first fired day, and the in-window lift uses fired days.

## Size

The heavy work runs in Spark, in one pass per count unit. Counting by customers repeats each (seller, customer, day) row once per anchor its windows cover, so a 90-day base window means about 90 copies. Counting by transactions is much cheaper. Only candidate rows come into pandas; above `max_rows` it stops and says what to change (raise `min_total`/`min_flag`, shorten the anchor range, or raise `max_rows`).

## Build and test

```bash
python html/fisher/build.py                 # src/ -> fisher_backtest_builder.html
python html/fisher/tests/test_engine.py     # Fisher vs scipy; full run vs a brute-force rebuild on local Spark
python html/fisher/tests/test_page.py       # Playwright at 1280px and 360px, then runs a downloaded .py on Spark
```

| File | What it is |
|---|---|
| `src/engine.py` | The backtest engine, embedded unchanged in every generated file |
| `src/ui.js`, `src/ui.css`, `src/shell.html` | The page |
| `src/theme.js` | Ghibli misty-hills theme (copy of the SQL Run Checker's) |

Edit `src/`, then rebuild. Don't edit `fisher_backtest_builder.html` by hand.
