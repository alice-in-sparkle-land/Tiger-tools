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

For every seller × anchor × **metric** × **comparison** × **count unit** × **test**, it builds a 2×2 table and runs a one-sided Fisher exact test (only increases count). The **money surge** metric uses a rank test instead (see below).

| Metric | Self test (recent vs base window) | Peer test (seller vs its peers, same recent window) |
|---|---|---|
| **Flag share** (any SQL condition, e.g. `utilization > 0.9`) | flagged / not flagged, recent vs base | seller flagged / not flagged vs peers flagged / not flagged |
| **Amount flag** (e.g. `txn_amount >= 5000000`) | same as flag share | same as flag share |
| **Amount range** (e.g. `txn_amount BETWEEN 9000000 AND 9999999`, many payments just under a limit) | same as flag share | same as flag share |
| **Relative amount** (`rel_amount`, e.g. above the 90th percentile of the peer group's amounts) | same as flag share | same as flag share |
| **Volume** | seller vs all other sellers, recent vs base | seller vs its peers, recent vs base |
| **Money surge** (`money`) | daily total amount, recent days vs base days (Mann-Whitney) | none |

**Amount: why not Fisher on money.** Fisher counts independent items, and money isn't made of independent pieces: one 50M payment is one event. Put amounts in a 2×2 table and the answer changes with the unit (đồng or millions). So amount is used in three valid ways:

- **Amount flag / range:** each transaction is flagged or not, then counted.
- **Relative amount:** a transaction is flagged when its amount is above the chosen percentile of its peer group's amounts. That's one metric per peer group, named `<name>_<group columns>` (e.g. `big_rel_seller_group`), or `<name>_all` with no peer groups. The percentiles use **only data before the first anchor day**, so the backtest never uses future amounts. It needs `amount_col`, and it adapts to each segment's normal ticket size.
- **Money surge:** a one-sided Mann-Whitney rank test on the seller's **daily total amount**, recent days vs base days.
  - Days without sales count as 0, so a new seller shows as a surge.
  - Each day is one observation, so units don't matter and one huge payment can't dominate.
  - The p-value is exact when there are no ties and at most 40 days in all; otherwise it uses the normal approximation with tie and continuity correction (as `scipy.stats.mannwhitneyu`).
  - It's self only, and skipped for comparisons whose recent window is under 3 days (too few days to ever be significant).
  - Gate: at least `min_total` transactions in the recent window.
  - It fires on q alone (there's no odds ratio), and its rows have `unit = amount`, `rule_type = amount`.

- **Count unit.** `customer` counts each person once per window (a customer is flagged if they had a flagged transaction there). `txn` counts transactions. Repeated transfers from one customer are not independent, so `customer` is the safer test. `txn` also catches a few customers paying many times. Run both and compare them in the summary.
- **Peers.** `peer_groups` lists any number of groups, each its own test. The default is `[['seller_group'], ['h3_9'], ['seller_group', 'h3_9']]`, which gives the tests `peer:seller_group`, `peer:h3_9` and `peer:seller_group+h3_9`, so you can see whether a seller stands out in its business type, its area, or both.
  - Peers are the other sellers with the same values in that group's columns. The seller itself is always left out.
  - Counting by customers, the seller's own customers are also left out of the peer row, so the two rows never share a person.
  - A peer test runs only with at least `min_peers` other sellers. `peer_sellers` says how many there were, and `peer_group` holds the group's values.
  - Peer columns should describe the seller (one value per seller).
  - Older settings with `peer_cols` still run as one combined group. `peer_fallback_cols` was replaced by `peer_groups`.
- **Gate.** A test runs only when the seller has at least `min_total` in the recent window and at least `min_flag` flagged (for volume, its own count). Peers also need `min_total`.
- **Fires** when the Benjamini–Hochberg q-value is ≤ `q_max` **and** the odds ratio (0.5 added to every cell) is ≥ `or_min`. The odds ratio gate stops very large sellers firing on tiny changes that still give a tiny p.
- **BH scope** (`bh_scope`). `rule` (default): over all sellers tested that day for that rule, so each rule keeps its own 5% false-discovery rate (right when you pick one rule to go live). `day`: over every rule and seller that day, so the rate holds across all rules together (right when several rules go live at once). Rows leaning down (a·d ≤ b·c) are not brought into pandas, but they still count in the family size. Their p-values are large, so q is exact wherever it matters and can only be slightly higher elsewhere, never lower (tested against BH over every row).
- **Alert** is a fired day at least `cooldown` days after the same rule last alerted the same seller.

**Volume rules answer a different question** from flag rules: whether the seller's *share* of transactions (or customers) rose, which cancels platform growth and seasonality, not whether a fraud signal rose. The summary marks them `rule_type = volume`; compare them with each other rather than with flag rules.

Recent and base windows of the same seller share customers, so the self test is not strictly independent. Judge the rules by the backtest numbers, not by p alone. With short windows (e.g. 1 day) few sellers pass the gate on a given day, so BH has little to correct. The summary shows whether such rules are noisy.

## Output

`detail` (pandas) has one row per tested seller × day × rule that leans upward:

`anchor, recent_start, recent_end, ref_start, ref_end, merchant, peer_group, metric, comparison, unit, test, a, b, c, d, share_recent, share_ref, odds_ratio, or_lo, or_hi, p, q, n_tests` (the BH family size), `peer_sellers, fired, alert`.

- **Mapping back to `df`.** `recent_start` and `recent_end` are the days behind `a` and `b`; `ref_start` and `ref_end` are the days behind `c` and `d`.
  - For self and volume tests, the ref window is the base window.
  - For flag peer tests it equals the recent window (the peers over the same days).
  - Filter `df` on `merchant` and these dates to see the same transactions. For peer rows, also filter on the peer group's columns.
- **Money surge rows:**
  - `a` / `b`: total amount in the recent / base days;
  - `c` / `d`: number of recent / base days;
  - `share_recent` / `share_ref`: daily averages;
  - `odds_ratio` is empty.

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

## Outcomes by q band (extra cell)

`extras/fisher_q_bands_cell.py` shows whether smaller q-values go with more suspicious and defaulted transactions. Run it **after** the backtest, in the same notebook: it uses `df`, `spark`, `CONFIG` and `detail`, and gives `q_bands`.

- **Bands.** For each rule, each transaction in the anchor range gets the **smallest q** among the tested seller-days whose recent window covers it, so it counts once per rule.
  - The bands are `q 0-0.001`, `q 0.001-0.01`, `q 0.01-0.05`, `q 0.05-0.2` and `q 0.2-1.0`. Change the cut-offs in `BANDS` at the top of the file.
  - `not in detail` holds the rest: sellers that were never tested (below `min_total` / `min_flag`) or whose share went down (q close to 1).
- **Columns.** `seller_days`, `fired_days` and `alert_days` per band, then the same eight counts and four rates as `outcomes`. A client counts in every band where they have a transaction.

```python
q_bands[(q_bands.metric == 'high_util') & (q_bands.comparison == '7 vs 8-15')
        & (q_bands.unit == 'customer') & (q_bands.test == 'self')]
```

**How to read it** (one rule, top band to bottom):
- **Rates rise as q falls:** the test separates bad sellers from good. If fired rates still look low, look at the cut-off (`q_max`, `or_min`) or the gates.
- **Rates are flat across bands:** the metric doesn't separate bad sellers from good, whatever the q. Try another metric or the peer test.
- **`not in detail` has a high suspicious or default rate:** the bad sellers are never tested. Lower `min_total` / `min_flag`, or look at the small sellers separately.

## Size

The heavy work runs in Spark, in one pass per count unit. Counting by customers repeats each (seller, customer, day) row once per anchor its windows cover, so a 90-day base window means about 90 copies. Counting by transactions is much cheaper. Only candidate rows come into pandas; above `max_rows` it stops and says what to change (raise `min_total`/`min_flag`, shorten the anchor range, or raise `max_rows`).

## Build and test

```bash
python html/fisher/build.py                 # src/ -> fisher_backtest_builder.html
python html/fisher/tests/test_engine.py     # Fisher vs scipy; full run vs a brute-force rebuild on local Spark
python html/fisher/tests/test_page.py       # Playwright at 1280px and 360px, then runs a downloaded .py on Spark
python html/fisher/tests/test_q_bands.py    # the q-band cell vs a transaction-by-transaction rebuild
```

| File | What it is |
|---|---|
| `src/engine.py` | The backtest engine, embedded unchanged in every generated file |
| `src/ui.js`, `src/ui.css`, `src/shell.html` | The page |
| `src/theme.js` | Ghibli misty-hills theme (copy of the SQL Run Checker's) |
| `extras/fisher_q_bands_cell.py` | Notebook cell: outcomes by q band, run after the backtest |

Edit `src/`, then rebuild. Don't edit `fisher_backtest_builder.html` by hand.
