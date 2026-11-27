# Fisher trigger backtest: the method

How the trigger logic works and why each choice was made, for the team and reviewers. For how to run the tool and every option, see [README.md](README.md). The shareable version of this document, with two diagrams (the sliding windows, and the path from test to alert), is at https://claude.ai/artifact/EP8giJh2FjnHAsAY8fFyCf. Written 27 Nov 2026.

## Purpose

The method flags sellers whose BNPL traffic suddenly turns suspicious: more risky customers, bigger tickets, or more money than their own past or their peers would explain. Sellers say they sell legal products and services; the trigger looks for statistical evidence that their recent pattern is unusual.

It is built as a **backtest**. Every past day is replayed as if it were today, so we can measure how a trigger rule would have performed before it goes live: how many alerts it raises, how many of the alerted sellers were later found suspicious, and whether their loans default more.

The tool has two parts:

- **Fisher Backtest Builder** (`fisher_backtest_builder.html` in this folder): a page where you declare columns, metrics and comparisons; it writes one Python file.
- **The generated file**: runs on the notebook's `spark` and transaction-level `df`, and returns `detail` (every test), `summary` (one row per rule), `outcomes` (what each rule caught) and `z_bands` (the Ratio Z spectrum).

## Rolling backtest

Every day in the data plays "today" once, and each of those **anchor days** is tested only with data up to that day. Nothing from the future leaks into a decision.

- **Input:** `df`, one row per transaction (date, seller, customer, amount, flags, labels). It is never aggregated beforehand; the file does all counting in Spark.
- **Windows** are counted back from the anchor, with day 1 = the anchor itself. A comparison such as **7 vs 8–15** puts the last 7 days (the *recent* window) against the 8 days before them (the *base* window).
- **Every count is a window total**: transactions summed over all days in the window, or distinct customers over the whole window.
- **Anchor range:** from the first day on which every window fits inside the data, to the last day in `df` (or dates you set).

**Example**, rule 7 vs 8–15 on anchor **10 Nov**:
- recent = 4–10 Nov, base = 27 Oct–3 Nov;
- on 11 Nov both windows move one day later;
- so one transaction sits in the recent window of seven consecutive anchors, which is why a real change usually fires several days in a row.

The default comparisons are 1 vs 2–8, 7 vs 8–15, 7 vs 8–35 and 30 vs 31–90; any window up to 400 days can be added.

## The 2×2 table and Fisher's exact test

Each test asks one question: **is the flagged share now higher than in the comparison group, by more than luck explains?**

| | Flagged | Not flagged |
| --- | --- | --- |
| Recent window (the seller now) | a | b |
| Comparison (seller's base window, or its peers) | c | d |

Fisher's exact test fixes the row and column totals and computes the exact chance of seeing `a` this high or higher if both rows really had the same share: the **p-value**. It is **one-sided**: only increases count.

p = Σ over x ≥ a of C(K, x) · C(N − K, n − x) / C(N, n), with N = a + b + c + d, K = a + c, n = a + b.

**Why Fisher and not a normal (z) test.** A z-test assumes counts large enough for the bell curve. Most sellers are small, where the normal approximation is poor. Fisher computes the exact probability for any size, so small and large sellers are judged on the same footing. The file computes it without scipy and matches scipy to within 1e-9.

**Odds ratio** = (a × d) / (b × c), with 0.5 added to each cell. It says how much higher the odds are now (2 = double). The p-value says whether the change is real; the odds ratio whether it is big enough to matter.

**Example** (high-utilisation customers, 7 vs 8–15): a = 12, b = 38, c = 5, d = 95.
- The share rose from 5% to 24%.
- The odds ratio is about 5.6.
- p ≈ 0.00099.

## Self test and peer tests

| Test | Row c, d (the comparison) | Catches |
| --- | --- | --- |
| `self` | the same seller in the base window | a seller that changed |
| `peer:seller_group` | other sellers of the same business type, same recent window | a seller unusual for its kind of business |
| `peer:h3_9` | other sellers in the same H3 cell (any type), same recent window | a seller unusual for its area |
| `peer:seller_group+h3_9` | other sellers of the same type in the same cell | the strictest comparison |

- **The seller is always left out of its own peers.**
- **Counting by customers, the rows are disjoint:** the peer row only holds customers who did *not* buy from this seller in that window.
- **Minimum peers:** a peer test runs only with at least `min_peers` other sellers (3 by default).
- **Peer groups** are a list in the form; each becomes its own test.

## Metrics

| Metric type | Flagged means | Example |
| --- | --- | --- |
| Flag share | a transaction matches any SQL condition | `utilization > 0.9` |
| Amount flag | amount above or below a fixed value | `txn_amount >= 5000000` |
| Amount range | amount between two values | `BETWEEN 9000000 AND 9999999` (just under a 10M limit) |
| Relative amount | amount above a percentile of the peer group's amounts | above the 90th percentile of the seller's group |
| Volume | (no flag) the seller's share of all transactions, or of its peer group | a seller suddenly taking more of the market |
| Money surge | (not Fisher) daily total amount, recent vs base | next section |

- **Relative amount** gives one metric per peer group, e.g. `big_rel_seller_group`. Its percentiles use **only data before the first anchor day**.
- **Volume** compares the seller's share, which cancels platform growth and seasonality. It is reported as its own `rule_type`.

## Amount and the money surge test

**Money can't go straight into Fisher's table.** Fisher counts independent items, and one 50M payment is one event, not 50 million.
- Example: a seller's big-ticket money went from 50M of 100M to 60M of 100M.
- Counted in đồng, p comes out around 10⁻⁴⁰⁰.
- Counted in millions (60, 40, 50, 50), p ≈ 0.1.
- The answer depends on the unit, so the test is being misused.

So amount enters in three valid ways: as a **flag** that is then counted (amount flag, amount range, relative amount), or through the **money surge** test.

**Money surge** is a one-sided Mann–Whitney rank test on the seller's **daily** total amount, recent days vs base days:
- Days without sales count as 0.
- Ranks ignore the unit, and one huge payment can't dominate.
- **p-value:** exact when there are no ties and at most 40 days; otherwise the large-sample approximation with tie correction (as `scipy.stats.mannwhitneyu`).
- **Scope:** self only, and skipped for recent windows under 3 days.
- **Firing:** on q alone; its rows show `unit = amount` and `rule_type = amount`.
- **In `detail`:** `a` / `b` = recent / base totals, `c` / `d` = number of days.

A brand-new seller has an all-zero base, so it shows as a surge, by design.

## Counting unit: customers or transactions

Example: 60 transactions, 50 of them from high-utilisation customers, but those 50 came from just 2 people.

| Count by | Flagged | Total | Share |
| --- | --- | --- | --- |
| Transactions | 50 | 60 | 83% |
| Distinct customers | 2 | 12 | 17% |

- **Customers** is the safer test: 50 payments from 2 people aren't 50 independent pieces of evidence.
- **Transactions** catches a few customers paying many times, but its p-values look stronger than the evidence is.
- Customer counting is the heavy part in Spark.

## Testing many sellers at once: gate, q-value, fire, alert

1. **Gate:** at least `min_total` (10) in the recent window and `min_flag` (5) flagged. Peers also need `min_total`.
2. **q-value (Benjamini–Hochberg):** per rule and day, q ≈ p × number of tests ÷ rank. Example: q ≈ 0.00099 × 40 ÷ 1 ≈ 0.04.
3. **Fire:** q ≤ 0.05 **and** odds ratio ≥ 2. Money surge fires on q alone.
4. **Alert:** a fired day with no alert from the same rule on the same seller in the last 7 days.

**BH scope:** `rule` (default) keeps a 5% false-discovery rate per rule, which is right when choosing one rule. `day` keeps it across every rule that day, which is right when several rules go live together.

## Judging a rule

| Output | One row per | Use it to |
| --- | --- | --- |
| `summary` | rule (metric × comparison × unit × test) | rank rules |
| `outcomes` | rule × group (fired, not fired, alert, not alerted) | compare rates of what a rule caught against the rest |
| `detail` | seller × anchor × rule tested | single cases, traced back to transactions |
| `q_bands` (extra cell) | rule × q band | check that rates rise as q falls |

- **`summary`:**
  - workload: `alerts`, `alerts_per_day`, `sellers_alerted`;
  - precision; recall, `recall_early` and `median_lead_days`;
  - `lift_after_alert`: matured loans in the 30 days after each alert vs sellers never alerted. This is the main default measure;
  - `lift_in_window`, which looks back.
- **`outcomes`:** `txn`, `client`, `susp_txn`, `susp_client`, `obs_txn`, `obs_client`, `def_txn`, `def_client`, and the four rates. Each transaction counts once per rule.
- **`detail`:**
  - `recent_start`–`recent_end` and `ref_start`–`ref_end` are the days behind a, b and c, d;
  - `txn_cnt`, `susp_cnt`, `def_cnt`, `obs_cnt` and `loan_amt` are the seller's own totals in the recent window.
- **`q_bands`** (`extras/fisher_q_bands_cell.py`):
  - rates rising as q falls: the test works;
  - rates flat across bands: the metric doesn't separate sellers;
  - a high rate in `not in detail`: bad sellers are never tested.

## Ratio Z: the alternative approach, run for comparison

The team's note proposed a different rule: compare a seller's money in the monitoring period (MP) with its own past (RP) and with the average seller in its segment, through a bounded ratio and Fisher's z-transformation. The backtest runs it next to the tests so both can be judged on the same data.

- **The ratio:** r = (MP − ref) / (MP + ref) on **average daily** amounts, clipped to ±0.999, then z = 0.5 ln((1 + r)/(1 − r)).
- **What z really is:** z = 0.5 ln(MP ÷ ref), half the log of the ratio. 0.35 ≈ 2×, 0.55 ≈ 3×, 1 ≈ 7×, 1.96 ≈ 50×.
- **Not a test on its own:** this is Fisher's *z-transformation* (made for correlation coefficients), not Fisher's *exact test*. On its own z is a size measure with no p-value. The step that would give it meaning is comparing it with other sellers, so the backtest also computes a **standardised z** = (z − median) / (1.4826 × MAD) across the sellers tested that day, per peer group.
- **Rules:** `self` (vs own base window), `peer:<group>` (vs the average of the other active sellers in the group, same days), and `and:<group>` / `or:<group>` (the smaller / larger of the two z).
- **No cut-off yet:** instead of firing alerts, the backtest reports a **spectrum** in `z_bands`: for each rule and band of raw or standardised z, the transactions, clients, suspicious, defaults and amount they cover, and the rates.

**How to compare it with the tests:** for the same comparison, read down `z_bands` and down `q_bands`. The approach whose top bands carry clearly higher suspicious and default rates, at a workload the team can handle, separates bad sellers better. Its main weakness is that it ignores how much data a seller has: 1M → 3M and 1B → 3B get the same z.

## Design choices and limitations

**Choices a reviewer may question:**
- **Rows leaning down aren't kept,** but they still count in the BH family size. So q is exact where it matters, never lower; this was checked against full BH.
- **Disjoint peers make a ring seller stand out more, not less.**
- **Cooldown doesn't hide detections.** Precision, recall and lead time use the first alert, which is always the first fired day.
- **Volume and flag rules can both fire** on the same seller: two questions, marked apart.

**Known limitations:**
- **The self test catches changes, not levels.** The peer tests cover levels.
- **1-day windows are noisy,** because BH has little to correct.
- **Overlapping windows:** p-values are a ranking tool; judge rules by the backtest results.
- **Big sellers dominate pooled rates,** so compare rates by client and per seller as well.
- **Peer columns should describe the seller;** a seller spread over several H3 cells is counted once in each.
- **Run time grows** with peer groups and customer counting.
