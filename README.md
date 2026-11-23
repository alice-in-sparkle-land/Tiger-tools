# Tiger Tools

Work tools for the BNPL / risk datamart. These are SQL pipelines run by `raya.py` → `run_sql_folder()` on Spark, Impala and Oracle.

| Tool | Kind | Where it runs | What it's for |
|---|---|---|---|
| [`python/sql_pipeline/rerun_table.py`](python/sql_pipeline/) | Python function | Server, next to `sachGiaoKhoa.py` | Re-run one table and everything upstream of it, stopping at the first failure with the exact step and line |
| [`python/monitoring/change_monitor.py`](python/monitoring/) | Python function | Server, next to `sachGiaoKhoa.py` | Flag recent spikes and drops in category counts and shares (daily, weekly, monthly) as one offline HTML report |
| [`python/monitoring/risk_ratio_monitor.py`](python/monitoring/) | Python function | Server, next to `sachGiaoKhoa.py` | Flag recent changes in a default ratio (e.g. fpd30) by segment, by loans and by clients, with drill-down, top clients and an Explore tab |
| [`python/monitoring/send_report_email.py`](python/monitoring/) | Python function | Server | Email a monitor report: summary table in the body, the report zipped as an attachment |
| [`html/sql-run-checker/`](html/sql-run-checker/) | Standalone HTML | Browser (computer or phone), offline | Check a pipeline folder for technical mistakes before running it |
| [`html/sql-doc-builder/`](html/sql-doc-builder/) | Standalone HTML | Browser, offline | Draft documentation, data flow, column lineage and a shareable read-only copy |
| [`html/fisher/`](html/fisher/) | Standalone HTML → generated `.py` | Browser, then the server notebook (`spark`, `df`) | Declare columns, metrics and comparisons; get one `.py` that backtests Fisher-test triggers for sellers receiving suspiciously more BNPL money (rolling, self and peer tests, precision, recall, lead time, default lift) |

## Layout

```
docs/                     shared knowledge (how the pipeline really runs)
python/<topic>/           functions that run on the server, each with README + tests/
python/common/            helpers shared by several Python tools
html/<tool>/              one standalone .html per tool (+ src/ and build.py when it is built)
```

Start with [`docs/pipeline-rules.md`](docs/pipeline-rules.md). Every tool has to agree with it.
