# Tiger Tools

Work tools for the BNPL / risk datamart. These are SQL pipelines run by `raya.py` → `run_sql_folder()` on Spark, Impala and Oracle.

| Tool | Kind | Where it runs | What it's for |
|---|---|---|---|
| [`python/sql_pipeline/rerun_table.py`](python/sql_pipeline/) | Python function | Server, next to `sachGiaoKhoa.py` | Re-run one table and everything upstream of it, stopping at the first failure with the exact step and line |
| [`html/sql-run-checker/`](html/sql-run-checker/) | Standalone HTML | Browser (computer or phone), offline | Check a pipeline folder for technical mistakes before running it |
| [`html/sql-doc-builder/`](html/sql-doc-builder/) | Standalone HTML | Browser, offline | Draft documentation, data flow, column lineage and a shareable read-only copy |

## Layout

```
docs/                     shared knowledge (how the pipeline really runs)
python/<topic>/           functions that run on the server, each with README + tests/
python/common/            helpers shared by several Python tools
html/<tool>/              one standalone .html per tool (+ src/ and build.py when it is built)
```

Start with [`docs/pipeline-rules.md`](docs/pipeline-rules.md). Every tool has to agree with it.
