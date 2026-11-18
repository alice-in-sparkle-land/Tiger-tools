# sql_pipeline

Python helpers that run on the server, next to `sachGiaoKhoa.py`.

Copy [`../common/sql_errors.py`](../common/) next to them too: `rerun_table.py` imports it to show the failing query lines.

## rerun_table.py

Re-runs one table and everything upstream of it, for debugging.

```python
from rerun_table import rerun_table
master_path = '/home/.../sql_script/version2/'      # parent of all/, bnpl/, card/

rerun_table('rel_af_agg_trigger', master_path, sql_var, dry_run=True)   # look at the plan first
rerun_table('rel_af_agg_trigger', master_path, sql_var)                 # then run it
```

**How it works**

1. **Find the script.** It scans `master_path` and its subfolders for `<order>_<source>_<table>.sql`. `archive/` and hidden folders are skipped.
2. **Crawl back.** Starting from the target script, after applying `sql_var`, it reads the `FROM`/`JOIN` tables:
   - **`af_mon.X`** is reused if it already exists. If it doesn't, X's script is run.
   - **A name without a schema** that some script builds is always rebuilt, because temporary views don't survive between sessions.
   - **Anything else** (owner_dwh, lap_analytical, …) is a source and is left alone.
3. **Order the steps.** Dependencies run first. Among steps that are ready, **`all/` runs first**, then the other folders. Inside a folder, the file-name order is kept.
4. **Run each step like `run_sql_folder`.** It applies `sql_var`, sends impala scripts to `spark.sql` and others to Oracle, and uses **the script's own folder's** `output.py` to decide between a temporary view and `af_mon.<T>` partitioned by `C`.
5. **Print everything.** It prints the dependency tree, notes, the plan, and each step with its time. It **stops at the first failure** and shows:
   - the step number and the file;
   - the engine and the error;
   - the query lines around Spark's `line N pos M`, after replacement;
   - the steps that didn't run.

**Options**

| Option | Default | Meaning |
|---|---|---|
| `folder` | `None` | Which folder's script to use for the target, when the name exists in several folders. |
| `dry_run` | `False` | Print the tree and the plan only. |
| `rebuild_af_mon` | `False` | Also rebuild upstream `af_mon` tables that already exist. |
| `check_views` | `False` | Fetch one row after each view, so a broken view fails at its own step. Views are lazy, so without this the error shows up at a later step. |
| `spark`, `oracle_fn` | from `sachGiaoKhoa` | Pass fakes for testing. |

**Good to know**

- **The same table name in two folders:** it uses the target's own folder first, then `all/`, and prints which one it used.
- **A `_backup` script** reads its own final table from the previous run. That counts as reuse, and you get a warning if the table doesn't exist yet.
- **Before 6 pm,** an output table whose query still contains `run_after_6pm` is not written, as in the normal run. It is printed as SKIPPED.

**Tests**

```bash
python python/sql_pipeline/tests/test_rerun_table.py
```

34 checks with a fake Spark. They cover:

- the crawl, the `all/`-first order, and reusing existing tables;
- stopping on a failure, with the line shown;
- the 6 pm skip, `check_views`, and the folder preference;
- an unknown table or folder, and circular dependencies.
