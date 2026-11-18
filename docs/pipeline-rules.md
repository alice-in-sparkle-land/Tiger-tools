# How the datamart pipeline runs

The shared facts every tool in this repo has to match. They come from `raya.py` and `run_sql_folder()` in `sachGiaoKhoa.py`.

## Folders

- **One parent folder, `master_path`,** holds a folder per area, for example `all/`, `bnpl/` and `card/`.
- **Each folder** has its own `.sql` files and its own `output.py`. It can also have `debug.py`, `standalone.py`, `refresh_list.py` and `supported_text*.txt`.
- **`all/` runs first.** After that, `bnpl/` or `card/` can be run separately.

## The executor

`raya.py` (or `raya_<something>.py`) builds a `sql_var` list and calls:

```python
run_sql_folder(master_path, mode, timeframe=5, trunc_month=True, sql_var=sql_var)
```

It can call `run_sql_folder` more than once in the same session, for example a production run and then a `standalone` run for the Suspicious tables. Temporary views from the first call are still there in the second.

## Which files run, and in what order

- Only `*.sql` files **directly** in the folder run. Subfolders, `.txt` files and `.SQL` files are skipped.
- **Order** is Python `sorted()` of the full file name. The number prefix usually decides, but with prefixes of different lengths `202501011200_…` runs **before** `20250101_…`, because digits sort before `_`.

## File names

The name is `<order>_<source>_<table_name>.sql`, split on `_`:

- **source** is `parts[1]`. Only exactly `impala` runs on Spark; **anything else, typos included, is sent to Oracle**.
- **table_name** is the rest, with `.sql` removed.

## Running one script

1. **Replace text.** Every `sql_var` item `{"from": ..., "to": ...}` is applied in order as plain `str.replace`, over the whole file, comments included.
2. **Run it.**
   - `impala` scripts go through `spark.sql()`, which is Spark SQL and allows **one statement** only.
   - `oracle` scripts go through JDBC as `(your query)`. A `;` breaks them, and so does a trailing `--` comment with no newline after it.
3. **Store the result.**
   - **Not in `output.py`:** the result becomes a temporary view named `table_name`. It lives only for the session.
   - **In `output.py`:** the result is written to `af_mon.<table_name>`, partitioned by `C`. **No view is created**, so later scripts must read it as `af_mon.<table_name>`.
   - **The 6 pm rule:** while the text `run_after_6pm` is still in the query after replacement, the output write is skipped. `before_6_pm_2_1()` returns `2=1/*run_after_6pm*/` before 6 pm and `1=1` after.
4. **Errors are only logged.** They go to `run_log.txt` and the run continues, so the scripts that depend on a failed one fail quietly too.

## Config files

Each one is `exec()`ed.

| File | Must define | Notes |
|---|---|---|
| `output.py` | `output = [{"T": table, "C": partition_col}, ...]` | A trailing `# full load` comment is only a note. An empty list crashes every script. |
| `debug.py` | `debug = {"exact_table": [...], "exclude_table": [...]}` | Used when `mode = 'debug'`. |
| `standalone.py` | `standalone = {...}`, same shape | Optional, and read inside try/except, so an error in it is silent. |
| `refresh_list.py` | `refresh_list = [...]` | Each table is refreshed with `refresh table x` and `refresh x`. Errors are ignored. |

## Modes

- `debug` runs `exact_table` minus `exclude_table` from `debug.py`.
- `standalone` does the same with `standalone.py`.
- **Any other value runs everything.**

## Table types (for documentation)

- **Output tables** are the `%fact%` and `%agg%` tables.
- **Transformation tables** are everything else.

## The incremental pattern

Large tables are built in four steps: `<base>_daily`, then `<base>_backup`, then `<base>_union`, then `<base>`.

| Step | What it does |
|---|---|
| daily | Takes the latest partitions (`part_date >= sysdate - 3`). |
| backup | Takes the whole `af_mon.<base>` from the **previous run**. |
| union | Combines backup and daily. |
| final | `select * from` the union. |
