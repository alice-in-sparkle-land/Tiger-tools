# SQL Run Checker

Open `sql_run_checker.html` in Chrome, on a computer or a phone. Drop in a pipeline folder, or a `.zip` of it, after every change. It lists the **technical** mistakes, meaning anything that would stop a script from running or make a step silently do nothing. It never judges business logic.

It reads `raya*.py`, the config files and every `.sql` the way `run_sql_folder()` does (see [`docs/pipeline-rules.md`](../../docs/pipeline-rules.md)):

- **Python:** syntax errors, such as commas, brackets, quotes and indentation, reported the way Python reports them. It also warns about two strings joined in a list and duplicate dict keys.
- **Configs:** the shape of `output.py`, `debug.py`, `standalone.py` and `refresh_list.py`, and names that match no script.
- **Executor:** the `sql_var` replacements, flagging ones that never match, duplicates, replacements that also hit longer words, and placeholders left behind. It also checks the `mode` value and the folder path.
- **SQL:**
  - dangling `AND`/`OR`/commas, unclosed brackets or quotes, and `;`;
  - `JOIN` without `ON`, mistakes in CTEs and `UNION`s, and missing commas between columns;
  - Oracle syntax in Spark scripts and the reverse.
- **Between scripts:**
  - views read before they are built;
  - output tables read without `af_mon.`;
  - Oracle scripts reading Spark views;
  - partition columns missing from the query.

It works offline, and nothing is uploaded. Your ignored warnings are saved in the browser, with Export and Import settings.

## Build

The page is built from `src/`:

```bash
python html/sql-run-checker/build.py
```

| File | What it is |
|---|---|
| `src/pyparse.js` | Python tokenizer and parser (syntax errors, reading config values) |
| `src/sqlcheck.js` | SQL tokenizer and the SQL checks |
| `src/rules.js` | The `run_sql_folder` simulation and the checks between files |
| `src/theme.js` | Ghibli misty-hills theme: earth, forest, sea or wind at random, with a lock option |
| `src/ui.js`, `src/ui.css`, `src/shell.html` | The page |

Edit `src/`, then rebuild. Don't edit `sql_run_checker.html` by hand.
