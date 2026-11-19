# common

Shared Python helpers used by more than one tool, such as SQL table parsing and progress printing. It's empty for now.

When a second tool needs code that already lives in another tool (for example `tables_used()` in `sql_pipeline/rerun_table.py`), move that code here and import it from both.
