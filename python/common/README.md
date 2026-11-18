# common

Shared Python helpers used by more than one tool.

When a second tool needs code that already lives in another tool, move that code here and import it from both.

| File | What it has | Used by |
|---|---|---|
| `sql_errors.py` | `query_context(sql, msg)`: the query lines around Spark's `line N pos M`. `failure_block(...)`: the full `####` block a tool prints when it stops. | `sql_pipeline/rerun_table.py`, `monitoring/*.py` |

**On the server**, copy the helper next to the tool that imports it. Each tool tries `from sql_errors import ...` first. In the repo, it falls back to this folder.
