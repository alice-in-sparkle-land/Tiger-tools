# SQL Doc Builder

Open `sql_doc_builder.html` in Chrome and choose a pipeline folder. It drafts documentation for your teammate and your manager, and you add the "why" in plain words.

**Tabs**

- **Overview:** purpose, consumers, schedule and how to run, plus source and output tables.
- **How it runs:** explains `raya.py`, file naming, the modes, the config files and the daily/backup/union pattern. Its **run plan** switches between Production, Debug and Standalone.
- **Data flow:** a clickable lineage graph. Daily/backup/union helpers are grouped into one box.
- **Scripts:** every script, grouped by layer, with the tables it reads and writes, its parameters and your notes.
- **Columns:** type a column name to see how it is built and what it feeds. You can limit the trace to one table.
- **Table catalog** and **Review:** automatic checks such as run order, duplicates and backup files.

**Exports**

- **`.md`**, with a Contents list.
- **`.html`**, with a clickable outline and its own search. You choose which sections and sub-parts to include, and the choice is remembered.
- **Share interactive:** a read-only copy of the tool with the SQL and your notes built in, without the Review tab, to send to colleagues.

Your notes are saved in the browser, with Export and Import notes (Merge or Replace). The theme is the same Ghibli misty-hills theme as the other tools.

This page is a single hand-edited file. It carries its own copy of the Python parser from `../sql-run-checker/src/pyparse.js` (used to read `debug.py` and `standalone.py`). If you change that parser, copy the change here too.
