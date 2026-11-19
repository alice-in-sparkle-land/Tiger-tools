"""
rerun_table: re-run one table and everything it needs, for debugging.

    from rerun_table import rerun_table
    rerun_table('rel_af_agg_trigger', master_path, sql_var)               # run
    rerun_table('rel_af_agg_trigger', master_path, sql_var, dry_run=True) # only show the plan

How it works
  1. Looks at every folder under master_path (master_path itself and its subfolders,
     e.g. all/, bnpl/, card/). Each <order>_<source>_<table>.sql file builds <table>.
  2. Starts from your table's script, reads the tables it uses (FROM / JOIN), and
     walks back through the scripts that build them:
       - af_mon.X  -> reused if the table already exists, otherwise its script is run
       - X (no schema) and a script builds X -> always rebuilt (temporary views do
         not survive between sessions)
       - anything else (owner_dwh..., lap_analytical...) -> a source, left alone
  3. Runs the chain upstream-first, each script exactly like run_sql_folder does:
     sql_var replacement, impala -> spark.sql, oracle -> JDBC, and the output.py of
     the script's own folder decides view vs af_mon.<table> (partitioned by "C").
  4. Prints every step. Stops at the first failure and says where.
"""
import os, re, time, glob, traceback

# ----------------------------------------------------------------------------
# reading folders
# ----------------------------------------------------------------------------
def _read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()

def _exec_var(path, name):
    if not os.path.exists(path):
        return None
    ns = {}
    exec(_read(path), ns)
    return ns.get(name)

def _scan(master_path):
    """Return {folder_name: {'path', 'scripts': [...], 'output': {table: col}}}."""
    master_path = master_path if master_path.endswith('/') else master_path + '/'
    candidates = [master_path] + sorted(
        os.path.join(master_path, d) + '/' for d in os.listdir(master_path)
        if os.path.isdir(os.path.join(master_path, d)) and not d.startswith(('.', '_')) and d not in ('archive',))
    folders = {}
    for path in candidates:
        files = sorted(glob.glob(path + '*.sql'))          # same as load_sqls()
        if not files:
            continue
        name = os.path.basename(path.rstrip('/')) or path
        out = {}
        try:
            output = _exec_var(path + 'output.py', 'output') or []
            out = {m['T'].lower(): m['C'] for m in output}
        except Exception as e:
            raise RuntimeError(f"Could not read {path}output.py: {e}")
        scripts = []
        for i, f in enumerate(files):
            base = os.path.basename(f)
            parts = base.split('_')
            source = parts[1] if len(parts) > 1 else ''
            table = '_'.join(parts[2:]).replace('.sql', '')   # same as run_sql_folder()
            scripts.append({'folder': name, 'path': f, 'file': base, 'order': i,
                            'source': source, 'table': table, 'tl': table.lower()})
        folders[name] = {'path': path, 'scripts': scripts, 'output': out}
    if not folders:
        raise RuntimeError(f"No .sql files found in {master_path} or its subfolders.")
    return folders

# ----------------------------------------------------------------------------
# which tables does a query read?
# ----------------------------------------------------------------------------
_KW = set('''select from where group order having limit union intersect except minus on as left right inner
outer full cross join lateral view values with using natural semi anti window qualify into table by is null
case when then else end and or not in exists between like distinct all over partition set'''.split())
_FN_FROM = {'extract', 'trim', 'substring', 'substr', 'position', 'overlay'}

def _strip(sql):
    """Remove comments and string contents, keep everything else."""
    out, i, n = [], 0, len(sql)
    while i < n:
        c = sql[i]
        if sql.startswith('--', i):
            j = sql.find('\n', i); i = n if j < 0 else j; continue
        if sql.startswith('/*', i):
            j = sql.find('*/', i + 2); i = n if j < 0 else j + 2; out.append(' '); continue
        if c in "'\"":
            j = i + 1
            while j < n:
                if sql[j] == '\\': j += 2; continue
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c: j += 2; continue
                    break
                j += 1
            out.append("''"); i = j + 1; continue
        out.append(c); i += 1
    return ''.join(out)

def tables_used(sql):
    """Table names read by a query (lower case, CTE names removed)."""
    toks = re.findall(r"`[^`]*`|[A-Za-z_][\w$#]*|\d+|\S", _strip(sql))
    low = [t.strip('`').lower() for t in toks]
    ctes = set()
    for k in range(len(low) - 2):
        if low[k + 1] == 'as' and low[k + 2] == '(' and (k == 0 or low[k - 1] in ('with', ',', 'recursive')):
            ctes.add(low[k])
    refs, stack = [], []
    def read_ref(j):
        if j < len(low) and low[j] == '(':
            return j                         # subquery: its own FROM is found anyway
        if j >= len(low) or not re.match(r'[a-z_]', low[j]) or low[j] in _KW:
            return j
        name = low[j]; j += 1
        while j + 1 < len(low) and low[j] == '.' and re.match(r'[a-z_]', low[j + 1]):
            name += '.' + low[j + 1]; j += 2
        if j < len(low) and low[j] == '(':
            return j                         # table function, e.g. range(...)
        refs.append(name)
        if j < len(low) and low[j] == 'as': j += 1
        if j < len(low) and re.match(r'[a-z_]', low[j]) and low[j] not in _KW: j += 1
        return j
    k = 0
    while k < len(low):
        t = low[k]
        if t == '(':
            stack.append(low[k - 1] if k else '')
        elif t == ')':
            if stack: stack.pop()
        elif t == 'from':
            in_fn = bool(stack) and stack[-1] in _FN_FROM
            distinct_from = k >= 2 and low[k - 1] == 'distinct' and 'is' in (low[k - 2], low[k - 3] if k >= 3 else '')
            if not in_fn and not distinct_from:
                j = read_ref(k + 1)
                while j < len(low) and low[j] == ',':
                    j = read_ref(j + 1)
        elif t == 'join':
            read_ref(k + 1)
        k += 1
    seen, out = set(), []
    for r in refs:
        if r not in ctes and r not in seen:
            seen.add(r); out.append(r)
    return out

def replace_vars(sql, sql_var):
    for item in sql_var or []:
        sql = sql.replace(item["from"], str(item["to"]))
    return sql

# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def rerun_table(table, master_path, sql_var, folder=None, dry_run=False,
                rebuild_af_mon=False, check_views=False, spark=None, oracle_fn=None, log=print):
    """
    table          'rel_af_agg_trigger' or 'af_mon.rel_af_agg_trigger'
    master_path    parent folder that holds all/, bnpl/, card/ ...
    sql_var        the same list you pass to run_sql_folder
    folder         optional: which folder's script to use for the target (e.g. 'bnpl')
    dry_run        True = only print the tree and the plan
    rebuild_af_mon True = also rebuild upstream af_mon tables that already exist
    check_views    True = after each view, fetch 1 row so errors show at the right step
                   (views are lazy: without this, a broken view can fail later, at the
                   first step that writes data)
    Returns a dict with the plan and where it stopped.
    """
    t0 = time.time()
    if spark is None or oracle_fn is None:
        import sachGiaoKhoa as sgk
        spark = spark or sgk.SPARK
        oracle_fn = oracle_fn or sgk.oracle2sparkdf
    def say(msg=''):
        log(msg)

    folders = _scan(master_path)
    target = table.lower().strip()
    if target.startswith('af_mon.'):
        target = target[len('af_mon.'):]

    # index: table name -> scripts that build it (all folders)
    builders = {}
    for fname, f in folders.items():
        for s in f['scripts']:
            s['is_output'] = s['tl'] in f['output']
            s['part'] = f['output'].get(s['tl'])
            builders.setdefault(s['tl'], []).append(s)

    def pick(tl, prefer):
        cands = builders.get(tl, [])
        if not cands:
            return None, []
        order = [prefer, 'all'] if prefer else ['all']
        for p in order:
            for s in cands:
                if s['folder'] == p:
                    return s, [c for c in cands if c is not s]
        return cands[0], cands[1:]

    exists_cache = {}
    def af_mon_exists(tl):
        if tl in exists_cache:
            return exists_cache[tl]
        ok = None
        try:
            ok = bool(spark.catalog.tableExists('af_mon.' + tl))
        except Exception:
            try:
                ok = spark.sql(f"show tables in af_mon like '{tl}'").count() > 0
            except Exception:
                ok = False
        exists_cache[tl] = ok
        return ok

    # ---------------- crawl ----------------
    if folder and folder not in folders:
        raise RuntimeError(f"Folder '{folder}' not found under {master_path}. Folders: {', '.join(folders)}")
    root, others = pick(target, folder)
    if root is None:
        raise RuntimeError(f"No .sql file builds '{target}'. Looked in: {', '.join(folders)}")

    say('=' * 90)
    say(f"Re-run: {root['table']}   ({root['folder']}/{root['file']})")
    if others:
        say(f"  note: also built by {', '.join(o['folder'] + '/' + o['file'] for o in others)} -> using {root['folder']}/")
    say('=' * 90)

    plan, state, notes = [], {}, []      # state: tl -> 'visiting' | 'done'
    tree_lines = []

    def visit(s, depth, via):
        key = (s['folder'], s['tl'])
        if state.get(key) == 'done':
            tree_lines.append('   ' * depth + f"- {s['table']}  (already in plan)")
            return
        if state.get(key) == 'visiting':
            raise RuntimeError('Circular dependency: ' + ' -> '.join(via + [s['table']]))
        state[key] = 'visiting'
        my_deps = []
        tag = f"af_mon, by {s['part']}" if s['is_output'] else 'view'
        tree_lines.append('   ' * depth + f"- {s['table']}  [{s['folder']} | {s['source']} | {tag}]")
        sql = replace_vars(_read(s['path']), sql_var)
        if s['source'] not in ('impala', 'oracle'):
            notes.append(f"{s['folder']}/{s['file']}: engine '{s['source']}' is not impala, so it is sent to Oracle (typo?)")
        if s['source'] == 'impala':          # oracle scripts can only read Oracle tables
            for ref in tables_used(sql):
                parts = ref.split('.')
                if len(parts) == 1:
                    dep, alts = pick(ref, s['folder'])
                    if dep is None:
                        tree_lines.append('   ' * (depth + 1) + f"- {ref}  (no script builds it: temp view or default db)")
                        continue
                    if alts:
                        notes.append(f"'{ref}' is built in {len(alts) + 1} folders; using {dep['folder']}/{dep['file']}")
                    if dep['is_output']:
                        notes.append(f"{s['file']} reads '{ref}' without af_mon., but it is an output table (no view is created)")
                    visit(dep, depth + 1, via + [s['table']])
                    my_deps.append((dep['folder'], dep['tl']))
                elif len(parts) == 2 and parts[0] == 'af_mon' and parts[1] in builders:
                    dep, alts = pick(parts[1], s['folder'])
                    if state.get((dep['folder'], dep['tl'])) == 'visiting':
                        # e.g. _backup reads af_mon.<final> from the previous run
                        ex = af_mon_exists(parts[1])
                        tree_lines.append('   ' * (depth + 1) + f"- af_mon.{parts[1]}  (previous run, reused" + ('' if ex else ' - but it does NOT exist yet') + ')')
                        if not ex:
                            notes.append(f"{s['file']} reads af_mon.{parts[1]} from the previous run, but that table does not exist, so this step will fail")
                        continue
                    if dep['tl'] == s['tl']:
                        tree_lines.append('   ' * (depth + 1) + f"- af_mon.{parts[1]}  (previous run of itself, reused)")
                        continue
                    if not rebuild_af_mon and af_mon_exists(parts[1]):
                        tree_lines.append('   ' * (depth + 1) + f"- af_mon.{parts[1]}  (exists, reused)")
                        continue
                    if not dep['is_output']:
                        notes.append(f"{s['file']} reads af_mon.{parts[1]}, but '{parts[1]}' is not in {dep['folder']}/output.py, so running its script will not create it")
                    visit(dep, depth + 1, via + [s['table']])
                    my_deps.append((dep['folder'], dep['tl']))
                else:
                    tree_lines.append('   ' * (depth + 1) + f"- {ref}  (source)")
        state[key] = 'done'
        plan.append(dict(s, sql=sql, _deps=my_deps))

    visit(root, 0, [])

    # order: dependencies first; among steps that are ready, folder all/ first,
    # then the other folders, and inside a folder the file-name order (as run_sql_folder)
    deps = {id(p): set() for p in plan}
    by_key = {(p['folder'], p['tl']): p for p in plan}
    for p in plan:
        for d in p['_deps']:
            if d in by_key and by_key[d] is not p:
                deps[id(p)].add(id(by_key[d]))
    rank = lambda p: (0 if p['folder'] == 'all' else 1, p['folder'], p['order'])
    ordered, done, left = [], set(), sorted(plan, key=rank)
    while left:
        ready = [p for p in left if deps[id(p)] <= done]
        nxt = ready[0]
        ordered.append(nxt); done.add(id(nxt)); left.remove(nxt)
    for p in ordered:
        if p['folder'] == 'all':
            late = [q for q in ordered[:ordered.index(p)] if q['folder'] != 'all']
            if late:
                notes.append(f"all/{p['file']} needs {', '.join(q['folder'] + '/' + q['file'] for q in late)}, so a non-all step has to run before it")
                break
    plan[:] = ordered

    say('Dependency tree:')
    for l in tree_lines:
        say('  ' + l)
    if notes:
        say('')
        say('Notes:')
        for n in dict.fromkeys(notes):
            say('  ! ' + n)
    say('')
    say(f'Plan ({len(plan)} step{"s" if len(plan) != 1 else ""}):')
    w = max(len(p['table']) for p in plan)
    for i, p in enumerate(plan, 1):
        dest = f"af_mon.{p['table']} (by {p['part']})" if p['is_output'] else 'temp view'
        say(f"  {i:>2}. {p['table'].ljust(w)}  {p['folder']:<8} {p['source']:<7} -> {dest}")
    result = {'plan': [p['folder'] + '/' + p['file'] for p in plan], 'ok': None, 'failed_step': None, 'error': None}
    if dry_run:
        say('')
        say('Dry run: nothing was executed.')
        result['ok'] = True
        return result

    # ---------------- run ----------------
    say('')
    for i, p in enumerate(plan, 1):
        name, sql = p['table'], p['sql']
        head = f"[{i}/{len(plan)}] {name}  ({p['folder']}/{p['file']})"
        say(head + ' ...')
        st = time.time()
        try:
            if p['source'] == 'impala':
                df = spark.sql(sql)
            else:
                df = oracle_fn(f"({sql})")
            if not p['is_output']:
                df.createOrReplaceTempView(name)
                if check_views:
                    df.take(1)
                say(f"      done: temp view {name}  ({time.time() - st:.1f}s)")
            elif 'run_after_6pm' in sql:
                df.createOrReplaceTempView(name)
                say(f"      SKIPPED writing af_mon.{name}: the query still contains 'run_after_6pm' (before 6 pm).")
                say(f"      A temp view '{name}' was created instead, but af_mon.{name} is NOT refreshed.")
            else:
                pc = p['part']
                try:
                    df.repartition(pc).write.mode('overwrite').insertInto('af_mon.' + name, overwrite=True)
                except Exception as e1:
                    say(f"      insertInto failed ({str(e1).splitlines()[0][:150]}), trying saveAsTable ...")
                    df.repartition(pc).write.format('parquet').partitionBy(pc).mode('overwrite').saveAsTable('af_mon.' + name)
                exists_cache[p['tl']] = True
                say(f"      done: af_mon.{name}, partition {pc}  ({time.time() - st:.1f}s)")
        except Exception as e:
            msg = str(e).strip()
            say('')
            say('#' * 90)
            say(f"FAILED at step {i}/{len(plan)}: {name}")
            say(f"  file  : {p['folder']}/{p['file']}")
            say(f"  engine: {'Spark SQL' if p['source'] == 'impala' else 'Oracle'}")
            say(f"  error : {msg.splitlines()[0][:500] if msg else type(e).__name__}")
            more = msg.splitlines()[1:8]
            for l in more:
                say('          ' + l[:300])
            m = re.search(r'line (\d+),? pos (\d+)', msg)
            if m:
                ln = int(m.group(1)); lines = sql.splitlines()
                say(f"  query near line {ln} (after sql_var replacement):")
                for k in range(max(1, ln - 2), min(len(lines), ln + 2) + 1):
                    say(f"    {'>' if k == ln else ' '} {k:>4} | {lines[k - 1][:200]}")
            if i < len(plan):
                say(f"  not run: {', '.join(q['table'] for q in plan[i:])}")
            say('#' * 90)
            result.update(ok=False, failed_step=i, failed_table=name, failed_file=p['folder'] + '/' + p['file'], error=msg)
            return result
    say('')
    say(f"All {len(plan)} steps done in {time.time() - t0:.0f}s.")
    result['ok'] = True
    return result
