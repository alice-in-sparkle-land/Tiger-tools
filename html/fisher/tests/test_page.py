"""Browser checks for fisher_backtest_builder.html (Playwright + Chromium), then the
downloaded .py is run on a local Spark with fake data (if PySpark is installed).

    python html/fisher/build.py && python html/fisher/tests/test_page.py
"""
import os, sys, io, json, time, contextlib, tempfile
os.environ['TZ'] = 'UTC'; time.tzset()
from playwright.sync_api import sync_playwright

HERE = os.path.dirname(os.path.abspath(__file__))
PAGE = 'file://' + os.path.abspath(os.path.join(HERE, '..', 'fisher_backtest_builder.html'))
ENGINE = open(os.path.join(HERE, '..', 'src', 'engine.py'), encoding='utf-8').read().strip()
OUT = tempfile.mkdtemp()
passed = failed = 0


def check(name, ok, info=''):
    global passed, failed
    if ok:
        passed += 1; print('  ok  ', name)
    else:
        failed += 1; print('  FAIL', name, info)


with sync_playwright() as p:
    br = p.chromium.launch()
    for w, h in [(1280, 900), (360, 780)]:
        print(f'Width {w}px')
        ctx = br.new_context(viewport={'width': w, 'height': h}, accept_downloads=True)
        pg = ctx.new_page()
        errs = []
        pg.on('pageerror', lambda e: errs.append(str(e)))
        pg.on('console', lambda m: errs.append(m.text) if m.type == 'error' else None)
        pg.goto(PAGE)
        pg.wait_for_timeout(300)
        check('no JS errors on load', not errs, errs)
        sw = pg.evaluate('document.documentElement.scrollWidth')
        check(f'no sideways scrolling (scrollWidth {sw})', sw <= w)
        pg.screenshot(path=os.path.join(OUT, f'page_{w}.png'), full_page=True)
        code = pg.inner_text('#code')
        check('code generated from the example settings', 'CONFIG = {' in code and 'run_fisher_backtest(df, CONFIG, spark=spark)' in code)
        check('the engine is embedded unchanged', ENGINE in code)
        try:
            compile(code, 'generated.py', 'exec'); ok = True
        except SyntaxError as e:
            ok = False; print(e)
        check('generated file compiles', ok)
        check('4 comparisons listed', pg.locator('#comps .cmp').count() == 4)
        check('amount metric turns into an expression', '"txn_amount >= 5000000"' in code)

        # edit a field -> code follows
        pg.fill('[data-k="cols.merchant"]', 'seller_id')
        check('editing a column updates the file', '"merchant_col": "seller_id"' in pg.inner_text('#code'))
        # bad metric name -> errors, download blocked
        pg.locator('.item').first.locator('[data-m="name"]').fill('9bad')
        check('bad metric name: error shown', 'Fix 1 thing' in pg.inner_text('#errs'))
        check('  ... and no file', pg.inner_text('#code').startswith('# Fix the problems'))
        pg.click('#dl'); pg.wait_for_timeout(200)
        check('  ... download says to fix first', 'Fix the problems' in pg.inner_text('#banner'))
        pg.locator('.item').first.locator('[data-m="name"]').fill('high_util')
        # overlapping comparison
        pg.locator('#comps .cmp').nth(1).locator('[data-c="bs"]').fill('5')
        check('overlapping windows: error shown', 'overlaps' in pg.inner_text('#errs'))
        pg.locator('#comps .cmp').nth(1).locator('[data-c="bs"]').fill('8')
        # add / remove
        pg.click('#addMetric')
        check('add metric: new item, needs a condition', pg.locator('.item').count() == 6 and 'Write the SQL condition' in pg.inner_text('#errs'))
        pg.locator('.item').nth(5).locator('[data-m="kind"]').select_option('range')
        pg.locator('.item').nth(5).locator('[data-m="from"]').fill('9000000')
        pg.locator('.item').nth(5).locator('[data-m="to"]').fill('8000000')
        check('amount range: From must be below To', 'From must be below To' in pg.inner_text('#errs'))
        pg.locator('.item').nth(5).locator('[data-m="to"]').fill('9999999')
        check('amount range becomes a BETWEEN flag', '"txn_amount BETWEEN 9000000 AND 9999999"' in pg.inner_text('#code'))
        pg.locator('.item').nth(5).locator('[data-del-m]').click()
        code = pg.inner_text('#code')
        check('relative amount and money surge reach the file', '"kind": "rel_amount", "level": 0.9' in code
              and '"kind": "money"' in code and '"amount_col": "txn_amount"' in code)
        pg.click('#addComp')
        check('add comparison 14 vs 15-60', '14 vs 15-60' in pg.inner_text('#code'))
        pg.locator('#comps .cmp').nth(4).locator('[data-del-c]').click()
        # peers off
        pg.uncheck('[data-k="peer.on"]')
        check('peer check off: peer_groups []', '"peer_groups": []' in pg.inner_text('#code'))
        pg.check('[data-k="peer.on"]')
        code = pg.inner_text('#code')
        check('three peer groups by default', '["seller_group"]' in code and '["h3_9"]' in code and '["seller_group", "h3_9"]' in code)
        pg.click('#addGroup'); pg.locator('[data-g]').nth(3).fill('h3_9')
        check('same peer group twice: error', 'Same group twice' in pg.inner_text('#errs'))
        pg.locator('[data-g]').nth(3).fill('seller_group, city')
        check('a fourth group reaches the file', '["seller_group", "city"]' in pg.inner_text('#code'))
        pg.locator('[data-del-g]').nth(3).click()
        pg.select_option('[data-k="rule.bh"]', 'day')
        pg.fill('[data-k="labels.liftDays"]', '45')
        code = pg.inner_text('#code')
        check('BH scope and lift days reach the file', '"bh_scope": "day"' in code and '"lift_days": 45' in code)
        # settings survive reload
        pg.reload(); pg.wait_for_timeout(300)
        check('settings kept after reload', pg.input_value('[data-k="cols.merchant"]') == 'seller_id')
        # export -> reset -> import (replace)
        with pg.expect_download() as d:
            pg.locator('.expSet').first.click()
        ep = os.path.join(OUT, f'settings_{w}.json'); d.value.save_as(ep)
        check('export has the app id', json.load(open(ep))['app'] == 'tiger-fisher-backtest')
        pg.click('#reset'); pg.click('[data-s="reset"]')
        check('reset back to the example', pg.input_value('[data-k="cols.merchant"]') == 'merchant_id')
        pg.set_input_files('#setIn', ep); pg.wait_for_timeout(200)
        pg.click('[data-s="replace"]')
        check('import Replace restores the settings', pg.input_value('[data-k="cols.merchant"]') == 'seller_id')
        bad = os.path.join(OUT, 'bad.json'); json.dump({'app': 'other'}, open(bad, 'w'))
        pg.set_input_files('#setIn', bad); pg.wait_for_timeout(200)
        check('wrong file: clear message', 'different app' in pg.inner_text('#sheetc'))
        pg.click('[data-s="close"]')
        sw = pg.evaluate('document.documentElement.scrollWidth')
        check(f'still no sideways scrolling after edits ({sw})', sw <= w)
        check('no JS errors after all clicks', not errs, errs)
        ctx.close()

    # settings for the fake data, then download the file for the Spark run
    ctx = br.new_context(viewport={'width': 1280, 'height': 900}, accept_downloads=True)
    pg = ctx.new_page(); pg.goto(PAGE); pg.wait_for_timeout(300)
    pg.fill('[data-k="labels.susp"]', 'f_susp = 1')
    pg.uncheck('[data-k="units.customer"]')                         # transactions only: quicker
    for i in (3, 2):
        pg.locator('#comps .cmp').nth(i).locator('[data-del-c]').click()   # keep 1 vs 2-8 and 7 vs 8-15
    pg.fill('[data-k="anchor.start"]', '2026-08-15')
    with pg.expect_download() as d:
        pg.click('#dl')
    gen = os.path.join(OUT, d.value.suggested_filename); d.value.save_as(gen)
    check('download is a .py file', gen.endswith('.py') and 'CONFIG = {' in open(gen).read())
    ctx.close()
    br.close()

print('Generated file on local Spark')
try:
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master('local[2]').config('spark.sql.shuffle.partitions', '4')
             .config('spark.ui.showConsoleProgress', 'false').config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
except Exception as e:  # noqa
    spark = None; print('  (PySpark not available: skipped)', e)
if spark is not None:
    sys.path.insert(0, HERE)
    src = open(os.path.join(HERE, 'test_engine.py')).read()
    ns = {'__file__': os.path.join(HERE, 'test_engine.py')}
    exec(src[:src.index('# ---------------------------------------------------------------------------- Part B')], ns)
    with contextlib.redirect_stdout(io.StringIO()):
        df = spark.createDataFrame(ns['fake']())
    g = {'spark': spark, 'df': df}
    exec('from pyspark.sql.functions import *', g)            # the notebook case that broke sum()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        exec(open(gen).read(), g)
    log = buf.getvalue()
    check('the generated file runs on df and spark, after from pyspark.sql.functions import *', g['r']['ok'], log[-600:])
    check('  ... and leaves detail, summary and outcomes', len(g['detail']) > 0 and len(g['summary']) > 0 and len(g['outcomes']) > 0)
    check('  ... the planted sellers alert', set(g['detail'].loc[g['detail']['alert'], 'merchant']) >= {'M04', 'M07', 'M10'})
    dd = g['detail']
    check('  ... tests self + 3 peer groups, money surge rows, dates', {'peer:seller_group', 'peer:h3_9', 'peer:seller_group+h3_9'} <= set(dd['test'])
          and (dd['unit'] == 'amount').any() and {'recent_start', 'recent_end', 'ref_start', 'ref_end', 'txn_cnt', 'susp_cnt', 'def_cnt', 'obs_cnt', 'loan_amt'} <= set(dd.columns))
    print(g['summary'].head(8).to_string())
    spark.stop()

print(f'\nScreenshots in {OUT}\n{passed} passed, {failed} failed')
sys.exit(1 if failed else 0)
