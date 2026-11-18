"""Build sql_run_checker.html from src/.  Run: python build.py"""
import os
here = os.path.dirname(os.path.abspath(__file__))
src = lambda n: open(os.path.join(here, 'src', n), encoding='utf-8').read()
js = '\n'.join(src(n) for n in ['pyparse.js', 'sqlcheck.js', 'rules.js', 'theme.js', 'ui.js'])
assert '</script' not in js
html = src('shell.html').replace('/*CSS*/', src('ui.css')).replace('/*JS*/', js)
open(os.path.join(here, 'sql_run_checker.html'), 'w', encoding='utf-8').write(html)
print('built sql_run_checker.html', len(html), 'bytes')
