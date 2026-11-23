"""Build fisher_backtest_builder.html from src/.  Run: python build.py"""
import os, json
here = os.path.dirname(os.path.abspath(__file__))
src = lambda n: open(os.path.join(here, 'src', n), encoding='utf-8').read()
engine = 'const ENGINE=' + json.dumps(src('engine.py')).replace('</', '<\\/') + ';\n'
js = engine + '\n'.join(src(n) for n in ['theme.js', 'ui.js'])
assert '</script' not in js
html = src('shell.html').replace('/*CSS*/', src('ui.css')).replace('/*JS*/', js)
open(os.path.join(here, 'fisher_backtest_builder.html'), 'w', encoding='utf-8').write(html)
print('built fisher_backtest_builder.html', len(html), 'bytes')
