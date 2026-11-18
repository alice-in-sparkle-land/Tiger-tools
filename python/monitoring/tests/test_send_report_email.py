"""Tests for send_report_email with a fake SMTP server (nothing is sent).
Run:  python python/monitoring/tests/test_send_report_email.py
"""
import os, sys, io, zipfile, tempfile, email
from email import policy
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
import numpy as np
import pandas as pd
from fixtures import NOW, bnpl_daily, FakeSparkCounts, date_fm, replace_sql_var
from change_monitor import run_change_monitor, EXAMPLE_SOURCES
from send_report_email import send_report, build_summary_html

TMP = tempfile.mkdtemp(prefix='email_test_')
R = []
def ok(name, cond, extra=''):
    R.append(('PASS' if cond else 'FAIL', name, extra))

DAYS, DATA = bnpl_daily()
SRC = [dict(EXAMPLE_SOURCES[0], cols=['transaction_source', 'transaction_status'])]
result = run_change_monitor(SRC, os.path.join(TMP, 'change_monitor.html'), spark=FakeSparkCounts(DAYS, DATA),
                            date_fm=date_fm, replace_sql_var=replace_sql_var, now=NOW, log=lambda m: None)
REPORT = open(result['out_path'], 'rb').read()

class FakeSMTP:
    instances = []
    def __init__(self, host, port, ssl, fail_at=None, refuse=()):
        self.host, self.port, self.ssl, self.fail_at, self.refuse = host, port, ssl, fail_at, refuse
        self.calls, self.sent = [], None
        FakeSMTP.instances.append(self)
    def __enter__(self): return self
    def __exit__(self, *a): self.calls.append('quit')
    def starttls(self):
        self.calls.append('starttls')
        if self.fail_at == 'starttls': raise OSError('STARTTLS not supported')
    def login(self, u, p):
        self.calls.append(('login', u, p))
        if self.fail_at == 'login': raise Exception('(535, b"5.7.8 Authentication failed")')
    def send_message(self, msg, from_addr=None, to_addrs=None):
        self.calls.append('send'); self.sent = (msg, from_addr, to_addrs)
        return {r: (550, b'no such user') for r in self.refuse}

def factory(**kw):
    return lambda h, p, s: FakeSMTP(h, p, s, **kw)

def send(**kw):
    buf = []
    args = dict(to=['team@corp.vn'], smtp_host='smtp.corp.vn', smtp_user='reports@corp.vn', smtp_password='pw',
                smtp=factory(), log=buf.append)
    args.update(kw)
    return send_report(result, **args), '\n'.join(buf)

# ---------------- normal send ----------------
r, log = send(cc=['boss@corp.vn'])
s = FakeSMTP.instances[-1]
msg, frm, rcpt = s.sent
ok('send: ok, STARTTLS then login then send', r['ok'] and s.calls[:3] == ['starttls', ('login', 'reports@corp.vn', 'pw'), 'send'])
ok('send: To + Cc all receive it', rcpt == ['team@corp.vn', 'boss@corp.vn'] and r['sent_to'] == rcpt)
ok('send: subject has title and spike count', msg['Subject'].startswith('BNPL Change Monitor · ') and 'spikes' in msg['Subject'])
parsed = email.message_from_bytes(msg.as_bytes(), policy=policy.default)
body = parsed.get_body(('html',)).get_content()
ok('body: summary table with the top spike value', 'failed' in body and '<table' in body and 'z count' in body)
ok('body: no scripts or external links (email-safe)', '<script' not in body and 'http' not in body)
att = list(parsed.iter_attachments())
ok('attachment: one zip', len(att) == 1 and att[0].get_filename() == 'change_monitor.zip')
z = zipfile.ZipFile(io.BytesIO(att[0].get_content()))
ok('attachment: unzips to the exact report bytes', z.read('change_monitor.html') == REPORT)

r, _ = send(attach_as='html')
att = list(email.message_from_bytes(FakeSMTP.instances[-1].sent[0].as_bytes(), policy=policy.default).iter_attachments())
ok('attach_as=html: octet-stream attachment, exact bytes',
   att[0].get_content_type() == 'application/octet-stream' and att[0].get_content() == REPORT)

r, _ = send(smtp_port=465)
ok('port 465: SSL, no STARTTLS', FakeSMTP.instances[-1].ssl and 'starttls' not in FakeSMTP.instances[-1].calls)

os.environ['SMTP_PASS'] = 'from-env'
r, _ = send(smtp_password=None)
ok('password: taken from SMTP_PASS when not passed', ('login', 'reports@corp.vn', 'from-env') in FakeSMTP.instances[-1].calls)
del os.environ['SMTP_PASS']

# ---------------- dry run ----------------
n = len(FakeSMTP.instances)
r, log = send(dry_run=True)
ok('dry run: nothing sent, no connection', r['ok'] and r['dry_run'] and len(FakeSMTP.instances) == n)
ok('dry run: body preview saved next to the report', os.path.exists(r['preview']) and 'failed' in open(r['preview'], encoding='utf-8').read())

# ---------------- failures ----------------
r, log = send(smtp=factory(fail_at='login'))
ok('login failure: stops, says which step and server', not r['ok'] and r['failed_step'] == 'login'
   and 'FAILED at email step: login' in log and 'smtp.corp.vn:587' in log and 'Authentication failed' in log)
r, log = send(smtp=factory(fail_at='starttls'))
ok('starttls failure: reported at its step', not r['ok'] and r['failed_step'] == 'starttls')
r, log = send(smtp=factory(refuse=('team@corp.vn',)), cc=['boss@corp.vn'])
ok('refused recipient: not ok, refused listed, others sent', not r['ok'] and r['refused'] == ['team@corp.vn']
   and r['sent_to'] == ['boss@corp.vn'] and 'refused: team@corp.vn' in log)

def raises(fn, text):
    try:
        fn()
    except Exception as e:
        return text in str(e)
    return False
ok('no password -> clear error', raises(lambda: send(smtp_password=None), 'no password'))
ok('no recipients -> clear error', raises(lambda: send(to=[]), 'to is empty'))
ok('bad attach_as -> clear error', raises(lambda: send(attach_as='pdf'), "attach_as must be"))
ok('result not ok -> refuses to send', raises(lambda: send_report({'ok': False}, ['a@b'], 'h'), 'not ok'))

# ---------------- summary for both monitors ----------------
risk = pd.DataFrame({'source': ['pd30'], 'column_name': ['a'], 'column_value': ['<web>'], 'date_name': ['weekly'],
                     'period': ['wk 2026-09-14'], 'def_n': [19.0], 'obs_n': [270.0], 'ratio': [0.0704],
                     'base_ratio': [0.0356], 'z_ratio': [3.8], 'ratio_cl': [0.0704], 'z_cl': [np.nan],
                     'direction': ['spike']})
html = build_summary_html({'res': risk, 'z_th': 2.0, 'title': 'Risk Ratio Monitor', 'kind': 'risk',
                           'source_labels': {'pd30': 'PD30'}})
ok('risk summary: ratio columns, values escaped, missing z shown as a dash',
   'z ratio' in html and '7.04%' in html and '&lt;web&gt;' in html and '<web>' not in html and '–' in html and 'PD30' in html)

for st, nme, x in R:
    if st != 'PASS':
        print(f'{st}: {nme}  {x}')
fails = sum(1 for st, *_ in R if st == 'FAIL')
print(f"{len(R)} checks {fails} failures")
sys.exit(1 if fails else 0)
