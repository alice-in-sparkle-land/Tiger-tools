"""
send_report_email: email a monitor report.

    from change_monitor import run_change_monitor
    from send_report_email import send_report
    r = run_change_monitor(SOURCES, '/tmp/change_monitor.html')
    send_report(r, to=['team@company.com'], smtp_host='smtp.company.com', smtp_user='reports@company.com',
                dry_run=True)                       # build the email, print what would be sent, save a preview
    send_report(r, to=['team@company.com'], smtp_host='smtp.company.com', smtp_user='reports@company.com')

  body        a static summary of the top spikes (tables + inline styles: email clients strip scripts),
              so it reads well in Outlook / Gmail
  attachment  the interactive report, zipped by default (mail gateways often strip .html files),
              read as bytes so nothing gets garbled

Works with the result of run_change_monitor and run_risk_ratio_monitor.
The SMTP password comes from smtp_password=... or the SMTP_PASS environment variable,
never from the code. On Databricks: smtp_password=dbutils.secrets.get('scope', 'key').
"""
import io, os, smtplib, zipfile
from email.message import EmailMessage
from html import escape
import pandas as pd

_TD = 'padding:6px 10px;border-bottom:1px solid #e3e2dd;font-size:13px;'
_TH = 'padding:6px 10px;border-bottom:2px solid #d6d4cd;font-size:11px;color:#6b6b66;text-align:left;'
_NUM = 'text-align:right;'
_HOT = 'color:#c2410c;background:#fff1e6;font-weight:bold;'


def _z(v, z_th, flag=''):
    if flag == 'NEW':
        return f'<td style="{_TD}{_NUM}{_HOT}">NEW</td>'
    if v is None or v != v:
        return f'<td style="{_TD}{_NUM}">–</td>'
    return f'<td style="{_TD}{_NUM}{_HOT if v > z_th else ""}">{v:.1f}</td>'


def _pct(v):
    return '–' if v is None or v != v else f'{v * 100:.2f}%'


def build_summary_html(result, top_n=15, attach_as='zip'):
    """Email-safe summary of a monitor result (the dict returned by run_change_monitor / run_risk_ratio_monitor)."""
    res, z_th, labels = result['res'], result['z_th'], result.get('source_labels') or {}
    kind = result.get('kind') or ('risk' if 'z_ratio' in res.columns else 'change')
    sp = res[res['direction'] == 'spike'].head(top_n)
    n_sp, n_dr = int((res['direction'] == 'spike').sum()), int((res['direction'] == 'drop').sum())
    cell = lambda v, extra='': f'<td style="{_TD}{extra}">{v}</td>'

    def common(r):
        return (cell(escape(str(labels.get(r.source, r.source)))) + cell(escape(str(r.column_name))) + cell(r.date_name)
                + cell(escape(str(r.column_value)), 'font-weight:bold;') + cell(r.period))

    if kind == 'risk':
        heads = ['Source', 'Column', 'Gran.', 'Value', 'Period', 'Def / obs', 'Ratio', 'Baseline', 'z ratio',
                 'Client ratio', 'z client']
        nums = heads[5:]
        rows = ''.join(f'<tr>{common(r)}{cell(f"{r.def_n:,.0f} / {r.obs_n:,.0f}", _NUM)}{cell(_pct(r.ratio), _NUM)}'
                       f'{cell(_pct(r.base_ratio), _NUM)}{_z(r.z_ratio, z_th)}{cell(_pct(r.ratio_cl), _NUM)}'
                       f'{_z(r.z_cl, z_th)}</tr>' for r in sp.itertuples())
        what = 'matured periods'
    else:
        heads = ['Source', 'Column', 'Gran.', 'Value', 'Period', 'Count', 'Baseline', 'z count', 'z share']
        nums = heads[5:]
        rows = ''.join(f'<tr>{common(r)}{cell(f"{r.cnt:,.0f}", _NUM)}{cell(f"{r.base_cnt:,.0f}", _NUM)}'
                       f'{_z(r.z_cnt, z_th, r.flag)}{_z(r.z_share, z_th, r.flag)}</tr>' for r in sp.itertuples())
        what = 'complete periods'
    head = ''.join(f'<th style="{_TH}{_NUM if h in nums else ""}">{h}</th>' for h in heads)
    empty = f'<tr><td style="{_TD}" colspan="{len(heads)}">No spikes.</td></tr>'
    return f"""<!doctype html><html><body style="margin:0;padding:16px;font-family:Segoe UI,Arial,sans-serif;color:#1d1d1b;">
<h2 style="margin:0 0 4px;font-size:18px;">{escape(result['title'])}</h2>
<p style="margin:0 0 14px;color:#6b6b66;font-size:13px;">
<b style="color:#c2410c;">{n_sp} spikes</b> · {n_dr} drops in the recent {what} (|z| &gt; {z_th}).
Top {len(sp)} below, most recent first. The full interactive report is attached.</p>
<table cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
<thead><tr>{head}</tr></thead><tbody>{rows or empty}</tbody></table>
<p style="margin:14px 0 0;color:#6b6b66;font-size:12px;">To open the report: download the attachment, {'unzip it, ' if attach_as == 'zip' else ''}
then open the .html file in Chrome or Edge. The email preview can't run the charts.</p>
</body></html>"""


def build_message(result, to, mail_from, cc=(), subject=None, attach_as='zip', top_n=15, now=None):
    if attach_as not in ('zip', 'html'):
        raise ValueError("attach_as must be 'zip' or 'html'.")
    if not to:
        raise ValueError('to is empty: give at least one recipient.')
    path = result['out_path']
    with open(path, 'rb') as f:                      # bytes: no charset mangling
        data = f.read()
    n_sp = int((result['res']['direction'] == 'spike').sum())
    day = pd.Timestamp(now) if now is not None else pd.Timestamp.now()
    msg = EmailMessage()
    msg['Subject'] = subject or f"{result['title']} · {n_sp} spikes · {day:%d %b %Y}"
    msg['From'] = mail_from
    msg['To'] = ', '.join(to)
    if cc:
        msg['Cc'] = ', '.join(cc)
    msg.set_content('Your mail client does not show HTML. The report is attached.')
    msg.add_alternative(build_summary_html(result, top_n, attach_as), subtype='html')
    name = os.path.basename(path)
    if attach_as == 'zip':
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
            z.writestr(name, data)
        msg.add_attachment(buf.getvalue(), maintype='application', subtype='zip', filename=name.rsplit('.', 1)[0] + '.zip')
    else:                                            # octet-stream: always a real attachment, never shown inline
        msg.add_attachment(data, maintype='application', subtype='octet-stream', filename=name)
    return msg, len(data)


def send_report(result, to, smtp_host, smtp_port=587, smtp_user=None, smtp_password=None, mail_from=None,
                cc=(), subject=None, attach_as='zip', top_n=15, dry_run=False, smtp=None, log=print):
    """
    result         the dict returned by run_change_monitor or run_risk_ratio_monitor (must be ok)
    to, cc         lists of addresses
    smtp_port      587 = STARTTLS, 465 = SSL
    smtp_password  or the SMTP_PASS environment variable (needed when smtp_user is set)
    attach_as      'zip' (safest) or 'html'
    dry_run        True = build the email, save a preview of the body next to the report, send nothing
    smtp           optional factory(host, port, ssl) -> server, for testing
    Returns a dict: ok, sent_to, size, and the error if sending failed.
    """
    if not result or not result.get('ok') or result.get('res') is None:
        raise ValueError('The monitor result is not ok (it stopped or was a dry run): nothing to send.')
    mail_from = mail_from or smtp_user
    if not mail_from:
        raise ValueError('Give mail_from (or smtp_user).')
    msg, size = build_message(result, list(to), mail_from, list(cc), subject, attach_as, top_n)
    rcpt = list(to) + list(cc)
    log(f"email: '{msg['Subject']}' to {', '.join(rcpt)}; report {size / 1e6:.1f} MB as {attach_as}")
    if dry_run:
        preview = result['out_path'].rsplit('.', 1)[0] + '_email_preview.html'
        with open(preview, 'w', encoding='utf-8') as f:
            f.write(msg.get_body(('html',)).get_content())
        log(f"Dry run: nothing sent. Body preview: {preview}")
        return {'ok': True, 'dry_run': True, 'sent_to': [], 'size': size, 'preview': preview, 'message': msg}

    password = smtp_password if smtp_password is not None else os.environ.get('SMTP_PASS')
    if smtp_user and not password:
        raise ValueError('smtp_user is set but there is no password: pass smtp_password=... or set SMTP_PASS.')
    ssl = smtp_port == 465
    step = 'connect'
    try:
        if smtp is not None:
            server = smtp(smtp_host, smtp_port, ssl)
        else:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=60) if ssl else smtplib.SMTP(smtp_host, smtp_port, timeout=60)
        with server:
            if not ssl:
                step = 'starttls'
                server.starttls()
            if smtp_user:
                step = 'login'
                server.login(smtp_user, password)
            step = 'send'
            refused = server.send_message(msg, from_addr=mail_from, to_addrs=rcpt) or {}
    except Exception as e:
        msg_ = str(e).strip() or type(e).__name__
        for l in ['', '#' * 90, f"FAILED at email step: {step}", f"  server: {smtp_host}:{smtp_port} ({'SSL' if ssl else 'STARTTLS'})",
                  f"  user  : {smtp_user or '-'}", f"  error : {msg_.splitlines()[0][:300]}", '#' * 90]:
            log(l)
        return {'ok': False, 'failed_step': step, 'error': msg_, 'sent_to': [], 'size': size}
    sent = [r for r in rcpt if r not in refused]
    log(f"sent to {', '.join(sent)}" + (f"; refused: {', '.join(refused)}" if refused else ''))
    return {'ok': not refused, 'sent_to': sent, 'refused': list(refused), 'size': size}
