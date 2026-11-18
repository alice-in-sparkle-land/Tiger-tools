"""Tests for rerun_table with a fake Spark (no cluster needed).
Run:  python python/sql_pipeline/tests/test_rerun_table.py
"""
import os, sys, shutil, tempfile, re
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
TMP = tempfile.mkdtemp(prefix='rerun_test_')

def make_fixture(root):
    shutil.rmtree(root,ignore_errors=True)
    F={
    'all/20250101_impala_rel_af_transform_bnpl_transaction_base.sql':"""-- base
    select t.id, t.amt, t.d from owner_dwh.ft_card_transaction_tt t where t.d >= '${start_refreshing_date}'""",
    'all/20250102_impala_rel_af_dim_customer.sql':"select c.id, c.name from owner_dwh_nopii.dct_client c",
    'all/202601010101_oracle_rel_af_transform_statement.sql':"select s.id, s.due from owner_dwh.f_statement s where s.d >= to_date('var_month_oracle','YYYY-MM-DD')\n",
    'all/20260102_impala_rel_af_fact_statement.sql':"select st.*, 'x' as month_decision_partition from rel_af_transform_statement st",
    'all/output.py':'output = [\n {"T":"rel_af_fact_statement","C":"month_decision_partition"},\n]\n',
    'bnpl/202502271440_impala_rel_af_transform_vector_transaction_ko_daily.sql':"select v.id, v.amt, v.part_date from VECTOR_TABLE v where v.part_date >= 'var_date_vector'",
    'bnpl/202502271441_impala_rel_af_transform_vector_transaction_ko_backup.sql':"select * from af_mon.rel_af_transform_vector_transaction_ko where part_date < 'var_date_vector'",
    'bnpl/202502271442_impala_rel_af_transform_vector_transaction_ko_union.sql':"select * from rel_af_transform_vector_transaction_ko_backup union all select * from rel_af_transform_vector_transaction_ko_daily",
    'bnpl/202502271443_impala_rel_af_transform_vector_transaction_ko.sql':"select * from rel_af_transform_vector_transaction_ko_union",
    'bnpl/20270101_impala_rel_af_fact_bnpl_transaction.sql':"""with k as (select * from af_mon.rel_af_transform_bnpl_kafka)
    select b.id, extract(year from b.d) y, 'select x from fake' s, c.name, 'm' month_decision_partition
    from rel_af_transform_bnpl_transaction_base b
    left join rel_af_dim_customer c on c.id = b.id
    left join k on k.id = b.id
    /* join something_commented on */
    -- from old_table""",
    'bnpl/20280101_impala_rel_af_agg_trigger.sql':"""select f.id, count(1) cnt, 'm' month_transaction_partition
    from af_mon.rel_af_fact_bnpl_transaction f, af_mon.rel_af_fact_statement s
    join (select id from af_mon.rel_af_transform_vector_transaction_ko) v on v.id = f.id
    where f.id = s.id and run_after_6pm
    group by f.id""",
    'bnpl/output.py':'output = [\n {"T":"rel_af_fact_bnpl_transaction","C":"month_decision_partition"}, # full load\n {"T":"rel_af_agg_trigger","C":"month_transaction_partition"},\n {"T":"rel_af_transform_vector_transaction_ko","C":"part_date"},\n]\n',
    'card/20270101_impala_rel_af_fact_card.sql':"select c.id, 'm' month_decision_partition from rel_af_dim_customer c",
    'card/20270102_impala_rel_af_dim_customer.sql':"select 1 id, 'card' name",
    'card/output.py':'output = [\n {"T":"rel_af_fact_card","C":"month_decision_partition"},\n]\n',
    }
    for k,v in F.items():
        p=os.path.join(root,k);os.makedirs(os.path.dirname(p),exist_ok=True);open(p,'w').write(v)
    os.makedirs(os.path.join(root,'archive'),exist_ok=True);open(os.path.join(root,'archive','20990101_impala_rel_af_fact_card.sql'),'w').write('select broken')

make_fixture(os.path.join(TMP,'master'))

from rerun_table import rerun_table, tables_used
R=[]
def ok(n,c,x=''):R.append(('PASS' if c else 'FAIL',n,x))
class W:
    def __init__(s,log,df):s.log=log;s.df=df
    def mode(s,*a):return s
    def format(s,*a):return s
    def partitionBy(s,*a):s.log.append(('partitionBy',a[0]));return s
    def insertInto(s,name,overwrite=False):
        if s.df.spark.insert_fails:raise Exception('Table not found '+name)
        s.log.append(('insertInto',name));s.df.spark.tables.add(name)
    def saveAsTable(s,name):s.log.append(('saveAsTable',name));s.df.spark.tables.add(name)
class DF:
    def __init__(s,spark,q):s.spark=spark;s.q=q
    def createOrReplaceTempView(s,n):s.spark.log.append(('view',n));s.spark.views.add(n)
    def take(s,n):
        if 'RUNTIME_FAIL' in s.q:raise Exception('Job aborted: divide by zero')
        return []
    def repartition(s,c):s.spark.log.append(('repartition',c));return s
    @property
    def write(s):return W(s.spark.log,s)
    def count(s):return 1 if s.q.startswith('show tables') and s.q.split("'")[1] in [t.split('.')[-1] for t in s.spark.tables] else 0
class Cat:
    def __init__(s,sp):s.sp=sp
    def tableExists(s,n):return n in s.sp.tables
class Spark:
    def __init__(s,tables=()):s.tables=set(tables);s.views=set();s.log=[];s.queries=[];s.catalog=Cat(s);s.insert_fails=False
    def sql(s,q):
        s.queries.append(q)
        if 'FAIL_HERE' in q:raise Exception("[TABLE_OR_VIEW_NOT_FOUND] The table or view `nope` cannot be found.; line 2 pos 5\nmore detail")
        return DF(s,q)
def oracle(q):return DF(SP,q)
SV=[{"from":'${start_refreshing_date}',"to":'2026-01-01'},{"from":'VECTOR_TABLE',"to":'lap_analytical.lap_vector_journal_v1_2026'},{"from":'var_date_vector',"to":'20261116'},{"from":'var_month_oracle',"to":'2026-06-01'},{"from":'run_after_6pm',"to":'1=1'}]
M=os.path.join(TMP,'master')+'/'
def run(table,tables=(),**kw):
    global SP;SP=Spark(tables);buf=[];r=rerun_table(table,M,SV,spark=SP,oracle_fn=oracle,log=buf.append,**kw);return r,'\n'.join(buf),SP
# --- tables_used
tu=tables_used("""with k as (select * from a.x) select extract(year from d), 'from fake' from k
 left join b.y yy on 1=1 cross join (select * from d.w) q join e.v using(id), c.z -- from cmt
 where a is distinct from b /* join hidden */""")
ok('tables_used',sorted(tu)==['a.x','b.y','d.w','e.v'],str(tu))
ok('tables_used comma list',tables_used('select 1 from a.x xx, b.y, c.z zz where 1=1')==['a.x','b.y','c.z'])
ok('tables_used range fn',tables_used("select * from range(10) r")==[],str(tables_used("select * from range(10) r")))
# --- dry run, nothing exists
r,out,sp=run('rel_af_agg_trigger',dry_run=True)
ok('dry no queries',not sp.queries)
plan=r['plan']
ok('dry plan has all deps','all/20250101_impala_rel_af_transform_bnpl_transaction_base.sql' in plan and 'bnpl/202502271441_impala_rel_af_transform_vector_transaction_ko_backup.sql' in plan,str(plan))
first_non_all=min(i for i,p in enumerate(plan) if not p.startswith('all/'));last_all=max(i for i,p in enumerate(plan) if p.startswith('all/'))
ok('all first',last_all<first_non_all,str(plan))
ok('target last',plan[-1]=='bnpl/20280101_impala_rel_af_agg_trigger.sql')
ok('dim from all not card',  'all/20250102_impala_rel_af_dim_customer.sql' in plan and 'card/20270102_impala_rel_af_dim_customer.sql' not in plan)
ok('oracle before fact_statement',plan.index('all/202601010101_oracle_rel_af_transform_statement.sql')<plan.index('all/20260102_impala_rel_af_fact_statement.sql'))
ok('backup reuses previous run','previous run' in out or 'exists' in out or 'rel_af_transform_vector_transaction_ko_backup' in out)
ok('archive ignored','archive' not in out)
ok('cte not crawled','- k ' not in out)
# --- af_mon exists -> reused
r,out,sp=run('rel_af_agg_trigger',tables={'af_mon.rel_af_fact_bnpl_transaction','af_mon.rel_af_fact_statement','af_mon.rel_af_transform_vector_transaction_ko'},dry_run=True)
ok('reuse existing',r['plan']==['bnpl/20280101_impala_rel_af_agg_trigger.sql'],str(r['plan']))
ok('reuse printed',out.count('(exists, reused)')==3)
# rebuild_af_mon forces
r,out,sp=run('rel_af_agg_trigger',tables={'af_mon.rel_af_fact_bnpl_transaction','af_mon.rel_af_fact_statement','af_mon.rel_af_transform_vector_transaction_ko'},dry_run=True,rebuild_af_mon=True)
ok('rebuild_af_mon',len(r['plan'])>5,str(len(r['plan'])))
# --- full run
r,out,sp=run('af_mon.rel_af_agg_trigger')
ok('run ok',r['ok'],out[-300:])
ok('replacement applied',any('lap_analytical.lap_vector_journal_v1_2026' in q for q in sp.queries) and not any('VECTOR_TABLE' in q for q in sp.queries))
ok('views created',('view','rel_af_transform_bnpl_transaction_base') in sp.log and ('view','rel_af_dim_customer') in sp.log)
ok('outputs written',('insertInto','af_mon.rel_af_agg_trigger') in sp.log and ('insertInto','af_mon.rel_af_fact_statement') in sp.log)
ok('output partition',('repartition','month_transaction_partition') in sp.log)
ok('oracle wrapped',True)
# --- failure stops
import shutil;shutil.copytree(M,os.path.join(TMP,'m2'),dirs_exist_ok=True)
p=os.path.join(TMP,'m2/all/20250102_impala_rel_af_dim_customer.sql');open(p,'w').write("select c.id,\n FAIL_HERE c.name\n from owner_dwh_nopii.dct_client c")
SP=Spark();buf=[];r=rerun_table('rel_af_agg_trigger',os.path.join(TMP,'m2/'),SV,spark=SP,oracle_fn=oracle,log=buf.append);out='\n'.join(buf)
ok('fail stops',r['ok'] is False and r['failed_table']=='rel_af_dim_customer',str(r.get('failed_table')))
ok('fail message','FAILED at step' in out and 'all/20250102_impala_rel_af_dim_customer.sql' in out and 'not run:' in out)
ok('fail shows line','>    2 |' in out,out[-600:])
ok('nothing after fail',not any('agg_trigger' in q for q in SP.queries))
# --- run_after_6pm skip
SV2=[x if x['from']!='run_after_6pm' else {"from":'run_after_6pm',"to":'2=1/*run_after_6pm*/'} for x in SV]
SP=Spark({'af_mon.rel_af_fact_bnpl_transaction','af_mon.rel_af_fact_statement','af_mon.rel_af_transform_vector_transaction_ko'});buf=[]
r=rerun_table('rel_af_agg_trigger',M,SV2,spark=SP,oracle_fn=oracle,log=buf.append);out='\n'.join(buf)
ok('6pm skip',r['ok'] and 'SKIPPED writing af_mon.rel_af_agg_trigger' in out and ('insertInto','af_mon.rel_af_agg_trigger') not in SP.log)
# --- check_views runtime error
p=os.path.join(TMP,'m2/card/20270102_impala_rel_af_dim_customer.sql');open(p,'w').write("select 1/0 RUNTIME_FAIL from owner_dwh_nopii.dct_client c")
SP=Spark();buf=[];r=rerun_table('rel_af_fact_card',os.path.join(TMP,'m2/'),SV,spark=SP,oracle_fn=oracle,log=buf.append,check_views=True)
ok('check_views catches',r['ok'] is False and r['failed_table']=='rel_af_dim_customer','\n'.join(buf)[-300:])
# --- folder preference: card target uses card dim? target folder preferred
r,out,sp=run('rel_af_fact_card',dry_run=True)
ok('prefers target folder dim',r['plan']==['card/20270102_impala_rel_af_dim_customer.sql','card/20270101_impala_rel_af_fact_card.sql'],str(r['plan']))
ok('dup note',"built in 2 folders" in out)
r,out,sp=run('rel_af_dim_customer',dry_run=True,folder='card')
ok('folder param',r['plan']==['card/20270102_impala_rel_af_dim_customer.sql'])
# --- unknown table / folder
try:run('nope_table',dry_run=True);ok('unknown table',False)
except RuntimeError as e:ok('unknown table','No .sql file builds' in str(e))
try:run('rel_af_dim_customer',dry_run=True,folder='xx');ok('unknown folder',False)
except RuntimeError as e:ok('unknown folder','not found' in str(e))
# --- cycle
shutil.rmtree(os.path.join(TMP,'m3'),ignore_errors=True);os_=__import__('os');os_.makedirs(os.path.join(TMP,'m3/all'))
open(os.path.join(TMP,'m3/all/1_impala_a.sql'),'w').write('select * from b');open(os.path.join(TMP,'m3/all/2_impala_b.sql'),'w').write('select * from a');open(os.path.join(TMP,'m3/all/output.py'),'w').write('output=[{"T":"zz","C":"c"}]')
try:SP=Spark();rerun_table('a',os.path.join(TMP,'m3'),SV,spark=SP,oracle_fn=oracle,log=lambda m:None,dry_run=True);ok('cycle',False)
except RuntimeError as e:ok('cycle','Circular' in str(e),str(e))
# missing output.py folder works
os_.remove(os.path.join(TMP,'m3/all/output.py'))
try:SP=Spark();r=rerun_table('a',os.path.join(TMP,'m3/'),SV,spark=SP,oracle_fn=oracle,log=lambda m:None,dry_run=True)
except RuntimeError as e:ok('no output.py','Circular' in str(e))
for x in R:
    if x[0]=='FAIL':print(*x)
print(len(R),'checks',sum(x[0]=='FAIL' for x in R),'failures')
shutil.rmtree(TMP,ignore_errors=True)
sys.exit(1 if any(x[0]=='FAIL' for x in R) else 0)
