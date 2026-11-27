/* ================= Fisher Backtest Builder ================= */
const APP_ID='tiger-fisher-backtest',SKEY='fisher.state';
const $=s=>document.querySelector(s);
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const cap=s=>s.charAt(0).toUpperCase()+s.slice(1);
const IDENT=/^[A-Za-z_][A-Za-z0-9_]*$/;
const RAW_E='0, 0.35, 0.55, 1, 1.96',STD_E='0, 1, 2, 3';
const DEF={
  cols:{date:'txn_date',merchant:'merchant_id',customer:'client_id',amount:'txn_amount'},
  units:{customer:true,txn:true},
  metrics:[{name:'high_util',kind:'flag',expr:'utilization > 0.9',op:'>=',value:'',from:'',to:'',level:'90'},
           {name:'big_ticket',kind:'amount',expr:'',op:'>=',value:'5000000',from:'',to:'',level:'90'},
           {name:'big_rel',kind:'rel',expr:'',op:'>=',value:'',from:'',to:'',level:'90'},
           {name:'volume',kind:'volume',expr:'',op:'>=',value:'',from:'',to:'',level:'90'},
           {name:'money_surge',kind:'money',expr:'',op:'>=',value:'',from:'',to:'',level:'90'},
           {name:'amount_ratio',kind:'ratio',expr:'',op:'>=',value:'',from:'',to:'',level:'90',rawEdges:RAW_E,stdEdges:STD_E}],
  comps:[{rs:1,re:1,bs:2,be:8},{rs:1,re:7,bs:8,be:15},{rs:1,re:7,bs:8,be:35},{rs:1,re:30,bs:31,be:90}],
  anchor:{start:'',end:'',skipLast:false},
  peer:{on:true,groups:['seller_group','h3_9','seller_group, h3_9'],min:'3'},
  labels:{susp:'f_suspicious = 1',suspDate:'',def:'f_pd30',obs:'f_pd30_obs',liftDays:'30'},
  rule:{q:'0.05',or:'2',cooldown:'7',minTotal:'10',minFlag:'5',maxRows:'3000000',bh:'rule'}
};
const clone=o=>JSON.parse(JSON.stringify(o));
const KINDS=[['flag','Flag share'],['amount','Amount flag'],['range','Amount range'],['rel','Relative amount'],['money','Money surge'],['ratio','Ratio Z (picture)'],['volume','Volume']];
const NEEDS_AMT=['amount','range','rel','money','ratio'];
const edges=v=>String(v).split(',').map(x=>x.trim()).filter(Boolean);
const edgesOk=v=>{const e=edges(v);return e.length>0&&e.every(isNum)&&e.every((x,i)=>i===0||+x>+e[i-1]);};
let state=clone(DEF),storageOk=true,pending=null;

/* ---------- state ---------- */
function sanitize(o){
  const s=clone(DEF);if(!o||typeof o!=='object')return s;
  ['cols','units','anchor','peer','labels','rule'].forEach(g=>{if(o[g]&&typeof o[g]==='object')Object.keys(s[g]).forEach(k=>{
    if(k in o[g]&&o[g][k]!==null&&typeof o[g][k]===typeof s[g][k]&&Array.isArray(o[g][k])===Array.isArray(s[g][k]))s[g][k]=o[g][k];});});
  s.peer.groups=s.peer.groups.map(String);
  if(Array.isArray(o.metrics))s.metrics=o.metrics.filter(m=>m&&typeof m==='object').map(m=>({name:String(m.name||''),
    kind:KINDS.map(k=>k[0]).includes(m.kind)?m.kind:'flag',expr:String(m.expr||''),op:['>=','>','<=','<'].includes(m.op)?m.op:'>=',
    value:String(m.value||''),from:String(m.from||''),to:String(m.to||''),level:String(m.level||'90'),
    rawEdges:String(m.rawEdges||RAW_E),stdEdges:String(m.stdEdges||STD_E)}));
  if(Array.isArray(o.comps))s.comps=o.comps.filter(c=>c&&typeof c==='object').map(c=>({rs:+c.rs||0,re:+c.re||0,bs:+c.bs||0,be:+c.be||0}));
  return s;
}
function load(){try{const v=localStorage.getItem(SKEY);if(v)state=sanitize(JSON.parse(v));}catch(e){storageOk=false;}}
function save(){try{localStorage.setItem(SKEY,JSON.stringify(state));}catch(e){storageOk=false;banner();}}
function banner(msg,err){const b=$('#banner');if(!msg&&storageOk){b.classList.add('hidden');return;}
  b.textContent=msg||'This browser is not letting the page save settings. Use Export settings to keep a copy.';
  b.classList.toggle('err',!!err||!msg);b.classList.remove('hidden');if(msg){clearTimeout(banner.t);banner.t=setTimeout(()=>banner(),2600);}}
const getK=k=>k.split('.').reduce((o,p)=>o[p],state);
function setK(k,v){const p=k.split('.'),last=p.pop();p.reduce((o,q)=>o[q],state)[last]=v;}

/* ---------- names and checks ---------- */
const cmpName=c=>(c.rs===1?String(c.re):c.rs+'-'+c.re)+' vs '+c.bs+'-'+c.be;
const list=s=>String(s).split(',').map(x=>x.trim()).filter(Boolean);
const isNum=v=>String(v).trim()!==''&&isFinite(+v);
const isInt=v=>isNum(v)&&Number.isInteger(+v);
const isDate=v=>/^\d{4}-\d{2}-\d{2}$/.test(v)&&!isNaN(new Date(v+'T00:00:00Z'));
function validate(){
  const E={},add=(k,m)=>{(E[k]=E[k]||[]).push(m);};
  ['date','merchant','customer'].forEach(k=>{const v=state.cols[k].trim();if(!v)add('cols.'+k,'Required.');else if(!IDENT.test(v))add('cols.'+k,'Use a plain column name (letters, digits, _).');});
  if(state.cols.amount.trim()&&!IDENT.test(state.cols.amount.trim()))add('cols.amount','Use a plain column name.');
  if(!state.units.customer&&!state.units.txn)add('units','Tick at least one way to count.');
  if(!state.metrics.length)add('metrics','Add at least one metric.');
  const seen={};
  state.metrics.forEach((m,i)=>{const n=m.name.trim();
    if(!n)add('m'+i,'Give the metric a name.');else if(!IDENT.test(n))add('m'+i,'Name: letters, digits and _ only, not starting with a digit.');
    else if(seen[n])add('m'+i,'Two metrics are called '+n+'.');seen[n]=1;
    if(m.kind==='flag'&&!m.expr.trim())add('m'+i,'Write the SQL condition.');
    if(m.kind==='flag'&&/;|--/.test(m.expr))add('m'+i,'The condition must be one expression, without ; or --.');
    if(NEEDS_AMT.includes(m.kind)&&!state.cols.amount.trim())add('m'+i,'Fill in the Amount column first (section 1).');
    if(m.kind==='amount'&&!isNum(m.value))add('m'+i,'Enter a number.');
    if(m.kind==='range'){if(!isNum(m.from)||!isNum(m.to))add('m'+i,'Enter both amounts.');else if(+m.from>=+m.to)add('m'+i,'From must be below To.');}
    if(m.kind==='rel'&&(!isNum(m.level)||+m.level<50||+m.level>=100))add('m'+i,'Percentile between 50 and 99.9.');
    if(m.kind==='ratio'&&(!edgesOk(m.rawEdges)||!edgesOk(m.stdEdges)))add('m'+i,'Band edges: numbers separated by commas, in increasing order.');});
  const exp=[];state.metrics.forEach(m=>{const n=m.name.trim();if(m.kind!=='rel'){exp.push(n);return;}
    const gs=state.peer.on?state.peer.groups.map(list).filter(g=>g.length):[];(gs.length?gs:[['all']]).forEach(g=>exp.push(n+'_'+g.join('_')));});
  const dup=exp.filter((n,i)=>n&&exp.indexOf(n)!==i);if(dup.length)add('metrics','Metric names clash after relative amount adds one per group: '+[...new Set(dup)].join(', ')+'.');
  if(!state.comps.length)add('comps','Add at least one comparison.');
  const cs={};
  state.comps.forEach((c,i)=>{const v=[c.rs,c.re,c.bs,c.be];
    if(!v.every(x=>Number.isInteger(x)&&x>=1&&x<=400))add('c'+i,'Use whole days from 1 to 400.');
    else if(c.rs>c.re||c.bs>c.be)add('c'+i,'Each window needs start ≤ end.');
    else if(c.bs<=c.re&&c.rs<=c.be)add('c'+i,'The base window overlaps the recent one.');
    const n=cmpName(c);if(cs[n])add('c'+i,'Same comparison twice.');cs[n]=1;});
  ['start','end'].forEach(k=>{const v=state.anchor[k].trim();if(v&&!isDate(v))add('anchor.'+k,'Use YYYY-MM-DD.');});
  if(isDate(state.anchor.start)&&isDate(state.anchor.end)&&state.anchor.start>state.anchor.end)add('anchor.end','Must be after From.');
  if(state.peer.on){if(!state.peer.groups.length)add('peer.groups','Add at least one peer group, or untick the peer check.');
    const seen2={};state.peer.groups.forEach((g,i)=>{const pc=list(g);
      if(!pc.length)add('g'+i,'List at least one column.');else if(!pc.every(c=>IDENT.test(c)))add('g'+i,'Comma-separated plain column names.');
      const key=pc.join('+');if(pc.length&&seen2[key])add('g'+i,'Same group twice.');seen2[key]=1;});
    if(!isInt(state.peer.min)||+state.peer.min<1)add('peer.min','A whole number, 1 or more.');}
  const L=state.labels;
  if(L.suspDate.trim()&&!IDENT.test(L.suspDate.trim()))add('labels.suspDate','Use a plain column name.');
  if(L.suspDate.trim()&&!L.susp.trim())add('labels.suspDate','Only used with a suspicious condition.');
  if(/;|--/.test(L.susp))add('labels.susp','One expression, without ; or --.');
  ['def','obs'].forEach(k=>{if(L[k].trim()&&!IDENT.test(L[k].trim()))add('labels.'+k,'Use a plain column name.');});
  if(L.def.trim()&&(!isInt(L.liftDays)||+L.liftDays<1||+L.liftDays>365))add('labels.liftDays','Whole days, 1 to 365.');
  if(!!L.def.trim()!==!!L.obs.trim())add(L.def.trim()?'labels.obs':'labels.def','Default lift needs both the default and the observed column.');
  const R=state.rule;
  if(!isNum(R.q)||+R.q<=0||+R.q>1)add('rule.q','Between 0 and 1.');
  if(!isNum(R.or)||+R.or<1)add('rule.or','1 or more.');
  [['cooldown',0],['minTotal',1],['minFlag',1],['maxRows',1000]].forEach(([k,mn])=>{if(!isInt(R[k])||+R[k]<mn)add('rule.'+k,'A whole number, '+mn+' or more.');});
  return E;
}

/* ---------- Python output ---------- */
const py=v=>v===null||v===undefined?'None':v===true?'True':v===false?'False':typeof v==='number'?String(v):JSON.stringify(String(v));
function metricExpr(m){const a=state.cols.amount.trim();
  return m.kind==='amount'?a+' '+m.op+' '+(+m.value):m.kind==='range'?a+' BETWEEN '+(+m.from)+' AND '+(+m.to):m.expr.trim();}
function metricCfg(m){const n=m.name.trim();
  if(m.kind==='volume')return {name:n,kind:'volume'};
  if(m.kind==='money')return {name:n,kind:'money'};
  if(m.kind==='ratio')return {name:n,kind:'ratio_z',raw_edges:edges(m.rawEdges).map(Number),std_edges:edges(m.stdEdges).map(Number)};
  if(m.kind==='rel')return {name:n,kind:'rel_amount',level:Math.round(+m.level*10)/1000};
  return {name:n,kind:'flag',expr:metricExpr(m)};}
function config(){
  const R=state.rule,L=state.labels;
  return {
    date_col:state.cols.date.trim(),merchant_col:state.cols.merchant.trim(),customer_col:state.cols.customer.trim(),
    metrics:state.metrics.map(metricCfg),
    comparisons:state.comps.map(c=>({name:cmpName(c),recent:[c.rs,c.re],base:[c.bs,c.be]})),
    units:['customer','txn'].filter(u=>state.units[u]),
    peer_groups:state.peer.on?state.peer.groups.map(list).filter(g=>g.length):[],min_peers:+state.peer.min||1,
    amount_col:state.cols.amount.trim()||null,
    anchor_start:state.anchor.start.trim()||null,anchor_end:state.anchor.end.trim()||null,skip_last_day:state.anchor.skipLast,
    min_total:+R.minTotal,min_flag:+R.minFlag,q_max:+R.q,or_min:+R.or,cooldown:+R.cooldown,max_rows:+R.maxRows,bh_scope:R.bh==='day'?'day':'rule',
    susp_cond:L.susp.trim()||null,susp_date_col:L.suspDate.trim()||null,def_col:L.def.trim()||null,obs_col:L.obs.trim()||null,lift_days:+L.liftDays||30
  };
}
function pyLit(v,ind){
  if(Array.isArray(v)){if(!v.length)return '[]';
    if(v.every(x=>typeof x!=='object'||x===null))return '['+v.map(py).join(', ')+']';
    return '[\n'+v.map(x=>ind+'    '+pyLit(x,ind+'    ')+',').join('\n')+'\n'+ind+']';}
  if(v&&typeof v==='object'){const ks=Object.keys(v);
    if(ks.every(k=>typeof v[k]!=='object'||v[k]===null||(Array.isArray(v[k])&&v[k].every(x=>typeof x!=='object'))))
      {const one='{'+ks.map(k=>py(k)+': '+pyLit(v[k],ind)).join(', ')+'}';if(one.length<=96)return one;}
    return '{\n'+ks.map(k=>ind+'    '+py(k)+': '+pyLit(v[k],ind+'    ')+',').join('\n')+'\n'+ind+'}';}
  return py(v);
}
function generate(){
  const c=config(),d=new Date().toISOString().slice(0,10);
  const tests='self'+c.peer_groups.map(g=>', peer:'+g.join('+')).join('');
  return ['"""Fisher backtest, generated '+d+' by Tiger Tools html/fisher (Fisher Backtest Builder).',
    '',
    'Paste into a notebook cell where `spark` and `df` (transaction level) already exist,',
    'or run: exec(open("/path/to/this_file.py").read())',
    '',
    'Every day in the anchor range plays "today". For each seller, metric, comparison, count unit',
    'and test (self: recent vs base window; peer: seller vs other sellers in its peer group), it',
    'builds a 2x2 table and runs a one-sided Fisher exact test. A rule fires when the BH q-value',
    '(over all sellers that day) <= q_max and the odds ratio >= or_min.',
    '',
    c.metrics.length+' metrics x '+c.comparisons.length+' comparisons x '+c.units.length+' count units; tests: '+tests+'.',
    'Money surge metrics run self only, on daily total amount (rank test), in unit "amount".',
    'detail has recent_start/recent_end (days behind a, b) and ref_start/ref_end (days behind c, d), and the',
    "seller's own totals in the recent window: txn_cnt, susp_cnt, def_cnt, obs_cnt, loan_amt (by transaction).",
    'Ratio Z metrics (the picture) add rows with unit "ratio" (z_raw, z_std) to detail and fill z_bands.',
    '',
    'Returns detail (one row per tested seller x day x rule), summary (one row per rule) and outcomes',
    '(per rule: transactions in the recent window of fired days / alerts vs the rest, by transaction and client).',
    '"""',
    'CONFIG = '+pyLit(c,''),
    '',
    ENGINE.trim(),
    '',
    '',
    '# ---------------------------------------------------------------------------- run',
    'r = run_fisher_backtest(df, CONFIG, spark=spark)',
    "detail, summary, outcomes, z_bands = r['detail'], r['summary'], r['outcomes'], r['z_bands']",
    'summary',''].join('\n');
}
function fileName(){return 'fisher_backtest_'+new Date().toISOString().slice(0,10).replace(/-/g,'')+'.py';}

/* ---------- render ---------- */
function renderLists(){
  $('#metrics').innerHTML=state.metrics.map((m,i)=>'<div class="item" data-i="'+i+'"><div class="ihead">'+
    '<input class="in" data-m="name" value="'+esc(m.name)+'" placeholder="metric_name" aria-label="Metric name">'+
    '<select data-m="kind" aria-label="Kind">'+KINDS.map(([v,t])=>'<option value="'+v+'"'+(m.kind===v?' selected':'')+'>'+t+'</option>').join('')+'</select>'+
    '<button class="x" data-del-m="'+i+'" aria-label="Remove metric">Remove</button></div>'+
    (m.kind==='flag'?'<div class="irow"><input class="in" data-m="expr" value="'+esc(m.expr)+'" placeholder="utilization > 0.9" aria-label="SQL condition"></div>':'')+
    (m.kind==='amount'?'<div class="irow"><code>'+esc(state.cols.amount||'amount')+'</code><select data-m="op" aria-label="Operator">'+['>=','>','<=','<'].map(o=>'<option'+(m.op===o?' selected':'')+'>'+esc(o)+'</option>').join('')+'</select><input class="in" data-m="value" value="'+esc(m.value)+'" inputmode="decimal" style="flex:1 1 120px" aria-label="Amount"></div>':'')+
    (m.kind==='range'?'<div class="irow"><code>'+esc(state.cols.amount||'amount')+'</code> from<input class="in" data-m="from" value="'+esc(m.from)+'" inputmode="decimal" style="flex:1 1 90px" aria-label="From amount">to<input class="in" data-m="to" value="'+esc(m.to)+'" inputmode="decimal" style="flex:1 1 90px" aria-label="To amount"></div>':'')+
    (m.kind==='rel'?'<div class="irow"><span>Percentile</span><input class="in num" data-m="level" value="'+esc(m.level)+'" inputmode="decimal" aria-label="Percentile"><span class="muted">of the peer group\'s amounts</span></div><div class="hint" style="margin:6px 0 0">Flags a transaction above that percentile. One metric per peer group (<code>'+esc(m.name||'name')+'_&lt;group&gt;</code>). Percentiles use only data before the first anchor day.</div>':'')+
    (m.kind==='money'?'<div class="irow muted">Rank test (Mann-Whitney) on the seller\'s daily total amount, recent days vs base days; no-sale days count as 0. Self only. Skips comparisons with a recent window under 3 days. Fires on q alone.</div>':'')+
    (m.kind==='ratio'?'<div class="irow"><span>Raw z bands</span><input class="in" data-m="rawEdges" value="'+esc(m.rawEdges)+'" style="flex:1 1 140px" aria-label="Raw z band edges"></div><div class="irow"><span>Standardised bands</span><input class="in" data-m="stdEdges" value="'+esc(m.stdEdges)+'" style="flex:1 1 140px" aria-label="Standardised z band edges"></div><div class="hint" style="margin:6px 0 0">The picture\'s method: r = (MP − ref) / (MP + ref) on average daily amount, clipped to ±0.999, z = 0.5 ln((1+r)/(1−r)). Ref = the seller\'s base window (self) or the other sellers\' average in each peer group (peer), plus AND / OR of the two. Raw z 0.35 ≈ 2×, 0.55 ≈ 3×, 1 ≈ 7×, 1.96 ≈ 50×. Standardised z = against the other sellers that day. Not a test: it fills <code>z_bands</code> and <code>detail</code>, no alerts.</div>':'')+
    (m.kind==='volume'?'<div class="irow muted">Seller\'s share of all transactions (self) or of each peer group (peer).</div>':'')+
    '<div class="err" id="e-m'+i+'"></div></div>').join('');
  $('#pgroups').innerHTML=state.peer.groups.map((g,i)=>'<div class="cmp pg" data-i="'+i+'"><div class="chead"><input class="in" data-g="'+i+'" value="'+esc(g)+'" placeholder="seller_group, h3_9" aria-label="Peer group columns" style="flex:1;margin:0"><button class="x" data-del-g="'+i+'" aria-label="Remove peer group">Remove</button></div><div class="muted" style="font-size:12.5px;margin-top:4px">test: <code>peer:'+esc(list(g).join('+')||'?')+'</code></div><div class="err" id="e-g'+i+'"></div></div>').join('');
  const win=(lab,k1,k2,c)=>'<span class="win"><span class="wl">'+lab+'</span><input class="in num" data-c="'+k1+'" value="'+c[k1]+'" inputmode="numeric" aria-label="'+lab+' from">–<input class="in num" data-c="'+k2+'" value="'+c[k2]+'" inputmode="numeric" aria-label="'+lab+' to"></span>';
  $('#comps').innerHTML=state.comps.map((c,i)=>'<div class="cmp" data-i="'+i+'"><div class="chead"><span class="name">'+esc(cmpName(c))+'</span><button class="x" data-del-c="'+i+'" aria-label="Remove comparison">Remove</button></div>'+
    '<div class="wins">'+win('Recent','rs','re',c)+win('Base','bs','be',c)+'</div><div class="err" id="e-c'+i+'"></div></div>').join('');
}
function renderFields(){document.querySelectorAll('[data-k]').forEach(el=>{const v=getK(el.dataset.k);if(el.type==='checkbox')el.checked=!!v;else if(document.activeElement!==el)el.value=v;});}
let lastErr={};
function refresh(){
  const E=validate();lastErr=E;
  document.querySelectorAll('[data-k]').forEach(el=>{el.classList.toggle('bad',!!E[el.dataset.k]);
    let n=el.parentElement.querySelector('.err.f');if(E[el.dataset.k]){if(!n){n=document.createElement('div');n.className='err f';el.parentElement.appendChild(n);}n.textContent=E[el.dataset.k].join(' ');}else if(n)n.remove();});
  state.metrics.forEach((m,i)=>{const e=$('#e-m'+i);if(e)e.textContent=(E['m'+i]||[]).join(' ');});
  state.peer.groups.forEach((g,i)=>{const e=$('#e-g'+i);if(e)e.textContent=(E['g'+i]||[]).join(' ');const t=document.querySelector('.pg[data-i="'+i+'"] code');if(t)t.textContent='peer:'+(list(g).join('+')||'?');});
  state.comps.forEach((c,i)=>{const e=$('#e-c'+i);if(e)e.textContent=(E['c'+i]||[]).join(' ');const n=document.querySelector('.cmp[data-i="'+i+'"] .name');if(n)n.textContent=cmpName(c);});
  const all=Object.entries(E);
  $('#errs').innerHTML=all.length?'<div class="errs"><b>Fix '+all.length+' '+(all.length===1?'thing':'things')+' before using the file:</b><ul>'+all.map(([k,v])=>'<li>'+esc(where(k))+': '+esc(v.join(' '))+'</li>').join('')+'</ul></div>':'';
  $('#code').textContent=all.length?'# Fix the problems above to generate the file.':generate();
}
function where(k){if(/^m\d+$/.test(k)){const m=state.metrics[+k.slice(1)];return 'Metric '+(m&&m.name?m.name:'#'+(+k.slice(1)+1));}
  if(/^c\d+$/.test(k))return 'Comparison '+(+k.slice(1)+1);
  if(/^g\d+$/.test(k))return 'Peer group '+(+k.slice(1)+1);
  const N={'cols.date':'Date column','cols.merchant':'Seller column','cols.customer':'Customer column','cols.amount':'Amount column',units:'Count by',metrics:'Metrics',comps:'Comparisons',
    'anchor.start':'Anchor from','anchor.end':'Anchor to','peer.groups':'Peer groups','peer.min':'Min other sellers','labels.liftDays':'Days after an alert','labels.susp':'Suspicious condition','labels.suspDate':'Flagged-on date',
    'labels.def':'Default column','labels.obs':'Observed column','rule.q':'q-value','rule.or':'Odds ratio','rule.cooldown':'Cooldown','rule.minTotal':'Min recent total','rule.minFlag':'Min flagged','rule.maxRows':'Max rows'};
  return N[k]||k;}
function all(){renderFields();renderLists();refresh();}

/* ---------- export / import ---------- */
function stamp(){const d=new Date(),z=n=>String(n).padStart(2,'0');return d.getFullYear()+z(d.getMonth()+1)+z(d.getDate())+'-'+z(d.getHours())+z(d.getMinutes());}
function download(name,text,type){const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([text],{type}));a.download=name;document.body.appendChild(a);a.click();setTimeout(()=>{URL.revokeObjectURL(a.href);a.remove();},800);}
function exportSettings(){download('fisher-backtest-settings-'+stamp()+'.json',JSON.stringify({app:APP_ID,v:1,exported:new Date().toISOString(),data:state},null,2),'application/json');}
function sheet(h){$('#sheetc').innerHTML=h;$('#sheet').classList.remove('hidden');}
function closeSheet(){$('#sheet').classList.add('hidden');pending=null;}
async function readSettings(file){
  let o;try{o=JSON.parse(await file.text());}catch(e){return sheet('<h3>Import settings</h3><p>This file is not valid JSON.</p><div class="row-end"><button data-s="close">Close</button></div>');}
  if(!o||o.app!==APP_ID||!o.data)return sheet('<h3>Import settings</h3><p>'+(o&&o.app?'This file is from a different app ('+esc(String(o.app).slice(0,40))+').':'This is not a Fisher Backtest Builder settings file.')+'</p><div class="row-end"><button data-s="close">Close</button></div>');
  pending=sanitize(o.data);
  sheet('<h3>Import settings</h3><p>The file has <b>'+pending.metrics.length+'</b> metrics and <b>'+pending.comps.length+'</b> comparisons.</p><ul class="muted" style="font-size:13px;padding-left:18px"><li><b>Merge</b> keeps your columns and rules, and adds the file’s metrics and comparisons you don’t have.</li><li><b>Replace</b> takes everything from the file.</li></ul><div class="row-end"><button data-s="close">Cancel</button><button data-s="merge">Merge</button><button class="primary" data-s="replace">Replace</button></div>');
}
function merge(p){const names=new Set(state.metrics.map(m=>m.name)),cs=new Set(state.comps.map(cmpName));
  p.metrics.forEach(m=>{if(!names.has(m.name))state.metrics.push(m);});p.comps.forEach(c=>{if(!cs.has(cmpName(c)))state.comps.push(c);});}

/* ---------- wiring ---------- */
document.addEventListener('DOMContentLoaded',()=>{
  load();
  document.addEventListener('input',e=>{const t=e.target;
    if(t.dataset.k&&t.type!=='checkbox'){setK(t.dataset.k,t.value);if(t.dataset.k==='cols.amount')document.querySelectorAll('.irow code').forEach(c=>c.textContent=t.value||'amount');}
    else if(t.dataset.m){const i=+t.closest('.item').dataset.i;state.metrics[i][t.dataset.m]=t.value;}
    else if(t.dataset.g!==undefined){state.peer.groups[+t.dataset.g]=t.value;}
    else if(t.dataset.c){const i=+t.closest('.cmp').dataset.i,v=t.value.trim();state.comps[i][t.dataset.c]=/^\d+$/.test(v)?+v:NaN;}
    else return;save();refresh();});
  document.addEventListener('change',e=>{const t=e.target;
    if(t.dataset.k&&t.type==='checkbox'){setK(t.dataset.k,t.checked);save();refresh();}
    if(t.dataset.m==='kind'||t.dataset.m==='op'){const i=+t.closest('.item').dataset.i;state.metrics[i][t.dataset.m]=t.value;save();renderLists();refresh();}
    if(t.classList.contains('themeSel')){try{localStorage.setItem(TKEY,t.value);}catch(err){}applyTheme(t.value);}});
  document.addEventListener('click',e=>{const t=e.target.closest('[data-del-m],[data-del-c],[data-del-g]');if(!t)return;
    if(t.dataset.delM!==undefined)state.metrics.splice(+t.dataset.delM,1);else if(t.dataset.delG!==undefined)state.peer.groups.splice(+t.dataset.delG,1);
    else state.comps.splice(+t.dataset.delC,1);save();renderLists();refresh();});
  $('#addGroup').onclick=()=>{state.peer.groups.push('');save();renderLists();refresh();const l=document.querySelectorAll('[data-g]');l[l.length-1].focus();};
  $('#addMetric').onclick=()=>{let n=state.metrics.length+1;while(state.metrics.some(m=>m.name==='metric_'+n))n++;
    state.metrics.push({name:'metric_'+n,kind:'flag',expr:'',op:'>=',value:'',from:'',to:'',level:'90',rawEdges:RAW_E,stdEdges:STD_E});save();renderLists();refresh();document.querySelector('.item:last-child [data-m="expr"]').focus();};
  $('#addComp').onclick=()=>{state.comps.push({rs:1,re:14,bs:15,be:60});save();renderLists();refresh();};
  $('#reset').onclick=()=>sheet('<h3>Reset</h3><p>Go back to the example settings? Your current settings are lost unless you export them first.</p><div class="row-end"><button data-s="close">Cancel</button><button class="primary" data-s="reset">Reset</button></div>');
  $('#copy').onclick=async()=>{if(Object.keys(lastErr).length)return banner('Fix the problems listed above first.',true);
    try{await navigator.clipboard.writeText(generate());banner('Copied.');}catch(e){const r=document.createRange();r.selectNodeContents($('#code'));const s=getSelection();s.removeAllRanges();s.addRange(r);banner('Selected: press Ctrl+C (or Copy) to copy.');}};
  $('#dl').onclick=()=>{if(Object.keys(lastErr).length)return banner('Fix the problems listed above first.',true);download(fileName(),generate(),'text/x-python');};
  document.querySelectorAll('.expSet').forEach(b=>b.onclick=exportSettings);
  document.querySelectorAll('.impSet').forEach(b=>b.onclick=()=>$('#setIn').click());
  $('#setIn').onchange=e=>{const f=e.target.files[0];e.target.value='';if(f)readSettings(f);};
  $('#sheet').addEventListener('click',e=>{const b=e.target.closest('[data-s]');if(e.target.id==='sheet'||(b&&b.dataset.s==='close'))return closeSheet();if(!b)return;
    if(b.dataset.s==='reset'){state=clone(DEF);save();closeSheet();all();banner('Settings reset.');}
    if(pending&&b.dataset.s==='replace'){state=pending;save();closeSheet();all();banner('Settings replaced.');}
    if(pending&&b.dataset.s==='merge'){merge(pending);save();closeSheet();all();banner('Settings merged.');}});
  document.addEventListener('keydown',e=>{if(e.key==='Escape')closeSheet();});
  try{localStorage.setItem('fisher.__t','1');localStorage.removeItem('fisher.__t');}catch(e){storageOk=false;banner();}
  applyTheme();all();
  if(window.matchMedia)try{matchMedia('(prefers-color-scheme: dark)').addEventListener('change',()=>applyTheme());}catch(e){}
  setInterval(()=>{if(document.visibilityState==='visible'&&phaseNow()!==themePhase)applyTheme();},60000);
});
