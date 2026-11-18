/* ===== UI ===== */
const APP_ID='sql-run-checker',SKEY='sqlcheck.v1';
const $=s=>document.querySelector(s);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const cap=s=>s.charAt(0).toUpperCase()+s.slice(1);
let state={ignored:{}},storageOk=true,R=null,FILES=null,FOLDER='',CHECKED_AT=null,tab='report';
let filt={error:true,warn:true,info:false,q:'',group:'file'};

/* ---------- storage ---------- */
function loadState(){try{const v=localStorage.getItem(SKEY);if(v){const o=JSON.parse(v);state=sanitizeState(o);}}catch(e){storageOk=false;}}
function saveState(){try{localStorage.setItem(SKEY,JSON.stringify(state));}catch(e){storageOk=false;banner();}}
function sanitizeState(o){const out={ignored:{}};if(o&&o.ignored&&typeof o.ignored==='object')Object.keys(o.ignored).slice(0,5000).forEach(k=>{const v=o.ignored[k];if(typeof k==='string'&&k.length<600&&v&&typeof v==='object')out.ignored[k]={title:String(v.title||'').slice(0,300),file:String(v.file||'').slice(0,300),at:String(v.at||'').slice(0,40)};});return out;}
function banner(msg,err){const b=$('#banner');if(!msg&&storageOk){b.classList.add('hidden');return;}b.textContent=msg||'This browser is not letting the page save settings. Use Export settings to keep your ignore list.';b.classList.toggle('err',!!err||!msg);b.classList.remove('hidden');if(msg){clearTimeout(banner.t);banner.t=setTimeout(()=>banner(),2600);}}
const fp=i=>i.rule+'|'+i.file+'|'+i.title;

/* ---------- reading files: folder, files, or .zip ---------- */
async function readZip(file){
  const buf=new Uint8Array(await file.arrayBuffer()),dv=new DataView(buf.buffer);
  let e=-1;for(let i=buf.length-22;i>=Math.max(0,buf.length-65558);i--){if(dv.getUint32(i,true)===0x06054b50){e=i;break;}}
  if(e<0)throw new Error('This is not a valid .zip file.');
  const n=dv.getUint16(e+10,true);let p=dv.getUint32(e+16,true);const out=[];const dec=new TextDecoder('utf-8');
  for(let k=0;k<n;k++){
    if(dv.getUint32(p,true)!==0x02014b50)throw new Error('The .zip file is damaged.');
    const method=dv.getUint16(p+10,true),csize=dv.getUint32(p+20,true),nl=dv.getUint16(p+28,true),xl=dv.getUint16(p+30,true),cl=dv.getUint16(p+32,true),lo=dv.getUint32(p+42,true);
    const name=dec.decode(buf.subarray(p+46,p+46+nl));p+=46+nl+xl+cl;
    if(name.endsWith('/')||/(^|\/)(__MACOSX|\.git|\.ipynb_checkpoints|__pycache__)\//.test(name))continue;
    if(!/\.(sql|py|txt|md)$/i.test(name))continue;
    const ds=lo+30+dv.getUint16(lo+26,true)+dv.getUint16(lo+28,true);const data=buf.subarray(ds,ds+csize);
    let bytes;
    if(method===0)bytes=data;
    else if(method===8){if(typeof DecompressionStream==='undefined')throw new Error('This browser can’t open .zip files. Choose the folder instead.');bytes=new Uint8Array(await new Response(new Blob([data]).stream().pipeThrough(new DecompressionStream('deflate-raw'))).arrayBuffer());}
    else continue;
    out.push({path:name,text:dec.decode(bytes)});
  }
  return out;
}
async function walkEntry(entry,path,out){
  if(entry.isFile){await new Promise(r=>entry.file(f=>{out.push({file:f,path:path+f.name});r();},()=>r()));return;}
  if(entry.isDirectory){const rd=entry.createReader(),all=[];for(;;){const b=await new Promise(r=>rd.readEntries(r,()=>r([])));if(!b.length)break;all.push(...b);}for(const e of all)await walkEntry(e,path+entry.name+'/',out);}
}
async function ingest(list){
  showBusy('Reading files…');
  try{
    let items=[];
    for(const it of list){
      if(/\.zip$/i.test(it.file.name)){const z=await readZip(it.file);items.push(...z.map(x=>({path:x.path,text:x.text})));continue;}
      if(!/\.(sql|py|txt|md)$/i.test(it.file.name)||it.file.size>5e6)continue;
      if(/(^|\/)(\.git|node_modules|__pycache__|\.ipynb_checkpoints|archive)\//.test(it.path))continue;
      items.push({path:it.path,text:await it.file.text()});
    }
    if(!items.some(x=>/\.(sql|py)$/i.test(x.path))){showLanding('No .sql or .py files found there. Choose the folder that has raya.py and the .sql files, or a .zip of it.');return;}
    // find the folder root: the folder that holds the most .sql files directly
    const dirs={};items.forEach(x=>{const d=x.path.includes('/')?x.path.slice(0,x.path.lastIndexOf('/')+1):'';if(/\.sql$/i.test(x.path))dirs[d]=(dirs[d]||0)+1;});
    let root=Object.keys(dirs).sort((a,b)=>dirs[b]-dirs[a]||a.length-b.length)[0];
    if(root===undefined){const ds=items.map(x=>x.path.includes('/')?x.path.slice(0,x.path.lastIndexOf('/')+1):'');root=ds.sort((a,b)=>a.length-b.length)[0]||'';}
    FOLDER=root?root.replace(/\/$/,'').split('/').pop():'';
    FILES=items.filter(x=>x.path.startsWith(root)).map(x=>({key:x.path.slice(root.length),name:x.path.split('/').pop(),text:x.text.replace(/^﻿/,'')}));
    runNow();
  }catch(err){showLanding(String(err.message||err));}
}
function runNow(){
  showBusy('Checking '+FILES.length+' files…');
  setTimeout(()=>{
    try{R=runCheck(FILES,{folder:FOLDER});CHECKED_AT=new Date();showApp();}
    catch(err){console.error(err);showApp();$('#main').innerHTML='<div class="card"><h3>Something went wrong while checking</h3><p>Your settings are safe. Please send me this text:</p><pre style="white-space:pre-wrap">'+esc(String(err&&err.stack||err).slice(0,1500))+'</pre></div>';}
  },30);
}

/* ---------- views ---------- */
function showBusy(t){$('#landing').classList.add('hidden');$('#app').classList.remove('hidden');$('#main').innerHTML='<div class="busy">'+esc(t)+'</div>';}
function showLanding(msg){$('#app').classList.add('hidden');$('#landing').classList.remove('hidden');$('#lmsg').textContent=msg||'';$('#lmsg').classList.toggle('hidden',!msg);}
function showApp(){$('#landing').classList.add('hidden');$('#app').classList.remove('hidden');render();}
function setTab(t){tab=t;render();window.scrollTo(0,0);}
function render(){
  document.querySelectorAll('#tabs button').forEach(b=>b.classList.toggle('on',b.dataset.tab===tab));
  $('#main').innerHTML=tab==='report'?renderReport():tab==='plan'?renderPlan():renderRules();
}
const SEVN={error:'Error',warn:'Warning',info:'Note'};
function visible(){
  const q=filt.q.trim().toLowerCase();
  return R.issues.filter(i=>!state.ignored[fp(i)]&&filt[i.sev]&&(!q||(i.file+' '+i.title+' '+i.detail+' '+i.hint).toLowerCase().includes(q)));
}
function renderReport(){
  if(!R)return '';
  const act=R.issues.filter(i=>!state.ignored[fp(i)]);
  const n={error:0,warn:0,info:0};act.forEach(i=>n[i.sev]++);
  const cls=n.error?'bad':n.warn?'meh':'ok';
  const head=n.error?n.error+' error'+(n.error>1?'s':'')+' — the run will fail somewhere':n.warn?'No errors — '+n.warn+' warning'+(n.warn>1?'s':'')+' to look at':'No problems found — it should run';
  let h='<div class="status '+cls+'"><h2>'+esc(head)+'</h2><div class="muted" style="font-size:13px">Folder <b>'+esc(FOLDER||'(files)')+'</b> · '+R.files+' files, '+R.scripts+' SQL scripts · checked '+esc(CHECKED_AT.toLocaleString())+'</div>';
  h+='<ul class="runs">'+R.runs.map(r=>'<li>'+(r.assumed?'Assumed one production run (no raya*.py call found)':esc(cap(r.label))+' in <code>'+esc(r.file)+'</code>')+': '+r.selected+' of '+r.total+' scripts, '+r.items+' text replacements</li>').join('')+'</ul></div>';
  h+='<div class="filters">'+['error','warn','info'].map(s=>'<label class="chip '+s[0]+'"><input type="checkbox" data-f="'+s+'"'+(filt[s]?' checked':'')+'> '+SEVN[s]+'s ('+n[s]+')</label>').join('')+
    '<input class="in" id="q" type="search" placeholder="Filter…" value="'+esc(filt.q)+'"><select id="grp"><option value="file"'+(filt.group==='file'?' selected':'')+'>Group by file</option><option value="sev"'+(filt.group==='sev'?' selected':'')+'>Group by severity</option></select></div>';
  h+='<div id="list">'+renderList()+'</div>';
  const ign=R.issues.filter(i=>state.ignored[fp(i)]);
  if(ign.length)h+='<details class="ign"><summary>Ignored ('+ign.length+')</summary><div style="margin-top:8px">'+ign.map(i=>issueHtml(i,true)).join('')+'</div></details>';
  return h;
}
function renderList(){
  const v=visible();
  if(!v.length)return '<div class="empty">'+(R.issues.length?'Nothing matches the filters.':'Nothing to show.')+'</div>';
  const order={error:0,warn:1,info:2};
  if(filt.group==='sev'){
    return ['error','warn','info'].map(s=>{const g=v.filter(i=>i.sev===s);return g.length?'<div class="fgroup"><div class="fhead">'+SEVN[s]+'s <span class="cnt muted">'+g.length+'</span></div>'+g.map(i=>issueHtml(i)).join('')+'</div>':'';}).join('');
  }
  const files=[...new Set(v.map(i=>i.file))];
  const rank=f=>{const e=v.some(i=>i.file===f&&i.sev==='error');return (e?0:2)+(/\.py$/i.test(f)||!f?0:1);};
  files.sort((a,b)=>rank(a)-rank(b)||(a<b?-1:1));
  return files.map(f=>{const g=v.filter(i=>i.file===f).sort((a,b)=>order[a.sev]-order[b.sev]||a.line-b.line);
    const c={error:0,warn:0,info:0};g.forEach(i=>c[i.sev]++);
    return '<div class="fgroup"><div class="fhead">'+(f?'<code>'+esc(f)+'</code>':'General')+'<span class="cnt muted">'+['error','warn','info'].filter(s=>c[s]).map(s=>c[s]+' '+SEVN[s].toLowerCase()+(c[s]>1?'s':'')).join(' · ')+'</span></div>'+g.map(i=>issueHtml(i)).join('')+'</div>';}).join('');
}
function snippet(i){
  if(!i.file||!i.line)return '';
  const f=FILES.find(x=>x.key===i.file);if(!f)return '';
  const L=f.text.split(/\r?\n/),a=Math.max(1,i.line-2),b=Math.min(L.length,i.line+2);
  let h='<div class="snip">';
  for(let k=a;k<=b;k++){h+='<div'+(k===i.line?' class="hl"':'')+'><span class="n">'+k+'</span>'+esc(L[k-1].replace(/\t/g,'    ').slice(0,400))+'</div>';
    if(k===i.line&&i.col!=null&&i.col>=0&&i.col<400)h+='<div><span class="n"></span><span class="caret">'+' '.repeat(Math.max(0,(L[k-1].slice(0,i.col).replace(/\t/g,'    ')).length))+'^</span></div>';}
  return h+'</div>';
}
function issueHtml(i,ign){
  const id=esc(fp(i));
  return '<div class="iss '+i.sev+'"><div class="ihead"><span class="pill '+i.sev+'">'+SEVN[i.sev]+'</span><span class="t">'+esc(i.title)+'</span>'+(i.line?'<span class="loc">line '+i.line+'</span>':'')+'</div>'+
    (i.detail?'<div class="idet">'+esc(i.detail)+'</div>':'')+(i.hint?'<div class="fix"><b>Fix:</b> '+esc(i.hint)+'</div>':'')+
    (i.sev!=='info'||i.line?snippet(i):'')+
    '<div class="iact">'+(ign?'<button data-unignore="'+id+'">Stop ignoring</button>':'<button data-ignore="'+id+'" title="Hide this until you un-ignore it. Remembered in this browser.">Ignore</button>')+'</div></div>';
}
function renderPlan(){
  if(!R)return '';
  let h='<div class="card"><h3 style="margin:0 0 4px">Run order</h3><p class="muted" style="margin:0;font-size:13px">The order the runner uses: every .sql file in the folder, sorted by the full file name. A tick means the script runs in that call of run_sql_folder.</p></div>';
  h+='<div class="card" style="padding:0"><div class="tw"><table><thead><tr><th>#</th><th>File</th><th>Engine</th><th>Result</th>'+R.runs.map(r=>'<th>'+esc(r.assumed?'Run':cap(r.mode)+(R.runs.length>1?' (L'+r.line+')':''))+'</th>').join('')+'</tr></thead><tbody>';
  R.plan.forEach((s,i)=>{h+='<tr><td>'+(i+1)+'</td><td class="fn">'+esc(s.key)+'</td><td>'+esc(s.engine==='impala'?'Spark SQL':s.source==='oracle'?'Oracle':'Oracle ('+s.source+'?)')+'</td><td>'+(s.output?'af_mon.'+esc(s.table)+(s.part?' <span class="muted">by '+esc(s.part)+'</span>':''):'<span class="muted">view</span> '+esc(s.table))+'</td>'+s.runs.map(x=>'<td class="'+(x?'yes':'no')+'">'+(x?'✓':'–')+'</td>').join('')+'</tr>';});
  return h+'</tbody></table></div></div>';
}
function renderRules(){
  let h='<div class="card rules"><p style="margin-top:0">These are all the checks. They copy how <code>run_sql_folder()</code> in sachGiaoKhoa works. They only look for things that stop a script from running or make a step silently do nothing. They never judge your logic.</p>';
  RULES.forEach(g=>{h+='<h3>'+esc(g[0])+'</h3>'+g[1].map(r=>'<div class="r"><b><span class="pill '+r[0]+'">'+SEVN[r[0]]+'</span> '+esc(r[1])+'</b><span>'+esc(r[2])+'</span></div>').join('');});
  return h+'</div>';
}
const RULES=[
 ['How the run works (what the checks assume)',[
  ['info','Only .sql files directly in the folder run','Sorted by the full file name (Python sorted()). Files in subfolders, .txt files and .SQL files are skipped.'],
  ['info','File name = <order>_<source>_<table_name>.sql','Split on "_": part 2 is the engine; everything after is the table name. Anything other than exactly "impala" is sent to Oracle.'],
  ['info','Impala scripts run with spark.sql(); Oracle scripts are sent as "(query)"','Tables not in output.py become temporary views named after the table. Output tables are written to af_mon.<table> and get no view.'],
  ['info','A failing script does not stop the run','It is only written to run_log.txt, so everything that depends on it fails quietly. That is why this checker exists.'],
  ['info','Modes','"debug" runs exact_table minus exclude_table from debug.py; "standalone" does the same with standalone.py; any other value runs everything.'],
  ['info','Text replacement','Each sql_var item replaces plain text in order, everywhere in the file (comments too). Output tables are skipped while the text "run_after_6pm" is still in the query.']]],
 ['Python files (raya*.py, output.py, debug.py, refresh_list.py, standalone.py)',[
  ['error','Syntax errors','Missing or extra commas, unclosed brackets or quotes, bad indentation, a missing ":" — checked the same way Python does. The executor or the config file would stop the run.'],
  ['warn','Two strings next to each other in a list','"a" "b" becomes "ab" — almost always a missing comma.'],
  ['warn','Same key twice in a dict','Only the last value is kept.'],
  ['error','Config variable missing or the wrong shape','debug.py must define debug = {...}, refresh_list.py refresh_list = [...], output.py output = [{"T":..,"C":..}]. An empty output list crashes every script.'],
  ['warn','Unknown keys / names that match no script','A typo in exact_table, exclude_table or a table name in output.py, debug.py or standalone.py.']]],
 ['The executor call',[
  ['error','Folder path without a trailing "/"','path + "debug.py" would point at the wrong file.'],
  ['warn','mode is not production, debug or standalone','Any other value runs every script.'],
  ['error','sql_var missing, or an item without "from"/"to"','The runner crashes.'],
  ['warn','A replacement that never matches','The "from" text is not in any .sql file.'],
  ['warn','The same "from" twice, or an earlier "from" that is part of a later one','The later item never matches.'],
  ['warn','"from" also matches inside a longer word','e.g. "day2" inside "day20".'],
  ['error','from_text() of a file that is not in the folder','That line fails.'],
  ['warn','from_text(x).replace(...) that misses placeholders','The file still contains other sql_var placeholders.']]],
 ['SQL structure (after replacement)',[
  ['error','Unclosed brackets, quotes or /* comments',''],
  ['error','";" in the file','spark.sql() runs one statement; Oracle queries are wrapped in brackets. (A single trailing ";" in Spark is a warning.)'],
  ['error','Dangling AND / OR / = / comma','e.g. "WHERE AND", "x = AND", "a, b, FROM".'],
  ['error','Empty SELECT, FROM, WHERE, ON or IN ()',''],
  ['error','Missing comma between columns','e.g. "a b c" or "t.a t.b" in the SELECT list.'],
  ['error','JOIN without ON / USING','Except CROSS and NATURAL joins.'],
  ['error','CASE without END, BETWEEN without AND, GROUP/ORDER without BY',''],
  ['error','CTE mistakes','A comma after the last CTE, or a missing comma between CTEs.'],
  ['error','UNION with a different number of columns on each side','Only checked when no side uses *.'],
  ['error','Placeholder left in the query','${...} that no sql_var replaces.'],
  ['error','Oracle syntax in a Spark script, or Spark syntax in Oracle','SYSDATE, ROWNUM, FROM dual, (+), CONNECT BY in Spark; LIMIT and `backticks` in Oracle.'],
  ['error','Oracle file ending in a -- comment with no line break','The closing ")" added by the runner ends up inside the comment.'],
  ['warn','Unknown alias before a dot','"z.col" where z is not a table or alias in the query.']]],
 ['Tables between scripts',[
  ['error','Reading a view that is built later, or not built in this mode','Temporary views only exist after their script has run in the same session.'],
  ['error','Reading an output table without af_mon.','Output tables get no temporary view.'],
  ['error','An Oracle script reading a pipeline table','Oracle can’t see Spark views or af_mon tables.'],
  ['error','Partition column "C" not in the query','Writing the output table fails.'],
  ['warn','af_mon.X where X is not in output.py','That copy is never refreshed by this pipeline.'],
  ['warn','A table name with no schema that no script builds','Usually a typo.'],
  ['warn','Two files that build the same table','The later one wins.'],
  ['info','Reading af_mon.X before X is rebuilt','You get the previous run’s data (expected for _backup scripts).']]]
];

/* ---------- export / import ---------- */
function stamp(){const d=new Date(),z=n=>String(n).padStart(2,'0');return d.getFullYear()+z(d.getMonth()+1)+z(d.getDate())+'-'+z(d.getHours())+z(d.getMinutes());}
function download(name,text,type){const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([text],{type}));a.download=name;document.body.appendChild(a);a.click();setTimeout(()=>{URL.revokeObjectURL(a.href);a.remove();},800);}
function reportMd(){
  const act=R.issues.filter(i=>!state.ignored[fp(i)]);
  const n={error:0,warn:0,info:0};act.forEach(i=>n[i.sev]++);
  const L=['# SQL run check — '+(FOLDER||'files'),'','Checked '+CHECKED_AT.toLocaleString()+' · '+R.files+' files, '+R.scripts+' SQL scripts','','**'+n.error+' errors, '+n.warn+' warnings, '+n.info+' notes**'+(R.issues.length-act.length?' ('+(R.issues.length-act.length)+' ignored)':''),''];
  R.runs.forEach(r=>L.push('- '+(r.assumed?'Assumed production run':cap(r.label)+' in '+r.file)+': '+r.selected+'/'+r.total+' scripts, '+r.items+' replacements'));
  ['error','warn','info'].forEach(s=>{const g=act.filter(i=>i.sev===s);if(!g.length)return;L.push('','## '+SEVN[s]+'s','');
    g.forEach(i=>L.push('- **'+(i.file||'general')+(i.line?':'+i.line:'')+'** — '+i.title+(i.detail?'. '+i.detail:'')+(i.hint?' _Fix: '+i.hint+'_':'')));});
  return L.join('\n')+'\n';
}
function exportSettings(){download('sql-run-checker-settings-'+stamp()+'.json',JSON.stringify({app:APP_ID,v:1,exported:new Date().toISOString(),data:state},null,2),'application/json');}
let pending=null;
async function readSettings(file){
  let o;try{o=JSON.parse(await file.text());}catch(e){return sheet('<h3>Import settings</h3><p>This file is not valid JSON.</p><div class="row-end"><button data-s="close">Close</button></div>');}
  if(!o||o.app!==APP_ID||!o.data)return sheet('<h3>Import settings</h3><p>'+(o&&o.app?'This file is from a different app ('+esc(String(o.app).slice(0,40))+').':'This is not a SQL Run Checker settings file.')+'</p><div class="row-end"><button data-s="close">Close</button></div>');
  pending=sanitizeState(o.data);const k=Object.keys(pending.ignored).length,cur=Object.keys(state.ignored).length;
  sheet('<h3>Import settings</h3><p>The file has <b>'+k+'</b> ignored warnings. You have <b>'+cur+'</b> now.</p><ul class="muted" style="font-size:13px;padding-left:18px"><li><b>Merge</b> adds the ones you don’t have.</li><li><b>Replace</b> swaps your list for the file’s.</li></ul><div class="row-end"><button data-s="close">Cancel</button><button data-s="merge">Merge</button><button class="primary" data-s="replace">Replace</button></div>');
}
function sheet(h){$('#sheetc').innerHTML=h;$('#sheet').classList.remove('hidden');}
function closeSheet(){$('#sheet').classList.add('hidden');pending=null;}

/* ---------- wiring ---------- */
document.addEventListener('DOMContentLoaded',()=>{
  loadState();
  const drop=$('#drop');
  $('#pickDir').onclick=()=>$('#dirIn').click();
  $('#pickFiles').onclick=()=>$('#filesIn').click();
  $('#recheck').onclick=()=>$('#dirIn').click();
  $('#dirIn').onchange=e=>{const fs=[...e.target.files];e.target.value='';if(fs.length)ingest(fs.map(f=>({file:f,path:f.webkitRelativePath||f.name})));};
  $('#filesIn').onchange=e=>{const fs=[...e.target.files];e.target.value='';if(fs.length)ingest(fs.map(f=>({file:f,path:f.name})));};
  ['dragenter','dragover'].forEach(ev=>drop.addEventListener(ev,e=>{e.preventDefault();drop.classList.add('over');}));
  drop.addEventListener('dragleave',()=>drop.classList.remove('over'));
  drop.addEventListener('drop',async e=>{e.preventDefault();drop.classList.remove('over');const ents=[...e.dataTransfer.items].map(i=>i.webkitGetAsEntry&&i.webkitGetAsEntry()).filter(Boolean),out=[];
    if(ents.length){for(const en of ents)await walkEntry(en,'',out);}else[...e.dataTransfer.files].forEach(f=>out.push({file:f,path:f.name}));ingest(out);});
  $('#expRep').onclick=()=>R&&download('sql-check-'+(FOLDER||'files').replace(/[^\w-]+/g,'_')+'-'+stamp()+'.md',reportMd(),'text/markdown');
  $('#copyRep').onclick=async()=>{if(!R)return;try{await navigator.clipboard.writeText(reportMd());banner('Report copied.');}catch(e){sheet('<h3>Copy the report</h3><textarea class="in" rows="12" readonly>'+esc(reportMd())+'</textarea><div class="row-end" style="margin-top:8px"><button data-s="close">Close</button></div>');}};
  document.querySelectorAll('.expSet').forEach(b=>b.onclick=exportSettings);
  document.querySelectorAll('.impSet').forEach(b=>b.onclick=()=>$('#setIn').click());
  $('#setIn').onchange=e=>{const f=e.target.files[0];e.target.value='';if(f)readSettings(f);};
  $('#sheet').addEventListener('click',e=>{const b=e.target.closest('[data-s]');if(e.target.id==='sheet'||(b&&b.dataset.s==='close'))return closeSheet();
    if(b&&pending&&(b.dataset.s==='merge'||b.dataset.s==='replace')){state.ignored=b.dataset.s==='replace'?pending.ignored:Object.assign({},pending.ignored,state.ignored);saveState();closeSheet();banner(b.dataset.s==='replace'?'Settings replaced.':'Settings merged.');if(R)render();}});
  document.addEventListener('keydown',e=>{if(e.key==='Escape')closeSheet();});
  document.addEventListener('click',e=>{
    const t=e.target.closest('[data-tab],[data-ignore],[data-unignore]');if(!t)return;
    if(t.dataset.tab)return setTab(t.dataset.tab);
    if(t.dataset.ignore){const i=R.issues.find(x=>fp(x)===t.dataset.ignore);if(i){state.ignored[fp(i)]={title:i.title,file:i.file,at:new Date().toISOString().slice(0,10)};saveState();render();}}
    if(t.dataset.unignore){delete state.ignored[t.dataset.unignore];saveState();render();}
  });
  document.addEventListener('change',e=>{
    if(e.target.dataset.f){filt[e.target.dataset.f]=e.target.checked;render();}
    if(e.target.id==='grp'){filt.group=e.target.value;$('#list').innerHTML=renderList();}
    if(e.target.classList.contains('themeSel')){try{localStorage.setItem(TKEY,e.target.value);}catch(err){}applyTheme(e.target.value);}
  });
  document.addEventListener('input',e=>{if(e.target.id==='q'){filt.q=e.target.value;$('#list').innerHTML=renderList();}});
  try{localStorage.setItem('sqlcheck.__t','1');localStorage.removeItem('sqlcheck.__t');}catch(e){storageOk=false;banner();}
  applyTheme();
  if(window.matchMedia)try{matchMedia('(prefers-color-scheme: dark)').addEventListener('change',()=>applyTheme());}catch(e){}
  setInterval(()=>{if(document.visibilityState==='visible'&&phaseNow()!==themePhase)applyTheme();},60000);
});
