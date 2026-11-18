/* ===== Workflow rules: mirrors run_sql_folder() in sachGiaoKhoa.py ===== */
const WF={
  configs:{debug:'debug.py',refresh:'refresh_list.py',output:'output.py',standalone:'standalone.py'},
  modes:['production','debug','standalone']
};
function lev(a,b){const m=a.length,n=b.length,d=[];for(let i=0;i<=m;i++){d[i]=[i];}for(let j=1;j<=n;j++)d[0][j]=j;for(let i=1;i<=m;i++)for(let j=1;j<=n;j++)d[i][j]=Math.min(d[i-1][j]+1,d[i][j-1]+1,d[i-1][j-1]+(a[i-1]===b[j-1]?0:1));return d[m][n];}
function lineStarts(t){const ls=[0];for(let i=0;i<t.length;i++)if(t[i]==='\n')ls.push(i+1);return ls;}
function posToLine(ls,p){let lo=0,hi=ls.length-1;while(lo<hi){const m=(lo+hi+1)>>1;if(ls[m]<=p)lo=m;else hi=m-1;}return {line:lo+1,col:p-ls[lo]};}
function pyCmp(a,b){return a<b?-1:a>b?1:0;}

function checkFolder(files,opts){
  opts=opts||{};
  const issues=[];
  const add=(sev,rule,file,line,title,detail,hint,extra)=>issues.push(Object.assign({sev,rule,file:file||'',line:line||0,title,detail:detail||'',hint:hint||''},extra||{}));
  const byKey=new Map(files.map(f=>[f.key,f]));
  const top=files.filter(f=>!f.key.includes('/'));
  const topNames=new Map(top.map(f=>[f.key,f]));
  const folder=opts.folder||'';
  // ---------------- files the runner picks up ----------------
  const sqlFiles=top.filter(f=>/\.sql$/.test(f.key)&&!f.key.startsWith('.')).sort((a,b)=>pyCmp(a.key,b.key));
  files.forEach(f=>{
    if(f.key.includes('/')&&/\.sql$/i.test(f.key))add('info','file-ignored',f.key,0,'Not run: it is in a subfolder','Only .sql files directly in the folder are run.');
    else if(!f.key.includes('/')&&/\.sql$/i.test(f.key)&&!/\.sql$/.test(f.key))add('warn','file-ignored',f.key,0,'Not run: the extension is "'+f.key.split('.').pop()+'", not ".sql"','The runner looks for "*.sql" in lower case, so this file is skipped.','Rename it to end with .sql.');
    else if(!f.key.includes('/')&&/\.txt$/i.test(f.key)&&/^\d/.test(f.key)&&/\b(select|with)\b[\s\S]*\bfrom\b/i.test(f.text))add('info','file-ignored',f.key,0,'Not run: SQL saved as .txt','Only .sql files are run. Ignore this if the file is parked on purpose.');
  });
  // script records
  const scripts=sqlFiles.map((f,idx)=>{
    const parts=f.key.split('_');
    const source=parts[1]||'';
    const table=parts.slice(2).join('_').split('.sql').join('');
    return {f,key:f.key,idx,order:parts[0],source,engine:source==='impala'?'impala':'oracle',table,tl:table.toLowerCase()};
  });
  scripts.forEach(s=>{
    if(s.f.key.split('_').length<3)add('error','file-name',s.key,0,'File name doesn’t follow <order>_<source>_<table_name>.sql','The runner splits the name on "_": part 2 is the engine and the rest is the table name. With fewer parts the table name is empty, so the script fails.','Rename it, e.g. 20290101_impala_my_table.sql.');
    else{
      if(s.source!=='impala'&&s.source!=='oracle')add('error','file-engine',s.key,0,'Engine "'+s.source+'" in the file name is not impala or oracle','Anything that isn’t exactly "impala" is sent to Oracle, so this script will run on the wrong database.','Rename the file with _impala_ or _oracle_.'+(lev(s.source.toLowerCase(),'impala')<=2?' (Looks like a typo of "impala".)':lev(s.source.toLowerCase(),'oracle')<=2?' (Looks like a typo of "oracle".)':''));
      if(!/^[A-Za-z_][A-Za-z0-9_]*$/.test(s.table))add('error','file-table',s.key,0,'Table name "'+s.table+'" is not a valid name','It becomes a view or table name, which can only use letters, digits and "_" and must not start with a digit.','Rename the file.');
      if(!/^\d+$/.test(s.order))add('warn','file-order',s.key,0,'The file name doesn’t start with a number','Files run in alphabetical order of the whole name, so this one may run at an unexpected point.');
    }
  });
  const byTable=new Map();scripts.forEach(s=>{if(!byTable.has(s.tl))byTable.set(s.tl,[]);byTable.get(s.tl).push(s);});
  byTable.forEach((arr,t)=>{if(arr.length>1)add('warn','dup-table',arr[1].key,0,'Two files build the same table "'+t+'"','Both run: '+arr.map(s=>s.key).join(' and ')+'. The later one replaces the earlier one.','Delete or rename the old copy.');});
  // ---------------- python files ----------------
  const py={};
  const wfPy=k=>/^raya.*\.py$/i.test(k)||Object.values(WF.configs).includes(k)||/^sachgiaokhoa\.py$/i.test(k);
  top.filter(f=>/\.py$/i.test(f.key)).forEach(f=>{
    const r=pyParse(f.text);py[f.key]=r;
    const wf=wfPy(f.key);
    if(!r.ok){
      const e=r.error;const sv=wf&&f.key!==WF.configs.standalone?'error':'warn';
      add(sv,'py-syntax',f.key,e.line,'Python syntax error: '+e.message,
        f.key===WF.configs.standalone?'The runner reads standalone.py inside try/except, so this error is hidden: standalone mode silently runs nothing.':wf?(/^raya/i.test(f.key)?'The executor won’t start at all.':'The runner exec()s this file, so the whole run stops here.'):'This file isn’t part of the run, but it won’t work either.',e.hint,{col:e.col});
    }
    r.warnings.forEach(w=>add('warn',w.kind==='implicit-concat'?'py-concat':'py-warn',f.key,w.line,w.msg,'',w.hint));
  });
  // ---------------- config files ----------------
  const cfgVal=(file,name,required)=>{
    const f=topNames.get(file);
    if(!f){if(required)add('error','cfg-missing',file,0,file+' is missing','The runner opens '+file+' at the start of every run, so the run stops with "file not found".');return {missing:true};}
    const r=py[file];if(!r||!r.ok)return {broken:true};
    const assigns=pyTopAssigns(r.body);
    if(!assigns.has(name)){add(file===WF.configs.standalone?'warn':'error','cfg-name',file,1,file+' doesn’t define "'+name+'"','The runner reads the variable "'+name+'" from this file'+(file===WF.configs.standalone?'; without it, standalone mode silently runs nothing.':', so the run stops with KeyError.'),'Make sure the file has a line like '+name+' = ...'+(([...assigns.keys()].length)?' (found: '+[...assigns.keys()].join(', ')+')':''));return {};}
    const node=assigns.get(name);return {node,val:pyLit(node),line:node.line};
  };
  const dbg=cfgVal(WF.configs.debug,'debug',true);
  const sta=cfgVal(WF.configs.standalone,'standalone',false);
  const rfl=cfgVal(WF.configs.refresh,'refresh_list',true);
  const out=cfgVal(WF.configs.output,'output',true);
  const modeLists={debug:{exact:[],exclude:[],ok:false},standalone:{exact:[],exclude:[],ok:false}};
  [['debug',dbg,WF.configs.debug],['standalone',sta,WF.configs.standalone]].forEach(([m,c,file])=>{
    if(!c.node)return;
    if(c.node.k!=='Dict'){add(m==='debug'?'error':'warn','cfg-shape',file,c.line,'"'+m+'" must be a dict like {"exact_table": [...], "exclude_table": [...]}','The runner calls '+m+'.get(...), which only works on a dict.');return;}
    const v=c.val||{};
    ['exact_table','exclude_table'].forEach(k=>{
      const kv=c.node.keys.findIndex(x=>x&&x.k==='Const'&&x.v===k);
      if(kv<0)return;
      const ln=c.node.vals[kv];
      if(!['List','Tuple','Set'].includes(ln.k)){add('warn','cfg-shape',file,ln.line,'"'+k+'" in '+file+' should be a list of table names','');return;}
      const arr=(pyLit(ln)||[]).filter(x=>typeof x==='string');
      modeLists[m][k==='exact_table'?'exact':'exclude']=arr;
    });
    const typo=c.node.keys.filter(x=>x&&x.k==='Const'&&typeof x.v==='string'&&!['exact_table','exclude_table'].includes(x.v));
    typo.forEach(x=>add('warn','cfg-key',file,x.line,'Unknown key "'+x.v+'" in '+file,'The runner only reads "exact_table" and "exclude_table", so this list is ignored.',lev(x.v,'exact_table')<=3?'Did you mean "exact_table"?':lev(x.v,'exclude_table')<=3?'Did you mean "exclude_table"?':''));
    modeLists[m].ok=true;
  });
  let refreshList=[];
  if(rfl.node){if(!['List','Tuple','Set'].includes(rfl.node.k)&&rfl.val===undefined)add('info','cfg-shape',WF.configs.refresh,rfl.line,'Couldn’t read refresh_list as a plain list','');else refreshList=(rfl.val||[]).filter(x=>typeof x==='string');
    const seen=new Set();refreshList.forEach(x=>{if(seen.has(x.toLowerCase()))add('info','cfg-dup',WF.configs.refresh,rfl.line,'"'+x+'" is listed twice in refresh_list','Harmless; it is just refreshed twice.');seen.add(x.toLowerCase());});}
  const outMap=new Map();
  if(out.node){
    if(out.node.k!=='List'&&out.node.k!=='Tuple')add('error','cfg-shape',WF.configs.output,out.line,'"output" must be a list of {"T": ..., "C": ...}','The runner loops over it and reads m["T"] and m["C"].');
    else{
      if(!out.node.elts.length)add('error','cfg-empty',WF.configs.output,out.line,'"output" is empty','For every script the runner looks up its partition column in this list; with an empty list that lookup crashes (IndexError) and the whole run stops.','Keep at least one entry.');
      out.node.elts.forEach(e=>{
        if(e.k!=='Dict'){add('error','cfg-shape',WF.configs.output,e.line,'An item in "output" is not a {"T": ..., "C": ...} dict','The runner reads m["T"] from every item, so the run stops.');return;}
        const get=k=>{const i=e.keys.findIndex(x=>x&&x.k==='Const'&&x.v===k);return i<0?undefined:e.vals[i];};
        const T=get('T'),C=get('C');
        if(!T||!C){add('error','cfg-key',WF.configs.output,e.line,'An item in "output" is missing '+(!T&&!C?'"T" and "C"':!T?'"T"':'"C"'),'The runner reads m["T"] and m["C"] from every item, so the run stops with KeyError.',e.keys.filter(x=>x&&x.k==='Const').map(x=>x.v).filter(k=>k!=='T'&&k!=='C').length?'Found keys: '+e.keys.filter(x=>x&&x.k==='Const').map(x=>JSON.stringify(x.v)).join(', ')+'. Keys are case-sensitive.':'');return;}
        const t=pyLit(T),c=pyLit(C);
        if(typeof t!=='string')return;
        if(outMap.has(t.toLowerCase()))add('warn','cfg-dup',WF.configs.output,e.line,'"'+t+'" is listed twice in output','Only the first partition column is used.');
        else outMap.set(t.toLowerCase(),{t,c:typeof c==='string'?c:null,line:e.line});
        if(typeof c==='string'&&!c.trim())add('error','cfg-key',WF.configs.output,e.line,'"'+t+'" has an empty partition column "C"','Writing a table partitioned by "" fails.');
      });
    }
  }
  outMap.forEach((o,t)=>{if(!byTable.has(t))add('warn','out-unmatched',WF.configs.output,o.line,'"'+o.t+'" in output.py has no matching .sql file','Nothing builds it, so nothing is written for it. Check the spelling against the file names.',nearest(t));});
  function nearest(t){let best=null,bd=99;byTable.forEach((_,k)=>{const d=lev(k,t);if(d<bd){bd=d;best=k;}});return best&&bd<=Math.max(2,Math.floor(t.length/8))?'Closest file table name: "'+best+'".':'';}
  // ---------------- executor (raya*.py) ----------------
  const rayaFiles=top.filter(f=>/^raya.*\.py$/i.test(f.key));
  const runs=[];const viewsByStep=[];
  if(!rayaFiles.length)add('info','raya-missing','',0,'No raya*.py found','The checks below assume one production run with no text replacements.');
  rayaFiles.forEach(f=>{
    const r=py[f.key];if(!r||!r.ok)return;
    const env=new Map();
    const resolve=(n,depth)=>{depth=depth||0;if(!n||depth>8)return undefined;
      if(n.k==='Const')return typeof n.v==='string'?n.v:undefined;
      if(n.k==='Name'){const v=env.get(n.id);return v?resolve(v,depth+1):undefined;}
      if(n.k==='BinOp'&&n.op==='+'){const a=resolve(n.l,depth+1),b=resolve(n.r,depth+1);return a!==undefined&&b!==undefined?a+b:undefined;}
      return undefined;};
    const nodeOf=(n,depth)=>{depth=depth||0;if(n&&n.k==='Name'&&env.has(n.id)&&depth<8)return nodeOf(env.get(n.id),depth+1);return n;};
    const fnName=c=>c.func.k==='Name'?c.func.id:c.func.k==='Attr'?c.func.attr:'';
    const walk=(n,fn)=>{if(!n||typeof n!=='object')return;if(Array.isArray(n)){n.forEach(x=>walk(x,fn));return;}if(n.k)fn(n);for(const key of ['value','func','args','obj','l','r','v','elts','keys','vals','targets','body','orelse']){if(n[key])walk(n[key],fn);}if(n.kws)n.kws.forEach(kw=>walk(kw.value,fn));};
    const fileFromPath=p=>{if(typeof p!=='string')return null;const base=p.split('/').pop();
      const root=runs.length?runs[0].path:resolve({k:'Name',id:'master_path'});
      if(root){const rt=root.endsWith('/')?root:root+'/';if(p.startsWith(rt))return p.slice(rt.length);if(p.startsWith(root))return p.slice(root.length);}
      if(!p.includes('/'))return p;
      if(folder&&p.includes('/'+folder+'/'))return p.split('/'+folder+'/').pop();
      return byKey.has(base)?base:null;};
    r.body.forEach((st,si)=>{
      // calls first (they use env as it is before this statement's assignment)
      walk(st,n=>{
        if(n.k!=='Call')return;
        const name=fnName(n);
        if(name==='run_sql_folder'){
          const arg=(i,kw)=>{const k=n.kws.find(x=>x.name===kw);return k?k.value:n.args[i];};
          const pN=arg(0,'path'),mN=arg(1,'mode'),vN=arg(4,'sql_var');
          const path=resolve(pN),mode=mN?resolve(mN):undefined;
          const run={file:f.key,line:n.line,path,mode:mode===undefined?(mN?null:'None'):mode,items:[],sqlVarMissing:!vN,stepIndex:si};
          if(path!==undefined){
            if(!path.endsWith('/'))add('error','raya-path',f.key,n.line,'The folder path doesn’t end with "/"','The runner builds file paths as path + "debug.py" and path + "*.sql", so without the "/" it looks in the wrong place and stops.','Add "/" at the end of master_path.');
            const last=path.replace(/\/+$/,'').split('/').pop();
            if(folder&&last&&last!==folder)add('warn','raya-folder',f.key,n.line,'The run points at folder "'+last+'", but you loaded "'+folder+'"','The checks use the files you loaded. If the run really uses "'+last+'", you’re checking the wrong folder.');
          }
          if(run.mode!==null&&run.mode!=='None'&&!['debug','standalone'].includes(run.mode)&&run.mode!=='production'){
            const near=['debug','standalone','production'].find(m=>lev(String(run.mode).toLowerCase(),m)<=2);
            add('warn','raya-mode',f.key,n.line,'mode = "'+run.mode+'" is not production, debug or standalone','Any value other than exactly "debug" or "standalone" runs EVERY script.',near?'Did you mean "'+near+'"? (It is case-sensitive.)':'');
          }
          if(!vN)add('error','raya-sqlvar',f.key,n.line,'run_sql_folder is called without sql_var','The runner loops over sql_var to replace text; with None it crashes on the first script.');
          else{
            const lst=nodeOf(vN);
            if(!lst||!['List','Tuple'].includes(lst.k))add('info','raya-sqlvar',f.key,n.line,'Couldn’t read sql_var as a list','Replacement checks are skipped for this run.');
            else lst.elts.forEach((e,ii)=>{
              if(e.k!=='Dict'){add('error','raya-sqlvar',f.key,e.line,'sql_var item #'+(ii+1)+' is not a {"from": ..., "to": ...} dict','The runner reads item["from"], so the run stops.');return;}
              const gi=k=>{const q=e.keys.findIndex(x=>x&&x.k==='Const'&&x.v===k);return q<0?undefined:e.vals[q];};
              const fr=gi('from'),to=gi('to');
              if(!fr||!to){add('error','raya-sqlvar',f.key,e.line,'sql_var item #'+(ii+1)+' is missing '+(!fr&&!to?'"from" and "to"':!fr?'"from"':'"to"'),'The runner reads item["from"] and item["to"], so it crashes with KeyError. Keys are case-sensitive.');return;}
              const frv=resolve(fr);
              if(fr.k==='Const'&&typeof fr.v!=='string'){add('error','raya-sqlvar',f.key,e.line,'"from" in sql_var item #'+(ii+1)+' is not text','str.replace() needs text here, so the run crashes.');return;}
              if(frv==='')add('error','raya-sqlvar',f.key,e.line,'"from" in sql_var item #'+(ii+1)+' is an empty string','Replacing "" inserts the value between every character of every script.');
              let toVal=null,toKind='dynamic',toFile=null;
              const tn=nodeOf(to);
              if(tn.k==='Const'&&typeof tn.v==='string'){toVal=tn.v;toKind='text';}
              else if(tn.k==='Call'&&fnName(tn)==='from_text'){const pth=resolve(tn.args[0]);const rel=fileFromPath(pth);if(rel&&byKey.has(rel)){toVal=byKey.get(rel).text;toKind='file';toFile=rel;}}
              else if(tn.k==='Call'&&fnName(tn)==='before_6_pm_2_1'){toVal='1=1';toKind='6pm';}
              run.items.push({from:frv,line:e.line,idx:ii+1,toVal,toKind,toFile});
            });
          }
          runs.push(run);
        }
        if(name==='dtc_'||name==='createOrReplaceTempView'){const v=resolve(n.args[0]);if(v)viewsByStep.push({name:v.toLowerCase(),step:si,line:n.line,file:f.key});}
        if(name==='from_text'){
          const pth=resolve(n.args[0]);
          if(pth!==undefined){const rel=fileFromPath(pth);if(rel&&!byKey.has(rel))add('error','raya-file',f.key,n.line,'from_text() reads "'+rel+'", which is not in the folder','That line fails with "file not found".',nearestFile(rel));}
        }
        if(name==='replace'&&n.func.k==='Attr'){
          // from_text(x).replace(a,b)[.replace(c,d)...]
          let base=n.func.obj,froms=[resolve(n.args[0])];
          while(base&&base.k==='Call'&&fnName(base)==='replace'&&base.func.k==='Attr'){froms.push(resolve(base.args[0]));base=base.func.obj;}
          if(base&&base.k==='Call'&&fnName(base)==='from_text'){
            const rel=fileFromPath(resolve(base.args[0]));
            if(rel&&byKey.has(rel)&&/\.sql$/.test(rel)){
              const prevMr=manualReplaces.find(m=>m.file===f.key&&m.line===n.line&&m.target===rel);
              if(prevMr){froms.forEach(x=>{if(x&&!prevMr.froms.includes(x))prevMr.froms.push(x);});}
              else manualReplaces.push({file:f.key,line:n.line,target:rel,froms:froms.filter(Boolean)});
            }
          }
        }
      });
      if(st.k==='Assign')st.targets.forEach(t=>{if(t.k==='Name')env.set(t.id,st.value);});
    });
  });
  function nearestFile(rel){let best=null,bd=99;byKey.forEach((_,k)=>{const d=lev(k,rel);if(d<bd){bd=d;best=k;}});return best&&bd<=4?'Closest file in the folder: "'+best+'".':'';}
  if(!runs.length&&rayaFiles.length&&rayaFiles.every(f=>py[f.key]&&py[f.key].ok))add('info','raya-norun',rayaFiles[0].key,0,'No run_sql_folder(...) call found','The checks below assume one production run with no text replacements.');
  const rayaBroken=rayaFiles.some(f=>py[f.key]&&!py[f.key].ok);
  if(!runs.length)runs.push({file:'',line:0,path:null,mode:'production',items:[],assumed:true,noVars:rayaBroken,stepIndex:0});
  if(rayaBroken)add('info','raya-broken','',0,'Text replacements were not checked','The executor has a syntax error, so its sql_var lists couldn\u2019t be read. Fix it and check again.');
  // ---------------- sql_var item checks per run ----------------
  const allFroms=new Map();runs.forEach(r=>r.items.forEach(it=>{if(it.from&&!allFroms.has(it.from))allFroms.set(it.from,r);}));
  runs.forEach((run,ri)=>{
    const seen=new Map();
    run.items.forEach((it,ii)=>{
      if(!it.from)return;
      if(seen.has(it.from)){add('warn','var-dup',run.file,it.line,'"'+it.from+'" is replaced twice in the same sql_var list','Item #'+seen.get(it.from).idx+' (line '+seen.get(it.from).line+') already replaces every "'+it.from+'", so this one never finds anything to replace.','Delete one of them.');return;}
      run.items.slice(0,ii).forEach(prev=>{if(prev.from&&prev.from!==it.from&&it.from.includes(prev.from))add('warn','var-order',run.file,it.line,'"'+it.from+'" is broken by an earlier replacement','Item #'+prev.idx+' replaces "'+prev.from+'" first, which is part of "'+it.from+'", so this item never matches.','Move this item above #'+prev.idx+'.');});
      seen.set(it.from,it);
    });
  });
  // ---------------- simulate each run ----------------
  const OUT_SET=new Set(outMap.keys());
  const outBroken=!out.node||out.broken||(py[WF.configs.output]&&!py[WF.configs.output].ok);
  const linted=new Set(),reported=new Set();
  const viewsGlobal=new Set();
  const once=(key,fn)=>{if(reported.has(key))return;reported.add(key);fn();};
  const runSummaries=[];
  runs.forEach((run,ri)=>{
    viewsByStep.filter(v=>v.step<run.stepIndex).forEach(v=>viewsGlobal.add(v.name));
    const mode=run.mode==='debug'||run.mode==='standalone'?run.mode:'production';
    const ml=mode==='production'?null:modeLists[mode];
    const selected=scripts.filter(s=>!ml||(ml.exact.map(x=>x.toLowerCase()).includes(s.tl)&&!ml.exclude.map(x=>x.toLowerCase()).includes(s.tl)));
    const selSet=new Set(selected);
    if(ml){
      const listFile=WF.configs[mode];
      ml.exact.concat(ml.exclude).forEach(x=>{if(!byTable.has(x.toLowerCase()))once('ml|'+mode+'|'+x,()=>add('warn','mode-unmatched',listFile,0,'"'+x+'" in '+listFile+' matches no .sql file','So it has no effect in '+mode+' mode.',nearest(x.toLowerCase())));});
      if(!selected.length)add('warn','mode-empty',run.file,run.line,'This '+mode+' run selects no scripts','exact_table in '+listFile+' is empty or matches nothing (after exclude_table), so the run does nothing.');
    }
    // how often each item matches, over ALL scripts (the lists are shared between modes)
    const counts=run.items.map(()=>0);
    scripts.forEach(s=>{let t=s.f.text;run.items.forEach((it,ii)=>{if(!it.from)return;const c=t.split(it.from).length-1;if(c){counts[ii]+=c;t=t.split(it.from).join(it.toVal!=null?it.toVal:SQL_VAR);}});});
    const views=new Set(viewsGlobal);
    const label=mode+(runs.length>1?' run (line '+run.line+')':' run');
    selected.forEach(s=>{
      // replacement simulation with position mapping
      let text=s.f.text;let map=null;
      run.items.forEach((it,ii)=>{
        if(!it.from)return;
        const rep=it.toVal!=null?it.toVal:SQL_VAR;
        if(!text.includes(it.from))return;
        let o='',m=[],pos=0,j;
        if(!map){map=new Int32Array(text.length);for(let q=0;q<text.length;q++)map[q]=q;}
        while((j=text.indexOf(it.from,pos))>=0){
          o+=text.slice(pos,j);for(let q=pos;q<j;q++)m.push(map[q]);
          o+=rep;for(let q=0;q<rep.length;q++)m.push(map[j]);
          pos=j+it.from.length;
        }
        o+=text.slice(pos);for(let q=pos;q<text.length;q++)m.push(map[q]);
        text=o;map=Int32Array.from(m);
      });
      const ls=s.ls||(s.ls=lineStarts(s.f.text));
      const lineOf=p=>posToLine(ls,map?(map[Math.min(p,map.length-1)]||0):p);
      const res=sqlLint(text,s.engine);
      if(!linted.has(s.key)){
        linted.add(s.key);
        res.issues.forEach(i=>{if(run.noVars&&i.rule==='sql-placeholder')return;const lc=lineOf(i.pos);add(i.sev,i.rule,s.key,lc.line,i.msg,s.engine==='oracle'?'Oracle script.':'Runs through spark.sql().',i.hint,{col:lc.col});});
        // oracle: last line is a -- comment with no newline
        if(s.engine==='oracle'){const lastNl=text.lastIndexOf('\n'),lastLine=text.slice(lastNl+1);if(/--/.test(lastLine.replace(/'[^']*'/g,''))&&!/\n\s*$/.test(text))add('error','sql-oracle-comment',s.key,lineOf(text.length-1).line,'The file ends with a "--" comment and no line break','The Oracle query is sent as "(" + your query + ")". The ")" lands inside your comment, so the query is never closed.','Press Enter after the last line, or remove the comment.');}
        // leftover placeholders from other runs
        allFroms.forEach((r2,fr)=>{if(r2===run)return;if(run.items.some(it=>it.from===fr))return;const j=text.indexOf(fr);if(j>=0&&!/^[A-Za-z0-9_]/.test(text[j+fr.length]||'')&&!/[A-Za-z0-9_]$/.test(text.slice(Math.max(0,j-1),j)))add('warn','var-other-run',s.key,lineOf(j).line,'"'+fr+'" is not replaced in the '+label,'Only the other sql_var list (line '+r2.line+') replaces it, so in this run the raw text reaches the database.');});
        // output partition column
        const o=outMap.get(s.tl);
        if(o&&o.c){const hasStar=/select\s+(\w+\.)?\*/i.test(text.replace(/--[^\n]*/g,''));if(!new RegExp('(^|[^A-Za-z0-9_])'+o.c.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'($|[^A-Za-z0-9_])','i').test(text)&&!hasStar)add('error','out-partition',s.key,0,'Partition column "'+o.c+'" is not in this query','output.py says "'+o.t+'" is partitioned by "'+o.c+'", but the query never mentions that column, so writing the table fails.','Add the column to the SELECT, or fix "C" in output.py.');}
      }
      // dependencies (per run)
      res.refs.forEach(ref=>{
        const name=ref.name;if(res.ctes.has(name))return;
        const ln=lineOf(ref.pos).line;
        const parts=name.split('.');
        if(parts.length===1){
          const prods=byTable.get(name);
          if(s.engine==='oracle'){
            if(prods||views.has(name)||viewsByStep.some(v=>v.name===name))once('ora|'+s.key+'|'+name,()=>add('error','dep-oracle-view',s.key,ln,'Oracle can’t read "'+name+'"','"'+name+'" is built by this pipeline as a Spark view (or af_mon table). Oracle scripts run inside Oracle and can’t see it.','Move this logic into an impala script, or read the data from an Oracle table.'));
            return;
          }
          if(prods){
            const p=prods[0];
            if(p===s)return;
            if(outBroken)return;
            if(OUT_SET.has(name)){once('out|'+s.key+'|'+name,()=>add('error','dep-output-bare',s.key,ln,'"'+name+'" is an output table, so there is no view with that name','Output tables are written to af_mon.'+name+' and no temporary view is created. Reading "'+name+'" without "af_mon." fails.','Write af_mon.'+name+' instead.'));return;}
            if(views.has(name))return;
            if(p.idx>s.idx)once('late|'+s.key+'|'+name+'|'+mode,()=>add('error','dep-later',s.key,ln,'"'+name+'" is built later in the run','It is built by '+p.key+', which runs after this script, so the view doesn’t exist yet ('+label+').','Rename the files so the order number of '+p.key+' is smaller.'));
            else if(!selSet.has(p))once('skip|'+s.key+'|'+name+'|'+mode,()=>add('error','dep-skipped',s.key,ln,'"'+name+'" is not built in the '+label,p.key+' is not selected in '+mode+' mode, and temporary views only exist inside the run, so this fails.','Add "'+p.table+'" to exact_table in '+(WF.configs[mode]||'the list')+'.'));
            return;
          }
          if(views.has(name)||viewsByStep.some(v=>v.name===name))return;
          once('unk|'+s.key+'|'+name,()=>add('warn','dep-unknown',s.key,ln,'"'+name+'" has no schema and no script builds it','Without a schema, Spark looks for a temporary view or a table in the default database. If neither exists the script fails.',nearest(name)||'Add the schema (e.g. af_mon.'+name+') or check the spelling.'));
        }else if(parts.length===2&&parts[0]==='af_mon'&&byTable.has(parts[1])){
          const t=parts[1],p=byTable.get(t)[0];
          if(s.engine==='oracle'){once('oraafmon|'+s.key+'|'+name,()=>add('error','dep-oracle-view',s.key,ln,'Oracle can’t read "'+name+'"','af_mon is a Hive/Impala schema. Oracle scripts run inside Oracle and can’t see it.'));return;}
          if(outBroken)return;
          if(!OUT_SET.has(t)){if(!(p.idx===s.idx))once('nomon|'+s.key+'|'+name,()=>add('warn','dep-not-output',s.key,ln,'"'+name+'" is never written by this pipeline','"'+t+'" is not in output.py, so it only exists as a temporary view called "'+t+'". af_mon.'+t+' is either an old copy or doesn’t exist.','Read "'+t+'" without af_mon., or add it to output.py.'));return;}
          const isBackup=/_backup$/.test(s.tl)&&(s.tl.replace(/_backup$/,'')===t||s.tl.replace(/_backup$/,'')+'_final'===t);
          if(!isBackup&&p!==s&&(p.idx>s.idx||!selSet.has(p)))once('prev|'+s.key+'|'+name+'|'+mode,()=>add('info','dep-previous',s.key,ln,'Reads af_mon.'+t+' from the previous run','It is rewritten by '+p.key+(p.idx>s.idx?' later in this run':' which doesn’t run in '+mode+' mode')+', so this script sees yesterday’s data. Fine if intended.'));
        }
      });
      // after this script runs: a view exists only for non-output tables
      if(!OUT_SET.has(s.tl))views.add(s.tl);
    });
    selected.forEach(s=>{if(!OUT_SET.has(s.tl))viewsGlobal.add(s.tl);});
    // replacements that never matched
    run.items.forEach((it,ii)=>{if(!it.from)return;if(counts[ii]===0&&!issues.some(x=>x.rule==='var-dup'&&x.line===it.line&&x.file===run.file)&&!issues.some(x=>x.rule==='var-order'&&x.line===it.line&&x.file===run.file))add('warn','var-unused',run.file,it.line,'"'+it.from+'" is not found in any .sql file','This replacement does nothing ('+label+').','Check the spelling against the SQL, or remove the line.');});
    // partial-word matches
    run.items.forEach(it=>{
      if(!it.from||!/^[A-Za-z0-9_]/.test(it.from)||!/[A-Za-z0-9_]$/.test(it.from))return;
      const esc=it.from.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
      const re=new RegExp('[A-Za-z0-9_]*'+esc+'[A-Za-z0-9_]*','g');
      for(const s of selected){
        let m;const txt=s.f.text;re.lastIndex=0;
        while((m=re.exec(txt))){if(m[0]!==it.from){const ln=posToLine(s.ls||(s.ls=lineStarts(txt)),m.index).line;once('part|'+it.from+'|'+m[0],()=>add('warn','var-partial',s.key,ln,'"'+it.from+'" also replaces part of "'+m[0]+'"','The replacement is plain text, so it also changes "'+m[0]+'" into something else (sql_var line '+it.line+' in '+run.file+').','Use a more unique placeholder name, e.g. ${'+it.from+'}.'));break;}}
      }
    });
    runSummaries.push({file:run.file,line:run.line,mode,label,selected:selected.length,total:scripts.length,items:run.items.length,assumed:!!run.assumed,sel:selSet});
  });
  // manual from_text(...).replace(...) in raya
  manualReplaces.forEach(mr=>{
    const txt=byKey.get(mr.target).text;
    const missing=[...allFroms.keys()].filter(fr=>!mr.froms.includes(fr)&&txt.includes(fr));
    if(missing.length)add('warn','raya-manual-replace',mr.file,mr.line,'This line runs '+mr.target+' but only replaces '+mr.froms.map(x=>'"'+x+'"').join(', '),'The file also contains '+missing.map(x=>'"'+x+'"').join(', ')+', which stay as raw text here (sql_var is not applied on this line).','Add .replace(...) for them, or use replace_sql_var(from_text(...), sql_var).');
  });
  const plan=scripts.map(s=>({key:s.key,engine:s.engine,source:s.source,table:s.table,output:OUT_SET.has(s.tl),part:(outMap.get(s.tl)||{}).c||'',runs:runSummaries.map(r=>r.sel.has(s))}));
  runSummaries.forEach(r=>delete r.sel);
  return {issues,runs:runSummaries,plan,scripts:scripts.length,files:files.length,pyFiles:Object.keys(py).length};
}
var manualReplaces=[];
function runCheck(files,opts){manualReplaces=[];return checkFolder(files,opts);}
if(typeof module!=='undefined'){const {pyParse,pyLit,pyTopAssigns}=require('./pyparse.js');const {sqlLint,SQL_VAR}=require('./sqlcheck.js');Object.assign(global,{pyParse,pyLit,pyTopAssigns,sqlLint,SQL_VAR});module.exports={runCheck};}
