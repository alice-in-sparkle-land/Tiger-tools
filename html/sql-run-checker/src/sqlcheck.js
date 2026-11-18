/* ===== SQL tokenizer + "will it run" checks ===== */
const SQL_VAR='__SQLVAR__';
const SQL_KW=new Set(('ALL AND ANTI ANY ARRAY AS ASC AT AUTHORIZATION BETWEEN BOTH BY CASE CAST CHECK CLUSTER COLLATE COLUMN CONNECT CONSTRAINT CREATE CROSS CUBE CURRENT DESC DISTINCT DISTRIBUTE DIV DROP ELSE END ESCAPE EXCEPT EXISTS FALSE FETCH FILTER FIRST FOLLOWING FOR FROM FULL GLOBAL GRANT GROUP GROUPING HAVING IF ILIKE IN INNER INSERT INTERSECT INTERVAL INTO IS JOIN LAST LATERAL LEADING LEFT LIKE LIMIT MINUS NATURAL NOT NULL NULLS OF OFFSET ON ONLY OR ORDER OUTER OVER OVERWRITE PARTITION PIVOT PRECEDING PRIOR QUALIFY RANGE RECURSIVE REGEXP RIGHT RLIKE ROLLUP ROW ROWS SELECT SEMI SET SETS SOME SORT START TABLE TABLESAMPLE THEN TO TRAILING TRUE UNBOUNDED UNION UNKNOWN UNPIVOT USING VALUES VIEW WHEN WHERE WINDOW WITH WITHIN').split(' '));
const SQL_STOP=new Set('WHERE GROUP ORDER HAVING LIMIT UNION INTERSECT EXCEPT MINUS JOIN INNER LEFT RIGHT FULL CROSS NATURAL ON USING WINDOW QUALIFY FETCH OFFSET CONNECT START PIVOT UNPIVOT LATERAL'.split(' '));
const SQL_FN_FROM=new Set('EXTRACT TRIM SUBSTRING SUBSTR POSITION OVERLAY'.split(' '));

function sqlTokenize(text,engine){
  const toks=[],errors=[],n=text.length;let i=0;
  const push=(t,v,s,e)=>toks.push({t,v,u:t==='w'?v.toUpperCase():v,s,e});
  while(i<n){
    const c=text[i],d=text[i+1];
    if(c===' '||c==='\t'||c==='\n'||c==='\r'||c==='\f'){i++;continue;}
    if(c==='-'&&d==='-'){while(i<n&&text[i]!=='\n')i++;continue;}
    if(c==='/'&&d==='*'){const j=text.indexOf('*/',i+2);if(j<0){errors.push({rule:'sql-unclosed-comment',pos:i,msg:'A /* comment is never closed',hint:'Everything after this point is treated as a comment, so the query is cut off. Add the closing */.'});break;}i=j+2;continue;}
    if(c==="'"||(c==='"'&&engine!=='oracle')){
      let j=i+1,ok=false;
      while(j<n){const ch=text[j];if(ch==='\\'&&engine!=='oracle'){j+=2;continue;}if(ch===c){if(text[j+1]===c){j+=2;continue;}ok=true;break;}j++;}
      if(!ok){errors.push({rule:'sql-unclosed-string',pos:i,msg:'A string that starts with '+c+' is never closed',hint:'Look for a missing '+c+' on this line. Everything after it is read as part of the string.'});break;}
      push('s',text.slice(i,j+1),i,j+1);i=j+1;continue;
    }
    if(c==='"'||c==='`'){
      const j=text.indexOf(c,i+1);
      if(j<0){errors.push({rule:'sql-unclosed-string',pos:i,msg:'A quoted name that starts with '+c+' is never closed',hint:'Add the closing '+c+'.'});break;}
      push('q',text.slice(i,j+1),i,j+1);i=j+1;continue;
    }
    if(c==='$'&&d==='{'){const j=text.indexOf('}',i);if(j>0&&j-i<80){push('ph',text.slice(i,j+1),i,j+1);i=j+1;continue;}}
    let m=/^(?:\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?)[LlSsYyDdFf]?/.exec(text.slice(i,i+40));
    if(m&&/[\d.]/.test(c)&&!(c==='.'&&!/\d/.test(d||''))){push('n',m[0],i,i+m[0].length);i+=m[0].length;continue;}
    m=/^[A-Za-z_À-￿][\w$#À-￿]*/.exec(text.slice(i,i+200));
    if(m){push('w',m[0],i,i+m[0].length);i+=m[0].length;continue;}
    m=/^(?:<=>|<>|!=|>=|<=|\|\||::|->|=>|==|[-+*\/%=<>(),.;:\[\]{}|&^~!?@#$])/.exec(text.slice(i,i+3));
    if(m){push('p',m[0],i,i+m[0].length);i+=m[0].length;continue;}
    push('p',c,i,i+1);i++;
  }
  return {toks,errors};
}

function sqlLint(text,engine){
  // returns {issues:[{sev,rule,pos,msg,hint}], refs:[{name,pos}], ctes:Set, aliases:Set}
  const out=[],add=(sev,rule,pos,msg,hint)=>out.push({sev,rule,pos,msg,hint:hint||''});
  const {toks,errors}=sqlTokenize(text,engine);
  errors.forEach(e=>add('error',e.rule,e.pos,e.msg,e.hint));
  if(!toks.length){add('error','sql-empty',0,'This file has no SQL in it','An empty query fails. Delete the file or add the query.');return {issues:out,refs:[],ctes:new Set(),aliases:new Set()};}
  if(errors.length)return {issues:out,refs:[],ctes:new Set(),aliases:new Set()};
  const T=toks,L=T.length,W=(k,u)=>T[k]&&T[k].t==='w'&&(u===undefined||T[k].u===u),P=(k,v)=>T[k]&&T[k].t==='p'&&T[k].v===v;
  const isKw=k=>T[k]&&T[k].t==='w'&&SQL_KW.has(T[k].u);
  const show=k=>!T[k]?'the end of the query':T[k].t==='w'?T[k].v.toUpperCase()===T[k].v||SQL_KW.has(T[k].u)?T[k].u:T[k].v:T[k].v;
  // ---- brackets
  const match=new Array(L).fill(-1),depth=new Array(L).fill(0),encl=new Array(L).fill(-1);const st=[];
  for(let k=0;k<L;k++){
    depth[k]=st.length;encl[k]=st.length?st[st.length-1]:-1;
    if(P(k,'(')){st.push(k);}
    else if(P(k,')')){if(!st.length){add('error','sql-paren',T[k].s,'There is a ")" with no "(" before it','Remove it, or add the missing "(".');}else{const o=st.pop();match[o]=k;match[k]=o;depth[k]=st.length;}}
  }
  st.forEach(o=>add('error','sql-paren',T[o].s,'This "(" is never closed','Add the missing ")". Brackets are counted across the whole file, so it may belong to an earlier line.'));
  // ---- semicolons
  const semis=[];for(let k=0;k<L;k++)if(P(k,';'))semis.push(k);
  if(semis.length){
    const last=semis[semis.length-1],trailingOnly=semis.length===1&&last===L-1;
    if(engine==='oracle')add('error','sql-semicolon',T[semis[0]].s,'";" in an Oracle script','The Oracle query is sent inside brackets, like "(your query)", so a ";" breaks it. Remove the ";".');
    else if(trailingOnly)add('warn','sql-semicolon',T[last].s,'The query ends with ";"','The script is run with spark.sql(), which takes one statement. Depending on the Spark version, a trailing ";" can fail. Safer to remove it.');
    else add('error','sql-semicolon',T[semis[0]].s,'More than one statement in one file (separated by ";")','spark.sql() runs exactly one statement, so this file fails. Split it into separate files or remove the ";".');
  }
  // ---- token-pair checks
  const cmpOps=new Set(['=','<>','!=','>=','<=','<','>','==','<=>']);
  const badAfterOp=k=>!T[k]||P(k,')')||P(k,',')||P(k,';')||(T[k].t==='w'&&(SQL_STOP.has(T[k].u)||['AND','OR','THEN','ELSE','END','FROM','WHEN','SELECT'].includes(T[k].u))&&!(['LEFT','RIGHT'].includes(T[k].u)&&P(k+1,'(')));
  const badBeforeOp=k=>!T[k]||P(k,'(')||P(k,',')||(T[k].t==='w'&&['WHERE','AND','OR','ON','HAVING','SELECT','WHEN','THEN','ELSE','BY','NOT'].includes(T[k].u));
  let generic=0;
  for(let k=0;k<L;k++){
    const t=T[k];
    if(t.t==='w'&&(t.u==='AND'||t.u==='OR')){
      if(badAfterOp(k+1))add('error','sql-dangling',t.s,'"'+t.u+'" with no condition after it (followed by '+show(k+1)+')','Remove the extra '+t.u+', or finish the condition.');
      else if(badBeforeOp(k-1)&&!(T[k-1]&&T[k-1].u==='NOT'))add('error','sql-dangling',t.s,'"'+t.u+'" with no condition before it (right after '+show(k-1)+')','Remove the extra '+t.u+'.');
    }
    if(t.t==='w'&&['ARRAY','MAP','STRUCT'].includes(t.u)&&P(k+1,'<'))generic++;
    if(t.t==='p'&&cmpOps.has(t.v)){
      if(t.v==='<'&&T[k-1]&&T[k-1].t==='w'&&['ARRAY','MAP','STRUCT'].includes(T[k-1].u))continue;
      if(t.v==='>'&&generic>0){generic--;continue;}
      if(badAfterOp(k+1))add('error','sql-dangling',t.s,'Nothing after "'+t.v+'" (followed by '+show(k+1)+')','A comparison needs a value on both sides.');
      else if(badBeforeOp(k-1))add('error','sql-dangling',t.s,'Nothing before "'+t.v+'" (right after '+show(k-1)+')','A comparison needs a value on both sides.');
    }
    if(P(k,',')){
      const nx=T[k+1];
      if(!nx||P(k+1,')')||P(k+1,',')||P(k+1,';')||(nx.t==='w'&&(SQL_STOP.has(nx.u)||nx.u==='FROM')&&nx.u!=='LATERAL'&&!(['LEFT','RIGHT'].includes(nx.u)&&P(k+2,'('))))
        add('error','sql-comma',t.s,P(k+1,',')?'Two commas in a row':'Extra comma before '+show(k+1),'Remove the "," at the end of the list.');
      else if(P(k-1,'(')||W(k-1,'SELECT')||W(k-1,'DISTINCT')||W(k-1,'BY'))
        add('error','sql-comma',t.s,'Comma right after '+show(k-1),'There is nothing before this ",". Remove it.');
    }
    if(t.t==='w'&&t.u==='SELECT'){
      let j=k+1;if(W(j,'DISTINCT')||W(j,'ALL'))j++;
      if(!T[j]||W(j,'FROM')||P(j,')')||P(j,';'))add('error','sql-empty-clause',t.s,'SELECT has no columns','Add the columns to select.');
    }
    if(t.t==='w'&&t.u==='FROM'&&!fnFrom(k)){
      const nx=T[k+1];
      if(!nx||P(k+1,')')||P(k+1,',')||P(k+1,';')||(nx.t==='w'&&SQL_STOP.has(nx.u)&&nx.u!=='LATERAL'))add('error','sql-empty-clause',t.s,'FROM has no table (followed by '+show(k+1)+')','Add the table name after FROM.');
    }
    if(t.t==='w'&&(t.u==='WHERE'||t.u==='HAVING'||t.u==='ON')){
      const nx=T[k+1];
      if(!nx||P(k+1,')')||P(k+1,';')||(nx.t==='w'&&(SQL_STOP.has(nx.u)||nx.u==='SELECT')&&!(['LEFT','RIGHT'].includes(nx.u)&&P(k+2,'('))))add('error','sql-empty-clause',t.s,t.u+' has no condition (followed by '+show(k+1)+')','Add the condition, or remove the '+t.u+'.');
    }
    if(t.t==='w'&&(t.u==='GROUP'||t.u==='ORDER')&&!W(k+1,'BY')&&!(P(k-1,'.'))&&!W(k-1,'WITHIN'))
      if(!(t.u==='ORDER'&&P(k+1,')'))&&!P(k+1,',')&&!P(k+1,'.')&&!(T[k-1]&&T[k-1].t==='p'&&cmpOps.has(T[k-1].v))&&!(T[k+1]&&T[k+1].t==='p'&&cmpOps.has(T[k+1].v))&&!W(k-1,'AS'))
        add('error','sql-missing-by',t.s,t.u+' must be followed by BY','Write "'+t.u+' BY".');
    if(t.t==='w'&&t.u==='IN'&&!W(k-1,'FOR')){
      if(P(k+1,'(')&&P(k+2,')'))add('error','sql-empty-in',t.s,'IN () has an empty list','An empty IN list fails. Add values, or remove the condition.');
      else if(!P(k+1,'(')&&!(W(k+1)&&P(k+2,'('))&&!(T[k+1]&&(T[k+1].v===SQL_VAR||T[k+1].t==='ph'))&&!W(k-1,'AS'))add('error','sql-in',t.s,'IN must be followed by "(...)"','Write IN (value1, value2) or IN (SELECT ...).');
    }
    if(t.t==='w'&&t.u==='BETWEEN'){
      let ok=false;for(let j=k+1;j<L;j++){if(depth[j]<depth[k])break;if(depth[j]>depth[k])continue;if(W(j,'AND')){ok=true;break;}if(P(j,',')||P(j,';')||(T[j].t==='w'&&SQL_STOP.has(T[j].u)))break;}
      if(!ok)add('error','sql-between',t.s,'BETWEEN without AND','Write BETWEEN low AND high.');
    }
  }
  function fnFrom(k){
    if(W(k-1,'DISTINCT')&&(W(k-2,'IS')||W(k-3,'IS')))return true;
    // FROM inside EXTRACT( / TRIM( ...
    for(let j=k-1;j>=0;j--){if(P(j,')')&&match[j]>=0){j=match[j];continue;}if(P(j,'(')){return T[j-1]&&T[j-1].t==='w'&&SQL_FN_FROM.has(T[j-1].u);}if(depth[j]<depth[k])return false;}
    return false;
  }
  // ---- CASE / END balance (per bracket level)
  {const stack=[];for(let k=0;k<L;k++){if(W(k,'CASE'))stack.push(k);else if(W(k,'END')&&!P(k+1,'(')&&!P(k-1,'.')){if(!stack.length)add('error','sql-case',T[k].s,'END without a matching CASE','Remove the extra END, or add the missing CASE.');else stack.pop();}}
   stack.forEach(k=>add('error','sql-case',T[k].s,'CASE without END','Every CASE needs a closing END.'));}
  // ---- JOIN must have ON/USING
  for(let k=0;k<L;k++){
    if(!W(k,'JOIN'))continue;
    if(W(k-1,'CROSS')||W(k-1,'NATURAL')||W(k-2,'NATURAL')||W(k-3,'NATURAL')||W(k+1,'LATERAL'))continue;
    let ok=false,j=k+1;
    for(;j<L;j++){
      if(P(j,'(')&&match[j]>j){j=match[j];continue;}
      if(depth[j]<depth[k]||P(j,';'))break;
      if(W(j,'ON')||W(j,'USING')){ok=true;break;}
      if(T[j].t==='w'&&['JOIN','WHERE','GROUP','ORDER','HAVING','LIMIT','UNION','INTERSECT','EXCEPT','MINUS','INNER','LEFT','RIGHT','FULL','CROSS','WINDOW','QUALIFY'].includes(T[j].u)&&!(['LEFT','RIGHT'].includes(T[j].u)&&P(j+1,'(')))break;
      if(P(j,','))break;
    }
    if(!ok)add('error','sql-join-on',T[k].s,'JOIN without ON','Every JOIN (except CROSS JOIN) needs "ON a.key = b.key" or USING (...).');
  }
  // ---- CTE structure
  for(let k=0;k<L;k++){
    if(!W(k,'WITH'))continue;
    if(!(k===0||P(k-1,'(')||P(k-1,';')||W(k-1,'AS')))continue;
    let j=k+1;if(W(j,'RECURSIVE'))j++;
    for(let guard=0;guard<200;guard++){
      if(!T[j]||!(T[j].t==='w'||T[j].t==='q'||(T[j].t==='s'&&T[j].v[0]==='"'))){add('error','sql-cte',T[k].s,'WITH must be followed by "name AS (...)"','Check the CTE definition after WITH.');break;}
      j++;if(P(j,'(')&&match[j]>j)j=match[j]+1;
      if(!W(j,'AS')){add('error','sql-cte',T[j]?T[j].s:T[k].s,'Expected AS after the CTE name, found '+show(j),'Each CTE is written: name AS (SELECT ...).');break;}
      j++;if(!P(j,'(')){add('error','sql-cte',T[j]?T[j].s:T[k].s,'Expected "(" after AS in the CTE, found '+show(j),'Each CTE is written: name AS (SELECT ...).');break;}
      j=match[j]+1;if(j<=0)break;
      if(P(j,',')){
        if(W(j+1,'SELECT')||W(j+1,'INSERT')||P(j+1,'('))add('error','sql-cte',T[j].s,'Comma after the last CTE','Remove the "," before the main SELECT.');
        else{j++;continue;}
      }else if(W(j)&&W(j+1,'AS')&&P(j+2,'('))add('error','sql-cte',T[j].s,'Missing comma between two CTEs (before '+show(j)+')','Add a "," after the closing ")" of the previous CTE.');
      break;
    }
  }
  // ---- select list: missing comma between columns, UNION column counts
  const branches=[];
  for(let k=0;k<L;k++){
    if(!W(k,'SELECT'))continue;
    const items=[],dk=depth[k];let cur=[],j=k+1;
    if(W(j,'DISTINCT')||W(j,'ALL'))j++;
    for(;j<L;j++){
      if(depth[j]<dk||P(j,';'))break;
      if(depth[j]===dk&&(W(j,'FROM')||W(j,'WHERE')||W(j,'UNION')||W(j,'INTERSECT')||W(j,'EXCEPT')||W(j,'MINUS')||W(j,'LIMIT')||W(j,'ORDER')||W(j,'GROUP')||W(j,'HAVING')||W(j,'INTO')||W(j,'WINDOW')||W(j,'LATERAL')))break;
      if(depth[j]===dk&&P(j,',')){items.push(cur);cur=[];continue;}
      cur.push(j);
    }
    items.push(cur);
    items.forEach(it=>{
      const top=it.filter(x=>depth[x]===dk);
      for(let q=1;q<top.length;q++){const x=top[q],pv=top[q-1];
        if(W(x)&&!SQL_KW.has(T[x].u)&&P(x+1,'.')&&!P(x-1,'.')&&x-1===pv&&((W(pv)&&!SQL_KW.has(T[pv].u))||T[pv].t==='n'||T[pv].t==='s'||T[pv].t==='q'||P(pv,')'))){
          add('error','sql-missing-comma',T[x].s,'Missing comma before "'+T[x].v+'.'+(T[x+2]?T[x+2].v:'')+'"?','A column name directly follows another value with nothing between them. Add a ",".');return;}}
      if(top.length<2)return;
      let tail=0;for(let q=top.length-1;q>=0;q--){const x=top[q];if(T[x].t==='w'&&!SQL_KW.has(T[x].u)&&!P(x-1,'.')&&!P(x+1,'.')&&!P(x+1,'(')&&T[x].v!==SQL_VAR)tail++;else break;}
      const before=top[top.length-1-tail];
      const onlyWords=before===undefined;
      const limit=onlyWords?2:(W(before,'AS')?1:1);
      if(tail>limit&&!(before!==undefined&&T[before].t==='p'&&!/^[)\]]$/.test(T[before].v)))add('error','sql-missing-comma',T[top[top.length-tail+limit]].s,'Missing comma between columns? "'+top.slice(top.length-tail).map(x=>T[x].v).join(' ')+'"','Words that follow each other in the SELECT list need a "," between them (only one alias is allowed after a column).');
    });
    // UNION branches
    let prev=k-1;while(prev>=0&&P(prev,'('))prev--;
    branches.push({k,dk,en:encl[k],count:items.length,star:items.some(it=>it.some(x=>P(x,'*')&&!P(x-1,'(')&&(P(x-1,'.')||it.length===1||items.length===1))),dyn:items.some(it=>it.some(x=>T[x].v===SQL_VAR||T[x].t==='ph')),afterSet:W(prev,'UNION')||W(prev,'ALL')||W(prev,'INTERSECT')||W(prev,'EXCEPT')||W(prev,'MINUS')||W(prev,'DISTINCT')});
  }
  for(let b=1;b<branches.length;b++){
    const B=branches[b];if(!B.afterSet)continue;
    let A=null;for(let a=b-1;a>=0;a--){if(branches[a].dk===B.dk&&branches[a].en===B.en){A=branches[a];break;}}
    if(!A||A.star||B.star||A.dyn||B.dyn)continue;
    if(A.count!==B.count)add('error','sql-union-cols',T[B.k].s,'The two sides of the UNION have a different number of columns ('+A.count+' vs '+B.count+')','Every SELECT joined by UNION must return the same number of columns.');
  }
  // ---- engine-specific syntax
  for(let k=0;k<L;k++){
    const t=T[k];
    if(engine==='oracle'){
      if(t.t==='q'&&t.v[0]==='`')add('error','sql-engine',t.s,'Backticks `name` don’t work in Oracle','Use "name" (double quotes) or no quotes in Oracle scripts.');
      if(W(k,'LIMIT')&&depth[k]>=0&&(T[k+1]&&(T[k+1].t==='n'||T[k+1].v===SQL_VAR)))add('error','sql-engine',t.s,'LIMIT doesn’t exist in Oracle','Use FETCH FIRST n ROWS ONLY, or WHERE ROWNUM <= n.');
    }else{
      if(W(k,'SYSDATE')&&!P(k+1,'('))add('error','sql-engine',t.s,'SYSDATE doesn’t exist in Spark SQL','This script runs through spark.sql(). Use current_date() or current_timestamp().');
      else if(W(k,'ROWNUM'))add('error','sql-engine',t.s,'ROWNUM doesn’t exist in Spark SQL','Use row_number() OVER (...) or LIMIT.');
      else if(W(k,'DUAL')&&W(k-1,'FROM'))add('error','sql-engine',t.s,'"FROM dual" doesn’t exist in Spark SQL','In Spark you can SELECT without a FROM.');
      else if(P(k,'(')&&P(k+1,'+')&&P(k+2,')'))add('error','sql-engine',t.s,'Oracle (+) join syntax doesn’t work in Spark SQL','Use LEFT JOIN ... ON ... instead.');
      else if(W(k,'CONNECT')&&W(k+1,'BY'))add('error','sql-engine',t.s,'CONNECT BY doesn’t exist in Spark SQL','This is Oracle-only syntax.');
    }
  }
  // ---- table references, CTE names, aliases
  const refs=[],ctes=new Set(),aliases=new Set(),refTok=new Set();
  for(let k=0;k<L;k++){
    if(W(k)&&W(k+1,'AS')&&P(k+2,'(')&&(P(k-1,',')||W(k-1,'WITH')||W(k-1,'RECURSIVE')))ctes.add(T[k].v.toLowerCase());
    if(W(k)&&P(k+1,'(')&&match[k+1]>0&&W(match[k+1]+1,'AS')&&P(match[k+1]+2,'(')&&(P(k-1,',')||W(k-1,'WITH')))ctes.add(T[k].v.toLowerCase());
  }
  const readAlias=j=>{if(W(j,'AS'))j++;if(T[j]&&(T[j].t==='w'&&!SQL_KW.has(T[j].u)||T[j].t==='q')){aliases.add(T[j].v.replace(/[`"]/g,'').toLowerCase());j++;if(P(j,'(')&&match[j]>j)j=match[j]+1;}return j;};
  const readRef=j=>{
    if(P(j,'(')){j=match[j]>j?match[j]+1:j+1;return readAlias(j);}
    if(T[j]&&(T[j].t==='w'||T[j].t==='q'||T[j].t==='ph')){
      const s=j;let name=T[j].v.replace(/[`"]/g,'');refTok.add(j);j++;
      while(P(j,'.')&&T[j+1]&&(T[j+1].t==='w'||T[j+1].t==='q'||T[j+1].t==='ph')){name+='.'+T[j+1].v.replace(/[`"]/g,'');refTok.add(j+1);j+=2;}
      if(P(j,'(')&&match[j]>j){j=match[j]+1;return readAlias(j);} // table function
      if(T[s].t==='w'&&SQL_KW.has(T[s].u)&&!name.includes('.'))return j;
      refs.push({name:name.toLowerCase(),pos:T[s].s,engineQuoted:T[s].t==='q'});
      name.toLowerCase().split('.').forEach(x=>aliases.add(x));
      return readAlias(j);
    }
    return j;
  };
  for(let k=0;k<L;k++){
    if(W(k,'FROM')&&!fnFrom(k)){
      let j=readRef(k+1);
      while(P(j,',')){j=readRef(j+1);}
    }else if(W(k,'JOIN')){readRef(k+1);}
    else if((W(k,'INTO')||W(k,'TABLE')||W(k,'UPDATE'))&&T[k+1]&&(T[k+1].t==='w'||T[k+1].t==='q')&&!SQL_KW.has(T[k+1].u)){const before=refs.length;let j=readRef(k+1);refs.length=before;}
    else if(W(k,'LATERAL')&&W(k+1,'VIEW')){let j=k+2;if(W(j,'OUTER'))j++;if(W(j)&&P(j+1,'(')&&match[j+1]>0){j=match[j+1]+1;if(W(j)&&!SQL_KW.has(T[j].u))aliases.add(T[j].v.toLowerCase());}}
    if(W(k,'AS')&&T[k+1]&&T[k+1].t==='w')aliases.add(T[k+1].v.toLowerCase());
    if(P(k,')')&&W(k+1)&&!SQL_KW.has(T[k+1].u))aliases.add(T[k+1].v.toLowerCase());
    if(P(k,'->')&&W(k-1))aliases.add(T[k-1].v.toLowerCase());
    if(P(k,'->')&&P(k-1,')')){for(let j=match[k-1]+1;j<k-1;j++)if(W(j))aliases.add(T[j].v.toLowerCase());}
  }
  ctes.forEach(c=>aliases.add(c));
  // ---- unknown alias prefixes  x.col
  const seenUnknown=new Set();
  for(let k=0;k<L;k++){
    if(!(W(k)&&P(k+1,'.')&&T[k+2]&&(T[k+2].t==='w'||T[k+2].t==='q'||P(k+2,'*'))))continue;
    if(P(k-1,'.')||refTok.has(k)||T[k].v===SQL_VAR)continue;
    const a=T[k].v.toLowerCase();
    if(aliases.has(a)||SQL_KW.has(T[k].u)||seenUnknown.has(a))continue;
    seenUnknown.add(a);
    add('warn','sql-alias',T[k].s,'"'+T[k].v+'." is not a table or alias defined in this query','Check the alias spelling. (If '+T[k].v+' is a struct column, you can ignore this.)');
  }
  // ---- placeholders left
  for(const t of T){
    if(t.t==='ph')add('error','sql-placeholder',t.s,'Placeholder '+t.v+' was not replaced','No sql_var entry replaces it, so the raw text reaches the database and fails.');
    else if(t.t==='s'){const re=/\$\{[^}'"]{1,60}\}/g;let m;while((m=re.exec(t.v)))add('error','sql-placeholder',t.s+m.index,'Placeholder '+m[0]+' was not replaced (inside a string)','No sql_var entry replaces it, so the query uses the literal text '+m[0]+' and fails or returns nothing.');}
  }
  return {issues:out,refs,ctes,aliases};
}
if(typeof module!=='undefined')module.exports={sqlTokenize,sqlLint,SQL_VAR,SQL_KW};
