/* ===== Python tokenizer + parser (syntax check and literal reading) =====
   Reports the first syntax error the way Python would, plus a few "valid but
   probably a typo" warnings (implicit string joins in lists, duplicate dict keys). */
const PY_KW=new Set('False None True and as assert async await break class continue def del elif else except finally for from global if import in is lambda nonlocal not or pass raise return try while with yield'.split(' '));
const PY_OPS=['**=','//=','>>=','<<=','...','->',':=','**','//','>>','<<','<=','>=','==','!=','+=','-=','*=','/=','%=','&=','|=','^=','@=','+','-','*','/','%','@','&','|','^','~','<','>','(',')','[',']','{','}',',',':','.',';','='];
const PY_CLOSE={')':'(',']':'[','}':'{'};
class PySyntaxError extends Error{constructor(msg,line,col,hint){super(msg);this.line=line;this.col=col;this.hint=hint||'';}}

function pyTokenize(src){
  const toks=[],brackets=[],indents=[0];
  let i=0,line=1,col=0,atStart=true,n=src.length,sawTab=false,sawSpace=false;
  const err=(m,l,c,h)=>{throw new PySyntaxError(m,l==null?line:l,c==null?col:c,h);};
  const push=(t,v,l,c,extra)=>{const o={t,v,line:l,col:c};if(extra)Object.assign(o,extra);toks.push(o);return o;};
  const adv=k=>{for(let j=0;j<k;j++){if(src[i]==='\n'){line++;col=0;}else col++;i++;}};
  if(src.charCodeAt(0)===0xFEFF)i=1;
  while(i<n){
    if(atStart&&!brackets.length){
      // measure indentation
      let w=0,j=i,tabs=false,spaces=false;
      while(j<n&&(src[j]===' '||src[j]==='\t'||src[j]==='\f')){if(src[j]==='\t'){w=(Math.floor(w/8)+1)*8;tabs=true;}else if(src[j]===' '){w++;spaces=true;}j++;}
      const ch=src[j];
      if(j>=n)break;
      if(ch==='\n'||ch==='#'||(ch==='\r'&&src[j+1]==='\n')){ // blank or comment line
        adv(j-i);
        if(ch==='#'){while(i<n&&src[i]!=='\n')adv(1);}
        if(src[i]==='\r')adv(1);
        if(src[i]==='\n')adv(1);
        continue;
      }
      if(ch==='\\'&&(src[j+1]==='\n'||src[j+1]==='\r')){adv(j-i);adv(src[j+1]==='\r'?3:2);continue;}
      if(tabs)sawTab=true;if(spaces)sawSpace=true;
      adv(j-i);
      const top=indents[indents.length-1];
      if(w>top){
        if(!toks.length||toks[toks.length-1].t!=='newline'||!toks.length)err('unexpected indent',line,0,'This line is indented more than the line before it, but the line before doesn\u2019t end with ":".');
        indents.push(w);push('indent','',line,0);
      }else if(w<top){
        while(indents.length&&w<indents[indents.length-1]){indents.pop();push('dedent','',line,0);}
        if(w!==indents[indents.length-1])err('unindent does not match any outer indentation level',line,0,'The indentation of this line doesn\u2019t line up with any block above it. Check spaces vs tabs.');
      }
      atStart=false;
    }
    const c=src[i];
    if(c==='\n'){
      if(!brackets.length){const last=toks[toks.length-1];if(last&&last.t!=='newline'&&last.t!=='indent'&&last.t!=='dedent')push('newline','',line,col);atStart=true;}
      adv(1);continue;
    }
    if(c===' '||c==='\t'||c==='\f'||c==='\r'){adv(1);continue;}
    if(c==='#'){while(i<n&&src[i]!=='\n')adv(1);continue;}
    if(c==='\\'){
      if(src[i+1]==='\n'){adv(2);continue;}
      if(src[i+1]==='\r'&&src[i+2]==='\n'){adv(3);continue;}
      err('unexpected character after line continuation character',line,col,'A backslash "\\" must be the last character on the line.');
    }
    // strings (with prefixes)
    const pm=/^([rRbBuUfF]{0,2})(['"])/.exec(src.slice(i,i+4));
    if(pm&&(pm[1]===''||/^(r|u|f|b|br|rb|fr|rf)$/i.test(pm[1]))){
      const q=pm[2],sl=line,sc=col,pre=pm[1].toLowerCase();
      const tri=src.substr(i+pm[1].length,3)===q+q+q;
      let j=i+pm[1].length+(tri?3:1);const startBody=j;
      let fdepth=0;const isF=pre.includes('f');
      for(;;){
        if(isF&&j<n){
          const ch0=src[j];
          if(ch0==='{'){if(src[j+1]==='{'&&fdepth===0){j+=2;continue;}fdepth++;j++;continue;}
          if(ch0==='}'&&fdepth>0){fdepth--;j++;continue;}
          if(fdepth>0&&(ch0==='"'||ch0==="'")){const qq=ch0;let k=j+1;while(k<n&&src[k]!==qq&&src[k]!=='\n'){if(src[k]==='\\')k++;k++;}j=k+1;continue;}
        }
        if(j>=n)err(tri?'unterminated triple-quoted string literal (started on line '+sl+')':'unterminated string literal',sl,sc,tri?'A string that starts with '+q+q+q+' is never closed.':'This string has no closing '+q+' on the same line.');
        const ch=src[j];
        if(ch==='\\'){j+=2;continue;}
        if(!tri&&ch==='\n')err('unterminated string literal',sl,sc,'This string has no closing '+q+' on the same line. Did you forget a quote?');
        if(tri?src.substr(j,3)===q+q+q:ch===q)break;
        j++;
      }
      const body=src.slice(startBody,j);j+=tri?3:1;
      let v=body;
      if(!pre.includes('r'))v=body.replace(/\\(\r?\n|\\|'|"|n|t|r|0)/g,(m,x)=>x==='n'?'\n':x==='t'?'\t':x==='r'?'\r':x==='0'?'\0':/\n/.test(x)?'':x);
      push('str',v,sl,sc,{f:pre.includes('f'),b:pre.includes('b'),endLine:null});
      adv(j-i);toks[toks.length-1].endLine=line;continue;
    }
    const nm=/^(?:0[xX][0-9a-fA-F_]+|0[oO][0-7_]+|0[bB][01_]+|(?:\d[\d_]*(?:\.[\d_]*)?|\.\d[\d_]*)(?:[eE][+-]?\d[\d_]*)?[jJ]?)/.exec(src.slice(i,i+80));
    if(nm&&/[\d.]/.test(c)&&!(c==='.'&&!/\d/.test(src[i+1]||''))){
      const after=src[i+nm[0].length]||'';
      if(/[A-Za-z_]/.test(after))err('invalid decimal literal',line,col,'A number is directly followed by letters. Is a space or operator missing?');
      push('num',nm[0],line,col);adv(nm[0].length);continue;
    }
    const idm=/^[A-Za-z_\u00A0-\uFFFF][\w\u00A0-\uFFFF]*/.exec(src.slice(i,i+200));
    if(idm){push(PY_KW.has(idm[0])?'kw':'name',idm[0],line,col);adv(idm[0].length);continue;}
    let op=null;for(const o of PY_OPS){if(src.startsWith(o,i)){op=o;break;}}
    if(!op){
      if(c==='!')err('invalid syntax: "!" on its own',line,col,'Python uses "not" or "!=" \u2014 a single "!" is not valid.');
      err('invalid character '+JSON.stringify(c),line,col,c==='$'?'"$" is not valid in Python code. Is it meant to be inside a string?':c==='`'?'Backticks are not valid in Python.':'This character is not valid in Python code.');
    }
    if('([{'.includes(op))brackets.push({ch:op,line,col});
    else if(')]}'.includes(op)){
      const top=brackets.pop();
      if(!top)err('unmatched \''+op+'\'',line,col,'There is a closing "'+op+'" with no opening bracket before it.');
      if(top.ch!==PY_CLOSE[op])err('closing parenthesis \''+op+'\' does not match opening parenthesis \''+top.ch+'\''+(top.line!==line?' on line '+top.line:''),line,col,'Brackets are mixed up: "'+top.ch+'" (line '+top.line+') is closed with "'+op+'".');
    }
    push('op',op,line,col);adv(op.length);
  }
  if(brackets.length){const b=brackets[brackets.length-1];err('\''+b.ch+'\' was never closed',b.line,b.col,'The "'+b.ch+'" opened on line '+b.line+' has no matching "'+({'(':')','[':']','{':'}'})[b.ch]+'".');}
  const last=toks[toks.length-1];
  if(last&&last.t!=='newline'&&last.t!=='dedent')push('newline','',line,col);
  while(indents.length>1){indents.pop();push('dedent','',line,0);}
  push('eof','',line,col);
  return {toks,mixedIndent:sawTab&&sawSpace};
}

/* ---------- parser ---------- */
function pyParse(src){
  const res={ok:true,error:null,warnings:[],body:[]};
  let T;
  try{const tk=pyTokenize(src);T=tk.toks;if(tk.mixedIndent)res.warnings.push({line:1,msg:'This file mixes tabs and spaces for indentation.',hint:'Python may read the indentation differently from how it looks. Use spaces only.'});}
  catch(e){if(e instanceof PySyntaxError){res.ok=false;res.error=e;return res;}throw e;}
  let p=0;
  const peek=(k)=>T[p+(k||0)];
  const at=(t,v)=>{const x=T[p];return x.t===t&&(v===undefined||x.v===v);};
  const isOp=v=>at('op',v),isKw=v=>at('kw',v);
  const desc=x=>x.t==='eof'?'end of file':x.t==='newline'?'end of line':x.t==='indent'?'an indented line':x.t==='dedent'?'end of block':x.t==='str'?'a string':x.t==='num'?'a number':'"'+x.v+'"';
  const fail=(msg,tok,hint)=>{tok=tok||T[p];throw new PySyntaxError(msg,tok.line,tok.col,hint);};
  const next=()=>T[p++];
  const expectOp=(v,ctx,hint)=>{if(isOp(v))return next();fail('expected "'+v+'"'+(ctx?' '+ctx:'')+' but found '+desc(T[p]),T[p],hint);};
  const startsExpr=x=>x.t==='name'||x.t==='num'||x.t==='str'||(x.t==='op'&&['(','[','{','-','+','~','*','**','...'].includes(x.v))||(x.t==='kw'&&['None','True','False','not','lambda','await','yield'].includes(x.v));
  const N=(k,tok,o)=>Object.assign({k,line:tok.line,col:tok.col},o||{});

  function file(){
    const body=[];
    while(!at('eof')){if(at('newline')){next();continue;}if(at('indent'))fail('unexpected indent',T[p],'This line is indented but nothing above it opens a block.');body.push(...stmt());}
    return body;
  }
  function suite(){
    if(at('newline')){next();if(!at('indent'))fail('expected an indented block',T[p],'The line before ends with ":", so the next line must be indented.');next();
      const b=[];while(!at('dedent')&&!at('eof')){if(at('newline')){next();continue;}b.push(...stmt());}if(at('dedent'))next();return b;}
    return simpleStmts();
  }
  function stmt(){
    const x=T[p];
    if(x.t==='kw'){
      switch(x.v){
        case 'if':{next();namedTest();if(isOp('='))fail('"=" assigns a value; to compare inside "if" use "=="',T[p]);expectOp(':','after the if condition','Every "if" line must end with ":".');const b=suite();const o=N('If',x,{body:b,orelse:[]});
          while(isKw('elif')){next();namedTest();expectOp(':','after elif');o.orelse.push(...suite());}
          if(isKw('else')){next();expectOp(':','after else');o.orelse.push(...suite());}return [o];}
        case 'while':{next();namedTest();expectOp(':','after the while condition');const b=suite();if(isKw('else')){next();expectOp(':');suite();}return [N('While',x,{body:b})];}
        case 'for':{next();targetList();if(!isKw('in'))fail('expected "in" in the for loop but found '+desc(T[p]));next();testList();expectOp(':','at the end of the for line','Every "for" line must end with ":".');const b=suite();if(isKw('else')){next();expectOp(':');suite();}return [N('For',x,{body:b})];}
        case 'try':{next();expectOp(':','after try');const b=suite();let any=false;
          while(isKw('except')){any=true;next();if(isOp('*'))next();if(!isOp(':')){test();if(isKw('as')){next();if(!at('name'))fail('expected a name after "as"');next();}else if(isOp(',')){fail('use "except (A, B):" or "except A as e:" \u2014 "except A, B:" is Python 2 syntax');}}expectOp(':','after except');suite();}
          if(isKw('else')){next();expectOp(':');suite();}
          if(isKw('finally')){any=true;next();expectOp(':');suite();}
          if(!any)fail('"try" needs an "except" or "finally" block',T[p]);return [N('Try',x,{body:b})];}
        case 'with':{next();
          if(isOp('(')&&parenWithAs()){next();do{if(isOp(')'))break;test();if(isKw('as')){next();target();}}while(isOp(',')&&next());expectOp(')','to close the with items');expectOp(':','at the end of the with line');return [N('With',x,{body:suite()})];}
          do{test();if(isKw('as')){next();target();}}while(isOp(',')&&next());expectOp(':','at the end of the with line');return [N('With',x,{body:suite()})];}
        case 'def':return [funcdef()];
        case 'class':{next();if(!at('name'))fail('expected a class name');next();skipGeneric();if(isOp('(')){next();callArgs(')');}expectOp(':','after the class line');return [N('Class',x,{body:suite()})];}
        case 'async':{next();if(isKw('def'))return [funcdef()];if(isKw('for')||isKw('with'))return stmt();fail('"async" must be followed by def, for or with');}
        case 'elif':case 'else':case 'except':case 'finally':fail('"'+x.v+'" without a matching '+(x.v==='elif'||x.v==='else'?'"if" (or the indentation is wrong)':'"try"'),x,'Check that this line is indented the same as the block it belongs to.');
      }
    }
    if(at('name','match')&&lineEndsWithColon()&&!(peek(1).t==='op'&&['=','.','(','[',',',')'].includes(peek(1).v)&&!lineHasTopColonAfter())){return [matchStmt()];}
    if(at('name','type')&&peek(1).t==='name'&&(peek(2).t==='op'&&(peek(2).v==='='||peek(2).v==='['))){const x=next();while(!at('newline')&&!at('eof'))next();next();return [N('Pass',x)];}
    if(isOp('@')){while(isOp('@')){next();namedTest();if(!at('newline'))fail('expected end of line after the decorator');next();}if(!(isKw('def')||isKw('class')||isKw('async')))fail('a decorator must be followed by def or class');return stmt();}
    return simpleStmts();
  }
  function lineEndsWithColon(){let d=0;for(let k=p;k<T.length;k++){const t=T[k];if(t.t==='newline'||t.t==='eof')return false;if(t.t==='op'){if('([{'.includes(t.v))d++;else if(')]}'.includes(t.v))d--;else if(t.v===':'&&d===0){const nx=T[k+1];return nx.t==='newline';}}}return false;}
  function lineHasTopColonAfter(){return true;}
  function skipToColonNewline(){let d=0;while(!at('eof')){const t=T[p];if(t.t==='op'){if('([{'.includes(t.v))d++;else if(')]}'.includes(t.v))d--;else if(t.v===':'&&d===0&&T[p+1].t==='newline'){next();return;}}if(t.t==='newline')fail('expected ":" at the end of this line',t);next();}}
  function matchStmt(){const x=next();skipToColonNewline();if(!at('newline'))fail('expected end of line after match');next();if(!at('indent'))fail('expected an indented block of "case" lines');next();
    while(!at('dedent')&&!at('eof')){if(at('newline')){next();continue;}if(!at('name','case'))fail('expected "case" inside match',T[p]);next();skipToColonNewline();suite();}
    if(at('dedent'))next();return N('Match',x);}
  function parenWithAs(){let d=0;for(let k=p;k<T.length;k++){const t=T[k];if(t.t==='op'){if('([{'.includes(t.v))d++;else if(')]}'.includes(t.v)){d--;if(d===0)return T[k+1]&&T[k+1].t==='op'&&T[k+1].v===':'&&false;}}if(t.t==='kw'&&t.v==='as'&&d===1)return true;if(t.t==='newline'||t.t==='eof')return false;}return false;}
  function skipGeneric(){if(isOp('[')){let d=0;do{if(isOp('['))d++;else if(isOp(']'))d--;next();}while(d>0&&!at('eof'));}}
  function funcdef(){const x=next();if(!at('name'))fail('expected a function name after def');next();skipGeneric();expectOp('(','after the function name');params(')',true);expectOp(')','to close the parameter list');if(isOp('->')){next();test();}expectOp(':','at the end of the def line','A "def" line must end with ":".');return N('Def',x,{body:suite()});}
  function params(close,ann){
    while(!isOp(close)){
      if(isOp('/')||isOp('*')){next();if(at('name')){next();if(ann&&isOp(':')){next();test();}}}
      else if(isOp('**')){next();if(!at('name'))fail('expected a name after **');next();if(ann&&isOp(':')){next();test();}}
      else if(at('name')){next();if(ann&&isOp(':')){next();test();}if(isOp('=')){next();test();}}
      else fail('invalid parameter: found '+desc(T[p]));
      if(isOp(','))next();else if(!isOp(close))fail('expected "," or "'+close+'" in the parameter list but found '+desc(T[p]));
    }
  }
  function simpleStmts(){
    const out=[small()];
    while(isOp(';')){next();if(at('newline')||at('eof'))break;out.push(small());}
    if(!at('newline')&&!at('eof')){
      const x=T[p],prev=T[p-1];
      if(startsExpr(x)&&prev&&(prev.t==='str'||prev.t==='name'||prev.t==='num'||(prev.t==='op'&&[')',']','}'].includes(prev.v))))fail('invalid syntax: two expressions next to each other',x,'Something is missing between them \u2014 often a comma, an operator, or a closing bracket.');
      fail('invalid syntax: unexpected '+desc(x),x);
    }
    next();return out;
  }
  function small(){
    const x=T[p];
    if(x.t==='kw'){
      switch(x.v){
        case 'pass':case 'break':case 'continue':next();return N('Pass',x);
        case 'return':next();if(!at('newline')&&!isOp(';'))testListStar();return N('Return',x);
        case 'raise':next();if(!at('newline')&&!isOp(';')){test();if(isKw('from')){next();test();}}return N('Raise',x);
        case 'global':case 'nonlocal':next();do{if(!at('name'))fail('expected a name');next();}while(isOp(',')&&next());return N('Global',x);
        case 'del':next();testList();return N('Del',x);
        case 'assert':next();test();if(isOp(',')){next();test();}return N('Assert',x);
        case 'yield':yieldExpr();return N('Expr',x);
        case 'import':{next();const names=[];do{names.push(dotted());if(isKw('as')){next();if(!at('name'))fail('expected a name after "as"');next();}}while(isOp(',')&&next());return N('Import',x,{names});}
        case 'from':{next();let mod='';while(isOp('.')||isOp('...')){mod+=next().v;}if(at('name'))mod+=dotted();if(!isKw('import'))fail('expected "import" after "from '+mod+'"');next();
          if(isOp('*')){next();return N('ImportFrom',x,{mod,names:['*']});}
          const paren=isOp('(');if(paren)next();const names=[];
          do{if(paren&&isOp(')'))break;if(!at('name'))fail('expected a name to import but found '+desc(T[p]));names.push(next().v);if(isKw('as')){next();next();}}while(isOp(',')&&next());
          if(paren)expectOp(')');return N('ImportFrom',x,{mod,names});}
      }
    }
    // expression statement / assignment
    const first=testListStar();
    if(isOp('=')){
      const targets=[first];let value;
      while(isOp('=')){const eq=next();if(at('newline')||at('eof'))fail('nothing after "="',eq,'An assignment needs a value on the right.');
        value=isKw('yield')?yieldExpr():testListStar();targets.push(value);}
      targets.pop();
      targets.forEach(t=>checkTarget(t));
      return N('Assign',x,{targets,value});
    }
    if(at('op')&&/^(\+|-|\*|\/|\/\/|%|@|&|\||\^|>>|<<|\*\*)=$/.test(T[p].v)){next();checkTarget(first);const value=isKw('yield')?yieldExpr():testList();return N('AugAssign',x,{target:first,value});}
    if(isOp(':')){next();test();let value=null;if(isOp('=')){next();value=test();}return N('AnnAssign',x,{target:first,value});}
    return N('Expr',x,{value:first});
  }
  function checkTarget(t){
    if(t.k==='Name'||t.k==='Attr'||t.k==='Sub'||t.k==='Starred')return;
    if(t.k==='Tuple'||t.k==='List'){t.elts.forEach(checkTarget);return;}
    fail(t.k==='Const'?'cannot assign to a literal':t.k==='Call'?'cannot assign to a function call':'cannot assign to this expression',t,'The left side of "=" must be a name. Did you mean "==" (compare)?');
  }
  function dotted(){if(!at('name'))fail('expected a module name but found '+desc(T[p]));let s=next().v;while(isOp('.')){next();if(!at('name'))fail('expected a name after "."');s+='.'+next().v;}return s;}
  function yieldExpr(){const x=next();if(isKw('from')){next();test();}else if(!isOp(')')&&!at('newline')&&!isOp('='))testListStar();return N('Other',x);}
  function targetList(){const t=testListStar(true);return t;}
  function target(){return orExpr();}
  function testList(){const x=T[p];const a=test();if(!isOp(','))return a;const elts=[a];while(isOp(',')){next();if(!startsExpr(T[p]))break;elts.push(test());}return N('Tuple',x,{elts});}
  function testListStar(noIn){const x=T[p];const one=()=>isOp('*')?(next(),N('Starred',x,{v:noIn?orExpr():orExpr()})):(noIn?orExpr():test());
    const a=one();if(!isOp(','))return a;const elts=[a];while(isOp(',')){next();if(!startsExpr(T[p]))break;elts.push(one());}return N('Tuple',x,{elts});}
  function namedTest(){if(at('name')&&peek(1).t==='op'&&peek(1).v===':='){const x=next();next();return N('Named',x,{v:test()});}return test();}
  function test(){
    const x=T[p];
    if(isKw('lambda')){next();params(':',false);expectOp(':','in the lambda');return N('Other',x,{v:test()});}
    const a=orTest();
    if(isKw('if')){next();orTest();if(!isKw('else'))fail('expected "else" in the "x if cond else y" expression but found '+desc(T[p]),T[p]);next();test();return N('Other',x);}
    return a;
  }
  function orTest(){let a=andTest();while(isKw('or')){const o=next();andTest();a=N('Other',o);}return a;}
  function andTest(){let a=notTest();while(isKw('and')){const o=next();notTest();a=N('Other',o);}return a;}
  function notTest(){if(isKw('not')){const x=next();notTest();return N('Other',x);}return comparison();}
  function comparison(){let a=orExpr();
    for(;;){
      if(at('op')&&['<','>','==','>=','<=','!='].includes(T[p].v)){const o=next();if(!startsExpr(T[p]))fail('nothing after "'+o.v+'"',T[p]);orExpr();a=N('Other',o);continue;}
      if(isKw('in')||isKw('is')){const o=next();if(o.v==='is'&&isKw('not'))next();orExpr();a=N('Other',o);continue;}
      if(isKw('not')&&peek(1).t==='kw'&&peek(1).v==='in'){const o=next();next();orExpr();a=N('Other',o);continue;}
      return a;
    }
  }
  function binLevel(ops,nextFn){return function(){let a=nextFn();while(at('op')&&ops.includes(T[p].v)){const o=next();if(!startsExpr(T[p]))fail('nothing after "'+o.v+'"',T[p],'An operator needs a value on both sides.');const b=nextFn();a=N('BinOp',o,{op:o.v,l:a,r:b});}return a;};}
  function factor(){if(at('op')&&['+','-','~'].includes(T[p].v)){const o=next();const v=factor();return N('Unary',o,{op:o.v,v});}return power();}
  function power(){if(isKw('await')){next();}const a=atomExpr();if(isOp('**')){const o=next();const b=factor();return N('BinOp',o,{op:'**',l:a,r:b});}return a;}
  const term=binLevel(['*','/','//','%','@'],factor),arith=binLevel(['+','-'],term),shift=binLevel(['<<','>>'],arith),band=binLevel(['&'],shift),bxor=binLevel(['^'],band);
  const orExpr=binLevel(['|'],bxor);
  function atomExpr(){
    let a=atom();
    for(;;){
      if(isOp('(')){const o=next();const r=callArgs(')');a=N('Call',o,{func:a,args:r.args,kws:r.kws,line:a.line,col:a.col});continue;}
      if(isOp('[')){const o=next();subscripts();expectOp(']','to close the index');a=N('Sub',o,{obj:a,line:a.line,col:a.col});continue;}
      if(isOp('.')){next();if(!at('name')&&!at('kw'))fail('expected a name after "."',T[p]);const nm=next();a=N('Attr',nm,{obj:a,attr:nm.v,line:a.line,col:a.col});continue;}
      return a;
    }
  }
  function subscripts(){do{if(isOp(']'))break;if(!isOp(':'))isOp('*')?(next(),orExpr()):test();if(isOp(':')){next();if(!isOp(']')&&!isOp(',')&&!isOp(':'))test();if(isOp(':')){next();if(!isOp(']')&&!isOp(','))test();}}}while(isOp(',')&&next());}
  function callArgs(close){
    const args=[],kws=[];const open=T[p-1];
    while(!isOp(close)){
      if(isOp(','))fail('extra comma: "," with nothing before it',T[p],'Remove the extra ",".');
      const x=T[p];
      if(isOp('*')||isOp('**')){next();args.push(N('Starred',x,{v:test()}));}
      else if(at('name')&&peek(1).t==='op'&&peek(1).v==='='){const nm=next();next();kws.push({name:nm.v,value:test(),line:nm.line});}
      else{const v=namedTest();if(isKw('for')||isKw('async')){compFor();}args.push(v);}
      if(isOp(','))next();
      else if(!isOp(close))missingSep(close,open);
    }
    next();
    lintStrings(args,'call');
    return {args,kws};
  }
  function missingSep(close,open){
    const x=T[p];
    if(x.t==='newline'||x.t==='eof')fail('"'+open.v+'" opened on line '+open.line+' was never closed',open);
    if(startsExpr(x))fail('missing comma? Expected "," or "'+close+'" but found '+desc(x),x,'Two items are next to each other with nothing between them. Add a "," between them.');
    fail('expected "," or "'+close+'" but found '+desc(x),x);
  }
  function compFor(){while(isKw('for')||isKw('async')){if(isKw('async'))next();next();targetList();if(!isKw('in'))fail('expected "in" in the comprehension');next();orTest();while(isKw('if')){next();orTest();}}}
  function seq(close,kind,open){
    // list, tuple, set display; returns elts
    const elts=[];let first=true;
    while(!isOp(close)){
      if(isOp(','))fail(first?'extra comma: "," right after "'+open.v+'"':'extra comma: two commas in a row',T[p],'Remove the extra ",".');
      const x=T[p];const v=isOp('*')?(next(),N('Starred',x,{v:orExpr()})):namedTest();
      if(first&&(isKw('for')||isKw('async'))){compFor();if(!isOp(close))fail('expected "'+close+'" after the comprehension but found '+desc(T[p]));next();return {elts:[v],comp:true};}
      elts.push(v);first=false;
      if(isOp(','))next();else if(!isOp(close))missingSep(close,open);
    }
    next();return {elts};
  }
  function atom(){
    const x=T[p];
    if(x.t==='name'){next();return N('Name',x,{id:x.v});}
    if(x.t==='num'){next();return N('Const',x,{v:Number(x.v.replace(/_/g,''))||0});}
    if(x.t==='str'){
      const parts=[next()];while(at('str'))parts.push(next());
      const n=N('Const',x,{v:parts.map(s=>s.v).join(''),str:true,f:parts.some(s=>s.f),parts:parts.length,lines:[...new Set(parts.map(s=>s.line))]});return n;
    }
    if(x.t==='kw'&&(x.v==='None'||x.v==='True'||x.v==='False')){next();return N('Const',x,{v:x.v==='None'?null:x.v==='True'});}
    if(isOp('...')){next();return N('Const',x,{v:'...'});}
    if(isOp('(')){next();if(isOp(')')){next();return N('Tuple',x,{elts:[]});}
      if(isKw('yield')){yieldExpr();expectOp(')');return N('Other',x);}
      const first=isOp('*')?(next(),N('Starred',x,{v:orExpr()})):namedTest();
      if(isKw('for')||isKw('async')){compFor();expectOp(')','to close the generator');return N('Other',x);}
      if(isOp(')')){next();return Object.assign({},first,{paren:true});}
      if(!isOp(','))missingSep(')',x);
      const elts=[first];while(isOp(',')){next();if(isOp(')'))break;if(isOp(','))fail('extra comma: two commas in a row',T[p]);elts.push(isOp('*')?(next(),N('Starred',x,{v:orExpr()})):namedTest());if(!isOp(',')&&!isOp(')'))missingSep(')',x);}
      expectOp(')');lintStrings(elts,'tuple');return N('Tuple',x,{elts});}
    if(isOp('[')){next();const r=seq(']','list',x);if(!r.comp)lintStrings(r.elts,'list');return N(r.comp?'Other':'List',x,{elts:r.elts});}
    if(isOp('{')){next();return braces(x);}
    if(x.t==='kw')fail('invalid syntax: "'+x.v+'" can\u2019t be used here',x,x.v==='else'||x.v==='elif'?'Check the indentation and the ":" on the line above.':'');
    if(x.t==='newline'||x.t==='eof')fail('unexpected end of line \u2014 something is missing',x);
    if(x.t==='indent')fail('unexpected indent',x,'This line is indented more than the lines around it.');
    fail('invalid syntax: unexpected '+desc(x),x,x.t==='op'&&x.v===','?'There is a "," with nothing before it.':'');
  }
  function braces(open){
    if(isOp('}')){next();return N('Dict',open,{keys:[],vals:[]});}
    const keys=[],vals=[],elts=[];let isDict=null,first=true;
    while(!isOp('}')){
      if(isOp(','))fail(first?'extra comma: "," right after "{"':'extra comma: two commas in a row',T[p],'Remove the extra ",".');
      if(isOp('**')){next();orExpr();keys.push(null);vals.push(null);isDict=true;}
      else{
        const kx=T[p];const k=isOp('*')?(next(),N('Starred',kx,{v:orExpr()})):namedTest();
        if(isDict===null)isDict=isOp(':');
        if(isDict){
          if(!isOp(':'))fail('expected ":" after the dictionary key but found '+desc(T[p]),T[p],'Each item in a dict is written key: value.');
          next();const v=test();
          if(isOp(':')&&v&&v.k==='Const'&&v.parts>1)fail('missing comma? Two strings are joined here, so the next ":" makes no sense',T[p],'Python joined the value and the next key into '+JSON.stringify(v.v)+'. Add a "," after the value on line '+v.line+'.');
          if(first&&(isKw('for')||isKw('async'))){compFor();expectOp('}');return N('Other',open);}
          keys.push(k);vals.push(v);
        }else{
          if(isOp(':'))fail('unexpected ":" in a set',T[p]);
          if(first&&(isKw('for')||isKw('async'))){compFor();expectOp('}');return N('Other',open);}
          elts.push(k);
        }
      }
      first=false;
      if(isOp(','))next();
      else if(!isOp('}')){
        const y=T[p];
        if(isDict&&startsExpr(y))fail('missing comma? Expected "," or "}" but found '+desc(y),y,'Two dictionary items are next to each other with no "," between them.');
        missingSep('}',open);
      }
    }
    next();
    if(isDict){
      const seen=new Map();
      keys.forEach((k,i)=>{if(k&&k.k==='Const'&&typeof k.v==='string'){if(seen.has(k.v))res.warnings.push({line:k.line,msg:'Key '+JSON.stringify(k.v)+' appears twice in the same dict.',hint:'Only the last value is kept; the earlier one (line '+seen.get(k.v)+') is silently dropped.'});else seen.set(k.v,k.line);}});
      return N('Dict',open,{keys,vals});
    }
    lintStrings(elts,'set');return N('Set',open,{elts});
  }
  function lintStrings(elts,where){
    elts.forEach(e=>{if(e&&e.k==='Const'&&e.parts>1&&!e.paren)res.warnings.push({line:e.line,msg:'Two strings next to each other in a '+where+' are joined into one: '+JSON.stringify(e.v.length>80?e.v.slice(0,77)+'...':e.v)+'.',hint:'Python joins "a" "b" into "ab". If they were meant to be separate items, a comma is missing'+(e.lines.length>1?' at the end of line '+e.lines[0]:'')+'.',kind:'implicit-concat'});});
  }
  try{res.body=file();}
  catch(e){if(e instanceof PySyntaxError){res.ok=false;res.error=e;}else throw e;}
  return res;
}

/* ---------- reading values out of the tree ---------- */
function pyLit(n){
  if(!n)return undefined;
  switch(n.k){
    case 'Const':return n.v;
    case 'List':case 'Tuple':case 'Set':{const a=n.elts.map(pyLit);return a.some(x=>x===undefined)?undefined:a;}
    case 'Dict':{const o={};for(let i=0;i<n.keys.length;i++){if(!n.keys[i])return undefined;const k=pyLit(n.keys[i]),v=pyLit(n.vals[i]);if(k===undefined||v===undefined)return undefined;o[k]=v;}return o;}
    case 'Unary':{const v=pyLit(n.v);return typeof v==='number'?(n.op==='-'?-v:v):undefined;}
    default:return undefined;
  }
}
function pyTopAssigns(body){const m=new Map();body.forEach(s=>{if(s.k==='Assign')s.targets.forEach(t=>{if(t.k==='Name')m.set(t.id,s.value);});});return m;}
if(typeof module!=='undefined')module.exports={pyTokenize,pyParse,pyLit,pyTopAssigns,PySyntaxError};
