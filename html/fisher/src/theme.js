/* ================= Theme (Ghibli misty hills) ================= */
const THEMES={
  earth:{sky:['#EBC9A0','#F7E9D4'],ridges:['#E2B88E','#CF946A','#B0704C','#7E4A33'],sun:'#C8452C',acc:'#B0532A',acc2:'#C99230'},
  forest:{sky:['#C9DFCB','#EFF4E7'],ridges:['#AECBA8','#84AC82','#5C8C61','#3B6948'],sun:'#FFFDF4',acc:'#3B7A4D',acc2:'#C6613A'},
  sea:{sky:['#B8DCE8','#EDF6F4'],ridges:['#A3CEDC','#74B2C8','#4690B0','#256A8C'],sun:'#FFFFFF',acc:'#1D7898',acc2:'#E0784E'},
  wind:{sky:['#CFDBF0','#F4F2F8'],ridges:['#C3CEE6','#A6B4D7','#8894C2','#6772A6'],sun:'#FFFFFF',acc:'#4569B3',acc2:'#D9A32E'}
};
const PHASES={dawn:{sky:['#EDB9AC','#FAE3CF'],tint:'#F2B3A0',k:.22},dusk:{sky:['#D98A7E','#F4C9A6'],tint:'#6A3E62',k:.42},night:{sky:['#16203D','#34406A'],tint:'#111933',k:.66}};
const TKEY='fisher.theme';
function phaseNow(d){const h=(d||new Date()).getHours();return h>=5&&h<8?'dawn':h>=16&&h<19?'dusk':(h>=19||h<5)?'night':'day';}
function mix(a,b,k){const p=x=>[1,3,5].map(i=>parseInt(x.slice(i,i+2),16));const A=p(a),B=p(b);return '#'+A.map((v,i)=>Math.round(v+(B[i]-v)*k).toString(16).padStart(2,'0')).join('');}
function rng(seed){return function(){seed|=0;seed=seed+0x6D2B79F5|0;let t=Math.imul(seed^seed>>>15,1|seed);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296;};}
function ridge(yBase,amp,seed,freq,W,H){W=W||800;H=H||440;const r=rng(seed),p=[r()*6,r()*6];let d='M0 '+H+' L';
  for(let x=0;x<=W;x+=5){const y=yBase-amp*(0.7*Math.sin(x*freq+p[0])+0.3*Math.sin(x*freq*2.1+p[1]));d+=x+' '+y.toFixed(1)+' L';}
  return d+W+' '+H+'Z';}
function sceneSvg(name,phase,kind){
  const T=THEMES[name],P=PHASES[phase];const sky=P?P.sky:T.sky;
  const rc=T.ridges.map(c=>P?mix(c,P.tint,P.k):c);
  const band=kind==='band',W=band?1600:800,H=band?100:250;
  const R=band?[[46,9,.006,11],[60,8,.008,23],[74,7,.011,37],[88,6,.014,51]]:[[128,18,.0085,11],[156,15,.011,23],[184,12,.014,37],[212,10,.018,51]];
  const sx=band?1380:610,sy=band?(phase==='day'?28:40):(phase==='day'?62:84),sr=band?13:24;
  let s='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="xMidYMax slice"><defs><linearGradient id="k" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="'+sky[0]+'"/><stop offset="1" stop-color="'+sky[1]+'"/></linearGradient><linearGradient id="m" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#fff" stop-opacity="0"/><stop offset=".7" stop-color="#fff" stop-opacity="'+(phase==='night'?'.07':'.3')+'"/><stop offset="1" stop-color="#fff" stop-opacity="0"/></linearGradient></defs><rect width="'+W+'" height="'+H+'" fill="url(#k)"/>';
  if(phase==='night'){const r=rng(7);for(let i=0;i<(band?50:36);i++)s+='<circle cx="'+(r()*W).toFixed(0)+'" cy="'+(r()*H*.45).toFixed(0)+'" r="'+(r()*1.1+.4).toFixed(1)+'" fill="#fff" opacity="'+(r()*.5+.4).toFixed(2)+'"/>';
    s+='<circle cx="'+sx+'" cy="'+sy+'" r="'+sr+'" fill="#F4EFD8"/><circle cx="'+(sx+sr*.45)+'" cy="'+(sy-sr*.3)+'" r="'+(sr*.92)+'" fill="'+mix(sky[0],sky[1],(sy-sr*.3)/H)+'"/>';}
  else{s+='<circle cx="'+sx+'" cy="'+sy+'" r="'+sr+'" fill="'+T.sun+'" opacity="'+(name==='earth'?1:.92)+'"/>';
    if(!band)s+='<g fill="none" stroke="'+mix(rc[3],'#000000',.2)+'" stroke-width="1.6" stroke-linecap="round" opacity=".5"><path d="M470 70 q5 -5 10 0 q5 -5 10 0"/><path d="M500 58 q4 -4 8 0 q4 -4 8 0"/><path d="M520 76 q3 -3 6 0 q3 -3 6 0"/></g>';}
  R.forEach((q,i)=>{s+='<path d="'+ridge(q[0],q[1],q[3],q[2],W,H)+'" fill="'+rc[i]+'"/>';if(i<3)s+='<rect x="0" y="'+(q[0]-q[1]*1.2)+'" width="'+W+'" height="'+(q[1]*2.4)+'" fill="url(#m)"/>';});
  return 'data:image/svg+xml;charset=utf-8,'+encodeURIComponent(s+'</svg>');
}
let themeName='earth',themePhase='day';
function themePref(){try{return localStorage.getItem(TKEY)||'random';}catch(e){return 'random';}}
function applyTheme(pref){
  pref=pref||themePref();const names=Object.keys(THEMES);
  themeName=names.includes(pref)?pref:names[Math.floor(Math.random()*names.length)];themePhase=phaseNow();
  const T=THEMES[themeName],dark=window.matchMedia&&matchMedia('(prefers-color-scheme: dark)').matches;
  const acc=dark?mix(T.acc,'#FFFFFF',.35):T.acc,rs=document.documentElement.style;
  rs.setProperty('--accent',acc);rs.setProperty('--accent-soft',dark?mix(T.acc,'#1d1d1b',.75):mix(T.acc,'#FFFFFF',.88));rs.setProperty('--accent2',T.acc2);
  const hs=sceneSvg(themeName,themePhase,'hero'),bs=sceneSvg(themeName,themePhase,'band');
  document.querySelectorAll('img.scene').forEach(i=>i.src=i.closest('.band')?bs:hs);
  document.body.classList.toggle('sky-dark',themePhase==='dusk'||themePhase==='night');
  document.querySelectorAll('.themeSel').forEach(sel=>{sel.innerHTML=['random',...names].map(n=>'<option value="'+n+'"'+(n===pref?' selected':'')+'>'+(n==='random'?'Random each visit ('+themeName+')':cap(n))+'</option>').join('');});
}

