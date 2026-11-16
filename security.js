/* 受信値の検証。認証の代わりではない。Node でも同じ検証をテストする。 */
(function(root){
  'use strict';
  const own=(o,k)=>Object.prototype.hasOwnProperty.call(o,k);
  const dict=()=>Object.create(null);
  const int=(v,min=0,max=Number.MAX_SAFE_INTEGER)=>Number.isSafeInteger(v)&&v>=min&&v<=max;
  const num=(v,min=-1e9,max=1e9)=>Number.isFinite(v)&&v>=min&&v<=max;
  const record=v=>!!v&&typeof v==='object'&&!Array.isArray(v);
  const text=(v,n)=>typeof v==='string'&&v.length<=n;
  const key=k=>typeof k==='string'&&k.length>0&&k.length<=100&&!['__proto__','prototype','constructor'].includes(k);
  function bounded(v,depth=0,budget={n:0}){
    if(++budget.n>4096||depth>12)return false;
    if(v===null||typeof v==='boolean')return true;
    if(typeof v==='number')return Number.isFinite(v);
    if(typeof v==='string')return v.length<=32768;
    if(!v||typeof v!=='object')return false;
    const ks=Object.keys(v);if(ks.length>(Array.isArray(v)?256:128))return false;
    return ks.every(k=>key(k)&&bounded(v[k],depth+1,budget));
  }
  function randomCode(cryptoSource=root.crypto){
    if(!cryptoSource||!cryptoSource.getRandomValues)throw new Error('安全なルームコードの生成に対応したブラウザが必要です');
    return Array.from(cryptoSource.getRandomValues(new Uint8Array(16)),b=>b.toString(16).padStart(2,'0')).join('');
  }
  function item(s,depth=0){
    if(!text(s,2048)||depth>4)return false;
    if(!s)return true;
    if('KR'.includes(s[0])){const parts=s.slice(1).split('.');return parts.length<=24&&parts.every(x=>item(x,depth+1));}
    return s.length<=64&&/^[A-Za-z0-9!%#:\-]*$/.test(s);
  }
  function world(w){
    if(!record(w)||!bounded(w)||!text(w.ep,80)||!int(w.q,1))return null;
    if(!Array.isArray(w.fi)||!Array.isArray(w.cu)||!Array.isArray(w.rv))return null;
    return {...w,
      fi:w.fi.slice(0,90).filter(a=>Array.isArray(a)&&a.length===6&&int(a[0],1)&&item(a[1])&&num(a[2],-2000,2000)&&num(a[3],0,400)&&num(a[4],-2000,2500)&&num(a[5],-100000,100000)),
      cu:w.cu.slice(0,32).filter(a=>Array.isArray(a)&&a.length===9&&int(a[0],1)&&int(a[1],0,4294967295)&&num(a[2],-3000,3000)&&num(a[3],-3000,3000)&&num(a[4],-100000,100000)&&['',1,2,3,'1','2','3'].includes(a[5])&&int(a[6],0,1)&&int(a[7],0,5)&&int(a[8],0,3)),
      rv:w.rv.slice(0,6).filter(a=>Array.isArray(a)&&a.length===5&&int(a[0],1)&&int(a[1],1)&&int(a[2],1,5)&&text(a[3],40)&&num(a[4],-1000,1000))};
  }
  function game(g){
    if(!record(g)||!bounded(g)||g.v!==1||!text(g.ep,80)||!int(g.q,1)||!['title','prep','open','closing','summary','end'].includes(g.ph)||!num(g.t,0,1e7))return false;
    const sd=g.sd;if(!record(sd)||!num(sd.m)||!int(sd.d,1)||!int(sd.r,0,500)||!int(sd.o)||!Array.isArray(sd.st)||sd.st.length>32||!sd.st.every(v=>int(v,0,1e7))||!text(sd.up,512)||!text(sd.dv,2048))return false;
    for(const [k,max] of [['gr',6],['fr',2],['dk',16],['bd',16],['sw',4],['sl',4],['tt',8],['msg',8],['inf',2]])if(!Array.isArray(g[k])||g[k].length>max)return false;
    if(!g.gr.every(v=>num(v,-1,1e8))||!g.fr.every(v=>v===-1||Array.isArray(v)&&v.length===2&&num(v[0],0,1e8)&&int(v[1],0,8))||!g.dk.every(v=>num(v,-1,1e8)))return false;
    if(!g.bd.every(a=>Array.isArray(a)&&a.length===2&&text(a[0],48)&&int(a[1],0,1))||!g.sw.every(v=>int(v))||!g.tt.every(v=>item(v)))return false;
    if(!g.sl.every(a=>a===0||Array.isArray(a)&&a.length===13&&int(a[0],1)&&[1,2,3,7,8,9].every(i=>text(a[i],256))&&[4,5,6].every(i=>num(a[i],0,1e9))&&int(a[10],0,2)&&int(a[11],-1,7)&&int(a[12],0,1)))return false;
    if(!g.msg.every(a=>Array.isArray(a)&&a.length===5&&int(a[0],1)&&text(a[1],80)&&text(a[2],70)&&['','good','bad'].includes(a[3])&&int(a[4],0,1)))return false;
    if(!record(g.hd)||!Object.values(g.hd).every(v=>item(v))||!record(g.ak)||!Object.values(g.ak).every(v=>int(v))||!text(g.bs,32))return false;
    if(g.sum){
      if(!record(g.sum)||!Array.isArray(g.sum.rv)||g.sum.rv.length>6)return false;
      if(!['cl','cf','mess','rt','ed','tn','day','sv','lo','rev','tip','spent','waste','wrong','net','money','r0','r'].every(k=>num(g.sum[k])))return false;
      if(!g.sum.rv.every(a=>Array.isArray(a)&&a.length===5&&int(a[0],1)&&int(a[1],1)&&int(a[2],1,5)&&text(a[3],40)&&num(a[4],-1000,1000)))return false;
    }
    if(sd.en&&(!Array.isArray(sd.en)||sd.en.length!==5||!sd.en.every(v=>num(v))))return false;
    return g.inf.every(v=>int(v,0,1))&&(!g.ds||Array.isArray(g.ds)&&g.ds.length===12&&g.ds.every(v=>num(v)));
  }
  function action(a,limits){
    if(!Array.isArray(a)||!int(a[0],1)||typeof a[1]!=='string'||!own(limits,a[1])||!int(a[2]))return false;
    const k=a[1];if(a[2]>=limits[k])return false;
    if(k==='surface')return a.length===5&&num(a[3],-10000,10000)&&num(a[4],-10000,10000);
    if(k==='undo')return a.length===4&&int(a[3],0,1);
    return a.length===3;
  }
  function allow(rates,id,now){
    let r=rates[id];if(!r)r=rates[id]={at:now,tokens:40};
    r.tokens=Math.min(40,r.tokens+Math.max(0,now-r.at)*.02);r.at=now;
    if(r.tokens<1)return false;r.tokens--;return true;
  }
  const api={own,dict,int,num,record,text,key,bounded,randomCode,item,world,game,action,allow};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.BUNBUN_SAFE=Object.freeze(api);
})(typeof window!=='undefined'?window:globalThis);
