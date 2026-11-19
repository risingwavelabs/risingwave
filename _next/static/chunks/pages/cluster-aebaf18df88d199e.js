(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[150],{59784:(e,t,i)=>{"use strict";i.d(t,{EV:()=>c,VL:()=>a,ZS:()=>l,gd:()=>s,vM:()=>d});var n=i(58530),r=i(22222);async function s(){return await r.Ay.get("/metrics/cluster")}async function a(){return(await r.Ay.get("/clusters/1")).map(n.v4.fromJSON)}async function c(){return(await r.Ay.get("/clusters/2")).map(n.v4.fromJSON)}async function o(){return(await r.Ay.get("/clusters/4")).map(n.v4.fromJSON)}async function d(){let[e,t,i]=await Promise.all([c(),a(),o()]);return[...e,...t,...i]}async function l(){return await r.Ay.get("/version")}},75826:(e,t,i)=>{"use strict";i.r(t),i.d(t,{default:()=>F});var n=i(23798),r=i(90410),s=i.n(r),a=i(83585),c=i.n(a),o=i(43080),d=i.n(o),l=i(59857),u=i.n(l),h=i(21462),m=i(35560),p=i(31627),g=i(73977),x=i(64142),j=i(88397),f=i(43421),y=i(40757),w=i(59784),b=i(31702);let k=f.Ay.main`
  min-height: 100vh;
  padding: 32px 24px;
  color: ${b.Tj.foreground};
  background-color: ${b.Tj.background};
  background-image: ${b.ER.backgroundImage};
  background-size: ${b.ER.backgroundSize};
  font-family: ${b.lG.body};

  @media (min-width: 62rem) {
    padding: 48px 40px;
  }
`,v=f.Ay.h1`
  margin: 0;
  font-size: 24px;
  font-weight: 600;
  line-height: 1.2;
  letter-spacing: -0.01em;
`,A=f.Ay.p`
  margin: 8px 0 0;
  color: ${b.Tj.mutedForeground};
  font-size: 14px;
  line-height: 1.5;
`,T=f.Ay.section`
  margin-top: 32px;
`,N=f.Ay.h2`
  margin: 0 0 12px;
  color: ${b.Tj.foregroundSecondary};
  font-size: 14px;
  font-weight: 500;
  line-height: 1.4;
`,$=f.Ay.div`
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 16px;
`,E=f.Ay.div`
  padding: 16px;
  border: 1px solid rgba(227, 224, 216, 0.65);
  border-radius: ${b.O7.lg};
  background: rgba(255, 255, 255, 0.96);
  box-shadow: ${b.Eo.surfaceCard};
`,S=f.Ay.div`
  display: flex;
  align-items: center;
  gap: 8px;
`,_=f.Ay.span`
  width: 8px;
  height: 8px;
  flex-shrink: 0;
  border-radius: 50%;
  background: ${b.Tj.success};
`,C=f.Ay.h3`
  margin: 0;
  font-size: 14px;
  font-weight: 600;
  line-height: 1.4;
`,z=f.Ay.p`
  margin: 4px 0 0;
  color: ${b.Tj.mutedForeground};
  font-size: 12px;
  line-height: 1.45;
`,O=f.Ay.p`
  margin: 0 0 8px;
  color: ${b.Tj.mutedForeground};
  font-size: 12px;
  line-height: 1.45;

  strong {
    color: ${b.Tj.foregroundSecondary};
    font-weight: 600;
  }
`;function D({workerNodeType:e,workerNode:t}){return(0,n.jsxs)("div",{children:[(0,n.jsxs)(S,{children:[(0,n.jsx)(_,{}),(0,n.jsxs)(C,{children:[e," #",t.id]})]}),(0,n.jsx)(z,{children:"Running"}),(0,n.jsxs)(z,{children:[t.host?.host,":",t.host?.port]})]})}function M({job:e,instance:t,metrics:i,isCpuMetrics:r}){let a=(0,h.useCallback)(()=>{let e=[];if(0===i.length)return[];let t=i.at(-1).timestamp;for(let n of c()(s()(i))){for(;t-n.timestamp>0;)t-=60,e.push({timestamp:t,value:0});e.push(n),t-=60}for(;e.length<60;)e.push({timestamp:t,value:0}),t-=60;return c()(e)},[i]);return(0,n.jsxs)("div",{children:[(0,n.jsxs)(O,{children:[(0,n.jsx)("strong",{children:e})," ",t]}),(0,n.jsx)(m.u,{width:"100%",height:100,children:(0,n.jsxs)(p.Q,{data:a(),children:[(0,n.jsx)(g.W,{dataKey:"timestamp",type:"number",domain:["dataMin","dataMax"],hide:!0}),r&&(0,n.jsx)(x.h,{type:"number",domain:[0,1],hide:!0}),(0,n.jsx)(j.G,{isAnimationActive:!1,type:"linear",dataKey:"value",strokeWidth:1.5,stroke:b.Tj.accent,fill:"rgba(42, 157, 244, 0.12)"})]})})]})}function F(){let[e,t]=(0,h.useState)([]),[i,r]=(0,h.useState)([]),[s,a]=(0,h.useState)(),[c,o]=(0,h.useState)(),l=(0,y.A)();return(0,h.useEffect)(()=>((async function(){try{t(await (0,w.VL)()),r(await (0,w.EV)()),o(await (0,w.ZS)())}catch(e){l(e)}})(),()=>{}),[l]),(0,h.useEffect)(()=>((async function(){for(;;)try{let e=await (0,w.gd)();e.cpuData=d()(e.cpuData,e=>e.metric.instance),e.memoryData=d()(e.memoryData,e=>e.metric.instance),a(e),await new Promise(e=>setTimeout(e,5e3))}catch(e){l(e,"warning");break}})(),()=>{}),[l]),(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(u(),{children:(0,n.jsx)("title",{children:"Cluster Overview"})}),(0,n.jsxs)(k,{children:[(0,n.jsxs)("header",{children:[(0,n.jsx)(v,{children:"Cluster Overview"}),(0,n.jsxs)(A,{children:["Version: ",c]})]}),(0,n.jsxs)(T,{children:[(0,n.jsx)(N,{children:"Nodes"}),(0,n.jsxs)($,{children:[e.map(e=>(0,n.jsx)(E,{children:(0,n.jsx)(D,{workerNodeType:"Frontend",workerNode:e})},e.id)),i.map(e=>(0,n.jsx)(E,{children:(0,n.jsx)(D,{workerNodeType:"Compute",workerNode:e})},e.id))]})]}),(0,n.jsxs)(T,{children:[(0,n.jsx)(N,{children:"CPU Usage"}),(0,n.jsx)($,{children:s&&s.cpuData.map(e=>(0,n.jsx)(E,{children:(0,n.jsx)(M,{job:e.metric.job,instance:e.metric.instance,metrics:e.sample,isCpuMetrics:!0})},e.metric.instance))})]}),(0,n.jsxs)(T,{children:[(0,n.jsx)(N,{children:"Memory Usage"}),(0,n.jsx)($,{children:s&&s.memoryData.map(e=>(0,n.jsx)(E,{children:(0,n.jsx)(M,{job:e.metric.job,instance:e.metric.instance,metrics:e.sample,isCpuMetrics:!1})},e.metric.instance))})]})]})]})}},92445:(e,t,i)=>{(window.__NEXT_P=window.__NEXT_P||[]).push(["/cluster",function(){return i(75826)}])}},e=>{e.O(0,[863,324,421,50,339,636,593,792],()=>e(e.s=92445)),_N_E=e.O()}]);