(globalThis.TURBOPACK||(globalThis.TURBOPACK=[])).push(["object"==typeof document?document.currentScript:void 0,25953,(e,t,i)=>{let r="/await_tree";(window.__NEXT_P=window.__NEXT_P||[]).push([r,()=>e.r(33436)]),t.hot&&t.hot.dispose(function(){window.__NEXT_P.push([r])})},79733,e=>{"use strict";var t=e.i(49990),i=e.i(84025);async function r(){return await i.U.get("/metrics/cluster")}async function a(){return(await i.U.get("/clusters/1")).map(t.a.fromJSON)}async function n(){return(await i.U.get("/clusters/2")).map(t.a.fromJSON)}async function s(){return(await i.U.get("/clusters/4")).map(t.a.fromJSON)}async function o(){let[e,t,i]=await Promise.all([n(),a(),s()]);return[...e,...t,...i]}async function l(){return await i.U.get("/version")}e.s(["R",0,n,"q",0,a,"C",0,o,"x",0,r,"D",0,l])},33436,e=>{"use strict";var t=e.i(78902),i=e.i(35685),r=e.i(78435),a=e.i(18698),n=e.i(13318),s=e.i(5863),o=e.i(97702),l=e.i(40574),c=e.i(42113),u=e.i(76868),f=e.i(79559),d=e.i(89959),h=e.i(21018),p=e.i(8084),m=e.i(84025),w=e.i(79733),j=e.i(8516),$=e.i(27936);e.s(["default",0,function(){let{response:e}=(0,j.f)(w.R),[x,g]=(0,d.useState)(),[T,v]=(0,d.useState)("");(0,d.useEffect)(()=>{e&&!x&&g("")},[e,x]);let S=async()=>{let e,t;if(void 0!==x){e=""===x?"Await-Tree Dump of All Compute Nodes:":`Await-Tree Dump of Compute Node ${x}:`,v("Loading...");try{let i=$.D.fromJSON(await m.U.get(`/monitor/await_tree/${x}`)),r=(0,u.default)(i.actorTraces).entries().map(([e,t])=>`[Actor ${e}]
${t}`).join("\n"),a=(0,u.default)(i.rpcTraces).entries().map(([e,t])=>`[RPC ${e}]
${t}`).join("\n"),n=(0,u.default)(i.compactionTaskTraces).entries().map(([e,t])=>`[Compaction ${e}]
${t}`).join("\n"),s=(0,u.default)(i.inflightBarrierTraces).entries().map(([e,t])=>`[Barrier ${e}]
${t}`).join("\n"),o=(0,u.default)(i.barrierWorkerState).entries().map(([e,t])=>`[BarrierWorkerState (Worker ${e})]
${t}`).join("\n"),l=(0,u.default)(i.jvmStackTraces).entries().map(([e,t])=>`[JVM (Worker ${e})]
${t}`).join("\n");t=`${e}

${r}
${a}
${n}
${s}
${o}

${l}`}catch(i){t=`${e}

ERROR: ${i.message}
${i.cause}`}v(t)}},y=(0,t.jsxs)(a.f,{p:3,height:"calc(100vh - 20px)",flexDirection:"column",children:[(0,t.jsx)(p.f,{children:"Await Tree Dump"}),(0,t.jsxs)(a.f,{flexDirection:"row",height:"full",width:"full",children:[(0,t.jsx)(l.f,{mr:3,spacing:3,alignItems:"flex-start",width:200,height:"full",children:(0,t.jsxs)(n.K,{children:[(0,t.jsx)(s.f,{children:"Compute Nodes"}),(0,t.jsxs)(l.f,{children:[(0,t.jsxs)(o.f,{onChange:e=>g(e.target.value),children:[e&&(0,t.jsx)("option",{value:"",children:"All"},""),e&&e.map(e=>(0,t.jsxs)("option",{value:e.id,children:["(",e.id,") ",e.host?.host,":",e.host?.port]},e.id))]}),(0,t.jsx)(r.f,{onClick:e=>S(),width:"full",children:"Dump"})]})]})}),(0,t.jsx)(i.f,{flex:1,height:"full",ml:3,overflowX:"scroll",overflowY:"scroll",children:void 0===T?(0,t.jsx)(h.f,{}):(0,t.jsx)(c.f,{language:"sql",options:{fontSize:13,readOnly:!0,renderWhitespace:"boundary",wordWrap:"on"},defaultValue:'Select a compute node and click "Dump"...',value:T})})]})]});return(0,t.jsxs)(d.Fragment,{children:[(0,t.jsx)(f.default,{children:(0,t.jsx)("title",{children:"Await Tree Dump"})}),y]})}])}]);