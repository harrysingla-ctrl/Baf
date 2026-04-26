const { useState, useEffect, useCallback } = React;

const FUND_META = {
  icici_baf:     { shortName:"ICICI BAF",      color:"#F97316", amc:"ICICI Prudential", category:"BAF" },
  hdfc_baf:      { shortName:"HDFC BAF",       color:"#EF4444", amc:"HDFC AMC",         category:"BAF" },
  edelweiss_baf: { shortName:"Edelweiss BAF",  color:"#8B5CF6", amc:"Edelweiss MF",     category:"BAF" },
  dsp_daa:       { shortName:"DSP DAA",        color:"#06B6D4", amc:"DSP MF",           category:"DAA" },
  kotak_baf:     { shortName:"Kotak BAF",      color:"#10B981", amc:"Kotak MF",         category:"BAF" },
  nippon_baf:    { shortName:"Nippon BAF",     color:"#F59E0B", amc:"Nippon India MF",  category:"BAF" },
  bajaj_baf:     { shortName:"Bajaj BAF",      color:"#EC4899", amc:"Bajaj Finserv MF", category:"BAF" },
  axis_daa:      { shortName:"Axis DAA",       color:"#64748b", amc:"Axis MF",          category:"DAA" },
};

function buildSampleData() {
  const periods = Array.from({length:19},(_,i)=>{
    const d=new Date(2024,9+i,1); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}`;
  });
  const base = {
    icici_baf:     [52,51,50,49,47,44,43,42,43,44,46,48,46,45,43,41,42,44,45],
    hdfc_baf:      [58,57,56,54,52,49,48,47,48,50,52,54,52,50,48,46,47,49,50],
    edelweiss_baf: [55,54,53,51,49,46,45,44,45,47,49,51,49,47,45,43,44,46,47],
    dsp_daa:       [60,59,58,56,53,50,49,48,49,51,54,57,55,52,50,48,49,51,52],
    kotak_baf:     [54,53,52,50,48,45,44,43,44,46,48,50,48,46,44,42,43,45,46],
    nippon_baf:    [56,55,54,52,50,47,46,45,46,48,50,52,50,48,46,44,45,47,48],
  };
  const funds = {};
  Object.entries(base).forEach(([id,vals])=>{
    funds[id]={ name:(FUND_META[id]?.shortName||id)+" Fund", periods,
      net_equity:vals, gross_equity:vals.map(v=>v+8),
      hedged:vals.map(()=>8), debt:vals.map(v=>100-v-8) };
  });
  return { funds, generated_at:new Date().toISOString(), _isSample:true };
}

async function loadData() {
  try {
    const r = await fetch("./allocations_export.json");
    if (!r.ok) throw new Error();
    return { ...(await r.json()), _isSample:false };
  } catch { return buildSampleData(); }
}

const sig = eq => eq>=60 ? {label:"Very Bullish",color:"#10B981",bg:"#D1FAE5"}
  : eq>=52 ? {label:"Bullish",color:"#22c55e",bg:"#f0fdf4"}
  : eq>=45 ? {label:"Neutral",color:"#F59E0B",bg:"#FEF3C7"}
  : eq>=38 ? {label:"Cautious",color:"#F97316",bg:"#FFF7ED"}
  :          {label:"Defensive",color:"#EF4444",bg:"#FEF2F2"};

function Spark({vals,color}){
  if(!vals||vals.length<2) return null;
  const mn=Math.min(...vals),mx=Math.max(...vals),r=mx-mn||1,W=80,H=28;
  const pts=vals.map((v,i)=>`${(i/(vals.length-1))*W},${H-((v-mn)/r)*H}`).join(" ");
  return <svg width={W} height={H}><polyline points={pts} fill="none"
    stroke={vals[vals.length-1]>=vals[vals.length-2]?color:"#EF4444"}
    strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>;
}

function Bar({equity,prev}){
  const c=prev!==undefined?equity-prev:0;
  return <div>
    <div style={{display:"flex",borderRadius:6,overflow:"hidden",height:10,background:"#f1f5f9"}}>
      <div style={{width:`${equity}%`,background:"linear-gradient(90deg,#6366f1,#8b5cf6)",transition:"width .6s"}}/>
      <div style={{width:`${100-equity}%`,background:"linear-gradient(90deg,#e2e8f0,#cbd5e1)"}}/>
    </div>
    <div style={{display:"flex",justifyContent:"space-between",fontSize:10,color:"#94a3b8",marginTop:3}}>
      <span>Equity {equity.toFixed(1)}%</span>
      {c!==0&&<span style={{color:c>0?"#10B981":"#EF4444",fontWeight:600}}>{c>0?"▲":"▼"}{Math.abs(c).toFixed(1)}%</span>}
      <span>Debt {(100-equity).toFixed(1)}%</span>
    </div>
  </div>;
}

function Gauge({avg}){
  const pct=Math.min(100,Math.max(0,((avg-30)/50)*100));
  const angle=-130+(pct/100)*260;
  const rad=d=>(d*Math.PI)/180;
  const cx=60,cy=60,r=45,s=sig(avg);
  return <div style={{textAlign:"center"}}>
    <svg viewBox="0 0 120 70" width={160} height={95}>
      {[["#EF4444",-130,-78],["#F97316",-78,-26],["#F59E0B",-26,26],["#6EE7B7",26,78],["#10B981",78,130]]
        .map(([c,s0,e],i)=>{
          const x1=cx+r*Math.cos(rad(s0)),y1=cy+r*Math.sin(rad(s0));
          const x2=cx+r*Math.cos(rad(e)),y2=cy+r*Math.sin(rad(e));
          return <path key={i} d={`M${cx} ${cy} L${x1} ${y1} A${r} ${r} 0 0 1 ${x2} ${y2}Z`} fill={c} opacity=".25"/>;
        })}
      <line x1="60" y1="60" x2={60+38*Math.cos(rad(angle))} y2={60+38*Math.sin(rad(angle))}
        stroke="#1e293b" strokeWidth="2.5" strokeLinecap="round"/>
      <circle cx="60" cy="60" r="4" fill="#1e293b"/>
      <text x="60" y="52" textAnchor="middle" fontSize="11" fontWeight="700" fill="#1e293b">{avg.toFixed(1)}%</text>
    </svg>
    <div style={{fontSize:12,fontWeight:700,color:s.color,marginTop:-8}}>{s.label}</div>
    <div style={{fontSize:10,color:"#94a3b8",marginTop:2}}>Avg Net Equity</div>
  </div>;
}

export default function App(){
  const [data,setData]=useState(null);
  const [tab,setTab]=useState("dashboard");
  const [aiText,setAiText]=useState("");
  const [aiLoading,setAiLoading]=useState(false);
  const [pinned,setPinned]=useState(null);

  useEffect(()=>{ loadData().then(setData); },[]);

  const funds=data ? Object.entries(data.funds).map(([id,f])=>{
    const m=FUND_META[id]||{shortName:id,color:"#64748b",amc:"AMC",category:"BAF"};
    const n=f.net_equity.length;
    return{id,...m,name:f.name,curr:f.net_equity[n-1],prev:f.net_equity[n-2],
      spark:f.net_equity.slice(-8),periods:f.periods,net_equity:f.net_equity,debt:f.debt};
  }) : [];

  const visible=pinned?funds.filter(f=>f.id===pinned):funds;
  const avg=visible.length?visible.reduce((s,f)=>s+f.curr,0)/visible.length:0;
  const latestPeriod=funds[0]?.periods?.slice(-1)[0]||"—";

  const runAI=useCallback(async()=>{
    if(!data) return;
    setAiLoading(true); setAiText("");
    const summary=funds.map(f=>`${f.shortName}: ${f.curr.toFixed(1)}% net equity (`+
      `${f.curr-f.prev>=0?"+":""}${(f.curr-f.prev).toFixed(1)}% MoM)`).join("\n");
    const trend=funds.map(f=>`${f.shortName}: ${f.spark[0].toFixed(1)}% → ${f.spark[f.spark.length-1].toFixed(1)}%`).join("\n");
    const prompt=`You are an expert Indian mutual fund analyst. Analyse these BAF/DAA net equity allocations:

CURRENT (${latestPeriod}):
${summary}

8-MONTH TREND:
${trend}

Consensus avg: ${avg.toFixed(1)}%
${data._isSample?"Note: This is SAMPLE data for demonstration.":"Note: This is LIVE scraped data."}

Give 4 concise paragraphs:
1. What collective valuation signal are fund managers sending?
2. Which fund diverges most from consensus and why that matters?
3. What the trend reveals about the market cycle?
4. Practical takeaway for a retail investor with 3-year horizon.
Use ₹ and Indian market context. Be direct.`;
    try{
      const r=await fetch("https://api.anthropic.com/v1/messages",{
        method:"POST", headers:{"Content-Type":"application/json"},
        body:JSON.stringify({model:"claude-sonnet-4-20250514",max_tokens:1000,
          messages:[{role:"user",content:prompt}]})});
      const d=await r.json();
      setAiText(d.content?.map(b=>b.text||"").join("")||"No response.");
    } catch{ setAiText("Failed to fetch analysis."); }
    setAiLoading(false);
  },[data,funds,avg,latestPeriod]);

  const TABS=[{id:"dashboard",l:"📊 Dashboard"},{id:"trends",l:"📈 Trends"},
    {id:"signals",l:"🧠 AI Signals"},{id:"setup",l:"⚙️ Setup"}];

  if(!data) return(
    <div style={{display:"flex",alignItems:"center",justifyContent:"center",
      height:"100vh",fontFamily:"sans-serif",flexDirection:"column",gap:12}}>
      <div style={{fontSize:36}}>⚖️</div>
      <div style={{fontSize:14,fontWeight:600,color:"#6366f1"}}>Loading…</div>
    </div>);

  return(
  <div style={{fontFamily:"'DM Sans','Segoe UI',sans-serif",background:"#f8fafc",minHeight:"100vh",color:"#1e293b"}}>
    {/* Header */}
    <div style={{background:"linear-gradient(135deg,#0f172a,#1e1b4b,#312e81)",padding:"18px 18px 0",color:"white"}}>
      <div style={{maxWidth:900,margin:"0 auto"}}>
        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:3}}>
          <span style={{fontSize:20}}>⚖️</span>
          <h1 style={{margin:0,fontSize:18,fontWeight:800,letterSpacing:"-0.5px"}}>BAF/DAA Allocation Tracker</h1>
          <span style={{background:data._isSample?"#f59e0b":"#10B981",
            color:data._isSample?"#1e293b":"white",fontSize:9,fontWeight:800,padding:"2px 7px",borderRadius:20}}>
            {data._isSample?"DEMO DATA":"LIVE DATA"}
          </span>
        </div>
        <p style={{margin:"0 0 12px",fontSize:11,color:"#a5b4fc"}}>
          Track net equity allocation shifts in BAF/DAA funds as market valuation signals · {latestPeriod}
        </p>
        <div style={{display:"flex",gap:2}}>
          {TABS.map(t=>(
            <button key={t.id} onClick={()=>setTab(t.id)} style={{
              background:tab===t.id?"white":"transparent",color:tab===t.id?"#1e1b4b":"#a5b4fc",
              border:"none",padding:"7px 13px",borderRadius:"8px 8px 0 0",
              fontSize:12,fontWeight:600,cursor:"pointer"}}>{t.l}</button>
          ))}
        </div>
      </div>
    </div>

    <div style={{maxWidth:900,margin:"0 auto",padding:"16px 13px"}}>

      {/* DASHBOARD */}
      {tab==="dashboard"&&<div>
        {data._isSample&&(
          <div style={{background:"#fef3c7",borderRadius:10,padding:"9px 13px",
            fontSize:12,color:"#92400e",marginBottom:13,borderLeft:"3px solid #f59e0b"}}>
            ⚠️ <strong>Demo mode.</strong> Run <code>python baf_scraper.py --scrape --export</code> and serve <code>allocations_export.json</code> with this page to see real data.
          </div>
        )}
        <div style={{display:"flex",gap:13,marginBottom:16,flexWrap:"wrap"}}>
          <div style={{background:"white",borderRadius:14,padding:"15px 18px",
            boxShadow:"0 1px 4px rgba(0,0,0,.07)",flexShrink:0}}>
            <Gauge avg={avg}/>
          </div>
          <div style={{flex:1,minWidth:190}}>
            <div style={{background:"white",borderRadius:14,padding:"13px 15px",
              boxShadow:"0 1px 4px rgba(0,0,0,.07)",marginBottom:9}}>
              <div style={{fontSize:10,color:"#94a3b8",fontWeight:700,marginBottom:7}}>SIGNAL LEGEND</div>
              {[["Very Bullish","≥60%","#10B981"],["Bullish","52–60%","#22c55e"],
                ["Neutral","45–52%","#F59E0B"],["Cautious","38–45%","#F97316"],
                ["Defensive","<38%","#EF4444"]].map(([l,r,c])=>(
                <div key={l} style={{display:"flex",alignItems:"center",gap:7,marginBottom:4}}>
                  <div style={{width:9,height:9,borderRadius:"50%",background:c}}/>
                  <span style={{fontSize:11,fontWeight:600}}>{l}</span>
                  <span style={{fontSize:10,color:"#94a3b8",marginLeft:"auto"}}>{r}</span>
                </div>
              ))}
            </div>
            <div style={{background:"#eef2ff",borderRadius:10,padding:"9px 13px"}}>
              <div style={{fontSize:11,fontWeight:700,color:"#4338ca"}}>💡 Reading signals</div>
              <div style={{fontSize:11,color:"#6366f1",marginTop:3,lineHeight:1.5}}>
                Low equity → mkt seen as expensive.<br/>Rising equity → value detected.
              </div>
            </div>
          </div>
        </div>

        <div style={{fontSize:10,color:"#94a3b8",fontWeight:700,marginBottom:8}}>
          FUND CARDS · {latestPeriod} · Click to focus
        </div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(250px,1fr))",gap:10}}>
          {funds.map(f=>{
            const s=sig(f.curr), isSel=!pinned||pinned===f.id;
            return(
            <div key={f.id} onClick={()=>setPinned(p=>p===f.id?null:f.id)} style={{
              background:"white",borderRadius:13,padding:"13px 14px",cursor:"pointer",
              boxShadow:pinned===f.id?`0 0 0 2px ${f.color},0 2px 8px rgba(0,0,0,.1)`:"0 1px 4px rgba(0,0,0,.07)",
              opacity:isSel?1:0.5,transition:"all .2s"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:9}}>
                <div>
                  <div style={{display:"flex",alignItems:"center",gap:6}}>
                    <div style={{width:8,height:8,borderRadius:"50%",background:f.color}}/>
                    <span style={{fontSize:13,fontWeight:700}}>{f.shortName}</span>
                    <span style={{fontSize:9,fontWeight:700,background:s.bg,color:s.color,
                      padding:"1px 6px",borderRadius:10}}>{s.label}</span>
                  </div>
                  <div style={{fontSize:10,color:"#94a3b8",marginTop:2}}>{f.amc} · {f.category}</div>
                </div>
                <Spark vals={f.spark} color={f.color}/>
              </div>
              <Bar equity={f.curr} prev={f.prev}/>
            </div>);
          })}
        </div>
      </div>}

      {/* TRENDS */}
      {tab==="trends"&&<div>
        <div style={{background:"white",borderRadius:14,padding:16,
          boxShadow:"0 1px 4px rgba(0,0,0,.07)",marginBottom:13}}>
          <div style={{fontSize:12,fontWeight:700,color:"#64748b",marginBottom:13}}>
            📈 NET EQUITY — LAST 8 PERIODS
          </div>
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead><tr>
                <th style={{textAlign:"left",padding:"4px 8px",color:"#94a3b8",position:"sticky",left:0,background:"white"}}>Fund</th>
                {(funds[0]?.periods?.slice(-8)||[]).map(p=>(
                  <th key={p} style={{padding:"4px 6px",color:"#94a3b8",textAlign:"center",whiteSpace:"nowrap"}}>{p.slice(2)}</th>
                ))}
                <th style={{padding:"4px 6px",color:"#64748b",textAlign:"center"}}>Trend</th>
              </tr></thead>
              <tbody>
                {funds.map(f=>{
                  const rec=f.net_equity.slice(-8);
                  const dir=rec[rec.length-1]-rec[0];
                  return(
                  <tr key={f.id} style={{borderTop:"1px solid #f1f5f9"}}>
                    <td style={{padding:"7px 8px",fontWeight:600,whiteSpace:"nowrap",position:"sticky",left:0,background:"white"}}>
                      <div style={{display:"flex",alignItems:"center",gap:6}}>
                        <div style={{width:8,height:8,borderRadius:"50%",background:f.color,flexShrink:0}}/>
                        {f.shortName}
                      </div>
                    </td>
                    {rec.map((v,i)=>{
                      const c=i>0?v-rec[i-1]:0, s=sig(v);
                      return(
                      <td key={i} style={{padding:"5px 6px",textAlign:"center",background:s.bg,
                        fontSize:12,fontWeight:600,color:s.color}}>
                        {v.toFixed(1)}
                        {i>0&&c!==0&&<span style={{fontSize:9,display:"block",
                          color:c>0?"#10B981":"#EF4444"}}>{c>0?"▲":"▼"}{Math.abs(c).toFixed(1)}</span>}
                      </td>);
                    })}
                    <td style={{textAlign:"center",fontWeight:700,
                      color:dir>0?"#10B981":dir<0?"#EF4444":"#64748b"}}>
                      {dir>0?"▲ RISING":dir<0?"▼ FALLING":"→ FLAT"}
                    </td>
                  </tr>);
                })}
              </tbody>
            </table>
          </div>
        </div>

        <div style={{background:"white",borderRadius:14,padding:16,boxShadow:"0 1px 4px rgba(0,0,0,.07)"}}>
          <div style={{fontSize:12,fontWeight:700,color:"#64748b",marginBottom:11}}>🔥 MoM CHANGE HEATMAP</div>
          <div style={{display:"flex",gap:9,flexWrap:"wrap"}}>
            {funds.map(f=>{
              const vals=f.net_equity.slice(-8);
              const changes=vals.slice(1).map((v,i)=>v-vals[i]);
              return(
              <div key={f.id} style={{flex:"1 1 185px"}}>
                <div style={{fontSize:11,fontWeight:700,marginBottom:5}}>
                  <span style={{display:"inline-block",width:8,height:8,borderRadius:"50%",
                    background:f.color,marginRight:5}}/>
                  {f.shortName}
                </div>
                <div style={{display:"flex",gap:3}}>
                  {changes.map((c,i)=>(
                    <div key={i} style={{flex:1,height:30,borderRadius:4,
                      background:c>1?"#d1fae5":c>0?"#e7f7f0":c<-1?"#fee2e2":c<0?"#fff0f0":"#f1f5f9",
                      display:"flex",alignItems:"center",justifyContent:"center",
                      fontSize:10,fontWeight:700,
                      color:c>0?"#065f46":c<0?"#991b1b":"#64748b"}}>
                      {c>0?"+":""}{c.toFixed(1)}
                    </div>
                  ))}
                </div>
              </div>);
            })}
          </div>
          <div style={{fontSize:10,color:"#94a3b8",marginTop:9}}>
            Each cell = MoM Δ in net equity %. Green = adding equity. Red = trimming.
          </div>
        </div>
      </div>}

      {/* AI SIGNALS */}
      {tab==="signals"&&<div>
        <div style={{background:"white",borderRadius:14,padding:18,
          boxShadow:"0 1px 4px rgba(0,0,0,.07)",marginBottom:13}}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:14}}>
            <div>
              <div style={{fontSize:14,fontWeight:700}}>🧠 AI Market Signal Interpreter</div>
              <div style={{fontSize:11,color:"#94a3b8",marginTop:2}}>
                Claude reads collective BAF/DAA positioning as valuation signals
              </div>
            </div>
            <button onClick={runAI} disabled={aiLoading} style={{
              background:"linear-gradient(135deg,#6366f1,#8b5cf6)",color:"white",
              border:"none",padding:"10px 15px",borderRadius:10,fontWeight:700,
              fontSize:12,cursor:aiLoading?"wait":"pointer",opacity:aiLoading?0.7:1}}>
              {aiLoading?"Analysing…":"Run Analysis"}
            </button>
          </div>
          {aiLoading&&<div style={{textAlign:"center",padding:34,color:"#6366f1"}}>
            <div style={{fontSize:26,marginBottom:7}}>🔍</div>
            <div style={{fontSize:13,fontWeight:600}}>Reading fund manager signals…</div>
          </div>}
          {aiText&&!aiLoading&&(
            <div style={{background:"#f8fafc",borderRadius:10,padding:15,
              fontSize:13,lineHeight:1.75,color:"#334155",borderLeft:"3px solid #6366f1"}}>
              {aiText.split("\n\n").map((p,i)=><p key={i} style={{margin:"0 0 10px"}}>{p}</p>)}
            </div>
          )}
          {!aiText&&!aiLoading&&<div style={{textAlign:"center",padding:30,color:"#94a3b8"}}>
            <div style={{fontSize:30,marginBottom:7}}>📡</div>
            <div style={{fontSize:13}}>Click "Run Analysis" for Claude's market signal interpretation</div>
          </div>}
        </div>

        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(185px,1fr))",gap:9}}>
          {[
            {t:"Consensus",i:"🎯",v:sig(avg).label,s:`Avg: ${avg.toFixed(1)}%`,c:sig(avg).color},
            {t:"Most Bullish",i:"🟢",
              v:funds.reduce((a,b)=>a.curr>b.curr?a:b,funds[0]||{curr:0,shortName:"—"}).shortName,
              s:`${Math.max(...funds.map(f=>f.curr)).toFixed(1)}% equity`,c:"#10B981"},
            {t:"Most Defensive",i:"🔴",
              v:funds.reduce((a,b)=>a.curr<b.curr?a:b,funds[0]||{curr:100,shortName:"—"}).shortName,
              s:`${Math.min(...funds.map(f=>f.curr)).toFixed(1)}% equity`,c:"#EF4444"},
            {t:"Largest MoM Shift",i:"📊",
              v:(()=>{const x=funds.map(f=>({n:f.shortName,c:Math.abs(f.curr-f.prev)})).sort((a,b)=>b.c-a.c)[0];
                return x?`${x.n} (${x.c.toFixed(1)}%)`:"—"})(),
              s:"Biggest monthly move",c:"#F59E0B"},
          ].map(c=>(
            <div key={c.t} style={{background:"white",borderRadius:12,padding:"12px 14px",
              boxShadow:"0 1px 4px rgba(0,0,0,.07)"}}>
              <div style={{fontSize:18,marginBottom:4}}>{c.i}</div>
              <div style={{fontSize:10,color:"#94a3b8",fontWeight:700}}>{c.t}</div>
              <div style={{fontSize:13,fontWeight:800,color:c.c,marginTop:3}}>{c.v}</div>
              <div style={{fontSize:10,color:"#94a3b8",marginTop:2}}>{c.s}</div>
            </div>
          ))}
        </div>
      </div>}

      {/* SETUP */}
      {tab==="setup"&&<div>
        <div style={{background:"white",borderRadius:14,padding:18,
          boxShadow:"0 1px 4px rgba(0,0,0,.07)",marginBottom:13}}>
          <div style={{fontSize:14,fontWeight:700,marginBottom:3}}>⚙️ 3 Steps to Live Data</div>
          <div style={{fontSize:12,color:"#64748b",marginBottom:17}}>
            This dashboard is demo mode — here's how to wire in real scraped data
          </div>
          {[
            {n:"1",t:"Run the scraper",i:"🖥️",c:"#6366f1",
              code:`# Install dependencies\npip install requests pdfplumber pandas python-dateutil\n\n# Scrape latest month + export JSON\npython baf_scraper.py --scrape --export\n\n# Backfill 6 months of history\npython baf_scraper.py --backfill --months 6 --export`},
            {n:"2",t:"Serve alongside this dashboard",i:"🌐",c:"#8b5cf6",
              code:`# Simplest: Python local server\npython -m http.server 3000\n# Open http://localhost:3000\n\n# Or deploy to Vercel/Netlify/GitHub Pages\n# baf-tracker.jsx + allocations_export.json in same folder`},
            {n:"3",t:"Automate monthly (GitHub Actions)",i:"🤖",c:"#06B6D4",
              code:`# scrape.yml is already generated for you\n# Runs on 12th & 27th every month\n# Auto-commits updated JSON to your repo\n# Dashboard always shows latest real data`},
          ].map(s=>(
            <div key={s.n} style={{display:"flex",gap:12,marginBottom:18}}>
              <div style={{width:32,height:32,borderRadius:10,background:s.c,
                display:"flex",alignItems:"center",justifyContent:"center",
                color:"white",fontWeight:800,flexShrink:0}}>{s.n}</div>
              <div style={{flex:1}}>
                <div style={{fontSize:13,fontWeight:700}}>{s.i} {s.t}</div>
                <div style={{background:"#0f172a",color:"#a5b4fc",borderRadius:8,
                  padding:"8px 11px",fontSize:11,marginTop:6,
                  fontFamily:"monospace",whiteSpace:"pre",overflowX:"auto"}}>
                  {s.code}
                </div>
              </div>
            </div>
          ))}
        </div>
        <div style={{background:"#eef2ff",borderRadius:14,padding:15}}>
          <div style={{fontSize:12,fontWeight:700,color:"#4338ca",marginBottom:7}}>
            📋 Why scraping AMC factsheets is legal
          </div>
          <div style={{fontSize:12,color:"#4338ca",lineHeight:1.7}}>
            SEBI mandates all mutual funds to publicly disclose full portfolio by the 10th of each
            month. AMFI aggregates this at <strong>amfiindia.com</strong>. BAF/DAA equity allocation is
            public information — no login, no API key, no paywall. The scraper reads what funds are
            <em> legally required</em> to publish.
          </div>
        </div>
      </div>}

    </div>
  </div>);
}
const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);