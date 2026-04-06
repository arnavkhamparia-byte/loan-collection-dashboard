import { useState, useMemo, useEffect } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, CartesianGrid, Legend, ComposedChart, Line, Area } from "recharts";

// ═══════════════════════════════════════════════════════════════
// LIVE DATA — fetched from FastAPI /api/data (refreshes every 60s)
// Logic matches loan-dashboard exactly:
//   Connected     = task_status='Connected' (not disposition)
//   AI calls      = channel='AI Call' only (AI ASSISTANT excluded)
//   Scheduled     = eta_ist hour; Executed = modified_ist hour
//   PTPs          = Agree To Pay only
// ═══════════════════════════════════════════════════════════════

const T={bg:'#060a14',sf:'#0c1222',sa:'#111b2e',bd:'#1a2744',ac:'#6366f1',tx:'#e2e8f0',tm:'#8892a8',td:'#5a6478',ok:'#10b981',wn:'#f59e0b',dg:'#ef4444',in:'#06b6d4',pp:'#a855f7'};
const CC={'Financial Hardship':'#ef4444','Family Emergency':'#a855f7','Health Issues':'#f97316','Dispute':'#eab308','Payment Commitment':'#10b981','Wrong Number':'#6b7280','Busy/No Answer':'#3b82f6','General/Uncategorized':'#475569'};
const PC={High:'#ef4444',Medium:'#f59e0b',Low:'#5a6478'};
const PCOL=['#6366f1','#10b981','#f59e0b','#ef4444','#a855f7','#06b6d4','#f97316','#6b7280'];
const fmt=n=>n==null?'—':typeof n==='number'?n.toLocaleString():n;
const fp=n=>n==null?'—':`${n.toFixed(2)}%`;
const hl=h=>{const p=h>=12?'PM':'AM';return `${h%12||12}${p}`;};
const dl=d=>{const dt=new Date(d+'T00:00:00');return dt.toLocaleDateString('en-IN',{month:'short',day:'numeric',weekday:'short'});};
const bd=(c,f=11)=>({display:'inline-block',padding:'2px 8px',borderRadius:20,fontSize:f,fontWeight:600,background:c+'22',color:c,border:`1px solid ${c}44`});

const TT=({active,payload,label})=>{
  if(!active||!payload?.length)return null;
  return(<div style={{background:T.sf,border:`1px solid ${T.bd}`,borderRadius:8,padding:'10px 14px',fontSize:12}}>
    <div style={{color:T.tm,marginBottom:4}}>{label}</div>
    {payload.map((p,i)=>(<div key={i} style={{color:p.color||p.fill,display:'flex',gap:8,justifyContent:'space-between'}}>
      <span>{p.name}</span><span style={{fontWeight:600}}>{fmt(p.value)}</span>
    </div>))}
  </div>);
};

const HoverCard=({children,hoverContent})=>{
  const[h,setH]=useState(false);
  return(<div style={{background:T.sf,borderRadius:14,border:`1px solid ${T.bd}`,padding:20,position:'relative'}} onMouseEnter={()=>setH(true)} onMouseLeave={()=>setH(false)}>
    {children}
    {hoverContent&&<div style={{position:'absolute',inset:0,borderRadius:14,display:'flex',flexDirection:'column',justifyContent:'center',alignItems:'center',background:'rgba(6,10,20,0.95)',backdropFilter:'blur(6px)',opacity:h?1:0,transition:'opacity 0.2s',pointerEvents:'none',zIndex:5,padding:16}}>{hoverContent}</div>}
  </div>);
};

export default function Dashboard(){
  const[tab,setTab]=useState(0);
  const[date,setDate]=useState('');
  const[ef,setEf]=useState('All');
  const[ep,setEp]=useState(0);
  const[ea,setEa]=useState(null);
  const[liveData,setLiveData]=useState(null);
  const[loading,setLoading]=useState(true);
  const[error,setError]=useState(null);
  const[lastRefresh,setLastRefresh]=useState(null);

  useEffect(()=>{
    const fetchData=async()=>{
      try{
        const res=await fetch('/api/data');
        if(!res.ok)throw new Error(`HTTP ${res.status}`);
        const json=await res.json();
        setLiveData(json);
        setDate(prev=>prev||((json.dates||[]).slice(-1)[0]||''));
        setLastRefresh(new Date().toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit'}));
        setError(null);
      }catch(e){
        setError(e.message);
      }finally{
        setLoading(false);
      }
    };
    fetchData();
    const iv=setInterval(fetchData,60000);
    return()=>clearInterval(iv);
  },[]);

  const EMPTY={kpis:{total_activities:0,ai_calls:0,sms:0,whatsapp:0,connection_rate:0,ai_connected:0,accounts_total:0,accounts_reached:0,ptps_total:0,agree_to_pay:0,sms_rate:0,wa_rate:0},hourly_trend:[],hourly_exec:[],case_analysis:{attempted:0,connected:0,connected_with_more:0,no_success:0,no_ai_scheduled:0},disposition:{},category_distribution:{},sentiment:{},contacts:[],accounts:[],ptp_funnel:{commitments:0,agreed:0,paid:0},providers:[],channel_diversity:{},daily_summary:[]};
  const data=useMemo(()=>{
    if(!liveData)return EMPTY;
    return liveData.by_date?.[date]||liveData.all||EMPTY;
  },[date,liveData]);

  const dates=liveData?.dates||[];
  const minDate=dates[0]||'';
  const maxDate=dates[dates.length-1]||'';

  const tabs=['Executive Overview','Operations','Customer Intelligence','Analytics & Insights'];
  const cs={fontFamily:"'DM Sans',sans-serif",background:T.bg,color:T.tx,minHeight:'100vh',padding:'0 16px 32px'};
  const cd={background:T.sf,borderRadius:14,border:`1px solid ${T.bd}`,padding:20};

  if(loading)return(<div style={{...cs,display:'flex',alignItems:'center',justifyContent:'center',minHeight:'100vh'}}><div style={{textAlign:'center'}}><div style={{fontSize:32,marginBottom:12}}>⏳</div><div style={{fontSize:16,color:T.tm}}>Loading live data…</div><style>{`@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap')`}</style></div></div>);
  if(error&&!liveData)return(<div style={{...cs,display:'flex',alignItems:'center',justifyContent:'center',minHeight:'100vh'}}><div style={{textAlign:'center'}}><div style={{fontSize:32,marginBottom:12}}>❌</div><div style={{fontSize:14,color:T.dg}}>Failed to load data: {error}</div><div style={{fontSize:11,color:T.tm,marginTop:8}}>Check backend connection</div><style>{`@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap')`}</style></div></div>);

  // TAB 1: EXECUTIVE
  const Tab1=()=>{
    const k=data.kpis;
    const hrs=useMemo(()=>{const m={};(data.hourly_trend||[]).forEach(h=>{m[h.hour]=h;});return Array.from({length:24},(_,i)=>({hour:i,label:hl(i),s_ai:m[i]?.s_ai||0,s_wa:m[i]?.s_wa||0,s_sms:m[i]?.s_sms||0,e_ai:m[i]?.e_ai||0,e_wa:m[i]?.e_wa||0,e_sms:m[i]?.e_sms||0})).filter(h=>(h.s_ai+h.s_wa+h.s_sms+h.e_ai+h.e_wa+h.e_sms)>0);},[data]);
    const execHrs=useMemo(()=>{const m={};(data.hourly_exec||[]).forEach(h=>{m[h.hour]=h;});return Array.from({length:24},(_,i)=>({hour:i,label:hl(i),ai:m[i]?.ai||0,wa:m[i]?.wa||0,sms:m[i]?.sms||0})).filter(h=>(h.ai+h.wa+h.sms)>0);},[data]);
    const rp=k.accounts_total>0?Math.round(k.accounts_reached/k.accounts_total*100):0;
    return(<div>
      <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(210px,1fr))',gap:16,marginBottom:24}}>
        <HoverCard hoverContent={<div style={{textAlign:'center'}}><div style={{fontSize:13,fontWeight:600,marginBottom:12,color:T.tm}}>Channel Distribution</div>
          <div style={{display:'flex',flexDirection:'column',gap:8}}>
            <div style={{display:'flex',justifyContent:'space-between',gap:20}}><span style={{color:'#3b82f6'}}>🤖 AI Call</span><span style={{fontWeight:700,fontFamily:'monospace'}}>{fmt(k.ai_calls)}</span></div>
            <div style={{display:'flex',justifyContent:'space-between',gap:20}}><span style={{color:'#25D366'}}>💬 WhatsApp</span><span style={{fontWeight:700,fontFamily:'monospace'}}>{fmt(k.whatsapp)}</span></div>
            <div style={{display:'flex',justifyContent:'space-between',gap:20}}><span style={{color:'#06b6d4'}}>📱 SMS</span><span style={{fontWeight:700,fontFamily:'monospace'}}>{fmt(k.sms)}</span></div>
          </div></div>}>
          <div style={{fontSize:11,fontWeight:600,textTransform:'uppercase',letterSpacing:'1px',color:T.tm,marginBottom:6}}>Total Outreach</div>
          <div style={{fontSize:32,fontWeight:700,fontFamily:"'JetBrains Mono',monospace",color:T.ac}}>{fmt(k.total_activities)}</div>
          <div style={{fontSize:12,color:T.tm,marginTop:2}}>AI Call + SMS + WhatsApp</div>
        </HoverCard>
        <div style={cd}>
          <div style={{fontSize:11,fontWeight:600,textTransform:'uppercase',letterSpacing:'1px',color:T.tm,marginBottom:6}}>Connection Rate</div>
          <div style={{fontSize:32,fontWeight:700,fontFamily:"'JetBrains Mono',monospace",color:T.ok}}>{fp(k.connection_rate)}</div>
          <div style={{fontSize:12,color:T.tm,marginTop:2}}>{fmt(k.ai_connected)} connected / {fmt(k.ai_calls)} AI calls</div>
        </div>
        <div style={cd}>
          <div style={{fontSize:11,fontWeight:600,textTransform:'uppercase',letterSpacing:'1px',color:T.tm,marginBottom:6}}>Accounts Reached</div>
          <div style={{fontSize:32,fontWeight:700,fontFamily:"'JetBrains Mono',monospace",color:rp>=60?T.ok:rp>=30?T.wn:T.dg}}>{rp}%</div>
          <div style={{fontSize:12,color:T.tm,marginTop:2}}>{k.accounts_reached} of {k.accounts_total} accounts</div>
        </div>
        <div style={cd}>
          <div style={{fontSize:11,fontWeight:600,textTransform:'uppercase',letterSpacing:'1px',color:T.tm,marginBottom:6}}>PTPs Generated</div>
          <div style={{fontSize:32,fontWeight:700,fontFamily:"'JetBrains Mono',monospace",color:T.pp}}>{fmt(k.agree_to_pay)}</div>
          <div style={{fontSize:12,color:T.tm,marginTop:2}}>Agree To Pay only</div>
        </div>
      </div>

      <div style={{...cd,marginBottom:24}}>
        <div style={{fontSize:16,fontWeight:600,marginBottom:4,display:'flex',alignItems:'center',gap:8}}>📈 Hourly Activity Trend — {date?dl(date):'All Dates'} (IST)</div>
        <div style={{fontSize:11,color:T.tm,marginBottom:12}}>Scheduled (eta &gt; created) vs Executed at scheduled hour — how many scheduled activities ran on time (9 AM–7 PM)</div>
        <ResponsiveContainer width="100%" height={300}><BarChart data={hrs} margin={{left:-10,right:10}}>
          <CartesianGrid strokeDasharray="3 3" stroke={T.bd}/><XAxis dataKey="label" tick={{fill:T.tm,fontSize:10}}/><YAxis tick={{fill:T.tm,fontSize:10}}/>
          <Tooltip content={({active,payload,label})=>{if(!active||!payload?.length)return null;const d=payload[0]?.payload;const sTotal=(d.s_ai||0)+(d.s_wa||0)+(d.s_sms||0);const eTotal=(d.e_ai||0)+(d.e_wa||0)+(d.e_sms||0);return(<div style={{background:T.sf,border:`1px solid ${T.bd}`,borderRadius:8,padding:'10px 14px',fontSize:12}}>
            <div style={{fontWeight:600,marginBottom:6}}>{label}</div>
            <div style={{fontWeight:600,color:'#a78bfa',marginBottom:4}}>Scheduled: {sTotal}</div>
            {d.s_ai>0&&<div style={{marginLeft:8,color:'#c4b5fd'}}>AI Call: {d.s_ai}</div>}
            {d.s_wa>0&&<div style={{marginLeft:8,color:'#c4b5fd'}}>WhatsApp: {d.s_wa}</div>}
            {d.s_sms>0&&<div style={{marginLeft:8,color:'#c4b5fd'}}>SMS: {d.s_sms}</div>}
            <div style={{fontWeight:600,color:'#2563eb',marginTop:6,marginBottom:4}}>Executed on time: {eTotal} ({sTotal>0?Math.round(eTotal/sTotal*100):0}%)</div>
            {d.e_ai>0&&<div style={{marginLeft:8,color:'#3b82f6'}}>AI Call: {d.e_ai}</div>}
            {d.e_wa>0&&<div style={{marginLeft:8,color:'#22c55e'}}>WhatsApp: {d.e_wa}</div>}
            {d.e_sms>0&&<div style={{marginLeft:8,color:'#eab308'}}>SMS: {d.e_sms}</div>}
          </div>);}}/>
          <Legend wrapperStyle={{fontSize:10}}/>
          <Bar dataKey="s_ai" name="Scheduled: AI Call" stackId="sched" fill="#a78bfa" opacity={0.45}/>
          <Bar dataKey="s_wa" name="Scheduled: WhatsApp" stackId="sched" fill="#86efac" opacity={0.45}/>
          <Bar dataKey="s_sms" name="Scheduled: SMS" stackId="sched" fill="#93c5fd" opacity={0.45} radius={[4,4,0,0]}/>
          <Bar dataKey="e_ai" name="Executed: AI Call" stackId="exec" fill="#2563eb"/>
          <Bar dataKey="e_wa" name="Executed: WhatsApp" stackId="exec" fill="#16a34a"/>
          <Bar dataKey="e_sms" name="Executed: SMS" stackId="exec" fill="#eab308" radius={[4,4,0,0]}/>
        </BarChart></ResponsiveContainer>
      </div>

      <div style={{...cd,marginBottom:24}}>
        <div style={{fontSize:16,fontWeight:600,marginBottom:4,display:'flex',alignItems:'center',gap:8}}>⏰ Hourly Execution — {date?dl(date):'All Dates'} (IST)</div>
        <div style={{fontSize:11,color:T.tm,marginBottom:12}}>All executed activities (status=done, excl. rescheduled) by modified hour — stacked by channel</div>
        <ResponsiveContainer width="100%" height={280}><BarChart data={execHrs} margin={{left:-10,right:10}}>
          <CartesianGrid strokeDasharray="3 3" stroke={T.bd}/><XAxis dataKey="label" tick={{fill:T.tm,fontSize:10}}/><YAxis tick={{fill:T.tm,fontSize:10}}/>
          <Tooltip content={({active,payload,label})=>{if(!active||!payload?.length)return null;const d=payload[0]?.payload;return(<div style={{background:T.sf,border:`1px solid ${T.bd}`,borderRadius:8,padding:'10px 14px',fontSize:12}}>
            <div style={{fontWeight:600,marginBottom:6}}>{label}</div>
            <div style={{fontWeight:600,marginBottom:4}}>Total Executed: {(d.ai||0)+(d.wa||0)+(d.sms||0)}</div>
            {d.ai>0&&<div style={{color:'#3b82f6'}}>🤖 AI Call: {d.ai}</div>}
            {d.wa>0&&<div style={{color:'#25D366'}}>💬 WhatsApp: {d.wa}</div>}
            {d.sms>0&&<div style={{color:'#06b6d4'}}>📱 SMS: {d.sms}</div>}
          </div>);}}/>
          <Legend wrapperStyle={{fontSize:10}}/>
          <Bar dataKey="ai" name="AI Call" stackId="exec" fill="#3b82f6"/>
          <Bar dataKey="wa" name="WhatsApp" stackId="exec" fill="#25D366"/>
          <Bar dataKey="sms" name="SMS" stackId="exec" fill="#06b6d4" radius={[4,4,0,0]}/>
        </BarChart></ResponsiveContainer>
      </div>
    </div>);
  };

  // TAB 2: OPERATIONS
  const Tab2=()=>{
    const k=data.kpis,ca=data.case_analysis||{};
    const dispD=Object.entries(data.disposition||{}).sort((a,b)=>b[1]-a[1]).slice(0,12).map(([n,c])=>({name:n.length>18?n.slice(0,16)+'…':n,fullName:n,count:c}));
    const caseD=[
      {name:'AI Call Attempted',value:ca.attempted||0,color:T.ac},
      {name:'Successfully Connected',value:ca.connected||0,color:T.ok},
      {name:'Connected + More Scheduled',value:ca.connected_with_more||0,color:T.in},
      {name:'No Successful Attempts',value:ca.no_success||0,color:T.wn},
      {name:'No AI Calls Scheduled',value:ca.no_ai_scheduled||0,color:T.dg},
    ];
    const hrs=useMemo(()=>{const m={};(data.hourly_trend||[]).forEach(h=>{m[h.hour]=h;});return Array.from({length:24},(_,i)=>({hour:i,label:hl(i),s_ai:m[i]?.s_ai||0,e_ai:m[i]?.e_ai||0})).filter(h=>(h.s_ai+h.e_ai)>0);},[data]);
    return(<div>
      <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(200px,1fr))',gap:16,marginBottom:24}}>
        {[{t:'AI Calls',v:k.ai_calls,c:'#3b82f6',s:`Connected: ${fmt(k.ai_connected)} (${fp(k.connection_rate)})`},{t:'SMS',v:k.sms,c:'#1abc9c',s:`Delivery: ${fp(k.sms_rate)}`},{t:'WhatsApp',v:k.whatsapp,c:'#25D366',s:`Delivery: ${fp(k.wa_rate)}`}].map((ch,i)=>(
          <div key={i} style={{...cd,borderTop:`3px solid ${ch.c}`}}>
            <div style={{fontSize:11,fontWeight:600,textTransform:'uppercase',letterSpacing:'1px',color:T.tm,marginBottom:6}}>{ch.t}</div>
            <div style={{fontSize:28,fontWeight:700,fontFamily:"'JetBrains Mono',monospace",color:ch.c}}>{fmt(ch.v)}</div>
            <div style={{fontSize:12,color:T.tm,marginTop:8}}>{ch.s}</div>
          </div>
        ))}
      </div>

      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:16,marginBottom:24}}>
        <div style={cd}>
          <div style={{fontSize:16,fontWeight:600,marginBottom:12,display:'flex',alignItems:'center',gap:8}}>📊 AI Call Disposition Breakdown</div>
          <ResponsiveContainer width="100%" height={310}><BarChart data={dispD} margin={{left:10,right:10}}>
            <CartesianGrid strokeDasharray="3 3" stroke={T.bd}/><XAxis dataKey="name" tick={{fill:T.tm,fontSize:9}} angle={-30} textAnchor="end" height={60}/><YAxis tick={{fill:T.tm,fontSize:10}}/>
            <Tooltip content={({active,payload})=>{if(!active||!payload?.length)return null;const d=payload[0].payload;return(<div style={{background:T.sf,border:`1px solid ${T.bd}`,borderRadius:8,padding:'10px 14px',fontSize:12}}><div style={{fontWeight:600}}>{d.fullName}</div><div>Count: <b>{d.count}</b></div><div>% of AI Calls: <b>{k.ai_calls>0?(d.count/k.ai_calls*100).toFixed(1):0}%</b></div></div>);}}/>
            <Bar dataKey="count" fill={T.ac} radius={[4,4,0,0]} opacity={0.8}/>
          </BarChart></ResponsiveContainer>
        </div>
        <div style={cd}>
          <div style={{fontSize:16,fontWeight:600,marginBottom:12,display:'flex',alignItems:'center',gap:8}}>📋 AI Call Case Analysis ({ca.attempted||0} cases)</div>
          <div style={{fontSize:11,color:T.tm,marginBottom:12}}>Unique accounts per category</div>
          <ResponsiveContainer width="100%" height={280}><BarChart data={caseD} layout="vertical" margin={{left:60,right:30}}>
            <CartesianGrid strokeDasharray="3 3" stroke={T.bd} horizontal={false}/>
            <XAxis type="number" tick={{fill:T.tm,fontSize:10}} domain={[0,'dataMax+10']}/>
            <YAxis dataKey="name" type="category" tick={{fill:T.tm,fontSize:10}} width={190}/>
            <Tooltip content={({active,payload})=>{if(!active||!payload?.length)return null;const d=payload[0].payload;return(<div style={{background:T.sf,border:`1px solid ${T.bd}`,borderRadius:8,padding:'10px 14px',fontSize:12}}><div style={{fontWeight:600,marginBottom:4}}>{d.name}</div><div>Cases: <b>{d.value}</b></div></div>);}}/>
            <Bar dataKey="value" radius={[0,6,6,0]}>{caseD.map((d,i)=><Cell key={i} fill={d.color} opacity={0.85}/>)}</Bar>
          </BarChart></ResponsiveContainer>
        </div>
      </div>

      <div style={cd}>
        <div style={{fontSize:16,fontWeight:600,marginBottom:4,display:'flex',alignItems:'center',gap:8}}>⏰ Hourly AI Call Distribution — {date?dl(date):'All Dates'} (IST)</div>
        <div style={{fontSize:11,color:T.tm,marginBottom:12}}>Scheduled AI Calls (eta &gt; created) vs Executed (modified, excl. rescheduled)</div>
        <ResponsiveContainer width="100%" height={270}><ComposedChart data={hrs} margin={{left:-10,right:10}}>
          <CartesianGrid strokeDasharray="3 3" stroke={T.bd}/><XAxis dataKey="label" tick={{fill:T.tm,fontSize:10}}/><YAxis tick={{fill:T.tm,fontSize:10}}/>
          <Tooltip content={<TT/>}/><Legend wrapperStyle={{fontSize:11}}/>
          <Bar dataKey="s_ai" name="Scheduled AI Calls" fill="#818cf8" radius={[4,4,0,0]} opacity={0.5}/>
          <Bar dataKey="e_ai" name="Executed AI Calls" fill={T.ac} radius={[4,4,0,0]}/>
        </ComposedChart></ResponsiveContainer>
      </div>
    </div>);
  };

  // TAB 3: CUSTOMER INTELLIGENCE
  const Tab3=()=>{
    const catD=Object.entries(data.category_distribution||{}).filter(([k])=>k!=='General/Uncategorized').map(([n,v])=>({name:n,value:v})).sort((a,b)=>b.value-a.value);
    const sentD=Object.entries(data.sentiment||{}).map(([n,v])=>({name:n.charAt(0).toUpperCase()+n.slice(1),value:v}));
    const SC={Positive:T.ok,Neutral:'#6b7280',Negative:T.dg};
    const cts=(data.contacts||[]).map(c=>({name:c.type.replace(/_/g,' ').replace(/\b\w/g,l=>l.toUpperCase()),volume:c.volume,connected:c.connected,rate:c.rate}));
    const accs=data.accounts||[];
    const fa=ef==='All'?accs:accs.filter(a=>a.priority===ef);
    const ps=8,pc=Math.ceil(fa.length/ps),pa=fa.slice(ep*ps,(ep+1)*ps);
    const se={positive:'😊',neutral:'😐',negative:'😞','N/A':'—'};
    return(<div>
      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:16,marginBottom:24}}>
        <div style={cd}>
          <div style={{fontSize:16,fontWeight:600,marginBottom:12,display:'flex',alignItems:'center',gap:8}}>👥 Customer Segments</div>
          {catD.length===0?<div style={{color:T.tm,textAlign:'center',padding:40}}>No categorized segments</div>:
          <><ResponsiveContainer width="100%" height={220}><PieChart><Pie data={catD} dataKey="value" cx="50%" cy="50%" outerRadius={85} innerRadius={45} paddingAngle={2}>
            {catD.map((d,i)=><Cell key={i} fill={CC[d.name]||PCOL[i%PCOL.length]}/>)}</Pie>
            <Tooltip content={({active,payload})=>{if(!active||!payload?.length)return null;const d=payload[0];return(<div style={{background:T.sf,border:`1px solid ${T.bd}`,borderRadius:8,padding:'8px 12px',fontSize:12}}><div style={{fontWeight:600,color:d.payload.fill}}>{d.name}</div><div>Accounts: <b>{d.value}</b></div></div>);}}/>
          </PieChart></ResponsiveContainer>
          <div style={{display:'flex',flexWrap:'wrap',gap:6,justifyContent:'center'}}>{catD.map((d,i)=>(<span key={i} style={bd(CC[d.name]||PCOL[i],9)}>{d.name}: {d.value}</span>))}</div></>}
        </div>
        <div style={cd}>
          <div style={{fontSize:16,fontWeight:600,marginBottom:12,display:'flex',alignItems:'center',gap:8}}>💭 Sentiment Analysis</div>
          {sentD.length===0?<div style={{color:T.tm,textAlign:'center',padding:40}}>No sentiment data</div>:
          <><ResponsiveContainer width="100%" height={190}><BarChart data={sentD} margin={{left:-10}}>
            <CartesianGrid strokeDasharray="3 3" stroke={T.bd}/><XAxis dataKey="name" tick={{fill:T.tm,fontSize:11}}/><YAxis tick={{fill:T.tm,fontSize:10}}/>
            <Tooltip content={<TT/>}/><Bar dataKey="value" name="Count" radius={[6,6,0,0]}>{sentD.map((d,i)=><Cell key={i} fill={SC[d.name]||T.ac} opacity={0.85}/>)}</Bar>
          </BarChart></ResponsiveContainer>
          <div style={{display:'flex',gap:16,justifyContent:'center',marginTop:8}}>{sentD.map((d,i)=>(<div key={i} style={{textAlign:'center',fontSize:11}}><div style={{fontSize:20}}>{d.name==='Positive'?'😊':d.name==='Negative'?'😞':'😐'}</div><div style={{fontWeight:600}}>{d.value}</div><div style={{color:T.tm}}>{d.name}</div></div>))}</div></>}
        </div>
      </div>

      <div style={{...cd,marginBottom:24}}>
        <div style={{fontSize:16,fontWeight:600,marginBottom:12,display:'flex',alignItems:'center',gap:8}}>📱 Contact Strategy Performance</div>
        {cts.length===0?<div style={{color:T.tm,textAlign:'center',padding:20}}>No contact data</div>:
        <ResponsiveContainer width="100%" height={210}><BarChart data={cts} margin={{left:10}}>
          <CartesianGrid strokeDasharray="3 3" stroke={T.bd}/><XAxis dataKey="name" tick={{fill:T.tm,fontSize:10}}/><YAxis tick={{fill:T.tm,fontSize:10}}/>
          <Tooltip content={<TT/>}/><Legend wrapperStyle={{fontSize:11}}/>
          <Bar dataKey="volume" name="Attempts" fill={T.ac} opacity={0.5} radius={[4,4,0,0]}/>
          <Bar dataKey="connected" name="Connected" fill={T.ok} radius={[4,4,0,0]}/>
        </BarChart></ResponsiveContainer>}
      </div>

      <div style={cd}>
        <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:16,flexWrap:'wrap',gap:8}}>
          <div style={{fontSize:16,fontWeight:600,display:'flex',alignItems:'center',gap:8}}>🚨 Escalation Queue</div>
          <div style={{display:'flex',gap:4}}>{['All','High','Medium','Low'].map(f=>(<button key={f} onClick={()=>{setEf(f);setEp(0);}} style={{padding:'6px 12px',borderRadius:8,fontSize:11,fontWeight:500,cursor:'pointer',border:'none',background:ef===f?(PC[f]||T.ac):'transparent',color:ef===f?'#fff':T.tm}}>{f}</button>))}</div>
        </div>
        {pa.length===0?<div style={{color:T.tm,textAlign:'center',padding:30}}>No accounts match filter</div>:
        <div style={{overflowX:'auto'}}><table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
          <thead><tr style={{borderBottom:`2px solid ${T.bd}`}}>{['Priority','Account','Categories','Touches','Conn.','Ch.','Sent.','Last Contact','Summary'].map(h=>(
            <th key={h} style={{textAlign:'left',padding:'8px 6px',color:T.tm,fontSize:10,fontWeight:600,textTransform:'uppercase',letterSpacing:'0.5px'}}>{h}</th>
          ))}</tr></thead>
          <tbody>{pa.map((a,i)=>(<tr key={i} onClick={()=>setEa(ea===a.account_id?null:a.account_id)} style={{borderBottom:`1px solid ${T.bd}22`,cursor:'pointer',background:ea===a.account_id?T.sa:'transparent',transition:'background 0.15s'}}>
            <td style={{padding:'10px 6px'}}><span style={bd(PC[a.priority],10)}>{a.priority}</span></td>
            <td style={{padding:'10px 6px',fontWeight:600,fontFamily:'monospace'}}>#{a.account_id}</td>
            <td style={{padding:'10px 6px'}}><div style={{display:'flex',gap:3,flexWrap:'wrap'}}>{a.categories.filter(c=>c!=='General/Uncategorized').slice(0,2).map((c,j)=><span key={j} style={bd(CC[c]||T.td,9)}>{c.split('/')[0]}</span>)}{a.categories.filter(c=>c!=='General/Uncategorized').length===0&&<span style={bd(T.td,9)}>General</span>}</div></td>
            <td style={{padding:'10px 6px',fontFamily:'monospace'}}>{a.touchpoints}</td>
            <td style={{padding:'10px 6px',fontFamily:'monospace',color:a.connected>0?T.ok:T.td}}>{a.connected}</td>
            <td style={{padding:'10px 6px',fontFamily:'monospace'}}>{a.channels}</td>
            <td style={{padding:'10px 6px',fontSize:16}}>{se[a.last_sentiment]||'—'}</td>
            <td style={{padding:'10px 6px',fontSize:11,color:T.tm}}>{a.last_contact}</td>
            <td style={{padding:'10px 6px',fontSize:11,color:T.tm,maxWidth:160,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{a.last_summary||'—'}</td>
          </tr>))}</tbody>
        </table></div>}
        {pc>1&&<div style={{display:'flex',justifyContent:'center',gap:6,marginTop:12}}>{Array.from({length:pc},(_,i)=>(<button key={i} onClick={()=>setEp(i)} style={{width:28,height:28,borderRadius:6,border:`1px solid ${ep===i?T.ac:T.bd}`,background:ep===i?T.ac+'22':'transparent',color:T.tx,cursor:'pointer',fontSize:11}}>{i+1}</button>))}</div>}
      </div>
    </div>);
  };

  // TAB 4: ANALYTICS
  const Tab4=()=>{
    const ptp=data.ptp_funnel||{};
    const ptpD=[{name:'Commitments',value:ptp.commitments||0,fill:T.ac},{name:'Agreed to Pay',value:ptp.agreed||0,fill:T.ok},{name:'Paid',value:ptp.paid||0,fill:'#10b981'}];
    const provs=data.providers||[];
    const chDiv=data.channel_diversity||{};
    const chDivD=Object.entries(chDiv).filter(([k])=>k!=='0').map(([c,n])=>({name:`${c} Channel${parseInt(c)>1?'s':''}`,value:n})).sort((a,b)=>a.name.localeCompare(b.name));
    const wn=(data.disposition||{})['Wrong Number']||0;
    const tAI=data.kpis?.ai_calls||1;
    const wp=(wn/tAI*100).toFixed(2);
    const dq=(100-parseFloat(wp)).toFixed(2);
    const ds=liveData?.all?.daily_summary||[];
    const dt=ds.map(d=>({date:dl(d.date),activities:d.activities,connected:d.connected,rate:d.activities>0?parseFloat((d.connected/d.activities*100).toFixed(2)):0}));
    const insights=[
      {icon:'🎯',title:'PTP Broken = Highest Success',desc:'PTP Broken flow shows best connection rates. Re-engage aggressively.',color:T.ok},
      {icon:'📞',title:'Secondary Numbers Outperform',desc:'Secondary contacts yield higher connection rates than primary.',color:T.in},
      {icon:'⏰',title:'Peak Hours: 9-12 IST',desc:'Morning hours show highest connection success. Shift calling schedule.',color:T.wn},
      {icon:'🔍',title:'Provider Comparison',desc:'Compare VinFer vs Google performance. Check for connection gaps.',color:T.pp},
    ];
    return(<div>
      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:16,marginBottom:24}}>
        <div style={cd}>
          <div style={{fontSize:16,fontWeight:600,marginBottom:12,display:'flex',alignItems:'center',gap:8}}>🔄 PTP Funnel</div>
          <div style={{display:'flex',flexDirection:'column',gap:8,padding:'12px 0'}}>{ptpD.map((d,i)=>{const mx=Math.max(...ptpD.map(p=>p.value),1);const w=Math.max(d.value/mx*100,8);return(<div key={i}><div style={{display:'flex',justifyContent:'space-between',fontSize:12,marginBottom:4}}><span style={{color:T.tm}}>{d.name}</span><span style={{fontWeight:700,fontFamily:'monospace'}}>{d.value}</span></div><div style={{height:24,background:T.sa,borderRadius:6,overflow:'hidden'}}><div style={{width:`${w}%`,height:'100%',background:d.fill,borderRadius:6,opacity:0.8}}/></div></div>);})}</div>
        </div>
        <div style={cd}>
          <div style={{fontSize:16,fontWeight:600,marginBottom:12,display:'flex',alignItems:'center',gap:8}}>🏢 Provider Performance</div>
          {provs.length===0?<div style={{color:T.tm,textAlign:'center',padding:40}}>No provider data</div>:
          <div style={{display:'flex',flexDirection:'column',gap:12,padding:'8px 0'}}>{provs.map((p,i)=>(
            <HoverCard key={i} hoverContent={<div style={{textAlign:'center',width:'100%'}}><div style={{fontSize:14,fontWeight:700,marginBottom:10}}>{p.display_name}</div>
              <div style={{display:'flex',flexDirection:'column',gap:6,fontSize:13}}>
                <div style={{display:'flex',justifyContent:'space-between',gap:24}}><span style={{color:T.tm}}>Calls Made (status=done)</span><span style={{fontWeight:700,fontFamily:'monospace'}}>{fmt(p.calls_made)}</span></div>
                <div style={{display:'flex',justifyContent:'space-between',gap:24}}><span style={{color:T.tm}}>Connected Calls</span><span style={{fontWeight:700,fontFamily:'monospace',color:T.ok}}>{fmt(p.connected)}</span></div>
                <div style={{display:'flex',justifyContent:'space-between',gap:24}}><span style={{color:T.tm}}>Connection Rate</span><span style={{fontWeight:700,fontFamily:'monospace',color:T.ac}}>{p.rate}%</span></div>
              </div></div>}>
              <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                <div><div style={{fontSize:15,fontWeight:600}}>{p.display_name}</div><div style={{fontSize:11,color:T.tm}}>Connection Rate</div></div>
                <div style={{fontSize:26,fontWeight:700,fontFamily:"'JetBrains Mono',monospace",color:p.rate>=40?T.ok:p.rate>=20?T.wn:T.dg}}>{p.rate}%</div>
              </div>
              <div style={{width:'100%',height:5,background:T.bg,borderRadius:3,marginTop:8,overflow:'hidden'}}><div style={{width:`${Math.min(p.rate,100)}%`,height:'100%',background:p.rate>=40?T.ok:p.rate>=20?T.wn:T.dg,borderRadius:3}}/></div>
            </HoverCard>
          ))}</div>}
        </div>
      </div>

      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:16,marginBottom:24}}>
        <div style={cd}>
          <div style={{fontSize:16,fontWeight:600,marginBottom:4,display:'flex',alignItems:'center',gap:8}}>📡 Multi-Channel Penetration</div>
          <div style={{fontSize:11,color:T.tm,marginBottom:8}}>Channels: AI Call, WhatsApp, SMS, Manual Agent</div>
          {chDivD.length===0?<div style={{color:T.tm,textAlign:'center',padding:40}}>No data</div>:
          <><ResponsiveContainer width="100%" height={210}><PieChart><Pie data={chDivD} dataKey="value" cx="50%" cy="50%" outerRadius={75} innerRadius={38} paddingAngle={3}>
            {chDivD.map((_,i)=><Cell key={i} fill={PCOL[i%PCOL.length]}/>)}</Pie>
            <Tooltip content={({active,payload})=>{if(!active||!payload?.length)return null;const d=payload[0];const t=chDivD.reduce((s,x)=>s+x.value,0);return(<div style={{background:T.sf,border:`1px solid ${T.bd}`,borderRadius:8,padding:'8px 12px',fontSize:12}}><div style={{fontWeight:600}}>{d.name}</div><div>Accounts: <b>{d.value}</b> ({(d.value/t*100).toFixed(1)}%)</div></div>);}}/>
          </PieChart></ResponsiveContainer>
          <div style={{display:'flex',gap:8,justifyContent:'center',flexWrap:'wrap'}}>{chDivD.map((d,i)=>(<span key={i} style={bd(PCOL[i%PCOL.length],10)}>{d.name}: {d.value}</span>))}</div></>}
        </div>
        <div style={cd}>
          <div style={{fontSize:16,fontWeight:600,marginBottom:16,display:'flex',alignItems:'center',gap:8}}>🛡️ Data Quality</div>
          <div style={{display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',height:240}}>
            <div style={{position:'relative',width:130,height:130}}>
              <svg viewBox="0 0 130 130" style={{transform:'rotate(-90deg)'}}><circle cx="65" cy="65" r="55" fill="none" stroke={T.sa} strokeWidth="11"/><circle cx="65" cy="65" r="55" fill="none" stroke={parseFloat(dq)>=98?T.ok:T.wn} strokeWidth="11" strokeLinecap="round" strokeDasharray={`${parseFloat(dq)*3.46} 346`}/></svg>
              <div style={{position:'absolute',inset:0,display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center'}}>
                <div style={{fontSize:26,fontWeight:700,fontFamily:"'JetBrains Mono',monospace"}}>{dq}%</div>
                <div style={{fontSize:9,color:T.tm}}>Valid Contacts</div>
              </div>
            </div>
            <div style={{marginTop:10,textAlign:'center',fontSize:12,color:T.tm}}>Wrong Numbers: <span style={{color:T.dg,fontWeight:600}}>{wn}</span> ({wp}%)<br/>Target: &lt;2%</div>
          </div>
        </div>
      </div>

      <div style={{...cd,marginBottom:24}}>
        <div style={{fontSize:16,fontWeight:600,marginBottom:4,display:'flex',alignItems:'center',gap:8}}>📈 Daily Connection Rate Trend (All Dates)</div>
        <div style={{fontSize:11,color:T.tm,marginBottom:12}}>AI Call connection rate across all available dates</div>
        <ResponsiveContainer width="100%" height={230}><ComposedChart data={dt} margin={{left:-10,right:10}}>
          <CartesianGrid strokeDasharray="3 3" stroke={T.bd}/><XAxis dataKey="date" tick={{fill:T.tm,fontSize:10}}/><YAxis yAxisId="l" tick={{fill:T.tm,fontSize:10}}/><YAxis yAxisId="r" orientation="right" tick={{fill:T.tm,fontSize:10}} unit="%"/>
          <Tooltip content={({active,payload,label})=>{if(!active||!payload?.length)return null;return(<div style={{background:T.sf,border:`1px solid ${T.bd}`,borderRadius:8,padding:'10px 14px',fontSize:12}}><div style={{fontWeight:600,marginBottom:4}}>{label}</div>{payload.map((p,i)=>(<div key={i} style={{color:p.color,display:'flex',justifyContent:'space-between',gap:16}}><span>{p.name}</span><span style={{fontWeight:700}}>{p.name==='Rate'?`${p.value}%`:fmt(p.value)}</span></div>))}</div>);}}/>
          <Legend wrapperStyle={{fontSize:11}}/>
          <Area yAxisId="l" type="monotone" dataKey="activities" name="Activities" fill={T.ac+'22'} stroke={T.ac} strokeWidth={2}/>
          <Area yAxisId="l" type="monotone" dataKey="connected" name="Connected" fill={T.ok+'22'} stroke={T.ok} strokeWidth={2}/>
          <Line yAxisId="r" type="monotone" dataKey="rate" name="Rate" stroke={T.wn} strokeWidth={2} dot={{fill:T.wn,r:4}}/>
        </ComposedChart></ResponsiveContainer>
      </div>

      <div style={cd}>
        <div style={{fontSize:16,fontWeight:600,marginBottom:12,display:'flex',alignItems:'center',gap:8}}>💡 Key Insights</div>
        <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(240px,1fr))',gap:12}}>
          {insights.map((ins,i)=>(<div key={i} style={{background:T.sa,borderRadius:10,padding:14,borderLeft:`3px solid ${ins.color}`}}>
            <div style={{fontSize:20,marginBottom:6}}>{ins.icon}</div>
            <div style={{fontSize:13,fontWeight:600,marginBottom:4}}>{ins.title}</div>
            <div style={{fontSize:11,color:T.tm,lineHeight:1.5}}>{ins.desc}</div>
          </div>))}
        </div>
      </div>
    </div>);
  };

  return(<div style={cs}>
    <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',padding:'20px 0 12px',flexWrap:'wrap',gap:12}}>
      <div>
        <div style={{fontSize:22,fontWeight:700,letterSpacing:'-0.5px',background:'linear-gradient(135deg,#6366f1,#a855f7)',WebkitBackgroundClip:'text',WebkitTextFillColor:'transparent'}}>Loan Collection Dashboard</div>
        {lastRefresh&&<div style={{fontSize:10,color:T.td,marginTop:2}}>Live · Last refreshed {lastRefresh} · Auto-refresh every 60s{error?` · ⚠️ ${error}`:''}</div>}
      </div>
      <div style={{display:'flex',alignItems:'center',gap:10,background:T.sf,border:`1px solid ${T.bd}`,borderRadius:10,padding:'6px 14px'}}>
        <span style={{fontSize:14}}>📅</span>
        <label style={{fontSize:12,color:T.tm,fontWeight:500}}>Date:</label>
        <select value={date} onChange={e=>{setDate(e.target.value);setEp(0);setEa(null);}} style={{background:'transparent',border:'none',color:T.tx,fontSize:13,fontFamily:'inherit',outline:'none',cursor:'pointer'}}>
          {dates.slice().reverse().map(d=>(<option key={d} value={d} style={{background:T.sf}}>{dl(d)} ({d})</option>))}
        </select>
      </div>
    </div>
    <div style={{display:'flex',gap:4,background:T.sf,borderRadius:12,padding:4,marginBottom:20,overflowX:'auto'}}>
      {tabs.map((t,i)=>(<button key={i} onClick={()=>setTab(i)} style={{padding:'10px 18px',borderRadius:8,fontSize:13,fontWeight:500,cursor:'pointer',border:'none',transition:'all .2s',whiteSpace:'nowrap',background:tab===i?T.ac:'transparent',color:tab===i?'#fff':T.tm}}>{t}</button>))}
    </div>
    <div style={{animation:'fadeIn 0.3s ease'}}>
      {tab===0&&<Tab1/>}{tab===1&&<Tab2/>}{tab===2&&<Tab3/>}{tab===3&&<Tab4/>}
    </div>
    <div style={{textAlign:'center',padding:'32px 0 8px',fontSize:11,color:T.td}}>Live data · {liveData?.dates?.length||0} date(s) available · Times in IST (GMT+5:30) · Channels: AI Call, SMS, WhatsApp, Manual Call · AI ASSISTANT excluded</div>
    <style>{`@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap');@keyframes fadeIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}input[type="date"]::-webkit-calendar-picker-indicator{filter:invert(0.8);cursor:pointer}select option{background:#0c1222;color:#e2e8f0}::-webkit-scrollbar{width:6px;height:6px}::-webkit-scrollbar-track{background:${T.bg}}::-webkit-scrollbar-thumb{background:${T.bd};border-radius:3px}table tr:hover{background:${T.sa}!important}`}</style>
  </div>);
}
