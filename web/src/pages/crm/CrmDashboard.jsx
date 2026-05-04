import { useEffect, useState } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, PieChart, Pie, Legend } from 'recharts';
import { api } from '../../api/client';

const TARGET_REPS = ['山本 夏乃', '板金 慎太郎', '萩原 隼人', '藤原 一矢', '野村 尭弘'];
const REP_COLORS  = ['#6366f1','#0ea5e9','#10b981','#f59e0b','#ef4444','#8b5cf6'];

const ACT_CFG = {
  '初回商談': { color:'#059669', bg:'#dcfce7' },
  '架電':     { color:'#6366f1', bg:'#eef2ff' },
  'メール':   { color:'#0ea5e9', bg:'#e0f2fe' },
  '商談':     { color:'#d97706', bg:'#fef3c7' },
  'タスク':   { color:'#8b5cf6', bg:'#f3e8ff' },
  '見積':     { color:'#ec4899', bg:'#fce7f3' },
};

const PLAN_COLORS = ['#1e40af','#059669','#d97706','#7c3aed','#dc2626','#0ea5e9','#ec4899','#64748b'];

const YOMI_ORDER  = ['アポ化前','アポ化済商談前','E 5％','D 15％','C 30％','B 50％','A 70％','S 90％'];
const YOMI_LABELS = {
  'アポ化前':'アポ化前','アポ化済商談前':'商談前',
  'E 5％':'E (5%)','D 15％':'D (15%)','C 30％':'C (30%)',
  'B 50％':'B (50%)','A 70％':'A (70%)','S 90％':'S (90%)',
};
const YOMI_COLORS = {
  'アポ化前':'#cbd5e1','アポ化済商談前':'#94a3b8',
  'E 5％':'#c7d2fe','D 15％':'#a5b4fc','C 30％':'#6ee7b7',
  'B 50％':'#93c5fd','A 70％':'#3b82f6','S 90％':'#1d4ed8',
};

const fmtM = n => {
  if (!n) return '0万';
  const m = Number(n);
  if (m >= 1e8) return `${(m / 1e8).toFixed(1)}億`;
  if (m >= 1e4) return `${Math.round(m / 1e4).toLocaleString()}万`;
  return m.toLocaleString();
};
const fmtMYen  = n => `¥${fmtM(n)}`;
const fmtDate  = d => d ? String(d).substring(0, 10).replace(/-/g, '/') : '';
const diffPct  = (c, p) => p ? Math.round((c - p) / p * 100) : null;
const timeAgo  = dt => {
  const mins = Math.floor((Date.now() - new Date(dt)) / 60000);
  if (mins < 60) return `${Math.max(0, mins)}分前`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}時間前`;
  return `${Math.floor(hrs / 24)}日前`;
};

function DiffTag({ diff }) {
  if (diff == null) return null;
  const up = diff >= 0;
  return (
    <span style={{ fontSize:'0.68rem', fontWeight:700, color: up ? '#059669' : '#dc2626', display:'flex', alignItems:'center', gap:1 }}>
      {up ? '▲' : '▼'}{Math.abs(diff)}%
    </span>
  );
}

function buildRepTable(repTable, filterRep) {
  if (!repTable?.length) return [];
  if (filterRep && TARGET_REPS.includes(filterRep)) {
    const f = repTable.find(r => r.rep === filterRep);
    return [f ? { ...f } : { rep: filterRep, wonCount: 0, meetingCount: 0, paymentAmount: 0 }];
  }
  const rows = TARGET_REPS.map(name => {
    const f = repTable.find(r => r.rep === name);
    return f ? { ...f } : { rep: name, wonCount: 0, meetingCount: 0, paymentAmount: 0 };
  });
  const others = repTable.filter(r => !TARGET_REPS.includes(r.rep));
  if (others.length > 0) rows.push({
    rep: 'その他', isOther: true,
    wonCount:      others.reduce((s, r) => s + r.wonCount, 0),
    meetingCount:  others.reduce((s, r) => s + r.meetingCount, 0),
    paymentAmount: others.reduce((s, r) => s + r.paymentAmount, 0),
  });
  return rows;
}

function Drilldown({ rep, type, start, end, onClose }) {
  const [rows, setRows] = useState(null);
  useEffect(() => {
    api.crmDashboardDrilldown({ rep, type, start, end })
      .then(d => setRows(d.rows || []))
      .catch(() => setRows([]));
  }, []);

  const payTotal = type === 'payments' && rows
    ? rows.reduce((s, r) => s + Number(r.incentive_amount || 0), 0) : 0;

  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.55)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }} onClick={onClose}>
      <div style={{ background:'#f8fafc', borderRadius:16, width:'min(600px,92vw)', maxHeight:'80vh', overflow:'hidden', display:'flex', flexDirection:'column', boxShadow:'0 32px 64px rgba(0,0,0,0.3)' }} onClick={e => e.stopPropagation()}>

        {/* ヘッダー */}
        <div style={{ padding:'16px 20px', background:'#fff', borderBottom:'1px solid #f1f5f9', display:'flex', justifyContent:'space-between', alignItems:'flex-start' }}>
          <div>
            <div style={{ fontSize:'0.68rem', color:'#94a3b8', marginBottom:3 }}>
              {type === 'payments' ? '入金内訳' : '受注案件'}
            </div>
            <div style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a' }}>{rep}</div>
            {type === 'payments' && rows?.length > 0 && (
              <div style={{ marginTop:3, fontSize:'0.82rem', fontWeight:700, color:'#059669' }}>
                合計 {fmtMYen(payTotal)} / {rows.length}件
              </div>
            )}
          </div>
          <button onClick={onClose} style={{ width:30, height:30, borderRadius:8, background:'#f1f5f9', border:'none', cursor:'pointer', color:'#64748b', fontSize:16, fontWeight:700, display:'flex', alignItems:'center', justifyContent:'center' }}>×</button>
        </div>

        {/* コンテンツ */}
        <div style={{ overflowY:'auto', padding:'8px 16px 16px' }}>
          {rows === null ? (
            <div style={{ padding:40, textAlign:'center', color:'#94a3b8', fontSize:'0.85rem' }}>読み込み中…</div>
          ) : rows.length === 0 ? (
            <div style={{ padding:40, textAlign:'center', color:'#94a3b8', fontSize:'0.85rem' }}>データがありません</div>
          ) : type === 'payments' ? (
            <div style={{ display:'flex', flexDirection:'column', gap:6, marginTop:8 }}>
              {rows.map((r, i) => (
                <div key={i} style={{ background:'#fff', borderRadius:10, padding:'10px 14px', display:'flex', alignItems:'center', gap:12, border:'1px solid #f1f5f9', boxShadow:'0 1px 3px rgba(0,0,0,0.04)' }}>
                  <div style={{ width:36, fontSize:'0.68rem', color:'#94a3b8', flexShrink:0, textAlign:'center', background:'#f8fafc', borderRadius:6, padding:'4px 0', lineHeight:1.4 }}>
                    {r.payment_date ? fmtDate(r.payment_date).substring(5) : '—'}
                  </div>
                  <div style={{ flex:1, minWidth:0 }}>
                    <div style={{ fontWeight:600, color:'#0f172a', fontSize:'0.85rem', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{r.company}</div>
                    {r.plan && (
                      <span style={{ fontSize:'0.62rem', background:'#eff6ff', color:'#1e40af', borderRadius:4, padding:'1px 7px', marginTop:3, display:'inline-block', fontWeight:600 }}>{r.plan}</span>
                    )}
                  </div>
                  <div style={{ fontWeight:800, color:'#059669', fontSize:'0.9rem', flexShrink:0 }}>{fmtMYen(r.incentive_amount)}</div>
                </div>
              ))}
            </div>
          ) : (
            <div style={{ display:'flex', flexDirection:'column', gap:6, marginTop:8 }}>
              {rows.map((r, i) => (
                <div key={i} style={{ background:'#fff', borderRadius:10, padding:'10px 14px', display:'flex', alignItems:'center', gap:12, border:'1px solid #f1f5f9', boxShadow:'0 1px 3px rgba(0,0,0,0.04)' }}>
                  <div style={{ width:36, fontSize:'0.68rem', color:'#94a3b8', flexShrink:0, textAlign:'center', background:'#f8fafc', borderRadius:6, padding:'4px 0', lineHeight:1.4 }}>
                    {r.order_date ? fmtDate(r.order_date).substring(5) : '—'}
                  </div>
                  <div style={{ flex:1, fontWeight:600, color:'#0f172a', fontSize:'0.85rem' }}>{r.customer_name}</div>
                  <span style={{ fontSize:'0.65rem', background:'#dcfce7', color:'#059669', borderRadius:4, padding:'2px 8px', fontWeight:700, flexShrink:0 }}>受注</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default function CrmDashboard() {
  const now = new Date();
  const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

  const [data, setData]           = useState(null);
  const [summary, setSummary]     = useState(null);
  const [trend, setTrend]         = useState([]);
  const [activities, setActivities] = useState([]);
  const [loading, setLoading]     = useState(true);
  const [salesUser, setSalesUser] = useState('');
  const [period, setPeriod]       = useState('custom');
  const [customMonth, setCustomMonth] = useState(currentMonth);
  const [drill, setDrill]         = useState(null);
  const [alertOpen, setAlertOpen] = useState(true);
  const [syncing, setSyncing]     = useState(false);
  const [syncProgress, setSyncProgress] = useState(0);
  const [syncDone, setSyncDone]   = useState(false);

  const load = async (u, p, cm) => {
    setLoading(true);
    try {
      const params = { period: p };
      if (u) params.salesUser = u;
      if (p === 'custom' && cm) params.customMonth = cm;
      const summaryMonth = p === 'custom' && cm ? cm : undefined;
      const [d, s, t, acts] = await Promise.all([
        api.crmDashboard(params),
        api.crmMonthlySummary(summaryMonth),
        api.crmDashboardMonthlyTrend({ months: 6, ...(u ? { salesUser: u } : {}) }),
        api.crmDashboardRecentActivities(8),
      ]);
      setData(d); setSummary(s); setTrend(t.rows || []); setActivities(acts.rows || []);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  };

  useEffect(() => { load('', 'custom', currentMonth); }, []);

  const handleKintoneSync = async () => {
    if (syncing) return;
    setSyncing(true); setSyncDone(false); setSyncProgress(5);
    try {
      await api.kintoneSync();
      // 進捗アニメーション＋ポーリング
      let prog = 5;
      const tick = setInterval(async () => {
        prog = Math.min(prog + Math.random() * 8, 90);
        setSyncProgress(Math.round(prog));
        try {
          const st = await api.kintoneStatus();
          if (!st.inProgress) {
            clearInterval(tick);
            setSyncProgress(100);
            setSyncDone(true);
            setSyncing(false);
            // データ再読み込み
            setTimeout(() => {
              load(salesUser, period, customMonth);
              setSyncDone(false); setSyncProgress(0);
            }, 1500);
          }
        } catch { clearInterval(tick); setSyncing(false); }
      }, 2000);
    } catch { setSyncing(false); }
  };

  if (loading && !data) return (
    <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:'100%', color:'#94a3b8', fontSize:'0.88rem' }}>
      読み込み中…
    </div>
  );
  if (!data) return null;

  const {
    curr, prev, repTable, yomiBreakdown = [], overdueAlerts = [], stagnantAlerts = [],
    rangeStart, rangeEnd, prevStart, prevEnd, repTargetMap = {}, teamTarget = 0, planBreakdown = [],
  } = data;

  const reps = buildRepTable(repTable, salesUser);
  const termLabel = period === 'term' ? '期' : '月';
  const alertCount = overdueAlerts.length + stagnantAlerts.length;

  const forecast = summary ? {
    confirmed: summary.totals?.confirmed || 0,
    high:      summary.totals?.high      || 0,
    medium:    summary.totals?.medium    || 0,
    total:     summary.totals?.total     || 0,
    kpi:       summary.totals?.kpi       || 0,
  } : null;
  const forecastTotal = forecast?.total || 0;
  // teamTarget = 担当者役職別目標の合計（固定）/ フォールバック: 動的KPI
  const kpiDenom   = teamTarget > 0 ? teamTarget : (forecast?.kpi || 0);
  const kpiAchieve = kpiDenom > 0 ? Math.round(curr.paymentAmount / kpiDenom * 100) : null;
  const kpiRate    = kpiDenom > 0 ? Math.round(forecastTotal / kpiDenom * 100) : null;

  const winRate     = curr.meetingCount > 0 ? Math.round(curr.wonCount / curr.meetingCount * 100) : 0;
  const prevWinRate = prev?.meetingCount > 0 ? Math.round(prev.wonCount / prev.meetingCount * 100) : null;

  const trendData = trend.map(r => ({ month: r.month, amount: Number(r.amount) }));

  const yomiMap          = Object.fromEntries((yomiBreakdown).map(r => [r.yomi, r]));
  const totalActiveCount  = yomiBreakdown.reduce((s, r) => s + r.cnt, 0);
  const totalActiveAmount = yomiBreakdown.reduce((s, r) => s + Number(r.total_initial || 0), 0);

  const planData  = planBreakdown.map(r => ({ ...r, amount: Number(r.amount) }));
  const planTotal = planData.reduce((s, r) => s + r.amount, 0);

  const cardStyle = { background:'#fff', borderRadius:12, padding:'14px 18px', border:'1px solid #e2e8f0' };
  const sectionHead = (label, right) => (
    <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:10 }}>
      <div style={{ display:'flex', alignItems:'center', gap:6 }}>
        <span style={{ width:8, height:8, borderRadius:'50%', background:'#6366f1', display:'inline-block' }} />
        <span style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a' }}>{label}</span>
      </div>
      {right && <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>{right}</span>}
    </div>
  );

  return (
    <div style={{ padding:'14px 18px', background:'#f1f5f9', minHeight:'100%', display:'flex', flexDirection:'column', gap:10 }}>

      {/* ── フィルターバー ── */}
      <div style={{ display:'flex', alignItems:'center', gap:10, flexWrap:'wrap' }}>
        <div style={{ display:'flex', background:'#fff', borderRadius:8, padding:3, border:'1px solid #e2e8f0' }}>
          {[['custom','指定月'], ['term','今期']].map(([v, l]) => (
            <button key={v} onClick={() => { setPeriod(v); load(salesUser, v, customMonth); }}
              style={{ padding:'4px 16px', borderRadius:6, border:'none', cursor:'pointer', fontSize:'0.8rem',
                fontWeight:period===v ? 700 : 400, background:period===v ? '#1e40af' : 'transparent',
                color:period===v ? '#fff' : '#64748b', transition:'all 0.15s' }}>
              {l}
            </button>
          ))}
        </div>
        <input type="month" value={customMonth} disabled={period === 'term'}
          onChange={e => { setCustomMonth(e.target.value); load(salesUser, 'custom', e.target.value); }}
          style={{ padding:'4px 10px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.8rem',
            background: period === 'term' ? '#f1f5f9' : '#fff',
            color: period === 'term' ? '#cbd5e1' : '#0f172a',
            outline:'none', cursor: period === 'term' ? 'not-allowed' : 'auto' }} />
        <select value={salesUser} onChange={e => { setSalesUser(e.target.value); load(e.target.value, period, customMonth); }}
          style={{ padding:'5px 12px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.82rem', background:'#fff', cursor:'pointer', color:'#0f172a' }}>
          <option value="">全員</option>
          {TARGET_REPS.map(u => <option key={u} value={u}>{u}</option>)}
        </select>

        {/* kintone同期ボタン（暫定） */}
        <div style={{ marginLeft:'auto', display:'flex', flexDirection:'column', gap:4, alignItems:'flex-end' }}>
          <button onClick={handleKintoneSync} disabled={syncing}
            style={{ padding:'5px 14px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.78rem', fontWeight:600, cursor: syncing ? 'default' : 'pointer',
              background: syncDone ? '#f0fdf4' : syncing ? '#f8fafc' : '#fff',
              color: syncDone ? '#059669' : syncing ? '#94a3b8' : '#374151',
              display:'flex', alignItems:'center', gap:6, whiteSpace:'nowrap' }}>
            <span style={{ fontSize:'0.85rem' }}>{syncDone ? '✓' : '⟳'}</span>
            {syncDone ? '同期完了' : syncing ? 'kintone同期中…' : 'kintoneデータ取得'}
          </button>
          {syncing && (
            <div style={{ width:160, height:4, background:'#e2e8f0', borderRadius:2, overflow:'hidden' }}>
              <div style={{ height:'100%', width:`${syncProgress}%`, background:'#1e40af', borderRadius:2, transition:'width 0.4s ease' }} />
            </div>
          )}
          {syncing && (
            <span style={{ fontSize:'0.62rem', color:'#94a3b8' }}>{syncProgress}%</span>
          )}
        </div>
      </div>

      {/* ── KPIカード 5枚 ── */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(5,1fr)', gap:10 }}>
        {/* 入金額 — green */}
        <div style={{ background:'#fff', borderRadius:12, padding:'14px 18px', border:'1px solid #e2e8f0', borderTop:'3px solid #059669' }}>
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:8 }}>
            <span style={{ fontSize:'0.75rem', color:'#64748b', fontWeight:500 }}>入金額</span>
            <span style={{ width:28, height:28, borderRadius:8, background:'#f0fdf4', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.8rem', color:'#059669' }}>¥</span>
          </div>
          <div style={{ fontSize:'1.55rem', fontWeight:800, color:'#0f172a', lineHeight:1.1 }}>{fmtM(curr.paymentAmount)}</div>
          <div style={{ marginTop:4, display:'flex', alignItems:'center', gap:6 }}>
            <DiffTag diff={prev ? diffPct(curr.paymentAmount, prev.paymentAmount) : null} />
            <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>前{termLabel}比</span>
          </div>
        </div>

        {/* 受注件数 — blue */}
        <div style={{ background:'#fff', borderRadius:12, padding:'14px 18px', border:'1px solid #e2e8f0', borderTop:'3px solid #1e40af' }}>
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:8 }}>
            <span style={{ fontSize:'0.75rem', color:'#64748b', fontWeight:500 }}>受注件数</span>
            <span style={{ width:28, height:28, borderRadius:8, background:'#eff6ff', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.75rem', color:'#1e40af' }}>件</span>
          </div>
          <div style={{ fontSize:'1.55rem', fontWeight:800, color:'#0f172a', lineHeight:1.1 }}>
            {curr.wonCount}<span style={{ fontSize:'1rem', marginLeft:2 }}>件</span>
          </div>
          <div style={{ marginTop:4, display:'flex', alignItems:'center', gap:6 }}>
            <DiffTag diff={prev ? diffPct(curr.wonCount, prev.wonCount) : null} />
            <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>前{termLabel}比</span>
          </div>
        </div>

        {/* 初回商談数 — amber */}
        <div style={{ background:'#fff', borderRadius:12, padding:'14px 18px', border:'1px solid #e2e8f0', borderTop:'3px solid #d97706' }}>
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:8 }}>
            <span style={{ fontSize:'0.75rem', color:'#64748b', fontWeight:500 }}>初回商談数</span>
            <span style={{ width:28, height:28, borderRadius:8, background:'#fffbeb', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.75rem', color:'#d97706' }}>商</span>
          </div>
          <div style={{ fontSize:'1.55rem', fontWeight:800, color:'#0f172a', lineHeight:1.1 }}>
            {curr.meetingCount}<span style={{ fontSize:'1rem', marginLeft:2 }}>件</span>
          </div>
          <div style={{ marginTop:4, display:'flex', alignItems:'center', gap:6 }}>
            <DiffTag diff={prev ? diffPct(curr.meetingCount, prev.meetingCount) : null} />
            <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>前{termLabel}比</span>
          </div>
        </div>

        {/* 受注率 — purple */}
        <div style={{ background:'#fff', borderRadius:12, padding:'14px 18px', border:'1px solid #e2e8f0', borderTop:'3px solid #7c3aed' }}>
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:8 }}>
            <span style={{ fontSize:'0.75rem', color:'#64748b', fontWeight:500 }}>受注率</span>
            <span style={{ width:28, height:28, borderRadius:8, background:'#f5f3ff', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.75rem', color:'#7c3aed' }}>率</span>
          </div>
          <div style={{ fontSize:'1.55rem', fontWeight:800, color:'#0f172a', lineHeight:1.1 }}>
            {winRate}<span style={{ fontSize:'1rem', marginLeft:1 }}>%</span>
          </div>
          <div style={{ marginTop:4, display:'flex', alignItems:'center', gap:6 }}>
            <DiffTag diff={prevWinRate != null ? diffPct(winRate, prevWinRate) : null} />
            <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>前{termLabel}比</span>
          </div>
        </div>

        {/* KPI達成率 — dynamic color top border */}
        <div style={{ background:'#fff', borderRadius:12, padding:'14px 18px', border:'1px solid #e2e8f0',
          borderTop:`3px solid ${kpiAchieve == null ? '#94a3b8' : kpiAchieve >= 100 ? '#059669' : kpiAchieve >= 70 ? '#f59e0b' : '#ef4444'}` }}>
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:8 }}>
            <span style={{ fontSize:'0.75rem', color:'#64748b', fontWeight:500 }}>KPI達成率</span>
            <span style={{ width:28, height:28, borderRadius:8, background:'#fef9c3', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.75rem', color:'#a16207' }}>%</span>
          </div>
          <div style={{ fontSize:'1.55rem', fontWeight:800, lineHeight:1.1,
            color: kpiAchieve == null ? '#94a3b8' : kpiAchieve >= 100 ? '#059669' : kpiAchieve >= 70 ? '#d97706' : '#dc2626' }}>
            {kpiAchieve != null ? kpiAchieve : '—'}<span style={{ fontSize:'1rem', marginLeft:1 }}>%</span>
          </div>
          {kpiDenom > 0 && (
            <div style={{ marginTop:6 }}>
              <div style={{ height:5, background:'#f1f5f9', borderRadius:3, overflow:'hidden' }}>
                <div style={{ height:'100%', width:`${Math.min(100, kpiAchieve || 0)}%`, borderRadius:3, transition:'width 0.5s',
                  background: kpiAchieve >= 100 ? '#059669' : kpiAchieve >= 70 ? '#f59e0b' : '#ef4444' }} />
              </div>
              <div style={{ fontSize:'0.6rem', color:'#94a3b8', marginTop:2 }}>
                {fmtM(curr.paymentAmount)} / 目標 {fmtM(kpiDenom)}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* ── メイン2カラム ── */}
      <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10, flex:1 }}>

        {/* 左カラム */}
        <div style={{ display:'flex', flexDirection:'column', gap:10 }}>

          {/* 担当者別実績 */}
          <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden' }}>
            <div style={{ padding:'10px 16px', borderBottom:'1px solid #f1f5f9', display:'flex', alignItems:'center', justifyContent:'space-between' }}>
              <div style={{ display:'flex', alignItems:'center', gap:6 }}>
                <span style={{ width:8, height:8, borderRadius:'50%', background:'#6366f1', display:'inline-block' }} />
                <span style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a' }}>担当者別実績</span>
              </div>
              <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>6名　クリックでドリルダウン</span>
            </div>
            <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.8rem' }}>
              <thead>
                <tr style={{ background:'#f8fafc' }}>
                  {['担当者','入金額','受注','初回商談','受注率','達成率'].map((h, i) => (
                    <th key={h} style={{ padding:'8px 14px', textAlign:i === 0 ? 'left' : 'right', fontWeight:600, color:'#64748b', borderBottom:'1px solid #f1f5f9', fontSize:'0.72rem', whiteSpace:'nowrap' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {reps.map((r, ri) => {
                  const color       = REP_COLORS[ri % REP_COLORS.length];
                  const repWinRate  = r.meetingCount > 0 ? Math.round(r.wonCount / r.meetingCount * 100) : 0;
                  const repTarget = repTargetMap[r.rep] || 0;
                  const repAchieve  = (repTarget > 0 && !r.isOther) ? Math.round(r.paymentAmount / repTarget * 100) : null;
                  const [fam, given] = r.rep.split(/[\s　]/);
                  return (
                    <tr key={r.rep} style={{ borderBottom:'1px solid #f8fafc', transition:'background 0.1s' }}
                      onMouseEnter={e => e.currentTarget.style.background = '#f8fafc'}
                      onMouseLeave={e => e.currentTarget.style.background = ''}>
                      <td style={{ padding:'9px 14px' }}>
                        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                          <span style={{ width:24, height:24, borderRadius:6, flexShrink:0, display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.65rem', fontWeight:800,
                            background: r.isOther ? '#f1f5f9' : `${color}22`, color: r.isOther ? '#94a3b8' : color }}>
                            {r.isOther ? '他' : (fam?.[0] || '?')}
                          </span>
                          <div>
                            {r.isOther
                              ? <span style={{ color:'#94a3b8' }}>その他</span>
                              : <>
                                  <span style={{ fontWeight:700, color, fontSize:'0.68rem' }}>{fam}</span>
                                  <span style={{ color:'#374151', fontSize:'0.8rem', marginLeft:3 }}>{given || ''}</span>
                                </>
                            }
                          </div>
                        </div>
                      </td>
                      <td style={{ padding:'9px 14px', textAlign:'right' }}>
                        {r.isOther
                          ? <span style={{ color:'#94a3b8' }}>{fmtM(r.paymentAmount)}</span>
                          : <button onClick={() => setDrill({ rep:r.rep, type:'payments' })}
                              style={{ background:'none', border:'none', cursor:'pointer', fontWeight:700, color:'#059669', fontSize:'0.8rem', padding:0, borderBottom:'1px dotted #059669' }}>
                              {fmtM(r.paymentAmount)}
                            </button>}
                      </td>
                      <td style={{ padding:'9px 14px', textAlign:'right' }}>
                        {r.isOther
                          ? <span style={{ color:'#94a3b8' }}>{r.wonCount}件</span>
                          : <button onClick={() => setDrill({ rep:r.rep, type:'won' })}
                              style={{ background:'none', border:'none', cursor:'pointer', fontWeight:600, color:'#1e40af', fontSize:'0.8rem', padding:0, borderBottom:'1px dotted #1e40af' }}>
                              {r.wonCount}件
                            </button>}
                      </td>
                      <td style={{ padding:'9px 14px', textAlign:'right', color:'#374151' }}>{r.meetingCount}件</td>
                      <td style={{ padding:'9px 14px', textAlign:'right' }}>
                        <span style={{ fontWeight:600, color: repWinRate >= 30 ? '#059669' : repWinRate >= 15 ? '#d97706' : '#94a3b8' }}>
                          {repWinRate}%
                        </span>
                      </td>
                      <td style={{ padding:'9px 14px', textAlign:'right' }}>
                        {repAchieve != null
                          ? <span style={{ fontWeight:700, fontSize:'0.82rem', color: repAchieve >= 100 ? '#059669' : repAchieve >= 70 ? '#d97706' : '#dc2626' }}>{repAchieve}%</span>
                          : <span style={{ color:'#cbd5e1' }}>—</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* アラート — 常に表示 */}
          <div style={{ background:'#fff', borderRadius:12, border:`1px solid ${alertCount > 0 ? '#fca5a5' : '#e2e8f0'}`, overflow:'hidden' }}>
            <button onClick={() => setAlertOpen(v => !v)}
              style={{ width:'100%', padding:'10px 16px', background:'none', border:'none', cursor:'pointer', display:'flex', alignItems:'center', gap:8, textAlign:'left' }}>
              <span style={{ width:8, height:8, borderRadius:'50%', background: alertCount > 0 ? '#ef4444' : '#86efac', display:'inline-block' }} />
              <span style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a' }}>アラート</span>
              {alertCount > 0
                ? <span style={{ background:'#fef2f2', color:'#dc2626', borderRadius:10, padding:'1px 8px', fontSize:'0.72rem', fontWeight:700 }}>{alertCount}件</span>
                : <span style={{ background:'#f0fdf4', color:'#059669', borderRadius:10, padding:'1px 8px', fontSize:'0.72rem', fontWeight:600 }}>なし</span>
              }
              <span style={{ marginLeft:'auto', fontSize:'0.72rem', color:'#94a3b8' }}>{alertOpen ? '折りたたむ' : '展開'}</span>
            </button>
            {alertOpen && (
              alertCount === 0
                ? <div style={{ padding:'10px 16px 14px', fontSize:'0.78rem', color:'#94a3b8', textAlign:'center' }}>期限切れ・停滞中の案件はありません ✓</div>
                : (
                  <div style={{ padding:'0 16px 14px', display:'grid', gridTemplateColumns:'1fr 1fr', gap:12 }}>
                    {overdueAlerts.length > 0 && (
                      <div>
                        <div style={{ fontSize:'0.72rem', fontWeight:600, color:'#dc2626', marginBottom:6, display:'flex', alignItems:'center', gap:6 }}>
                          ⚠ アクション期限切れ
                          <span style={{ background:'#fef2f2', padding:'1px 6px', borderRadius:8 }}>{overdueAlerts.length}</span>
                        </div>
                        <div style={{ display:'flex', flexDirection:'column', gap:4 }}>
                          {overdueAlerts.map(d => {
                            const daysOver = Math.round((new Date() - new Date(d.next_action_date)) / 86400000);
                            return (
                              <div key={d.id} style={{ padding:'7px 10px', background:'#fff9f9', borderRadius:8, borderLeft:'3px solid #fca5a5' }}>
                                <div style={{ fontWeight:600, color:'#0f172a', fontSize:'0.78rem' }}>{d.customer_name}</div>
                                <div style={{ display:'flex', justifyContent:'space-between', marginTop:2 }}>
                                  <span style={{ fontSize:'0.65rem', color:'#94a3b8' }}>担当: {d.sales_person}</span>
                                  <span style={{ fontSize:'0.65rem', color:'#dc2626', fontWeight:600 }}>{fmtDate(d.next_action_date)} ({daysOver}日超過)</span>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    )}
                    {stagnantAlerts.length > 0 && (
                      <div>
                        <div style={{ fontSize:'0.72rem', fontWeight:600, color:'#b45309', marginBottom:6, display:'flex', alignItems:'center', gap:6 }}>
                          ‖ 停滞中案件
                          <span style={{ background:'#fffbeb', padding:'1px 6px', borderRadius:8 }}>{stagnantAlerts.length}</span>
                        </div>
                        <div style={{ display:'flex', flexDirection:'column', gap:4 }}>
                          {stagnantAlerts.map(d => (
                            <div key={d.id} style={{ padding:'7px 10px', background:'#fffdf5', borderRadius:8, borderLeft:'3px solid #fcd34d' }}>
                              <div style={{ fontWeight:600, color:'#0f172a', fontSize:'0.78rem' }}>{d.customer_name}</div>
                              <div style={{ display:'flex', justifyContent:'space-between', marginTop:2 }}>
                                <span style={{ fontSize:'0.65rem', color:'#94a3b8' }}>担当: {d.sales_person}</span>
                                <span style={{ fontSize:'0.65rem', color:'#b45309', fontWeight:600 }}>{d.days_since_update}日未更新</span>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )
            )}
          </div>
        </div>

        {/* 右カラム — 優先度順: 収支見込み → 入金推移 → プラン割合 → ファネル */}
        <div style={{ display:'flex', flexDirection:'column', gap:10 }}>

          {/* 収支見込み */}
          {forecast && (
            <div style={{ ...cardStyle, padding:'14px 16px' }}>
              {sectionHead('収支見込み', '対象月')}
              <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:12 }}>
                <div>
                  <div style={{ fontSize:'0.65rem', color:'#94a3b8', marginBottom:2 }}>見込み合計</div>
                  <div style={{ fontSize:'1.45rem', fontWeight:800, color:'#0f172a' }}>
                    {fmtM(forecastTotal)}<span style={{ fontSize:'0.8rem' }}>円</span>
                  </div>
                </div>
                {kpiRate != null && (
                  <div style={{ textAlign:'right' }}>
                    <div style={{ fontSize:'0.62rem', color: kpiRate >= 100 ? '#059669' : '#94a3b8', marginBottom:2 }}>KPI達成見込み {kpiRate}%</div>
                    <div style={{ fontSize:'1rem', fontWeight:800, color: kpiRate >= 100 ? '#059669' : kpiRate >= 70 ? '#d97706' : '#dc2626' }}>
                      {forecastTotal >= kpiDenom ? '+' : ''}{fmtM(forecastTotal - kpiDenom)}
                    </div>
                  </div>
                )}
              </div>
              <div style={{ display:'flex', flexDirection:'column', gap:8 }}>
                {[
                  { label:'入金確定',            amount:forecast.confirmed, color:'#059669', count:summary?.payments?.length },
                  { label:'締結ほぼ確実 [A/S]',  amount:forecast.high,      color:'#1e40af', count:summary?.highDeals?.length },
                  { label:'締結多分いける [B/C]', amount:forecast.medium,    color:'#d97706', count:summary?.mediumDeals?.length },
                ].map(item => {
                  const pct = forecastTotal > 0 ? Math.round((item.amount / forecastTotal) * 100) : 0;
                  return (
                    <div key={item.label}>
                      <div style={{ display:'flex', justifyContent:'space-between', marginBottom:3 }}>
                        <div style={{ display:'flex', alignItems:'center', gap:5 }}>
                          <span style={{ width:10, height:10, borderRadius:3, background:item.color, display:'inline-block' }} />
                          <span style={{ fontSize:'0.7rem', color:'#374151' }}>{item.label}</span>
                        </div>
                        <div>
                          <span style={{ fontSize:'0.78rem', fontWeight:700, color:item.color }}>{fmtM(item.amount)}</span>
                          {item.count != null && <span style={{ fontSize:'0.65rem', color:'#94a3b8', marginLeft:5 }}>{item.count}件</span>}
                        </div>
                      </div>
                      <div style={{ height:8, background:'#f1f5f9', borderRadius:4, overflow:'hidden' }}>
                        <div style={{ height:'100%', width:`${pct}%`, background:item.color, borderRadius:4, transition:'width 0.5s' }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* 入金推移 */}
          <div style={{ ...cardStyle, padding:'14px 16px' }}>
            {sectionHead('入金推移', '過去6ヶ月　単位: 万円')}
            <ResponsiveContainer width="100%" height={130}>
              <BarChart data={trendData} margin={{ top:0, right:0, left:0, bottom:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                <XAxis dataKey="month" tick={{ fontSize:9, fill:'#94a3b8' }} axisLine={false} tickLine={false} />
                <YAxis tickFormatter={v => `${Math.round(v / 1e4)}万`} tick={{ fontSize:9, fill:'#94a3b8' }} axisLine={false} tickLine={false} width={42} />
                <Tooltip formatter={v => [`${fmtM(v)}`, '入金額']}
                  contentStyle={{ background:'#fff', border:'1px solid #e2e8f0', borderRadius:8, fontSize:11, boxShadow:'0 4px 12px rgba(0,0,0,0.1)' }} />
                <Bar dataKey="amount" radius={[3, 3, 0, 0]}>
                  {trendData.map((_, i) => <Cell key={i} fill={i === trendData.length - 1 ? '#1e40af' : '#bfdbfe'} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* プラン割合（ドーナツグラフ） */}
          {planData.length > 0 && (
            <div style={{ ...cardStyle, padding:'14px 16px' }}>
              {sectionHead('受注プラン割合', `入金確定 ${planData.length}種`)}
              <div style={{ display:'flex', alignItems:'center', gap:12 }}>
                <div style={{ width:140, height:140, flexShrink:0 }}>
                  <PieChart width={140} height={140}>
                    <Pie data={planData} dataKey="amount" nameKey="plan"
                      cx="50%" cy="50%" outerRadius={62} innerRadius={36} paddingAngle={2}>
                      {planData.map((_, i) => <Cell key={i} fill={PLAN_COLORS[i % PLAN_COLORS.length]} />)}
                    </Pie>
                    <Tooltip formatter={(v, name) => [`${fmtM(v)}`, name]}
                      contentStyle={{ background:'#fff', border:'1px solid #e2e8f0', borderRadius:8, fontSize:11 }} />
                  </PieChart>
                </div>
                <div style={{ flex:1, display:'flex', flexDirection:'column', gap:5 }}>
                  {planData.map((p, i) => {
                    const exact = planTotal > 0 ? p.amount / planTotal * 100 : 0;
                    const pctLabel = exact <= 0 ? '0%' : exact < 1 ? '<1%' : `${Math.floor(exact)}%`;
                    return (
                      <div key={p.plan} style={{ display:'flex', alignItems:'center', gap:6 }}>
                        <span style={{ width:8, height:8, borderRadius:2, background:PLAN_COLORS[i % PLAN_COLORS.length], flexShrink:0 }} />
                        <span style={{ fontSize:'0.7rem', color:'#374151', flex:1, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{p.plan}</span>
                        <span style={{ fontSize:'0.68rem', color:'#64748b', flexShrink:0 }}>{p.cnt}件</span>
                        <span style={{ fontSize:'0.72rem', fontWeight:700, color:PLAN_COLORS[i % PLAN_COLORS.length], flexShrink:0, width:36, textAlign:'right' }}>{pctLabel}</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          )}

          {/* パイプラインファネル */}
          <div style={{ ...cardStyle, padding:'14px 16px' }}>
            {sectionHead('パイプラインファネル', `${totalActiveCount}件 / ${fmtM(totalActiveAmount)}`)}
            <div style={{ display:'flex', flexDirection:'column', gap:5 }}>
              {YOMI_ORDER.map(key => {
                const r = yomiMap[key];
                if (!r) return null;
                const color = YOMI_COLORS[key] || '#94a3b8';
                const pct   = totalActiveCount > 0 ? (r.cnt / totalActiveCount) * 100 : 0;
                return (
                  <div key={key} style={{ display:'flex', alignItems:'center', gap:8 }}>
                    <span style={{ fontSize:'0.62rem', color:'#64748b', width:54, flexShrink:0, textAlign:'right' }}>{YOMI_LABELS[key]}</span>
                    <div style={{ flex:1, height:18, background:'#f1f5f9', borderRadius:4, overflow:'hidden', position:'relative' }}>
                      <div style={{ height:'100%', width:`${Math.max(pct, r.cnt > 0 ? 8 : 0)}%`, background:color, borderRadius:4, display:'flex', alignItems:'center', paddingLeft:5, minWidth:r.cnt > 0 ? 24 : 0 }}>
                        {r.cnt > 0 && <span style={{ fontSize:'0.6rem', fontWeight:700, color: pct > 25 ? '#fff' : '#374151', whiteSpace:'nowrap' }}>{r.cnt}件</span>}
                      </div>
                    </div>
                    <span style={{ fontSize:'0.65rem', color:'#94a3b8', width:50, flexShrink:0, textAlign:'right' }}>{fmtM(r.total_initial)}</span>
                  </div>
                );
              })}
              {curr.wonCount > 0 && (
                <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                  <span style={{ fontSize:'0.62rem', color:'#059669', width:54, flexShrink:0, textAlign:'right', fontWeight:600 }}>受注</span>
                  <div style={{ flex:1, height:18, background:'#dcfce7', borderRadius:4, overflow:'hidden' }}>
                    <div style={{ height:'100%', width:'100%', background:'#059669', borderRadius:4, display:'flex', alignItems:'center', paddingLeft:5 }}>
                      <span style={{ fontSize:'0.6rem', fontWeight:700, color:'#fff' }}>{curr.wonCount}件</span>
                    </div>
                  </div>
                  <span style={{ fontSize:'0.65rem', color:'#94a3b8', width:50, flexShrink:0, textAlign:'right' }}>{fmtM(curr.wonAmount)}</span>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {drill && (
        <Drilldown rep={drill.rep} type={drill.type} start={rangeStart} end={rangeEnd} onClose={() => setDrill(null)} />
      )}
    </div>
  );
}
