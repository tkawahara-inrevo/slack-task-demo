import { useEffect, useState } from 'react';
import { useBreakpoint } from '../../hooks/useWindowWidth';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, PieChart, Pie, ReferenceLine, LabelList } from 'recharts';
import { api } from '../../api/client';

// TARGET_REPS はサーバー（/api/crm/dashboard の targetReps）から動的取得
const REP_COLORS  = ['#6366f1','#0ea5e9','#10b981','#f59e0b','#ef4444','#8b5cf6','#14b8a6','#f97316'];

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

// CSS変数ベースのスタイル定数（ダークモード対応）
const C = {
  surface:  'var(--surface)',
  surface2: 'var(--surface-2)',
  bg:       'var(--gray-50)',
  border:   'var(--gray-200)',
  text:     'var(--gray-900)',
  textSub:  'var(--gray-500)',
  textMid:  'var(--gray-600)',
  radius:   12,
};

const fmtM = n => {
  if (!n) return '0万';
  const m = Number(n);
  if (m >= 1e8) return `${(m / 1e8).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}億`;
  if (m >= 1e5) return `${Math.round(m / 1e4).toLocaleString()}万`;   // 10万以上 = 整数万
  if (m >= 1e4) return `${(m / 1e4).toFixed(1)}万`;                   // 1万〜10万 = 小数点1桁
  return m.toLocaleString();                                          // 1万未満 = 円のまま
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

function buildRepTable(repTable, targetReps, filterRep) {
  if (!repTable?.length) return [];
  if (filterRep && targetReps.includes(filterRep)) {
    const f = repTable.find(r => r.rep === filterRep);
    return [f ? { ...f } : { rep: filterRep, wonCount: 0, meetingCount: 0, paymentAmount: 0 }];
  }
  // targetReps の順に並べる（添田/リファラルも targetReps の末尾に含まれている前提）
  return targetReps.map(name => {
    const f = repTable.find(r => r.rep === name);
    return f ? { ...f } : { rep: name, wonCount: 0, meetingCount: 0, paymentAmount: 0 };
  });
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
      <div style={{ background:C.surface2, borderRadius:16, width:'min(600px,92vw)', maxHeight:'80vh', overflow:'hidden', display:'flex', flexDirection:'column', boxShadow:'0 32px 64px rgba(0,0,0,0.3)' }} onClick={e => e.stopPropagation()}>

        {/* ヘッダー */}
        <div style={{ padding:'16px 20px', background:C.surface, borderBottom:`1px solid ${C.border}`, display:'flex', justifyContent:'space-between', alignItems:'flex-start' }}>
          <div>
            <div style={{ fontSize:'0.68rem', color:C.textSub, marginBottom:3 }}>
              {type === 'payments' ? '入金内訳' : '受注案件'}
            </div>
            <div style={{ fontWeight:800, fontSize:'1rem', color:C.text }}>{rep}</div>
            {type === 'payments' && rows?.length > 0 && (
              <div style={{ marginTop:3, fontSize:'0.82rem', fontWeight:700, color:'#059669' }}>
                合計 {fmtMYen(payTotal)} / {rows.length}件
              </div>
            )}
          </div>
          <button onClick={onClose} style={{ width:30, height:30, borderRadius:8, background:C.surface2, border:'none', cursor:'pointer', color:C.textSub, fontSize:16, fontWeight:700, display:'flex', alignItems:'center', justifyContent:'center' }}>×</button>
        </div>

        {/* コンテンツ */}
        <div style={{ overflowY:'auto', padding:'8px 16px 16px' }}>
          {rows === null ? (
            <div style={{ padding:40, textAlign:'center', color:C.textSub, fontSize:'0.85rem' }}>読み込み中…</div>
          ) : rows.length === 0 ? (
            <div style={{ padding:40, textAlign:'center', color:C.textSub, fontSize:'0.85rem' }}>データがありません</div>
          ) : type === 'payments' ? (
            <div style={{ display:'flex', flexDirection:'column', gap:6, marginTop:8 }}>
              {rows.map((r, i) => (
                <div key={i} style={{ background:C.surface, borderRadius:10, padding:'10px 14px', display:'flex', alignItems:'center', gap:12, border:'1px solid #f1f5f9', boxShadow:'0 1px 3px rgba(0,0,0,0.04)' }}>
                  <div style={{ width:36, fontSize:'0.68rem', color:C.textSub, flexShrink:0, textAlign:'center', background:C.surface2, borderRadius:6, padding:'4px 0', lineHeight:1.4 }}>
                    {r.payment_date ? fmtDate(r.payment_date).substring(5) : '—'}
                  </div>
                  <div style={{ flex:1, minWidth:0 }}>
                    <div style={{ fontWeight:600, color:C.text, fontSize:'0.85rem', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{r.company}</div>
                    {r.plan && (
                      <span style={{ fontSize:'0.62rem', background:'#eff6ff', color:'#1e40af', borderRadius:4, padding:'1px 7px', marginTop:3, display:'inline-block', fontWeight:600 }}>
                        {r.plan?.includes('月額') && r.month_num ? `月額（${r.month_num}ヶ月目）` : r.plan}
                      </span>
                    )}
                  </div>
                  <div style={{ textAlign:'right', flexShrink:0 }}>
                    <div style={{ fontWeight:800, color:'#059669', fontSize:'0.9rem' }}>{fmtMYen(r.incentive_amount)}</div>
                    {r.amount > 0 && r.amount !== r.incentive_amount && (
                      <div style={{ fontSize:'0.62rem', color:C.textSub, marginTop:1 }}>入金 {fmtMYen(r.amount)}</div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div style={{ display:'flex', flexDirection:'column', gap:6, marginTop:8 }}>
              {rows.map((r, i) => (
                <div key={i} style={{ background:C.surface, borderRadius:10, padding:'10px 14px', display:'flex', alignItems:'center', gap:10, border:'1px solid #f1f5f9', boxShadow:'0 1px 3px rgba(0,0,0,0.04)' }}>
                  <div style={{ width:36, fontSize:'0.68rem', color:C.textSub, flexShrink:0, textAlign:'center', background:C.surface2, borderRadius:6, padding:'4px 0', lineHeight:1.4 }}>
                    {r.order_date ? fmtDate(r.order_date).substring(5) : '—'}
                  </div>
                  <div style={{ flex:1, minWidth:0 }}>
                    <div style={{ fontWeight:600, color:C.text, fontSize:'0.85rem', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{r.customer_name}</div>
                    {r.contract_type && (
                      <span style={{ fontSize:'0.65rem', color:'#6366f1', background:'#eef2ff', borderRadius:4, padding:'1px 6px', marginTop:2, display:'inline-block' }}>
                        {r.contract_type}
                      </span>
                    )}
                  </div>
                  {(r.initial_fee > 0 || r.monthly_fee > 0) && (
                    <span style={{ fontSize:'0.72rem', color:'#059669', fontWeight:700, flexShrink:0 }}>
                      {r.contract_type?.includes('月額')
                        ? (r.monthly_fee > 0 ? `月${fmtM(r.monthly_fee)}` : fmtM(r.initial_fee))
                        : fmtM(r.initial_fee || r.monthly_fee)}
                    </span>
                  )}
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
  const { isTablet } = useBreakpoint();
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
  const [planMode, setPlanMode]   = useState('amount'); // 'amount' | 'count'
  const [syncing, setSyncing]     = useState(false);
  const [syncProgress, setSyncProgress] = useState(0);
  const [syncDone, setSyncDone]   = useState(false);
  const [lastSync, setLastSync]   = useState(null);
  const [kpiDrill, setKpiDrill]   = useState(null); // { type: 'payment'|'won'|'meeting', rows: [] }

  // 最終同期時刻取得
  useEffect(() => {
    api.kintoneStatus().then(r => setLastSync(r.lastSync)).catch(() => {});
  }, [syncDone]);

  const load = async (u, p, cm) => {
    setLoading(true);
    try {
      const params = { period: p };
      if (u) params.salesUser = u;
      if (p === 'custom' && cm) params.customMonth = cm;
      const summaryMonth = p === 'custom' && cm ? cm : undefined;
      const [d, s, t, acts] = await Promise.all([
        api.crmDashboard(params),
        api.crmMonthlySummary(summaryMonth, u || undefined),
        api.crmDashboardMonthlyTrend({ months: p === 'term' ? 12 : 6, ...(u ? { salesUser: u } : {}) }),
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
    <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:'100%', color:C.textSub, fontSize:'0.88rem' }}>
      読み込み中…
    </div>
  );
  if (!data) return null;

  const {
    curr, prev, repTable, yomiBreakdown = [], overdueAlerts = [], stagnantAlerts = [],
    rangeStart, rangeEnd, prevStart, prevEnd, repTargetMap = {}, repRoleInferred = {}, teamTarget = 0, planBreakdown = [],
    targetReps = [],
  } = data;

  const reps = buildRepTable(repTable, targetReps, salesUser);
  const termLabel = period === 'term' ? '期' : '月';
  const alertCount = overdueAlerts.length + stagnantAlerts.length;

  const forecast = summary ? {
    confirmed:          summary.totals?.confirmed          || 0,
    confirmedIncentive: summary.totals?.confirmedIncentive || 0,
    high:               summary.totals?.high               || 0,
    highKpi:            summary.totals?.highKpi            || 0,
    medium:             summary.totals?.medium             || 0,
    mediumKpi:          summary.totals?.mediumKpi          || 0,
    total:              summary.totals?.total              || 0,
    kpiTotal:           summary.totals?.kpiTotal           || 0,
    kpi:                summary.totals?.kpi                || 0,
  } : null;
  const forecastTotal    = forecast?.kpiTotal || 0;  // インセンベース（KPI計算用）
  const forecastDispTotal = forecast?.total   || 0;  // 実入金ベース（表示・バー用）

  // 今期モード専用: termMonthsをkpiDenom計算より先に定義
  const termMonths = (period === 'term' && rangeStart && rangeEnd)
    ? Math.max(1, Math.round((new Date(rangeEnd) - new Date(rangeStart)) / (30.44 * 86400000)))
    : 1;
  // 担当者フィルタ時は個人目標、全員表示時はチーム合計
  const effectiveMonthlyTarget = salesUser && repTargetMap[salesUser] > 0
    ? repTargetMap[salesUser]
    : teamTarget;
  const termKpiTarget = period === 'term' ? effectiveMonthlyTarget * termMonths : 0;

  // KPI分母: 今期は月次目標×月数、指定月は月次目標（担当者フィルタ時は個人目標）
  const kpiDenom   = period === 'term'
    ? (termKpiTarget > 0 ? termKpiTarget : (forecast?.kpi || 0))
    : (effectiveMonthlyTarget > 0 ? effectiveMonthlyTarget : (forecast?.kpi || 0));
  const kpiAchieve = kpiDenom > 0 ? Math.round((curr.incentiveAmount || 0) / kpiDenom * 100) : null;
  const kpiRate    = kpiDenom > 0 ? Math.round(forecastTotal / kpiDenom * 100) : null;

  const winRate     = curr.meetingCount > 0 ? Math.round(curr.wonCount / curr.meetingCount * 100) : 0;
  const prevWinRate = prev?.meetingCount > 0 ? Math.round(prev.wonCount / prev.meetingCount * 100) : null;

  const trendData = trend.map(r => ({ month: r.month, amount: Number(r.amount) }));

  const yomiMap          = Object.fromEntries((yomiBreakdown).map(r => [r.yomi, r]));
  const totalActiveCount  = yomiBreakdown.reduce((s, r) => s + r.cnt, 0);
  const totalActiveAmount = yomiBreakdown.reduce((s, r) => s + Number(r.total_initial || 0), 0);

  const planData  = planBreakdown.map(r => ({ ...r, amount: Number(r.amount) }));
  const planTotal = planData.reduce((s, r) => s + r.amount, 0);
  const termAchieve     = termKpiTarget > 0 ? Math.round((curr.incentiveAmount || 0) / termKpiTarget * 100) : null;
  const prevTermAchieve = (termKpiTarget > 0 && prev) ? Math.round((prev.incentiveAmount || 0) / termKpiTarget * 100) : null;

  const cardStyle = { background:C.surface, borderRadius:C.radius, padding:'14px 18px', border:`1px solid ${C.border}` };
  const sectionHead = (label, right) => (
    <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:10 }}>
      <div style={{ display:'flex', alignItems:'center', gap:6 }}>
        <span style={{ width:8, height:8, borderRadius:'50%', background:'#6366f1', display:'inline-block' }} />
        <span style={{ fontWeight:700, fontSize:'0.85rem', color:C.text }}>{label}</span>
      </div>
      {right && <span style={{ fontSize:'0.68rem', color:C.textSub }}>{right}</span>}
    </div>
  );

  return (
    <div style={{ padding:'14px 18px', background:C.bg, minHeight:'100%', display:'flex', flexDirection:'column', gap:10 }}>

      {/* ── フィルターバー ── */}
      <div style={{ display:'flex', alignItems:'center', gap:8, flexWrap:'wrap' }}>
        <div style={{ display:'flex', background:C.surface, borderRadius:8, padding:3, border:`1px solid ${C.border}` }}>
          {[['custom','指定月'], ['term','今期']].map(([v, l]) => (
            <button key={v} onClick={() => { setPeriod(v); load(salesUser, v, customMonth); }}
              style={{ padding:'4px 16px', borderRadius:6, border:'none', cursor:'pointer', fontSize:'0.8rem',
                fontWeight:period===v ? 700 : 400, background:period===v ? 'var(--primary)' : 'transparent',
                color:period===v ? '#fff' : C.textMid, transition:'all 0.15s' }}>
              {l}
            </button>
          ))}
        </div>
        <input type="month" value={customMonth} disabled={period === 'term'}
          onChange={e => { setCustomMonth(e.target.value); load(salesUser, 'custom', e.target.value); }}
          style={{ padding:'4px 10px', border:`1px solid ${C.border}`, borderRadius:8, fontSize:'0.8rem',
            background: period === 'term' ? C.surface2 : C.surface,
            color: period === 'term' ? 'var(--gray-300)' : C.text,
            outline:'none', cursor: period === 'term' ? 'not-allowed' : 'auto' }} />
        <select value={salesUser} onChange={e => { setSalesUser(e.target.value); load(e.target.value, period, customMonth); }}
          style={{ padding:'5px 12px', border:`1px solid ${C.border}`, borderRadius:8, fontSize:'0.82rem', background:C.surface, cursor:'pointer', color:C.text }}>
          <option value="">全員</option>
          {targetReps.map(u => <option key={u} value={u}>{u}</option>)}
        </select>

        {/* kintone同期ボタン + 最終同期時刻 */}
        <div style={{ marginLeft:'auto', display:'flex', flexDirection:'column', gap:4, alignItems:'flex-end' }}>
          <div style={{ display:'flex', alignItems:'center', gap:8 }}>
            {lastSync && !syncing && (
              <span style={{ fontSize:'0.68rem', color:C.textSub }}>
                最終同期: {new Date(lastSync).toLocaleString('ja-JP', { month:'numeric', day:'numeric', hour:'2-digit', minute:'2-digit' })}
              </span>
            )}
            <button onClick={handleKintoneSync} disabled={syncing}
              style={{ padding:'5px 14px', border:`1px solid ${C.border}`, borderRadius:8, fontSize:'0.78rem', fontWeight:600, cursor: syncing ? 'default' : 'pointer',
                background: syncDone ? '#f0fdf4' : C.surface,
                color: syncDone ? '#059669' : syncing ? C.textSub : C.textMid,
                display:'flex', alignItems:'center', gap:6, whiteSpace:'nowrap' }}>
              <span style={{ fontSize:'0.85rem' }}>{syncDone ? '✓' : '⟳'}</span>
              {syncDone ? '同期完了' : syncing ? 'kintone同期中…' : 'kintoneデータ取得'}
            </button>
          </div>
          {syncing && (
            <div style={{ display:'flex', alignItems:'center', gap:6 }}>
              <div style={{ width:160, height:4, background:`${C.border}`, borderRadius:2, overflow:'hidden' }}>
                <div style={{ height:'100%', width:`${syncProgress}%`, background:'var(--primary)', borderRadius:2, transition:'width 0.4s ease' }} />
              </div>
              <span style={{ fontSize:'0.62rem', color:C.textSub }}>{syncProgress}%</span>
            </div>
          )}
        </div>
      </div>

      {/* ── KPIカード（クリックで詳細） ── */}
      <div style={{ display:'grid', gridTemplateColumns: isTablet ? 'repeat(3,1fr)' : 'repeat(5,1fr)', gap:10 }}>
        {/* 入金額 */}
        <div onClick={() => setKpiDrill({ type:'payment', title:'入金額 内訳', rows: summary?.payments || [] })}
          style={{ ...cardStyle, borderTop:'3px solid #059669', cursor:'pointer', transition:'box-shadow 0.15s' }}
          onMouseEnter={e => e.currentTarget.style.boxShadow='0 4px 12px rgba(0,0,0,0.08)'}
          onMouseLeave={e => e.currentTarget.style.boxShadow='none'}>
          <div style={{ fontSize:'0.75rem', color:C.textSub, fontWeight:500, marginBottom:8 }}>入金額 <span style={{ fontSize:'0.6rem', opacity:0.6 }}>↗</span></div>
          <div style={{ fontSize:'1.55rem', fontWeight:800, color:C.text, lineHeight:1.1 }}>{fmtM(curr.paymentAmount)}</div>
          <div style={{ marginTop:4, display:'flex', alignItems:'center', gap:6 }}>
            <DiffTag diff={prev ? diffPct(curr.paymentAmount, prev.paymentAmount) : null} />
            <span style={{ fontSize:'0.68rem', color:C.textSub }}>前{termLabel}比</span>
          </div>
        </div>

        {/* 受注件数 */}
        <div onClick={() => setKpiDrill({ type:'won', title:'受注一覧', rows: repTable?.flatMap(r => r.won_details || []) || [] })}
          style={{ ...cardStyle, borderTop:'3px solid var(--primary)', cursor:'pointer', transition:'box-shadow 0.15s' }}
          onMouseEnter={e => e.currentTarget.style.boxShadow='0 4px 12px rgba(0,0,0,0.08)'}
          onMouseLeave={e => e.currentTarget.style.boxShadow='none'}>
          <div style={{ fontSize:'0.75rem', color:C.textSub, fontWeight:500, marginBottom:8 }}>受注件数 <span style={{ fontSize:'0.6rem', opacity:0.6 }}>↗</span></div>
          <div style={{ fontSize:'1.55rem', fontWeight:800, color:C.text, lineHeight:1.1 }}>
            {curr.wonCount}<span style={{ fontSize:'1rem', marginLeft:2 }}>件</span>
          </div>
          <div style={{ marginTop:4, display:'flex', alignItems:'center', gap:6 }}>
            <DiffTag diff={prev ? diffPct(curr.wonCount, prev.wonCount) : null} />
            <span style={{ fontSize:'0.68rem', color:C.textSub }}>前{termLabel}比</span>
          </div>
        </div>

        {/* 初回商談数 */}
        <div onClick={() => setKpiDrill({ type:'meeting', title:'初回商談一覧', rows: repTable?.flatMap(r => r.meeting_details || []) || [] })}
          style={{ ...cardStyle, borderTop:'3px solid #d97706', cursor:'pointer', transition:'box-shadow 0.15s' }}
          onMouseEnter={e => e.currentTarget.style.boxShadow='0 4px 12px rgba(0,0,0,0.08)'}
          onMouseLeave={e => e.currentTarget.style.boxShadow='none'}>
          <div style={{ fontSize:'0.75rem', color:C.textSub, fontWeight:500, marginBottom:8 }}>初回商談数 <span style={{ fontSize:'0.6rem', opacity:0.6 }}>↗</span></div>
          <div style={{ fontSize:'1.55rem', fontWeight:800, color:C.text, lineHeight:1.1 }}>
            {curr.meetingCount}<span style={{ fontSize:'1rem', marginLeft:2 }}>件</span>
          </div>
          <div style={{ marginTop:4, display:'flex', alignItems:'center', gap:6 }}>
            <DiffTag diff={prev ? diffPct(curr.meetingCount, prev.meetingCount) : null} />
            <span style={{ fontSize:'0.68rem', color:C.textSub }}>前{termLabel}比</span>
          </div>
        </div>

        {/* 受注率 */}
        <div style={{ ...cardStyle, borderTop:'3px solid #7c3aed' }}>
          <div style={{ fontSize:'0.75rem', color:C.textSub, fontWeight:500, marginBottom:8 }}>受注率</div>
          <div style={{ fontSize:'1.55rem', fontWeight:800, color:C.text, lineHeight:1.1 }}>
            {winRate}<span style={{ fontSize:'1rem', marginLeft:1 }}>%</span>
          </div>
          <div style={{ marginTop:4, display:'flex', alignItems:'center', gap:6 }}>
            <DiffTag diff={prevWinRate != null ? diffPct(winRate, prevWinRate) : null} />
            <span style={{ fontSize:'0.68rem', color:C.textSub }}>前{termLabel}比</span>
          </div>
        </div>

        {/* KPI達成率 */}
        {(() => {
          const kpiColor = kpiAchieve == null ? C.textSub : kpiAchieve >= 100 ? '#059669' : kpiAchieve >= 70 ? '#d97706' : '#ef4444';
          return (
            <div style={{ ...cardStyle, borderTop:`3px solid ${kpiColor}` }}>
              <div style={{ fontSize:'0.75rem', color:C.textSub, fontWeight:500, marginBottom:8 }}>KPI達成率</div>
              <div style={{ fontSize:'1.55rem', fontWeight:800, lineHeight:1.1, color:kpiColor }}>
                {kpiAchieve != null ? kpiAchieve : '—'}<span style={{ fontSize:'1rem', marginLeft:1 }}>%</span>
              </div>
              {kpiDenom > 0 && (
                <div style={{ marginTop:6 }}>
                  <div style={{ height:5, background:C.surface2, borderRadius:3, overflow:'hidden' }}>
                    <div style={{ height:'100%', width:`${Math.min(100, kpiAchieve || 0)}%`, borderRadius:3, transition:'width 0.5s', background:kpiColor }} />
                  </div>
                  <div style={{ fontSize:'0.6rem', color:C.textSub, marginTop:2 }}>
                    インセン {fmtM(curr.incentiveAmount || 0)} / 目標 {fmtM(kpiDenom)}
                    {curr.paymentAmount > 0 && <span style={{ marginLeft:6 }}>（入金 {fmtM(curr.paymentAmount)}）</span>}
                  </div>
                  {curr.allianceIncentive > 0 && (
                    <div style={{ fontSize:'0.6rem', color:C.textSub, marginTop:1 }}>＋アライアンス {fmtM(curr.allianceIncentive)}（KPI除外）</div>
                  )}
                </div>
              )}
            </div>
          );
        })()}
      </div>

      {/* ── メイン2カラム ── */}
      <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10, flex:1 }}>

        {/* 左カラム */}
        <div style={{ display:'flex', flexDirection:'column', gap:10 }}>

          {/* 担当者別実績 */}
          <div style={{ ...cardStyle, padding:0, overflow:'hidden' }}>
            <div style={{ padding:'10px 16px', borderBottom:`1px solid ${C.border}`, display:'flex', alignItems:'center', justifyContent:'space-between' }}>
              <div style={{ display:'flex', alignItems:'center', gap:6 }}>
                <span style={{ width:8, height:8, borderRadius:'50%', background:'#6366f1', display:'inline-block' }} />
                <span style={{ fontWeight:700, fontSize:'0.85rem', color:C.text }}>担当者別実績</span>
              </div>
              <span style={{ fontSize:'0.68rem', color:C.textSub }}>{reps.length}名　クリックでドリルダウン</span>
            </div>
            <div style={{ overflowX:'auto', WebkitOverflowScrolling:'touch' }}>
              <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.8rem' }}>
                <thead>
                  <tr style={{ background:C.surface2 }}>
                    {['担当者','入金額','インセン','受注','初回商談','受注率','達成率'].map((h, i) => (
                      <th key={h} style={{ padding:'8px 14px', textAlign:i === 0 ? 'left' : 'right', fontWeight:600, color:C.textSub, borderBottom:`1px solid ${C.border}`, fontSize:'0.72rem', whiteSpace:'nowrap' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {reps.map((r, ri) => {
                    const color       = REP_COLORS[ri % REP_COLORS.length];
                    const repWinRate  = r.meetingCount > 0 ? Math.round(r.wonCount / r.meetingCount * 100) : 0;
                    const repTarget      = repTargetMap[r.rep] || 0;
                    const repTermTarget  = period === 'term' ? repTarget * termMonths : repTarget;
                    const repAchieve     = (repTermTarget > 0 && !r.isOther) ? Math.round((r.incentiveAmount || 0) / repTermTarget * 100) : null;
                    const [fam, given] = r.rep.split(/[\s　]/);
                    return (
                      <tr key={r.rep} style={{
                          borderBottom:'1px solid #f8fafc', transition:'background 0.1s',
                          background: r.groupType === 'alliance' ? '#fafaf0' : '',
                        }}
                        onMouseEnter={e => e.currentTarget.style.background = r.groupType === 'alliance' ? '#f5f5e0' : '#f8fafc'}
                        onMouseLeave={e => e.currentTarget.style.background = r.groupType === 'alliance' ? '#fafaf0' : ''}>
                        <td style={{ padding:'9px 14px' }}>
                          <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                            <span style={{ width:24, height:24, borderRadius:6, flexShrink:0, display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.65rem', fontWeight:800,
                              background: r.isOther ? '#f1f5f9' : r.groupType === 'alliance' ? '#fef9c322' : r.groupType === 'adda_ref' ? '#f0fdf4' : `${color}22`,
                              color: r.isOther ? '#94a3b8' : r.groupType === 'alliance' ? '#92400e' : r.groupType === 'adda_ref' ? '#059669' : color }}>
                              {r.isOther ? '他' : r.groupType === 'alliance' ? '提' : r.groupType === 'adda_ref' ? '添' : (fam?.[0] || '?')}
                            </span>
                            <div>
                              {r.isOther
                                ? <span style={{ color:C.textSub }}>その他</span>
                                : r.groupType === 'alliance'
                                ? <><span style={{ fontWeight:700, color:'#92400e', fontSize:'0.75rem' }}>アライアンス</span><span style={{ fontSize:'0.65rem', color:C.textSub, marginLeft:4 }}>KPI除外</span></>
                                : r.groupType === 'adda_ref'
                                ? <span style={{ fontWeight:700, color:'#059669', fontSize:'0.75rem' }}>添田/リファラル</span>
                                : <><span style={{ fontWeight:700, color, fontSize:'0.68rem' }}>{fam}</span><span style={{ color:C.textMid, fontSize:'0.8rem', marginLeft:3 }}>{given || ''}</span></>
                              }
                            </div>
                          </div>
                        </td>
                        <td style={{ padding:'9px 10px', textAlign:'right', color: r.isOther ? '#94a3b8' : '#374151', fontSize:'0.8rem' }}>{fmtM(r.paymentAmount)}</td>
                        <td style={{ padding:'9px 10px', textAlign:'right' }}>
                          {r.isOther
                            ? <span style={{ color:C.textSub }}>{fmtM(r.incentiveAmount || 0)}</span>
                            : <button onClick={() => setDrill({ rep:r.rep, type:'payments' })}
                                style={{ background:'none', border:'none', cursor:'pointer', fontWeight:700, color:'#0891b2', fontSize:'0.8rem', padding:0, borderBottom:'1px dotted #0891b2' }}>
                                {fmtM(r.incentiveAmount || 0)}
                              </button>}
                        </td>
                        <td style={{ padding:'9px 14px', textAlign:'right' }}>
                          {r.isOther
                            ? <span style={{ color:C.textSub }}>{r.wonCount}件</span>
                            : <button onClick={() => setDrill({ rep:r.rep, type:'won' })}
                                style={{ background:'none', border:'none', cursor:'pointer', fontWeight:600, color:'#1e40af', fontSize:'0.8rem', padding:0, borderBottom:'1px dotted #1e40af' }}>
                                {r.wonCount}件
                              </button>}
                        </td>
                        <td style={{ padding:'9px 14px', textAlign:'right', color:C.textMid }}>{r.meetingCount}件</td>
                        <td style={{ padding:'9px 14px', textAlign:'right' }}>
                          <span style={{ fontWeight:600, color: repWinRate >= 30 ? '#059669' : repWinRate >= 15 ? '#d97706' : '#94a3b8' }}>{repWinRate}%</span>
                        </td>
                        <td style={{ padding:'9px 14px', textAlign:'right' }}>
                          {repAchieve != null
                            ? <div>
                                <span style={{ fontWeight:700, fontSize:'0.82rem', color: repAchieve >= 100 ? '#059669' : repAchieve >= 70 ? '#d97706' : '#dc2626' }}>{repAchieve}%</span>
                                <div style={{ fontSize:'0.58rem', color:C.textSub, marginTop:1 }}>{repRoleInferred[r.rep] || ''}</div>
                              </div>
                            : <span style={{ color:'#cbd5e1' }}>—</span>}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* アラート — 常に表示 */}
          <div style={{ background:C.surface, borderRadius:12, border:`1px solid ${alertCount > 0 ? '#fca5a5' : '#e2e8f0'}`, overflow:'hidden' }}>
            <button onClick={() => setAlertOpen(v => !v)}
              style={{ width:'100%', padding:'10px 16px', background:'none', border:'none', cursor:'pointer', display:'flex', alignItems:'center', gap:8, textAlign:'left' }}>
              <span style={{ width:8, height:8, borderRadius:'50%', background: alertCount > 0 ? '#ef4444' : '#86efac', display:'inline-block' }} />
              <span style={{ fontWeight:700, fontSize:'0.85rem', color:C.text }}>アラート</span>
              {alertCount > 0
                ? <span style={{ background:'#fef2f2', color:'#dc2626', borderRadius:10, padding:'1px 8px', fontSize:'0.72rem', fontWeight:700 }}>{alertCount}件</span>
                : <span style={{ background:'#f0fdf4', color:'#059669', borderRadius:10, padding:'1px 8px', fontSize:'0.72rem', fontWeight:600 }}>なし</span>
              }
              <span style={{ marginLeft:'auto', fontSize:'0.72rem', color:C.textSub }}>{alertOpen ? '折りたたむ' : '展開'}</span>
            </button>
            {alertOpen && (
              alertCount === 0
                ? <div style={{ padding:'10px 16px 14px', fontSize:'0.78rem', color:C.textSub, textAlign:'center' }}>期限切れ・停滞中の案件はありません ✓</div>
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
                                <div style={{ fontWeight:600, color:C.text, fontSize:'0.78rem' }}>{d.customer_name}</div>
                                <div style={{ display:'flex', justifyContent:'space-between', marginTop:2 }}>
                                  <span style={{ fontSize:'0.65rem', color:C.textSub }}>担当: {d.sales_person}</span>
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
                              <div style={{ fontWeight:600, color:C.text, fontSize:'0.78rem' }}>{d.customer_name}</div>
                              <div style={{ display:'flex', justifyContent:'space-between', marginTop:2 }}>
                                <span style={{ fontSize:'0.65rem', color:C.textSub }}>担当: {d.sales_person}</span>
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

        {/* 右カラム */}
        <div style={{ display:'flex', flexDirection:'column', gap:10 }}>

          {/* 今期KPI達成状況（今期モードのみ） */}
          {period === 'term' && (
            <div style={{ ...cardStyle, padding:'18px 16px' }}>
              {sectionHead('今期 KPI達成状況', `${fmtDate(rangeStart)} 〜 ${fmtDate(rangeEnd)}`)}
              <div style={{ display:'flex', alignItems:'flex-end', justifyContent:'space-between', marginBottom:14 }}>
                <div>
                  <div style={{ fontSize:'0.65rem', color:C.textSub, marginBottom:2 }}>今期インセン合計</div>
                  <div style={{ fontSize:'1.5rem', fontWeight:800, color:C.text, lineHeight:1.1 }}>
                    {fmtM(curr.incentiveAmount || 0)}
                    <span style={{ fontSize:'0.78rem', color:C.textSub, marginLeft:4 }}>/ {fmtM(termKpiTarget)}</span>
                  </div>
                  <div style={{ fontSize:'0.65rem', color:C.textSub, marginTop:2 }}>
                    入金額 {fmtM(curr.paymentAmount)}
                  </div>
                </div>
                <div style={{ textAlign:'right' }}>
                  <div style={{ fontSize:'2rem', fontWeight:900, lineHeight:1,
                    color: termAchieve == null ? '#94a3b8' : termAchieve >= 100 ? '#059669' : termAchieve >= 70 ? '#d97706' : '#dc2626' }}>
                    {termAchieve != null ? `${termAchieve}%` : '—'}
                  </div>
                  <div style={{ fontSize:'0.68rem', color:C.textSub, marginTop:2 }}>達成率</div>
                </div>
              </div>
              {/* 達成プログレスバー */}
              <div style={{ height:12, background:C.surface2, borderRadius:6, overflow:'hidden', marginBottom:10, position:'relative' }}>
                <div style={{ height:'100%', width:`${Math.min(100, termAchieve || 0)}%`, borderRadius:6, transition:'width 0.6s ease',
                  background: termAchieve == null ? '#e2e8f0' : termAchieve >= 100 ? '#059669' : termAchieve >= 70 ? '#f59e0b' : '#ef4444' }} />
              </div>
              {/* 前期比 */}
              {prev && (
                <div style={{ display:'flex', alignItems:'center', gap:6, padding:'8px 12px', background:C.surface2, borderRadius:8 }}>
                  <span style={{ fontSize:'0.72rem', color:C.textSub }}>前期入金</span>
                  <span style={{ fontSize:'0.82rem', fontWeight:700, color:C.textMid }}>{fmtM(prev.paymentAmount)}</span>
                  {prev.paymentAmount > 0 && (
                    <span style={{ fontSize:'0.72rem', fontWeight:700, marginLeft:4,
                      color: curr.paymentAmount >= prev.paymentAmount ? '#059669' : '#dc2626' }}>
                      {curr.paymentAmount >= prev.paymentAmount ? '▲' : '▼'}
                      {Math.abs(Math.round((curr.paymentAmount - prev.paymentAmount) / prev.paymentAmount * 100))}%
                    </span>
                  )}
                  {prevTermAchieve != null && (
                    <span style={{ fontSize:'0.68rem', color:C.textSub, marginLeft:'auto' }}>
                      前期達成率 {prevTermAchieve}%
                    </span>
                  )}
                </div>
              )}
            </div>
          )}

          {/* 収支見込み（指定月モードのみ） */}
          {period !== 'term' && forecast && (
            <div style={{ ...cardStyle, padding:'14px 16px' }}>
              {sectionHead('収支見込み', '対象月')}
              <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:12 }}>
                <div>
                  <div style={{ fontSize:'0.65rem', color:C.textSub, marginBottom:2 }}>見込み合計</div>
                  <div style={{ fontSize:'1.45rem', fontWeight:800, color:C.text }}>
                    {fmtM(forecastDispTotal)}<span style={{ fontSize:'0.8rem' }}>円</span>
                  </div>
                  <div style={{ fontSize:'0.62rem', color:C.textSub, marginTop:1 }}>
                    KPI見込み（インセン） {fmtM(forecastTotal)}
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
                  { label:'入金確定',            amount:forecast.confirmed,  sub:`インセン ${fmtM(forecast.confirmedIncentive)}`,       color:'#059669', count:summary?.payments?.length },
                  { label:'締結ほぼ確実 [A/S]',  amount:forecast.high,       sub:`インセン見込 ${fmtM(forecast.highKpi)}`,             color:'#1e40af', count:summary?.highDeals?.length },
                  { label:'締結多分いける [B/C]', amount:forecast.medium,     sub:`インセン見込 ${fmtM(forecast.mediumKpi)}`,           color:'#d97706', count:summary?.mediumDeals?.length },
                ].map(item => {
                  const pct = forecastDispTotal > 0 ? Math.round((item.amount / forecastDispTotal) * 100) : 0;
                  return (
                    <div key={item.label}>
                      <div style={{ display:'flex', justifyContent:'space-between', marginBottom:3 }}>
                        <div style={{ display:'flex', alignItems:'center', gap:5 }}>
                          <span style={{ width:10, height:10, borderRadius:3, background:item.color, display:'inline-block' }} />
                          <div>
                            <span style={{ fontSize:'0.7rem', color:C.textMid }}>{item.label}</span>
                            {item.sub && <div style={{ fontSize:'0.6rem', color:C.textSub }}>{item.sub}</div>}
                          </div>
                        </div>
                        <div style={{ textAlign:'right' }}>
                          <span style={{ fontSize:'0.78rem', fontWeight:700, color:item.color }}>{fmtM(item.amount)}</span>
                          {item.count != null && <span style={{ fontSize:'0.65rem', color:C.textSub, marginLeft:5 }}>{item.count}件</span>}
                        </div>
                      </div>
                      <div style={{ height:8, background:C.surface2, borderRadius:4, overflow:'hidden' }}>
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
            {sectionHead('入金推移（実入金額）', period === 'term' ? '今期 月別' : '過去6ヶ月　単位: 万円')}
            <ResponsiveContainer width="100%" height={period === 'term' ? 160 : 130}>
              <BarChart data={trendData} margin={{ top:8, right:0, left:0, bottom:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                <XAxis dataKey="month" tick={{ fontSize:9, fill:'#94a3b8' }} axisLine={false} tickLine={false} />
                <YAxis tickFormatter={v => `${Math.round(v / 1e4)}万`} tick={{ fontSize:9, fill:'#94a3b8' }} axisLine={false} tickLine={false} width={42} />
                <Tooltip formatter={v => [`${fmtM(v)}`, '実入金額']}
                  contentStyle={{ background:C.surface, border:`1px solid ${C.border}`, borderRadius:8, fontSize:11, boxShadow:'0 4px 12px rgba(0,0,0,0.1)' }} />
                <Bar dataKey="amount" radius={[3, 3, 0, 0]}>
                  <LabelList dataKey="amount" position="top"
                    formatter={v => v > 0 ? `${Math.round(v / 1e4)}万` : ''}
                    style={{ fontSize:8, fill:'#94a3b8' }} />
                  {trendData.map((_, i) => <Cell key={i} fill={i === trendData.length - 1 ? '#1e40af' : '#bfdbfe'} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* プラン割合（ドーナツグラフ） */}
          {planData.length > 0 && (() => {
            const totalCnt = planData.reduce((s, r) => s + r.cnt, 0);
            const pieData  = planMode === 'count'
              ? planData.map(p => ({ ...p, pieValue: p.cnt }))
              : planData.map(p => ({ ...p, pieValue: p.amount }));
            const pieTotal = planMode === 'count' ? totalCnt : planTotal;
            return (
              <div style={{ ...cardStyle, padding:'14px 16px' }}>
                <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:10 }}>
                  <div style={{ display:'flex', alignItems:'center', gap:6 }}>
                    <span style={{ width:8, height:8, borderRadius:'50%', background:'#6366f1', display:'inline-block' }} />
                    <span style={{ fontWeight:700, fontSize:'0.85rem', color:C.text }}>受注プラン割合</span>
                  </div>
                  <div style={{ display:'flex', background:C.surface2, borderRadius:6, padding:2, gap:1 }}>
                    {[['amount','金額'], ['count','件数']].map(([v, l]) => (
                      <button key={v} onClick={() => setPlanMode(v)}
                        style={{ padding:'2px 10px', borderRadius:5, border:'none', cursor:'pointer', fontSize:'0.68rem', fontWeight: planMode===v ? 700 : 400,
                          background: planMode===v ? '#fff' : 'transparent', color: planMode===v ? '#1e40af' : '#64748b', boxShadow: planMode===v ? '0 1px 3px rgba(0,0,0,0.1)' : 'none' }}>
                        {l}
                      </button>
                    ))}
                  </div>
                </div>
                <div style={{ display:'flex', alignItems:'center', gap:12 }}>
                  <div style={{ width:140, height:140, flexShrink:0 }}>
                    <PieChart width={140} height={140}>
                      <Pie data={pieData} dataKey="pieValue" nameKey="plan"
                        cx="50%" cy="50%" outerRadius={62} innerRadius={36} paddingAngle={2}>
                        {pieData.map((_, i) => <Cell key={i} fill={PLAN_COLORS[i % PLAN_COLORS.length]} />)}
                      </Pie>
                      <Tooltip formatter={(v, name) => [planMode === 'count' ? `${v}件` : `${fmtM(v)}`, name]}
                        contentStyle={{ background:C.surface, border:`1px solid ${C.border}`, borderRadius:8, fontSize:11 }} />
                    </PieChart>
                  </div>
                  <div style={{ flex:1, display:'flex', flexDirection:'column', gap:5 }}>
                    {pieData.map((p, i) => {
                      const exact = pieTotal > 0 ? p.pieValue / pieTotal * 100 : 0;
                      const pctLabel = exact <= 0 ? '0%' : exact < 1 ? '<1%' : `${Math.floor(exact)}%`;
                      return (
                        <div key={p.plan} style={{ display:'flex', alignItems:'center', gap:5 }}>
                          <span style={{ width:8, height:8, borderRadius:2, background:PLAN_COLORS[i % PLAN_COLORS.length], flexShrink:0 }} />
                          <span style={{ fontSize:'0.68rem', color:C.textMid, flex:1, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{p.plan}</span>
                          <span style={{ fontSize:'0.65rem', color:C.textSub, flexShrink:0 }}>{p.cnt}件</span>
                          <span style={{ fontSize:'0.65rem', color:C.textSub, flexShrink:0, width:46, textAlign:'right' }}>{fmtM(p.amount)}</span>
                          <span style={{ fontSize:'0.72rem', fontWeight:700, color:PLAN_COLORS[i % PLAN_COLORS.length], flexShrink:0, width:34, textAlign:'right' }}>{pctLabel}</span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            );
          })()}

        </div>
      </div>

      {drill && (
        <Drilldown rep={drill.rep} type={drill.type} start={rangeStart} end={rangeEnd} onClose={() => setDrill(null)} />
      )}

      {/* KPIカード詳細モーダル */}
      {kpiDrill && (
        <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.45)', zIndex:800, display:'flex', alignItems:'flex-start', justifyContent:'flex-end', padding:'56px 24px 20px' }}
          onClick={() => setKpiDrill(null)}>
          <div style={{ background:C.surface, borderRadius:14, width:'min(520px,90vw)', maxHeight:'80vh', display:'flex', flexDirection:'column', boxShadow:'0 20px 60px rgba(0,0,0,0.2)' }}
            onClick={e => e.stopPropagation()}>
            <div style={{ padding:'14px 20px', borderBottom:`1px solid ${C.border}`, display:'flex', justifyContent:'space-between', alignItems:'center', flexShrink:0 }}>
              <span style={{ fontWeight:700, fontSize:'0.95rem', color:C.text }}>{kpiDrill.title}</span>
              <button onClick={() => setKpiDrill(null)} style={{ background:C.surface2, border:'none', borderRadius:8, width:28, height:28, cursor:'pointer', color:C.textSub, fontSize:16 }}>×</button>
            </div>
            <div style={{ overflowY:'auto', padding:'12px 16px', display:'flex', flexDirection:'column', gap:6 }}>
              {/* 入金額詳細 */}
              {kpiDrill.type === 'payment' && (summary?.payments || []).length === 0 && (
                <div style={{ textAlign:'center', color:C.textSub, padding:'24px 0', fontSize:'0.85rem' }}>データなし</div>
              )}
              {kpiDrill.type === 'payment' && (summary?.payments || []).map((r, i) => (
                <div key={i} style={{ display:'flex', alignItems:'center', gap:10, padding:'8px 10px', background:C.surface2, borderRadius:8 }}>
                  <span style={{ fontSize:'0.72rem', color:C.textSub, flexShrink:0, minWidth:60 }}>{fmtDate(r.payment_date)}</span>
                  <span style={{ flex:1, fontSize:'0.82rem', fontWeight:500, color:C.text, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{r.company}</span>
                  <span style={{ fontSize:'0.72rem', color:C.textSub, flexShrink:0 }}>{r.plan || '—'}</span>
                  <span style={{ fontSize:'0.82rem', fontWeight:700, color:'#059669', flexShrink:0 }}>{fmtMYen(r.payment_amount ?? r.amount)}</span>
                </div>
              ))}
              {/* 受注・初回商談は担当者テーブルへ誘導 */}
              {(kpiDrill.type === 'won' || kpiDrill.type === 'meeting') && (
                <div style={{ textAlign:'center', padding:'20px 0' }}>
                  <div style={{ color:C.textSub, fontSize:'0.82rem', marginBottom:12 }}>
                    詳細は担当者別実績テーブルをご確認ください
                  </div>
                  <div style={{ display:'flex', flexDirection:'column', gap:6 }}>
                    {reps.filter(r => !r.isGrouped).map(r => (
                      <div key={r.rep} style={{ display:'flex', alignItems:'center', gap:10, padding:'8px 12px', background:C.surface2, borderRadius:8 }}>
                        <span style={{ flex:1, fontSize:'0.82rem', fontWeight:500, color:C.text }}>{r.rep}</span>
                        <span style={{ fontSize:'0.82rem', color:C.text, fontWeight:700 }}>
                          {kpiDrill.type === 'won' ? `${r.wonCount}件` : `${r.meetingCount}件`}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
            {kpiDrill.type === 'payment' && (summary?.payments || []).length > 0 && (
              <div style={{ padding:'10px 16px', borderTop:`1px solid ${C.border}`, display:'flex', justifyContent:'flex-end', alignItems:'center', gap:8, flexShrink:0 }}>
                <span style={{ fontSize:'0.72rem', color:C.textSub }}>合計</span>
                <span style={{ fontSize:'0.95rem', fontWeight:800, color:'#059669' }}>
                  {fmtMYen((summary?.payments || []).reduce((s, r) => s + Number(r.payment_amount ?? r.amount ?? 0), 0))}
                </span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
