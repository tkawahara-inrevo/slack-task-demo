import { useState, useEffect, useCallback } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Cell, LabelList, ReferenceLine,
  PieChart, Pie, Legend, Legend as RechartLegend,
} from 'recharts';
import { useEffect as useStorageEffect } from 'react';
import { api } from '../../api/client';

const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316','#84cc16','#ec4899','#6366f1'];

function TargetModal({ targets, onSave, onClose }) {
  const curYear = new Date().getFullYear();
  const [year, setYear] = useState(curYear);
  const [vals, setVals] = useState({});

  useEffect(() => {
    const v = {};
    for (let m = 1; m <= 12; m++) {
      const k = `${year}-${String(m).padStart(2,'0')}`;
      v[m] = targets[k] || '';
    }
    setVals(v);
  }, [year, targets]);

  const handleSave = () => {
    const next = { ...targets };
    for (let m = 1; m <= 12; m++) {
      const k = `${year}-${String(m).padStart(2,'0')}`;
      const n = Number(vals[m]);
      if (n > 0) next[k] = n; else delete next[k];
    }
    onSave(next);
    onClose();
  };

  return (
    <>
      <div onClick={onClose} style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.4)', zIndex:2000 }} />
      <div style={{ position:'fixed', top:'50%', left:'50%', transform:'translate(-50%,-50%)', background:'#fff', borderRadius:14, boxShadow:'0 8px 40px rgba(0,0,0,0.18)', zIndex:2001, width:420, maxWidth:'94vw', overflow:'hidden' }}>
        <div style={{ padding:'16px 20px', borderBottom:'1px solid #e5e7eb', display:'flex', alignItems:'center', justifyContent:'space-between' }}>
          <div style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a' }}>月次目標設定</div>
          <div style={{ display:'flex', alignItems:'center', gap:8 }}>
            <select value={year} onChange={e => setYear(Number(e.target.value))}
              style={{ border:'1px solid #d1d5db', borderRadius:6, padding:'4px 8px', fontSize:'0.85rem' }}>
              {[curYear-1, curYear, curYear+1].map(y => <option key={y} value={y}>{y}年</option>)}
            </select>
            <button onClick={onClose} style={{ background:'none', border:'none', fontSize:'1.2rem', cursor:'pointer', color:'#9ca3af' }}>✕</button>
          </div>
        </div>
        <div style={{ padding:'16px 20px' }}>
          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:10 }}>
            {Array.from({length:12}, (_,i) => i+1).map(m => (
              <div key={m}>
                <div style={{ fontSize:'0.72rem', color:'#6b7280', fontWeight:600, marginBottom:4 }}>{m}月</div>
                <div style={{ display:'flex', alignItems:'center', gap:4 }}>
                  <input type="number" value={vals[m]||''} onChange={e => setVals(p => ({...p, [m]: e.target.value}))}
                    placeholder="未設定"
                    style={{ width:'100%', border:'1px solid #d1d5db', borderRadius:6, padding:'5px 8px', fontSize:'0.85rem', outline:'none' }} />
                  <span style={{ fontSize:'0.72rem', color:'#9ca3af', flexShrink:0 }}>件</span>
                </div>
              </div>
            ))}
          </div>
        </div>
        <div style={{ padding:'12px 20px', borderTop:'1px solid #e5e7eb', display:'flex', justifyContent:'flex-end', gap:8 }}>
          <button onClick={onClose} style={{ padding:'7px 16px', border:'1px solid #d1d5db', borderRadius:8, background:'#fff', color:'#6b7280', fontSize:'0.85rem', cursor:'pointer' }}>キャンセル</button>
          <button onClick={handleSave} style={{ padding:'7px 20px', border:'none', borderRadius:8, background:'#1e293b', color:'#fff', fontSize:'0.85rem', fontWeight:700, cursor:'pointer' }}>保存</button>
        </div>
      </div>
    </>
  );
}
const fmtM = n => { if (!n) return '—'; const m = Math.round(Number(n)); return m >= 10000 ? `${Math.round(m/10000).toLocaleString()}万` : m.toLocaleString(); };
const TICK = { fontSize: 11, fill: '#6b7280' };

const YOMI_COLOR = {
  'アポ化前':  '#94a3b8',
  'アポ取得済': '#3b82f6',
  '商談中':    '#f59e0b',
  '受注':      '#10b981',
};

// チャンネル管理テーブルの行定義（セクション区切り付き）
const CHANNEL_ROWS = [
  { key: '_sec_cost',          label: 'コスト',              isSection: true  },
  { key: '_expected_appo_cpa', label: '想定アポCPA',          editable: false  },
  { key: 'lead_unit_price',    label: 'リード獲得単価',        editable: true,  type: 'money' },
  { key: '_cost_per_month',    label: 'コスト/月',             editable: false  },
  { key: 'vendor_note',        label: 'ベンダーより',           editable: true,  type: 'text'  },
  { key: '_sec_lead',          label: 'リード',               isSection: true  },
  { key: 'expected_leads',     label: '想定獲得リード',         editable: true,  type: 'num'   },
  { key: 'actual_leads',       label: '獲得リード',             editable: false, isActual: true },
  { key: '_leads_progress',    label: '進捗率',                 editable: false  },
  { key: '_sec_appo',          label: 'アポ',                 isSection: true  },
  { key: 'expected_appo_count',label: '想定獲得アポ数',        editable: true,  type: 'num'   },
  { key: 'actual_appo',        label: '初回商談数',             editable: false, isActual: true },
  { key: 'expected_appo_rate', label: '想定アポ割合',           editable: true,  type: 'pct'   },
  { key: '_actual_appo_rate',  label: 'アポ割合',               editable: false  },
  { key: '_sec_order',         label: '受注',                 isSection: true  },
  { key: 'expected_order_rate',label: '想定受注率',             editable: true,  type: 'pct'   },
  { key: '_expected_orders',   label: '想定受注数',             editable: false  },
  { key: 'expected_unit_price',label: '想定受注単価',           editable: true,  type: 'money' },
  { key: 'actual_orders',      label: '受注数',                 editable: false, isActual: true },
  { key: 'actual_revenue',     label: '受注金額',               editable: false, isActual: true },
  { key: '_sec_result',        label: '成果',                 isSection: true  },
  { key: '_expected_revenue',  label: '想定売上',               editable: false, yellow: true  },
  { key: '_expected_roi',      label: '想定ROI',               editable: false, yellow: true  },
  { key: '_roi',               label: 'ROI',                   editable: false  },
  { key: '_appo_diff',         label: '初回商談 vs 想定アポ',   editable: false  },
];

const pad = n => String(n).padStart(2, '0');
const ymd = (d) => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
const endOfMonth = (y, m) => ymd(new Date(y, m, 0)); // month is 1-based

function getPreset(key) {
  const now = new Date();
  const y = now.getFullYear(), m = now.getMonth() + 1;
  switch (key) {
    case 'this_month':   return { from: `${y}-${pad(m)}-01`, to: endOfMonth(y, m) };
    case 'last_month': {
      const ly = m === 1 ? y-1 : y, lm = m === 1 ? 12 : m-1;
      return { from: `${ly}-${pad(lm)}-01`, to: endOfMonth(ly, lm) };
    }
    case 'last3': {
      const d3 = new Date(now); d3.setMonth(d3.getMonth()-3);
      return { from: ymd(d3), to: ymd(now) };
    }
    case 'this_year': return { from: `${y}-04-01`, to: `${y+1}-03-31` };
    default: return { from: '', to: '' };
  }
}

export default function LeadsDashboard() {
  const [activeTab, setActiveTab]   = useState('dashboard'); // 'dashboard' | 'channels'
  const [data, setData]         = useState(null);
  const [loading, setLoading]   = useState(false);
  const [from, setFrom]         = useState('');
  const [to, setTo]             = useState('');
  const [activePreset, setActivePreset] = useState('this_month');
  const [page, setPage]         = useState(1);
  const [listData, setListData] = useState(null);
  const [listLoading, setListLoading] = useState(false);
  const [drill, setDrill]       = useState(null);
  const [drillLoading, setDrillLoading] = useState(false);
  const [targets, setTargets]      = useState(() => { try { return JSON.parse(localStorage.getItem('lead_month_targets') || '{}'); } catch { return {}; } });
  const [filterOpen, setFilterOpen] = useState(false);
  const [repFilter, setRepFilter]   = useState('');
  const [srcFilter, setSrcFilter]   = useState('');
  const [appoOnly, setAppoOnly]     = useState(false);
  const [showTargetModal, setShowTargetModal] = useState(false);
  const monthKey = from ? from.slice(0, 7) : new Date().toISOString().slice(0, 7);
  const target = targets[monthKey] || 0;
  const saveTargets = (next) => {
    setTargets(next);
    localStorage.setItem('lead_month_targets', JSON.stringify(next));
  };

  const load = useCallback(async (f, t, rep, src, appo) => {
    setLoading(true);
    try {
      const d = await api.crmLeadsDashboard({ from: f, to: t, page: 1, rep: rep||'', source_filter: src||'', appo_only: appo||'' });
      setData(d);
      setListData({ recent: d.recent, pagination: d.pagination });
      setPage(1);
    } catch (e) { console.error(e); }
    setLoading(false);
  }, []);

  const loadPage = useCallback(async (f, t, p) => {
    setListLoading(true);
    try {
      const d = await api.crmLeadsList({ from: f, to: t, page: p });
      setListData(d);
      setPage(p);
    } catch (e) { console.error(e); }
    setListLoading(false);
  }, []);

  // 初期表示: 今月
  useEffect(() => {
    const { from: f, to: t } = getPreset('this_month');
    setFrom(f); setTo(t);
    load(f, t, '', '', false);
  }, []);

  const applyPreset = (key) => {
    const { from: f, to: t } = getPreset(key);
    setFrom(f); setTo(t); setActivePreset(key);
    if (activeTab === 'channels') { loadChannels(f, t); }
    else { load(f, t, repFilter, srcFilter, appoOnly); }
  };

  const handleSearch = () => {
    setActivePreset(null);
    if (activeTab === 'channels') { loadChannels(from, to); }
    else { load(from, to, repFilter, srcFilter, appoOnly); }
  };
  const handleReset  = () => { setFrom(''); setTo(''); setRepFilter(''); setSrcFilter(''); setAppoOnly(false); setActivePreset(null); load('', '', '', '', false); };
  const applyFilters = () => load(from, to, repFilter, srcFilter, appoOnly);

  const openDrill = async (source, drillType) => {
    setDrill({ source, drillType, rows: null });
    setDrillLoading(true);
    try {
      const d = await api.crmLeadsDrilldown(source, from, to, drillType);
      setDrill({ source, drillType, rows: d.drilldown || [] });
    } catch { setDrill({ source, drillType, rows: [] }); }
    setDrillLoading(false);
  };

  const openYomiDrill = async (yomiType, label) => {
    setDrill({ source: label, yomiType, rows: null });
    setDrillLoading(true);
    try {
      const d = await api.crmLeadsYomiDrill(yomiType, from, to);
      setDrill({ source: label, yomiType, rows: d.drilldown || [] });
    } catch { setDrill({ source: label, yomiType, rows: [] }); }
    setDrillLoading(false);
  };

  // チャンネル管理
  const [chData, setChData]         = useState(null);
  const [chLoading, setChLoading]   = useState(false);
  const [chEdits, setChEdits]       = useState({}); // {source: {field: value}}
  const [chSaving, setChSaving]     = useState({}); // {source: bool}

  const loadChannels = useCallback(async (f, t) => {
    setChLoading(true);
    try {
      const d = await api.crmChannelPerformance(f, t);
      setChData(d);
    } catch (e) { console.error(e); }
    setChLoading(false);
  }, []);

  const handleTabChange = (tab) => {
    setActiveTab(tab);
    if (tab === 'channels' && !chData) loadChannels(from, to);
  };

  const saveChannelTarget = async (source) => {
    const edits = chEdits[source] || {};
    if (!Object.keys(edits).length) return;
    setChSaving(p => ({ ...p, [source]: true }));
    const row = chData?.rows?.find(r => r.source === source) || {};
    const vn = (k) => Number(edits[k] !== undefined ? edits[k] : (row[k] || 0));
    const vt = (k) => String(edits[k] !== undefined ? edits[k] : (row[k] || ''));
    const lead_unit_price    = vn('lead_unit_price');
    const expected_appo_count = vn('expected_appo_count');
    const body = {
      source,
      lead_unit_price,
      vendor_note:         vt('vendor_note'),
      expected_leads:      vn('expected_leads'),
      expected_appo_count,
      expected_appo_rate:  vn('expected_appo_rate'),
      expected_order_rate: vn('expected_order_rate'),
      expected_unit_price: vn('expected_unit_price'),
    };
    try {
      await api.crmChannelTargetUpdate(body);
      setChData(p => p ? {
        ...p,
        rows: p.rows.map(r => r.source === source ? {
          ...r, ...body,
          cost_per_month: lead_unit_price * expected_appo_count,
        } : r),
      } : p);
      setChEdits(p => { const n = { ...p }; delete n[source]; return n; });
    } catch (e) { console.error(e); }
    setChSaving(p => { const n = { ...p }; delete n[source]; return n; });
  };

  const setChEdit = (source, field, value) =>
    setChEdits(p => ({ ...p, [source]: { ...(p[source]||{}), [field]: value } }));

  // 当月の経過日数ベースの予測計算
  const calcProjection = () => {
    const now = new Date();
    const isThisMonth = from && new Date(from).getMonth() === now.getMonth() && new Date(from).getFullYear() === now.getFullYear();
    if (!isThisMonth) return null;
    const day = now.getDate();
    const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    const ratio = day / daysInMonth;
    return { day, daysInMonth, ratio };
  };

  const presets = [
    { key: 'this_month', label: '今月' },
    { key: 'last_month', label: '先月' },
    { key: 'last3',      label: '直近3ヶ月' },
    { key: 'this_year',  label: '今年度' },
  ];

  const total = data?.funnel?.reduce((s, r) => s + r.cnt, 0) || 0;

  return (
    <div style={{ padding: '0 0 32px' }}>
      <div style={{ marginBottom: 16, display:'flex', alignItems:'center', gap:16 }}>
        <h2 style={{ margin: 0, fontSize: '1.1rem', fontWeight: 800, color: '#111827' }}>リード管理</h2>
        <div style={{ display:'flex', gap:4 }}>
          {[{key:'dashboard',label:'ダッシュボード'},{key:'channels',label:'流入経路管理'}].map(t => (
            <button key={t.key} onClick={() => handleTabChange(t.key)} style={{
              padding:'4px 14px', borderRadius:20, fontSize:'0.8rem', fontWeight:600, cursor:'pointer', border:'none',
              background: activeTab===t.key ? '#1e293b' : '#f1f5f9',
              color: activeTab===t.key ? '#fff' : '#374151',
            }}>{t.label}</button>
          ))}
        </div>
        {activeTab==='dashboard' && data?.period && (
          <p style={{ margin: 0, fontSize: '0.8rem', color: '#6b7280' }}>
            {(() => {
              const f = data.period.from, t = data.period.to;
              if (!f || !t) return null;
              const fd = new Date(f), td = new Date(t);
              const fmt = d => `${d.getFullYear()}/${d.getMonth()+1}/${d.getDate()}`;
              if (fd.getFullYear() === td.getFullYear() && fd.getMonth() === td.getMonth())
                return `${fd.getFullYear()}年${fd.getMonth()+1}月`;
              return `${fmt(fd)} 〜 ${fmt(td)}`;
            })()}
          </p>
        )}
      </div>

      {/* 期間フィルター */}
      <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '12px 16px', marginBottom: 20 }}>
        {/* クイック選択 */}
        <div style={{ display: 'flex', gap: 6, marginBottom: 10, flexWrap: 'wrap' }}>
          {presets.map(p => (
            <button key={p.key} onClick={() => applyPreset(p.key)} style={{
              padding: '4px 12px', borderRadius: 20, fontSize: '0.8rem', fontWeight: 600, cursor: 'pointer',
              background: activePreset === p.key ? '#3b82f6' : '#f1f5f9',
              color: activePreset === p.key ? '#fff' : '#374151',
              border: 'none',
            }}>{p.label}</button>
          ))}
        </div>
        {/* カスタム期間 */}
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <span style={{ fontSize: '0.8rem', color: '#6b7280' }}>カスタム</span>
          <input type="date" value={from} onChange={e => { setFrom(e.target.value); setActivePreset(null); }}
            style={{ border: '1px solid #d1d5db', borderRadius: 6, padding: '4px 8px', fontSize: '0.82rem' }} />
          <span style={{ color: '#9ca3af' }}>〜</span>
          <input type="date" value={to} onChange={e => { setTo(e.target.value); setActivePreset(null); }}
            style={{ border: '1px solid #d1d5db', borderRadius: 6, padding: '4px 8px', fontSize: '0.82rem' }} />
          <button onClick={handleSearch}
            style={{ background: '#3b82f6', color: '#fff', border: 'none', borderRadius: 6, padding: '5px 14px', fontSize: '0.82rem', cursor: 'pointer', fontWeight: 600 }}>
            適用
          </button>
          {(from || to || activePreset) && (
            <button onClick={handleReset}
              style={{ background: 'none', border: '1px solid #d1d5db', borderRadius: 6, padding: '4px 10px', fontSize: '0.8rem', cursor: 'pointer', color: '#6b7280' }}>
              リセット
            </button>
          )}
        </div>
      </div>

      {/* 絞り込みフィルター（ダッシュボードタブのみ） */}
      {activeTab === 'dashboard' && <div style={{ marginBottom:16 }}>
        <button onClick={() => setFilterOpen(v => !v)}
          style={{ background: (repFilter||srcFilter||appoOnly)?'#eff6ff':'none', border:'1px solid '+(repFilter||srcFilter||appoOnly?'#3b82f6':'#e2e8f0'), borderRadius:8, padding:'5px 14px', fontSize:'0.8rem', cursor:'pointer', color: (repFilter||srcFilter||appoOnly)?'#1d4ed8':'#374151', fontWeight:600 }}>
          絞り込み {(repFilter||srcFilter||appoOnly) ? '●' : ''} {filterOpen ? '▲' : '▼'}
        </button>
        {filterOpen && (
          <div style={{ marginTop:10, background:'#fff', border:'1px solid #e2e8f0', borderRadius:10, padding:'14px 16px', display:'flex', gap:14, flexWrap:'wrap', alignItems:'flex-end' }}>
            <div>
              <div style={{ fontSize:'0.72rem', color:'#6b7280', fontWeight:600, marginBottom:4 }}>担当者</div>
              <select value={repFilter} onChange={e => setRepFilter(e.target.value)}
                style={{ border:'1px solid #d1d5db', borderRadius:6, padding:'5px 10px', fontSize:'0.82rem', minWidth:140 }}>
                <option value="">すべて</option>
                {(data?.byRep||[]).map(r => <option key={r.rep} value={r.rep}>{r.rep}</option>)}
              </select>
            </div>
            <div>
              <div style={{ fontSize:'0.72rem', color:'#6b7280', fontWeight:600, marginBottom:4 }}>流入経路</div>
              <select value={srcFilter} onChange={e => setSrcFilter(e.target.value)}
                style={{ border:'1px solid #d1d5db', borderRadius:6, padding:'5px 10px', fontSize:'0.82rem', minWidth:140 }}>
                <option value="">すべて</option>
                {(data?.bySource||[]).slice(0,30).map(r => <option key={r.source} value={r.source}>{r.source}</option>)}
              </select>
            </div>
            <label style={{ display:'flex', alignItems:'center', gap:6, cursor:'pointer', fontSize:'0.82rem', color:'#374151' }}>
              <input type="checkbox" checked={appoOnly} onChange={e => setAppoOnly(e.target.checked)} />
              アポ化済みのみ
            </label>
            <button onClick={applyFilters}
              style={{ background:'#3b82f6', color:'#fff', border:'none', borderRadius:7, padding:'6px 16px', fontSize:'0.82rem', fontWeight:600, cursor:'pointer' }}>
              適用
            </button>
            {(repFilter||srcFilter||appoOnly) && (
              <button onClick={() => { setRepFilter(''); setSrcFilter(''); setAppoOnly(false); load(from, to, '', '', false); }}
                style={{ background:'none', border:'1px solid #d1d5db', borderRadius:7, padding:'5px 12px', fontSize:'0.8rem', cursor:'pointer', color:'#6b7280' }}>
                リセット
              </button>
            )}
          </div>
        )}
      </div>}

      {activeTab === 'dashboard' && loading && <div style={{ color: '#9ca3af', marginBottom: 16 }}>読み込み中...</div>}

      {activeTab === 'dashboard' && data && !loading && (
        <>
          {/* 目標設定 */}
          <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:16, flexWrap:'wrap' }}>
            <span style={{ fontSize:'0.8rem', fontWeight:700, color:'#374151' }}>月次目標</span>
            <span style={{ fontSize:'0.72rem', color:'#9ca3af' }}>{monthKey}</span>
            <span style={{ fontSize:'0.88rem', fontWeight:700, color:'#0f172a' }}>{target > 0 ? `${target.toLocaleString()}件` : '未設定'}</span>
            <button onClick={() => setShowTargetModal(true)}
              style={{ background:'none', border:'1px solid #e2e8f0', borderRadius:6, padding:'3px 12px', fontSize:'0.75rem', cursor:'pointer', color:'#6b7280' }}>
              目標を設定
            </button>
          </div>

          {/* 目標設定モーダル */}
          {showTargetModal && <TargetModal targets={targets} onSave={saveTargets} onClose={() => setShowTargetModal(false)} />}

          {/* ファネル KPI */}
          {(() => {
            const proj = calcProjection();
            const projected = proj ? Math.round(data.periodTotal / proj.ratio) : null;
            const dayAdjAvg = proj ? Math.round(data.stats.avg12 * proj.ratio) : null;
            const vsDayAvg = dayAdjAvg > 0 ? Math.round((data.periodTotal - dayAdjAvg) / dayAdjAvg * 100) : null;
            const achievement = target > 0 ? Math.round(data.periodTotal / target * 100) : null;
            const yomiMap = { 'アポ化前':'apo_before','アポ化済み':'appo_active','失注':'lost_with_appo','見送り':'miokuri','受注':'ordered' };
            return (
              <>
              <div style={{ display:'flex', gap:12, flexWrap:'wrap', marginBottom: data.byLostReason?.length > 0 ? 6 : 20 }}>
                {/* 期間内合計 */}
                <div style={{ background:'#fff', border:'1px solid #e5e7eb', borderRadius:10, padding:'14px 18px', minWidth:160, flex:1, cursor:'pointer' }}
                  onClick={() => openYomiDrill('all', '全件')}
                  onMouseEnter={e=>e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.1)'}
                  onMouseLeave={e=>e.currentTarget.style.boxShadow='none'}>
                  <div style={{ fontSize:'0.72rem', color:'#6b7280', marginBottom:4 }}>期間内合計</div>
                  <div style={{ fontSize:'2rem', fontWeight:800, color:'#0f172a' }}>{data.periodTotal.toLocaleString()}</div>
                  {proj && <div style={{ fontSize:'0.72rem', color:'#94a3b8', marginTop:2 }}>{proj.day}日経過 / 月末予測 {projected}件</div>}
                  {vsDayAvg !== null && (
                    <div style={{ fontSize:'0.75rem', fontWeight:600, color: vsDayAvg >= 0 ? '#16a34a' : '#dc2626', marginTop:2 }}>
                      日割平均比 {vsDayAvg >= 0 ? '+' : ''}{vsDayAvg}%
                    </div>
                  )}
                  {achievement !== null && (
                    <div style={{ marginTop:6 }}>
                      <div style={{ display:'flex', justifyContent:'space-between', fontSize:'0.7rem', color:'#6b7280', marginBottom:2 }}>
                        <span>目標達成率 {achievement}%</span><span>目標{target}件</span>
                      </div>
                      <div style={{ height:5, background:'#f1f5f9', borderRadius:3 }}>
                        <div style={{ width:`${Math.min(100,achievement)}%`, height:'100%', background: achievement>=100?'#10b981':achievement>=70?'#f59e0b':'#3b82f6', borderRadius:3, transition:'width 0.4s' }} />
                      </div>
                    </div>
                  )}
                </div>

                {/* ファネルカード（5枚＋フロー未記入）*/}
                {data.funnel.map((f) => {
                  const rate = data.periodTotal > 0 ? Math.round(f.cnt / data.periodTotal * 100) : 0;
                  const colors = {
                    'アポ化前':  { bg:'#fff',     border:'1px solid #e5e7eb', text:'#94a3b8' },
                    'アポ化済み':{ bg:'#f0fdf4',  border:'2px solid #86efac', text:'#15803d' },
                    '失注':      { bg:'#fef2f2',  border:'2px solid #fca5a5', text:'#dc2626' },
                    '見送り':    { bg:'#fff7ed',  border:'1px solid #fed7aa', text:'#c2410c' },
                    '受注':      { bg:'#f0fdf4',  border:'1px solid #bbf7d0', text:'#059669' },
                  };
                  const c = colors[f.label] || { bg:'#fff', border:'1px solid #e5e7eb', text:'#374151' };
                  return (
                    <div key={f.label} style={{ background:c.bg, border:c.border, borderRadius:10, padding:'14px 18px', minWidth:110, flex:1, cursor:'pointer', transition:'box-shadow 0.15s' }}
                      onClick={() => openYomiDrill(f.key, f.label)}
                      title={f.desc}
                      onMouseEnter={e=>e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.1)'}
                      onMouseLeave={e=>e.currentTarget.style.boxShadow='none'}>
                      <div style={{ fontSize:'0.72rem', color:c.text, fontWeight:700, marginBottom:4 }}>{f.label}</div>
                      <div style={{ fontSize:'2rem', fontWeight:800, color:c.text }}>{f.cnt.toLocaleString()}</div>
                      <div style={{ fontSize:'0.72rem', color:'#9ca3af', marginTop:2 }}>{rate}%</div>
                      {f.label === 'アポ化済み' && proj && f.cnt > 0 && (
                        <div style={{ fontSize:'0.68rem', color:'#4ade80', marginTop:1 }}>月末予測 {Math.round(f.cnt/proj.ratio)}件</div>
                      )}
                    </div>
                  );
                })}
                {/* フロー未記入 */}
                {(data.noFlowTotal > 0) && (
                  <div style={{ background:'#f8fafc', border:'1px dashed #cbd5e1', borderRadius:10, padding:'14px 18px', minWidth:90, cursor:'pointer', transition:'box-shadow 0.15s' }}
                    onClick={() => openYomiDrill('no_flow', 'フロー未記入')}
                    title="ヨミ経過フロー未記入のため分類不明"
                    onMouseEnter={e=>e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.1)'}
                    onMouseLeave={e=>e.currentTarget.style.boxShadow='none'}>
                    <div style={{ fontSize:'0.68rem', color:'#94a3b8', fontWeight:600, marginBottom:4 }}>フロー未記入</div>
                    <div style={{ fontSize:'1.6rem', fontWeight:800, color:'#94a3b8' }}>{data.noFlowTotal}</div>
                  </div>
                )}
              </div>

              {/* 失注理由タグ（カード行の下に） */}
              {(data.byLostReason?.length > 0) && (
                <div style={{ display:'flex', gap:6, flexWrap:'wrap', marginBottom:8, alignItems:'center' }}>
                  <span style={{ fontSize:'0.72rem', color:'#dc2626', fontWeight:600 }}>失注理由:</span>
                  {data.byLostReason.map((r,i) => (
                    <div key={i} style={{ background:'#fef2f2', borderRadius:20, padding:'2px 9px', display:'flex', gap:5, alignItems:'center' }}>
                      <span style={{ fontSize:'0.73rem', color:'#dc2626', fontWeight:600 }}>{r.reason}</span>
                      <span style={{ fontSize:'0.68rem', color:'#9ca3af' }}>{r.cnt}</span>
                    </div>
                  ))}
                </div>
              )}
              </>
            );
          })()}

          {/* 月平均（全リード・アポ化済み） */}
          {data.stats && (
            <div style={{ display:'flex', gap:16, flexWrap:'wrap', marginBottom:20 }}>
              <div style={{ background:'#fff', border:'1px solid #e5e7eb', borderRadius:10, padding:'12px 18px' }}>
                <div style={{ fontSize:'0.72rem', color:'#6b7280', marginBottom:2 }}>月平均・全リード（直近12ヶ月）</div>
                <div style={{ fontSize:'1.4rem', fontWeight:800, color:'#374151' }}>{data.stats.avg12}件/月</div>
              </div>
              {data.avg12Appo > 0 && (
                <div style={{ background:'#f0fdf4', border:'1px solid #bbf7d0', borderRadius:10, padding:'12px 18px' }}>
                  <div style={{ fontSize:'0.72rem', color:'#15803d', marginBottom:2 }}>月平均・アポ化済み（直近12ヶ月）</div>
                  <div style={{ fontSize:'1.4rem', fontWeight:800, color:'#15803d' }}>{data.avg12Appo}件/月</div>
                </div>
              )}
            </div>
          )}

          {/* 月次推移（常に直近12ヶ月） */}
          <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 12px', marginBottom: 20 }}>
            <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 4, paddingLeft: 8 }}>月次リード推移（直近12ヶ月）</div>
            <div style={{ fontSize: '0.72rem', color: '#9ca3af', paddingLeft: 8, marginBottom: 10 }}>点線: 12ヶ月平均 {data.stats?.avg12}件</div>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={data.trend} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis dataKey="month" tick={TICK} />
                <YAxis tick={TICK} allowDecimals={false} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const sorted = [...payload].sort((a, b) => b.value - a.value);
                  return (
                    <div style={{ background:'#fff', border:'1px solid #e5e7eb', padding:'8px 12px', borderRadius:8, fontSize:'0.8rem' }}>
                      <div style={{ fontWeight:700, marginBottom:4, color:'#374151' }}>{label}</div>
                      {sorted.map((p, i) => <div key={i} style={{ color: p.color }}>{p.name}: {p.value.toLocaleString()}件</div>)}
                    </div>
                  );
                }} />
                {data.stats?.avg12 > 0 && (
                  <ReferenceLine y={data.stats.avg12} stroke="#f59e0b" strokeDasharray="4 3"
                    label={{ value: `平均 ${data.stats.avg12}`, fill: '#f59e0b', fontSize: 11, position: 'right' }} />
                )}
                <Line type="monotone" dataKey="appo" stroke="#10b981" strokeWidth={2} dot={{ r: 3 }} name="アポ化済" strokeDasharray="5 3" />
                <Line type="monotone" dataKey="cnt"  stroke="#3b82f6" strokeWidth={2} dot={{ r: 3 }} name="全リード" />
                <Legend wrapperStyle={{ fontSize:'0.72rem' }} />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* 流入経路別 総合テーブル（円グラフ廃止）*/}
          {(() => {
            const total = data.bySource.reduce((s, r) => s + r.cnt, 0);
            const srcData = data.bySource.map(r => ({
              ...r,
              pct:       total > 0 ? Math.round(r.cnt / total * 100) : 0,
              appo_rate: r.cnt > 0 ? Math.round((r.appo_cnt||0) / r.cnt * 100) : 0,
              order_rate:r.cnt > 0 ? Math.round((r.order_cnt||0) / r.cnt * 100) : 0,
              lost_rate: r.cnt > 0 ? Math.round((r.lost_cnt||0) / r.cnt * 100) : 0,
            }));
            return (
              <div style={{ background:'#fff', border:'1px solid #e5e7eb', borderRadius:10, overflow:'hidden', marginBottom:20 }}>
                <div style={{ padding:'12px 16px', borderBottom:'1px solid #f3f4f6', fontWeight:700, fontSize:'0.85rem' }}>
                  流入経路別 <span style={{ fontSize:'0.72rem', color:'#9ca3af', fontWeight:400 }}>クリックで詳細</span>
                </div>
                <div style={{ overflowY:'auto', maxHeight:360 }}>
                  <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.78rem' }}>
                    <thead>
                      <tr style={{ background:'#f8fafc', position:'sticky', top:0 }}>
                        {['経路','件数','割合','アポ化数','アポ化率','受注数','受注率','失注数','失注率'].map(h => (
                          <th key={h} style={{ padding:'7px 10px', textAlign:h==='経路'?'left':'right', fontWeight:600, color:'#64748b', fontSize:'0.68rem', borderBottom:'1px solid #e5e7eb', whiteSpace:'nowrap' }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {srcData.map((r, i) => (
                        <tr key={i} onClick={() => openDrill(r.source)}
                          style={{ borderBottom:'1px solid #f8fafc', cursor:'pointer', background:i%2===0?'#fff':'#fafafa' }}
                          onMouseEnter={e => e.currentTarget.style.background='#eff6ff'}
                          onMouseLeave={e => e.currentTarget.style.background=i%2===0?'#fff':'#fafafa'}>
                          <td style={{ padding:'6px 10px', maxWidth:130, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{r.source}</td>
                          <td style={{ padding:'6px 10px', textAlign:'right', fontWeight:600 }}>{r.cnt}</td>
                          <td style={{ padding:'6px 10px', textAlign:'right', color:'#6b7280' }}>{r.pct}%</td>
                          <td style={{ padding:'6px 10px', textAlign:'right', color:'#3b82f6' }}>{r.appo_cnt||0}</td>
                          <td style={{ padding:'6px 10px', textAlign:'right', color:'#3b82f6', fontWeight:600 }}>{r.appo_rate}%</td>
                          <td style={{ padding:'6px 10px', textAlign:'right', color:'#10b981' }}>{r.order_cnt||0}</td>
                          <td style={{ padding:'6px 10px', textAlign:'right', color:'#10b981', fontWeight:600 }}>{r.order_rate}%</td>
                          <td style={{ padding:'6px 10px', textAlign:'right', color:'#ef4444' }}>{r.lost_cnt||0}</td>
                          <td style={{ padding:'6px 10px', textAlign:'right' }}>
                            <span style={{ color:r.lost_rate>=50?'#ef4444':r.lost_rate>=30?'#f59e0b':'#6b7280', fontWeight:r.lost_rate>=30?600:400 }}>{r.lost_rate}%</span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            );
          })()}

          {/* アポ化・受注・失注 統合グラフ */}
          {(() => {
            const sources = new Set([
              ...(data.appoBySource||[]).map(r=>r.source),
              ...(data.orderBySource||[]).map(r=>r.source),
              ...(data.bySource||[]).filter(r=>r.lost_cnt>0).map(r=>r.source),
            ]);
            const merged = Array.from(sources).map(src => ({
              source: src,
              appo:  data.appoBySource?.find(r=>r.source===src)?.cnt || 0,
              order: data.orderBySource?.find(r=>r.source===src)?.cnt || 0,
              lost:  data.bySource?.find(r=>r.source===src)?.lost_cnt || 0,
            })).sort((a,b) => (b.appo+b.order+b.lost)-(a.appo+a.order+a.lost));
            return (
              <div style={{ background:'#fff', border:'1px solid #e5e7eb', borderRadius:10, padding:'16px 12px', marginBottom:20 }}>
                <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:12, paddingLeft:8 }}>
                  <span style={{ fontWeight:700, fontSize:'0.85rem' }}>アポ化・受注・失注 流入経路別</span>
                  <span style={{ fontSize:'0.7rem', color:'#9ca3af' }}>クリックで詳細</span>
                  <div style={{ marginLeft:'auto', display:'flex', gap:12, fontSize:'0.72rem' }}>
                    <span><span style={{ display:'inline-block', width:10, height:10, background:'#3b82f6', borderRadius:2, marginRight:4 }}/>アポ化</span>
                    <span><span style={{ display:'inline-block', width:10, height:10, background:'#10b981', borderRadius:2, marginRight:4 }}/>受注</span>
                    <span><span style={{ display:'inline-block', width:10, height:10, background:'#ef4444', borderRadius:2, marginRight:4 }}/>失注</span>
                  </div>
                </div>
                {merged.length === 0
                  ? <div style={{ color:'#9ca3af', fontSize:'0.8rem', textAlign:'center', padding:16 }}>データなし</div>
                  : (
                    <ResponsiveContainer width="100%" height={Math.max(160, merged.length * 36)}>
                      <BarChart data={merged} layout="vertical" margin={{ top:0, right:60, left:4, bottom:0 }}
                        onClick={e => {
                          const src = e?.activePayload?.[0]?.payload?.source;
                          const key = e?.activePayload?.[0]?.dataKey;
                          if (src) openDrill(src, key === 'order' ? 'order' : 'appo');
                        }}
                        style={{ cursor:'pointer' }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                        <XAxis type="number" tick={TICK} allowDecimals={false} />
                        <YAxis type="category" dataKey="source" tick={{ fontSize:11, fill:'#6b7280' }} width={130} />
                        <Tooltip formatter={(v, n) => [`${v}件`, n === 'appo' ? 'アポ化' : n === 'order' ? '受注' : '失注']} cursor={{ fill:'#f8fafc' }} />
                        <Bar dataKey="appo"  fill="#3b82f6" radius={[0,0,0,0]} name="appo"  barSize={8}>
                          <LabelList dataKey="appo"  position="right" style={{ fontSize:10, fill:'#374151' }} formatter={v => v > 0 ? v : ''} />
                        </Bar>
                        <Bar dataKey="order" fill="#10b981" radius={[0,0,0,0]} name="order" barSize={8}>
                          <LabelList dataKey="order" position="right" style={{ fontSize:10, fill:'#374151' }} formatter={v => v > 0 ? v : ''} />
                        </Bar>
                        <Bar dataKey="lost"  fill="#ef4444" radius={[0,4,4,0]} name="lost"  barSize={8}>
                          <LabelList dataKey="lost"  position="right" style={{ fontSize:10, fill:'#374151' }} formatter={v => v > 0 ? v : ''} />
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  )
                }
              </div>
            );
          })()}

          {/* 流入経路ドリルダウンパネル */}
          {drill && (
            <>
              <div onClick={() => setDrill(null)} style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.35)', zIndex:1000 }} />
              <div style={{ position:'fixed', top:0, right:0, bottom:0, width:560, maxWidth:'94vw', background:'#fff', zIndex:1001, display:'flex', flexDirection:'column', boxShadow:'-4px 0 24px rgba(0,0,0,0.15)' }}>
                <div style={{ padding:'16px 20px', borderBottom:'1px solid #e5e7eb', display:'flex', alignItems:'center', gap:12 }}>
                  <div>
                    <div style={{ fontSize:'0.75rem', color:'#6b7280' }}>流入経路: {drill.source}</div>
                    <div style={{ fontWeight:700, fontSize:'0.95rem' }}>
                      {drillLoading ? '読み込み中...' : `${drill.rows?.length || 0}件`}
                    </div>
                  </div>
                  <button onClick={() => setDrill(null)} style={{ marginLeft:'auto', background:'none', border:'none', fontSize:'1.2rem', cursor:'pointer', color:'#6b7280' }}>✕</button>
                </div>
                <div style={{ flex:1, overflowY:'auto' }}>
                  {drillLoading
                    ? <div style={{ padding:24, color:'#9ca3af', textAlign:'center' }}>読み込み中...</div>
                    : !drill.rows?.length
                    ? <div style={{ padding:24, color:'#9ca3af', textAlign:'center' }}>データなし</div>
                    : (
                      <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.8rem' }}>
                        <thead>
                          <tr style={{ background:'#f8fafc', position:'sticky', top:0 }}>
                            {/* アポ取得済のドリルダウンは現在ヨミ・初回ヨミを非表示（自明のため） */}
                            {(drill.yomiType === 'apo_got'
                              ? ['流入日','会社名','現在ヨミ','担当者']
                              : ['流入日','会社名','流入経路','現在ヨミ','担当者']
                            ).map(h => (
                              <th key={h} style={{ padding:'8px 12px', textAlign:'left', fontWeight:600, color:'#64748b', whiteSpace:'nowrap', borderBottom:'1px solid #e5e7eb', fontSize:'0.72rem' }}>{h}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {drill.rows.map((r, i) => (
                            <tr key={i} style={{ borderBottom:'1px solid #f3f4f6', background: i%2===0?'#fff':'#fafafa' }}>
                              <td style={{ padding:'7px 12px', whiteSpace:'nowrap', color:'#6b7280' }}>{r.inflow_date?.slice(0,10)||'—'}</td>
                              <td style={{ padding:'7px 12px', maxWidth:160, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', fontWeight:500 }}>{r.customer||'—'}</td>
                              {drill.yomiType !== 'apo_got' && (
                                <td style={{ padding:'7px 12px', whiteSpace:'nowrap', color:'#374151', fontSize:'0.78rem' }}>{r.source||'—'}</td>
                              )}
                              <td style={{ padding:'7px 12px', whiteSpace:'nowrap' }}>
                                <span style={{ fontSize:'0.7rem', background:'#eff6ff', color:'#3b82f6', borderRadius:4, padding:'2px 6px' }}>{r.yomi}</span>
                              </td>
                              <td style={{ padding:'7px 12px', whiteSpace:'nowrap', color:'#374151' }}>{r.rep||'—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )
                  }
                </div>
              </div>
            </>
          )}

          {/* リード一覧 */}
          <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, overflow: 'hidden' }}>
            <div style={{ padding: '12px 16px', borderBottom: '1px solid #f3f4f6', display: 'flex', alignItems: 'center', gap: 10 }}>
              <span style={{ fontWeight: 700, fontSize: '0.85rem' }}>リード一覧</span>
              <span style={{ fontSize: '0.75rem', color: '#9ca3af' }}>全{(listData?.pagination?.total || data?.pagination?.total || 0).toLocaleString()}件</span>
              {listLoading && <span style={{ fontSize: '0.72rem', color: '#9ca3af' }}>読み込み中...</span>}
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
                <thead>
                  <tr style={{ background: '#f8fafc' }}>
                    {['流入日', '会社名', 'ヨミ', '流入経路', '担当者'].map(h => (
                      <th key={h} style={{ padding: '8px 12px', textAlign: 'left', fontWeight: 600, color: '#64748b', whiteSpace: 'nowrap', borderBottom: '1px solid #e5e7eb', fontSize: '0.72rem' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(listData?.recent || data?.recent || []).map((r, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid #f8fafc', background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap', color: '#6b7280' }}>{r.inflow_date?.slice(0,10) || '—'}</td>
                      <td style={{ padding: '7px 12px', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontWeight: 500 }}>{r.customer || '—'}</td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap' }}>
                        <span style={{ fontSize: '0.72rem', background: r.yomi === 'アポ化前' ? '#f1f5f9' : '#eff6ff', color: r.yomi === 'アポ化前' ? '#64748b' : '#3b82f6', borderRadius: 4, padding: '2px 6px' }}>
                          {r.yomi}
                        </span>
                      </td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap', color: '#374151' }}>{r.source || '—'}</td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap', color: '#374151' }}>{r.rep || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {/* ページング（チャートを再描画しない） */}
            {(() => {
              const pg = listData?.pagination || data?.pagination;
              if (!pg || pg.total <= pg.limit) return null;
              const maxPage = Math.ceil(pg.total / pg.limit);
              return (
              <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', gap: 8, padding: '12px 16px', borderTop: '1px solid #f3f4f6' }}>
                <button onClick={() => loadPage(from, to, page - 1)} disabled={page <= 1 || listLoading}
                  style={{ padding: '5px 12px', border: '1px solid #d1d5db', borderRadius: 6, cursor: page <= 1 ? 'not-allowed' : 'pointer', background: '#fff', color: page <= 1 ? '#d1d5db' : '#374151', fontSize: '0.8rem' }}>
                  ← 前
                </button>
                <span style={{ fontSize: '0.8rem', color: '#6b7280' }}>{page} / {maxPage} ページ</span>
                <button onClick={() => loadPage(from, to, page + 1)} disabled={page >= maxPage || listLoading}
                  style={{ padding: '5px 12px', border: '1px solid #d1d5db', borderRadius: 6, cursor: page >= maxPage ? 'not-allowed' : 'pointer', background: '#fff', color: page >= maxPage ? '#d1d5db' : '#374151', fontSize: '0.8rem' }}>
                  次 →
                </button>
              </div>
              );
            })()}
          </div>
        </>
      )}

      {/* チャンネル管理タブ（転置テーブル：縦=項目・横=チャンネル） */}
      {activeTab === 'channels' && (
        <div>
          {chLoading && <div style={{ color:'#9ca3af', marginBottom:16 }}>読み込み中...</div>}
          {chData && !chLoading && (() => {
            const channels = chData.rows;
            const safeN = v => Number(v) || 0;
            const fmtYen = v => !v ? '—' : `¥${Math.round(v).toLocaleString()}`;
            const fmtPct = v => (v == null || isNaN(v)) ? '—' : `${v}%`;
            const fmtNum = v => v != null ? String(v) : '—';

            // チャンネルごとの全計算値を返す
            const calc = (ch) => {
              const e = chEdits[ch.source] || {};
              const vn = (k) => safeN(e[k] !== undefined ? e[k] : ch[k]);
              const vt = (k) => String(e[k] !== undefined ? e[k] : (ch[k] || ''));
              const leadUp     = vn('lead_unit_price');
              const expAppo    = vn('expected_appo_count');
              const cost       = leadUp * expAppo;
              const expLeads   = vn('expected_leads');
              const expAR      = vn('expected_appo_rate');
              const expOR      = vn('expected_order_rate');
              const expUP      = vn('expected_unit_price');
              const expOrders  = expAppo * (expOR / 100);
              const expRev     = expOrders * expUP;
              return {
                lead_unit_price:     leadUp,
                vendor_note:         vt('vendor_note'),
                cost_per_month:      cost,
                expected_leads:      expLeads,
                expected_appo_count: expAppo,
                expected_appo_rate:  vn('expected_appo_rate'),
                expected_order_rate: vn('expected_order_rate'),
                expected_unit_price: expUP,
                _expected_appo_cpa:  expAppo > 0 ? cost / expAppo : null,
                _cost_per_month:     cost,
                _leads_progress:     expLeads > 0 ? Math.round(ch.actual_leads / expLeads * 100) : null,
                _actual_appo_rate:   ch.actual_leads > 0 ? Math.round(ch.actual_appo / ch.actual_leads * 100) : null,
                _expected_orders:    expOrders > 0 ? expOrders.toFixed(1) : null,
                _expected_revenue:   expRev > 0 ? expRev : null,
                _expected_roi:       cost > 0 && expRev > 0 ? expRev / cost : null,
                _roi:                cost > 0 && ch.actual_revenue > 0 ? ch.actual_revenue / cost : null,
                _appo_diff:          ch.actual_appo - expAppo,
                actual_leads:        ch.actual_leads,
                actual_appo:         ch.actual_appo,
                actual_orders:       ch.actual_orders,
                actual_revenue:      ch.actual_revenue,
              };
            };

            const dispCell = (rowDef, cv, ch) => {
              const k = rowDef.key;
              const v = cv[k];
              if (rowDef.editable) return null; // handled separately
              switch (k) {
                case '_expected_appo_cpa': return fmtYen(v);
                case '_cost_per_month':    return fmtYen(v);
                case '_leads_progress':    return fmtPct(v);
                case '_actual_appo_rate':  return fmtPct(v);
                case '_expected_orders':   return v != null ? String(v) : '—';
                case '_expected_revenue':  return fmtYen(v);
                case '_expected_roi':      return v != null ? `${v.toFixed(1)}x` : '—';
                case '_roi':               return v != null ? `${v.toFixed(1)}x` : '—';
                case '_appo_diff': {
                  if (v == null) return '—';
                  const style = v < 0 ? { color:'#ef4444', fontWeight:700 } : v === 0 ? {} : { color:'#059669', fontWeight:700 };
                  return <span style={style}>{v > 0 ? `+${v}` : String(v)}</span>;
                }
                case 'actual_leads':   return fmtNum(ch.actual_leads);
                case 'actual_appo':    return fmtNum(ch.actual_appo);
                case 'actual_orders':  return fmtNum(ch.actual_orders);
                case 'actual_revenue': return fmtYen(ch.actual_revenue);
                default: return '—';
              }
            };

            // 行タイプ別スタイル定義
            const rowBg = (row) => {
              if (row.yellow)    return '#fffbeb';
              if (row.isActual)  return '#f0fdf4';
              if (row.editable)  return '#f8faff';
              return '#fafafa';
            };
            const labelColor = (row) => {
              if (row.yellow)    return '#92400e';
              if (row.isActual)  return '#064e3b';
              if (row.editable)  return '#1d4ed8';
              return '#6b7280';
            };

            return (
              <div style={{ background:'#fff', border:'1px solid #e5e7eb', borderRadius:10, overflow:'hidden' }}>
                {/* ヘッダー */}
                <div style={{ padding:'12px 16px', borderBottom:'1px solid #e5e7eb', display:'flex', alignItems:'center', gap:10 }}>
                  <span style={{ fontWeight:700, fontSize:'0.85rem' }}>流入経路別パフォーマンス</span>
                  <div style={{ display:'flex', gap:10, fontSize:'0.7rem', color:'#9ca3af' }}>
                    <span style={{ display:'flex', alignItems:'center', gap:3 }}><span style={{ width:8, height:8, borderRadius:2, background:'#dbeafe', display:'inline-block' }}/>入力</span>
                    <span style={{ display:'flex', alignItems:'center', gap:3 }}><span style={{ width:8, height:8, borderRadius:2, background:'#dcfce7', display:'inline-block' }}/>実績</span>
                    <span style={{ display:'flex', alignItems:'center', gap:3 }}><span style={{ width:8, height:8, borderRadius:2, background:'#fef9c3', display:'inline-block' }}/>成果</span>
                  </div>
                  <button onClick={() => loadChannels(from, to)} style={{ marginLeft:'auto', background:'none', border:'1px solid #e2e8f0', borderRadius:6, padding:'3px 10px', fontSize:'0.72rem', cursor:'pointer', color:'#6b7280' }}>
                    再読み込み
                  </button>
                </div>

                <div style={{ overflowX:'auto', overflowY:'auto', maxHeight:'80vh' }}>
                  <table style={{ borderCollapse:'collapse', fontSize:'0.78rem', minWidth: `${170 + channels.length * 120}px` }}>
                    <thead>
                      <tr style={{ background:'#f1f5f9' }}>
                        <th style={{ padding:'8px 14px', textAlign:'left', fontWeight:600, color:'#64748b', fontSize:'0.72rem', whiteSpace:'nowrap', borderBottom:'2px solid #e2e8f0', borderRight:'2px solid #e2e8f0', position:'sticky', left:0, top:0, zIndex:3, background:'#f1f5f9', minWidth:165 }}>
                          流入経路
                        </th>
                        {channels.map(ch => {
                          const dirty  = chEdits[ch.source] && Object.keys(chEdits[ch.source]).length > 0;
                          const saving = chSaving[ch.source];
                          return (
                            <th key={ch.source} style={{ padding:'6px 10px', textAlign:'center', fontWeight:600, color:'#374151', fontSize:'0.75rem', whiteSpace:'nowrap', borderBottom:'2px solid #e2e8f0', borderRight:'1px solid #e5e7eb', position:'sticky', top:0, zIndex:2, background:'#f1f5f9', minWidth:115, maxWidth:140 }}>
                              <div style={{ overflow:'hidden', textOverflow:'ellipsis', maxWidth:130, margin:'0 auto', lineHeight:1.3 }} title={ch.source}>
                                {ch.source}
                              </div>
                              {saving && <div style={{ fontSize:'0.6rem', color:'#9ca3af', fontWeight:400, marginTop:2 }}>保存中...</div>}
                              {dirty && !saving && <div style={{ fontSize:'0.6rem', color:'#3b82f6', fontWeight:400, marginTop:2 }}>● 未保存</div>}
                            </th>
                          );
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {CHANNEL_ROWS.map((row, ri) => {
                        // セクションヘッダー行
                        if (row.isSection) {
                          return (
                            <tr key={row.key}>
                              <td colSpan={channels.length + 1} style={{ padding:'5px 14px 4px', background:'#334155', color:'#94a3b8', fontSize:'0.62rem', fontWeight:700, letterSpacing:'0.08em', textTransform:'uppercase', borderTop: ri > 0 ? '1px solid #1e293b' : 'none' }}>
                                {row.label}
                              </td>
                            </tr>
                          );
                        }

                        const bg = rowBg(row);
                        const lc = labelColor(row);
                        return (
                          <tr key={row.key} style={{ borderBottom:'1px solid #e5e7eb' }}>
                            {/* 行ラベル（sticky） */}
                            <td style={{ padding:'6px 14px', whiteSpace:'nowrap', fontWeight: row.isActual ? 700 : 500, fontSize:'0.75rem', color: lc, background: bg, position:'sticky', left:0, zIndex:1, borderRight:'2px solid #e2e8f0', borderLeft: row.editable ? '3px solid #93c5fd' : row.isActual ? '3px solid #6ee7b7' : row.yellow ? '3px solid #fcd34d' : '3px solid transparent' }}>
                              {row.label}
                            </td>
                            {/* データセル */}
                            {channels.map(ch => {
                              const cv = calc(ch);
                              if (row.editable) {
                                const e = chEdits[ch.source] || {};
                                const currentVal = e[row.key] !== undefined ? e[row.key] : (ch[row.key] || '');
                                const isDirty = e[row.key] !== undefined;
                                return (
                                  <td key={ch.source} style={{ padding:'4px 8px', textAlign:'right', background: isDirty ? '#eff6ff' : bg, borderRight:'1px solid #e5e7eb' }}>
                                    <input
                                      type={row.type === 'text' ? 'text' : 'number'}
                                      value={currentVal}
                                      onChange={ev => setChEdit(ch.source, row.key, ev.target.value)}
                                      onBlur={() => saveChannelTarget(ch.source)}
                                      placeholder="—"
                                      style={{
                                        width: row.type === 'text' ? 90 : row.type === 'money' ? 84 : 56,
                                        border: `1px solid ${isDirty ? '#93c5fd' : '#e2e8f0'}`,
                                        borderRadius:5, padding:'3px 6px', fontSize:'0.75rem',
                                        textAlign: row.type === 'text' ? 'left' : 'right',
                                        outline:'none', background: isDirty ? '#fff' : '#fff',
                                        color:'#1e293b',
                                      }}
                                    />
                                  </td>
                                );
                              }
                              const val = dispCell(row, cv, ch);
                              const isZero = val === '—' || val === '¥0' || val === '0';
                              return (
                                <td key={ch.source} style={{ padding:'6px 12px', textAlign:'right', whiteSpace:'nowrap', background: bg, borderRight:'1px solid #e5e7eb', color: isZero ? '#d1d5db' : row.isActual ? '#064e3b' : row.yellow ? '#92400e' : '#374151', fontWeight: row.isActual ? 600 : 400, fontSize:'0.78rem' }}>
                                  {val}
                                </td>
                              );
                            })}
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            );
          })()}
        </div>
      )}
    </div>
  );
}
