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
    load(f, t, repFilter, srcFilter, appoOnly);
  };

  const handleSearch = () => { setActivePreset(null); load(from, to, repFilter, srcFilter, appoOnly); };
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
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ margin: 0, fontSize: '1.1rem', fontWeight: 800, color: '#111827' }}>リード管理</h2>
        {data?.period && (
          <p style={{ margin: '3px 0 0', fontSize: '0.8rem', color: '#6b7280' }}>
            {(() => {
              const f = data.period.from, t = data.period.to;
              if (!f || !t) return null;
              const fd = new Date(f), td = new Date(t);
              const fmt = d => `${d.getFullYear()}/${d.getMonth()+1}/${d.getDate()}`;
              // 同月なら "2026年5月" 表記
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

      {/* 絞り込みフィルター */}
      <div style={{ marginBottom:16 }}>
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
      </div>

      {loading && <div style={{ color: '#9ca3af', marginBottom: 16 }}>読み込み中...</div>}

      {data && !loading && (
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
            const yomiMap = { 'アポ化前':'apo_before','アポ取得済':'apo_got','商談中':'in_deal','受注':'order' };
            return (
              <div style={{ display:'flex', gap:12, flexWrap:'wrap', marginBottom:20 }}>
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

                {/* アポ化済みカード（ヨミ経過フロー = 一度でもアポ化に到達）*/}
                <div style={{ background:'#f0fdf4', border:'2px solid #86efac', borderRadius:10, padding:'14px 18px', minWidth:160, flex:1, cursor:'pointer', transition:'box-shadow 0.15s' }}
                  onClick={() => openYomiDrill('apo_got', 'アポ化済み')}
                  onMouseEnter={e=>e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.1)'}
                  onMouseLeave={e=>e.currentTarget.style.boxShadow='none'}>
                  <div style={{ fontSize:'0.72rem', color:'#15803d', fontWeight:700, marginBottom:4 }}>アポ化済み</div>
                  <div style={{ fontSize:'2rem', fontWeight:800, color:'#15803d' }}>{(data.appoTotal||0).toLocaleString()}</div>
                  <div style={{ fontSize:'0.75rem', color:'#16a34a', marginTop:2 }}>
                    {data.periodTotal > 0 ? `アポ化率 ${Math.round((data.appoTotal||0)/data.periodTotal*100)}%` : '—'}
                  </div>
                  {proj && <div style={{ fontSize:'0.7rem', color:'#4ade80', marginTop:1 }}>月末予測 {Math.round((data.appoTotal||0)/proj.ratio)}件</div>}
                </div>

                {/* ファネルカード（アポ化前・商談中・受注）*/}
                {data.funnel.filter(f => f.label !== 'アポ取得済').map((f) => {
                  const rate = data.periodTotal > 0 ? Math.round(f.cnt / data.periodTotal * 100) : 0;
                  const yt = yomiMap[f.label];
                  // 商談中・受注はアポ化済みに含まれるため ※を付ける
                  const isSubset = f.label === '商談中' || f.label === '受注';
                  return (
                    <div key={f.label} style={{ background:'#fff', border:'1px solid #e5e7eb', borderRadius:10, padding:'14px 18px', minWidth:130, flex:1, cursor:'pointer', transition:'box-shadow 0.15s' }}
                      onClick={() => yt && openYomiDrill(yt, f.label)}
                      onMouseEnter={e=>e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.1)'}
                      onMouseLeave={e=>e.currentTarget.style.boxShadow='none'}
                      title={isSubset ? 'アポ化済みの内数' : ''}>
                      <div style={{ fontSize:'0.72rem', color:'#6b7280', marginBottom:4 }}>
                        {f.label}{isSubset && <span style={{ fontSize:'0.65rem', color:'#10b981', marginLeft:4 }}>（アポ化済み内数）</span>}
                      </div>
                      <div style={{ fontSize:'2rem', fontWeight:800, color: YOMI_COLOR[f.label] || '#374151' }}>{f.cnt.toLocaleString()}</div>
                      <div style={{ fontSize:'0.72rem', color:'#9ca3af', marginTop:2 }}>{rate}%</div>
                    </div>
                  );
                })}
              </div>
            );
          })()}

          {/* 失注カード + 失注理由テーブル */}
          {(data.lostTotal > 0 || data.byLostReason?.length > 0) && (
            <div style={{ display:'grid', gridTemplateColumns:'180px 1fr', gap:16, marginBottom:20 }}>
              <div style={{ background:'#fef2f2', border:'2px solid #fca5a5', borderRadius:10, padding:'14px 18px', cursor:'pointer' }}
                onClick={() => openYomiDrill('lost', '失注')}
                onMouseEnter={e=>e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.1)'}
                onMouseLeave={e=>e.currentTarget.style.boxShadow='none'}>
                <div style={{ fontSize:'0.72rem', color:'#dc2626', fontWeight:700, marginBottom:4 }}>失注</div>
                <div style={{ fontSize:'2rem', fontWeight:800, color:'#dc2626' }}>{(data.lostTotal||0).toLocaleString()}</div>
                <div style={{ fontSize:'0.75rem', color:'#ef4444', marginTop:2 }}>
                  {data.periodTotal > 0 ? `失注率 ${Math.round((data.lostTotal||0)/data.periodTotal*100)}%` : '—'}
                </div>
              </div>
              <div style={{ background:'#fff', border:'1px solid #e5e7eb', borderRadius:10, overflow:'hidden' }}>
                <div style={{ padding:'8px 14px', borderBottom:'1px solid #f1f5f9', fontSize:'0.78rem', fontWeight:700, color:'#374151' }}>失注理由</div>
                <div style={{ display:'flex', flexWrap:'wrap', gap:6, padding:'10px 14px' }}>
                  {(data.byLostReason||[]).map((r,i) => (
                    <div key={i} style={{ display:'flex', alignItems:'center', gap:6, background:'#fef2f2', borderRadius:20, padding:'3px 10px' }}>
                      <span style={{ fontSize:'0.75rem', color:'#dc2626', fontWeight:600 }}>{r.reason}</span>
                      <span style={{ fontSize:'0.72rem', color:'#9ca3af' }}>{r.cnt}件</span>
                    </div>
                  ))}
                  {(!data.byLostReason?.length) && <span style={{ fontSize:'0.78rem', color:'#d1d5db' }}>データなし</span>}
                </div>
              </div>
            </div>
          )}

          {/* 統計サマリー */}
          {data.stats && (
            <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 20 }}>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '12px 18px' }}>
                <div style={{ fontSize: '0.72rem', color: '#6b7280', marginBottom: 2 }}>月平均（直近12ヶ月）</div>
                <div style={{ fontSize: '1.4rem', fontWeight: 800, color: '#374151' }}>{data.stats.avg12}件/月</div>
              </div>
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
                <Tooltip formatter={(v, n) => [`${v}件`, n]} />
                {data.stats?.avg12 > 0 && (
                  <ReferenceLine y={data.stats.avg12} stroke="#f59e0b" strokeDasharray="4 3"
                    label={{ value: `平均 ${data.stats.avg12}`, fill: '#f59e0b', fontSize: 11, position: 'right' }} />
                )}
                <Line type="monotone" dataKey="cnt"  stroke="#3b82f6" strokeWidth={2} dot={{ r: 3 }} name="全リード" />
                <Line type="monotone" dataKey="appo" stroke="#10b981" strokeWidth={2} dot={{ r: 3 }} name="アポ化済" strokeDasharray="5 3" />
                <Legend wrapperStyle={{ fontSize:'0.72rem' }} />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* 流入経路: 円グラフ + 失注率テーブル */}
          {(() => {
            const total = data.bySource.reduce((s, r) => s + r.cnt, 0);
            const srcData = data.bySource.map(r => ({
              ...r,
              pct:      total > 0 ? Math.round(r.cnt / total * 100) : 0,
              lost_rate: r.cnt > 0 ? Math.round(r.lost_cnt / r.cnt * 100) : 0,
            }));
            const top10 = srcData.slice(0, 10);
            const othersTotal = srcData.slice(10).reduce((s, r) => s + r.cnt, 0);
            const pieData = othersTotal > 0 ? [...top10, { source: 'その他', cnt: othersTotal, pct: Math.round(othersTotal / total * 100) }] : top10;

            return (
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 20 }}>
                {/* 円グラフ */}
                <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 12px' }}>
                  <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 8, paddingLeft: 8 }}>
                    流入経路 <span style={{ fontSize: '0.72rem', color: '#9ca3af', fontWeight: 400 }}>クリックで詳細</span>
                  </div>
                  <ResponsiveContainer width="100%" height={300}>
                    <PieChart margin={{ top: 10, right: 10, bottom: 10, left: 10 }}>
                      <Pie data={pieData} dataKey="cnt" nameKey="source" cx="50%" cy="50%" outerRadius={85}
                        label={({ percent }) => percent > 0.05 ? `${(percent*100).toFixed(0)}%` : ''}
                        labelLine style={{ cursor: 'pointer' }}
                        onClick={d => d?.source && d.source !== 'その他' && openDrill(d.source)}>
                        {pieData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                      </Pie>
                      <Tooltip formatter={(v, n) => [`${v}件`, n]} />
                      <Legend wrapperStyle={{ fontSize: '0.7rem' }} layout="vertical" align="right" verticalAlign="middle" />
                    </PieChart>
                  </ResponsiveContainer>
                </div>

                {/* 失注率テーブル */}
                <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, overflow: 'hidden' }}>
                  <div style={{ padding: '12px 16px', borderBottom: '1px solid #f3f4f6', fontWeight: 700, fontSize: '0.85rem' }}>
                    流入経路別 失注率
                  </div>
                  <div style={{ overflowY: 'auto', maxHeight: 320 }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
                      <thead>
                        <tr style={{ background: '#f8fafc', position: 'sticky', top: 0 }}>
                          {['経路', '件数', '割合', '失注数', '失注率'].map(h => (
                            <th key={h} style={{ padding: '7px 10px', textAlign: h === '経路' ? 'left' : 'right', fontWeight: 600, color: '#64748b', fontSize: '0.7rem', borderBottom: '1px solid #e5e7eb', whiteSpace: 'nowrap' }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {srcData.map((r, i) => (
                          <tr key={i} onClick={() => openDrill(r.source)}
                            style={{ borderBottom: '1px solid #f8fafc', cursor: 'pointer', background: i%2===0?'#fff':'#fafafa' }}
                            onMouseEnter={e => e.currentTarget.style.background='#eff6ff'}
                            onMouseLeave={e => e.currentTarget.style.background=i%2===0?'#fff':'#fafafa'}>
                            <td style={{ padding: '6px 10px', maxWidth: 130, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.source}</td>
                            <td style={{ padding: '6px 10px', textAlign: 'right', fontWeight: 600 }}>{r.cnt}</td>
                            <td style={{ padding: '6px 10px', textAlign: 'right', color: '#6b7280' }}>{r.pct}%</td>
                            <td style={{ padding: '6px 10px', textAlign: 'right', color: '#ef4444' }}>{r.lost_cnt}</td>
                            <td style={{ padding: '6px 10px', textAlign: 'right' }}>
                              <span style={{ color: r.lost_rate >= 50 ? '#ef4444' : r.lost_rate >= 30 ? '#f59e0b' : '#6b7280', fontWeight: r.lost_rate >= 30 ? 600 : 400 }}>
                                {r.lost_rate}%
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            );
          })()}

          {/* アポ化・受注 統合グラフ */}
          {(() => {
            const sources = new Set([...(data.appoBySource||[]).map(r=>r.source), ...(data.orderBySource||[]).map(r=>r.source)]);
            const merged = Array.from(sources).map(src => ({
              source: src,
              appo: data.appoBySource?.find(r=>r.source===src)?.cnt || 0,
              order: data.orderBySource?.find(r=>r.source===src)?.cnt || 0,
            })).sort((a,b) => (b.appo+b.order)-(a.appo+a.order)).slice(0,15);
            return (
              <div style={{ background:'#fff', border:'1px solid #e5e7eb', borderRadius:10, padding:'16px 12px', marginBottom:20 }}>
                <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:12, paddingLeft:8 }}>
                  <span style={{ fontWeight:700, fontSize:'0.85rem' }}>アポ化・受注につながった流入経路</span>
                  <span style={{ fontSize:'0.7rem', color:'#9ca3af' }}>クリックで詳細</span>
                  <div style={{ marginLeft:'auto', display:'flex', gap:12, fontSize:'0.72rem' }}>
                    <span><span style={{ display:'inline-block', width:10, height:10, background:'#3b82f6', borderRadius:2, marginRight:4 }}/>アポ化</span>
                    <span><span style={{ display:'inline-block', width:10, height:10, background:'#10b981', borderRadius:2, marginRight:4 }}/>受注</span>
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
                        <Tooltip formatter={(v, n) => [`${v}件`, n === 'appo' ? 'アポ化' : '受注']} cursor={{ fill:'#f8fafc' }} />
                        <Bar dataKey="appo" fill="#3b82f6" radius={[0,0,0,0]} name="appo" barSize={10}>
                          <LabelList dataKey="appo" position="right" style={{ fontSize:10, fill:'#374151' }} formatter={v => v > 0 ? v : ''} />
                        </Bar>
                        <Bar dataKey="order" fill="#10b981" radius={[0,4,4,0]} name="order" barSize={10}>
                          <LabelList dataKey="order" position="right" style={{ fontSize:10, fill:'#374151' }} formatter={v => v > 0 ? v : ''} />
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
    </div>
  );
}
