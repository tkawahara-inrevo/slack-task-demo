import { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

// ── 定数 ────────────────────────────────────────────────────────
const STAGE_CFG = {
  'リード獲得':    { color:'var(--gray-500)', bg:'var(--surface-2)', border:'#cbd5e1' },
  '初回商談待ち':  { color:'#d97706', bg:'#fef3c7', border:'#fcd34d' },
  '商談中':        { color:'#1d4ed8', bg:'#dbeafe', border:'#93c5fd' },
  '受注済':        { color:'#059669', bg:'#dcfce7', border:'#6ee7b7' },
  '失注':          { color:'#dc2626', bg:'#fee2e2', border:'#fca5a5' },
  '見送り':        { color:'var(--gray-400)', bg:'var(--surface-2)', border:'var(--gray-200)' },
  '初回商談待ち':  { color:'#d97706', bg:'#fef3c7', border:'#fcd34d' },
};
const YOMI_CFG = {
  'S 90％':{ color:'#7c3aed',bg:'#ede9fe' },'A 70％':{ color:'#1d4ed8',bg:'#dbeafe' },
  'B 50％':{ color:'#0891b2',bg:'#cffafe' },'C 30％':{ color:'#059669',bg:'#d1fae5' },
  'D 15％':{ color:'var(--gray-500)',bg:'var(--surface-2)' },'E 5％':{ color:'var(--gray-400)',bg:'var(--surface-2)' },
};
const STAGES = ['リード獲得','初回商談待ち','商談中','受注済','失注','見送り'];
const PLANS  = ['月額：コンサルのみ','月額：実務のみ','月額：フルコミット','後払い：媒体費弊社','後払い：媒体費クライアント','一括払い：分析付き','一括払い：人材紹介案件'];
const QUICK_FILTERS = [
  { key:'all',           label:'全て' },
  { key:'self',          label:'自分の担当' },
  { key:'high_priority', label:'高優先度' },
  { key:'yomi_mgmt',    label:'ヨミ管理中' },
  { key:'watch',         label:'要注視' },
];
const fmtM = n => { if(!n)return '—'; return `¥${Math.round(Number(n)).toLocaleString()}`; };
const daysSince = dt => Math.floor((Date.now()-new Date(dt))/86400000);

// ── ヘルスバー ─────────────────────────────────────────────────
function HealthBar({ score }) {
  const color = score >= 70 ? '#059669' : score >= 40 ? '#d97706' : '#dc2626';
  return (
    <div style={{ display:'flex', alignItems:'center', gap:6 }}>
      <div style={{ flex:1, height:4, background:'var(--surface-2)', borderRadius:2, overflow:'hidden' }}>
        <div style={{ height:'100%', width:`${score}%`, background:color, borderRadius:2, transition:'width 0.3s' }} />
      </div>
      <span style={{ fontSize:'0.68rem', fontWeight:700, color, minWidth:22 }}>{score}</span>
    </div>
  );
}

// ── 会社イニシャル ────────────────────────────────────────────
function Initial({ name }) {
  const colors = ['#6366f1','#0891b2','#059669','#d97706','#dc2626','#7c3aed','#ec4899'];
  const color = colors[(name?.charCodeAt(0)||0)%colors.length];
  const ch = name?.replace(/^(株式会社|有限会社|合同会社)/,'').charAt(0)||'?';
  return (
    <div style={{ width:36, height:36, borderRadius:9, background:color+'15', border:`1.5px solid ${color}30`,
      display:'flex', alignItems:'center', justifyContent:'center', flexShrink:0, fontWeight:800, fontSize:'0.88rem', color }}>
      {ch}
    </div>
  );
}

// ── KPI カード ─────────────────────────────────────────────────
function KpiCard({ label, value, sub, color = 'var(--gray-900)', bg = 'var(--surface)', highlight }) {
  return (
    <div style={{ background:bg, borderRadius:10, padding:'10px 16px', minWidth:120, border:`1px solid ${highlight?'#fcd34d':'var(--gray-200)'}`, flex:1 }}>
      <div style={{ fontSize:'0.68rem', color:'var(--gray-400)', fontWeight:600, marginBottom:4 }}>{label}</div>
      <div style={{ fontSize:'1.3rem', fontWeight:800, color, lineHeight:1 }}>{value}</div>
      {sub && <div style={{ fontSize:'0.65rem', color:'var(--gray-400)', marginTop:3 }}>{sub}</div>}
    </div>
  );
}

// ── 見送りモーダル ─────────────────────────────────────────────
function DormantModal({ deal, onClose, onDone }) {
  const [reason, setReason] = useState('');
  const [saving, setSaving] = useState(false);
  const REASONS = ['連絡が取れない','予算・タイミング合わず','競合先に決定','担当者交代待ち','その他'];
  const handleSave = async () => {
    setSaving(true);
    try { await api.crmSetDormant(deal.id, reason); onDone(); onClose(); }
    catch { alert('更新に失敗しました'); }
    finally { setSaving(false); }
  };
  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.5)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }}
      onClick={onClose}>
      <div style={{ background:'var(--surface)', borderRadius:14, width:'min(440px,92vw)', overflow:'hidden', boxShadow:'0 20px 50px rgba(0,0,0,0.2)' }}
        onClick={e => e.stopPropagation()}>
        <div style={{ padding:'14px 20px', borderBottom:'1px solid var(--gray-200)' }}>
          <div style={{ fontWeight:800, fontSize:'0.95rem', color:'var(--gray-900)' }}>見送りに変更</div>
          <div style={{ fontSize:'0.75rem', color:'var(--gray-400)', marginTop:2 }}>{deal.customer_name} — {deal.name}</div>
        </div>
        <div style={{ padding:'16px 20px' }}>
          <div style={{ fontSize:'0.78rem', fontWeight:600, color:'var(--gray-700)', marginBottom:8 }}>理由（任意）</div>
          <div style={{ display:'flex', flexWrap:'wrap', gap:6, marginBottom:12 }}>
            {REASONS.map(r => (
              <button key={r} onClick={() => setReason(r)}
                style={{ padding:'4px 12px', borderRadius:20, fontSize:'0.75rem', border:`1.5px solid ${reason===r?'#6366f1':'var(--gray-200)'}`,
                  background:reason===r?'#ede9fe':'var(--surface)', color:reason===r?'#6d28d9':'var(--gray-500)', cursor:'pointer' }}>
                {r}
              </button>
            ))}
          </div>
          <textarea value={reason} onChange={e=>setReason(e.target.value)} rows={2} placeholder="自由記述（任意）"
            style={{ width:'100%', boxSizing:'border-box', padding:'8px 10px', border:'1.5px solid var(--gray-200)', borderRadius:8, fontSize:'0.82rem', outline:'none', resize:'vertical' }} />
        </div>
        <div style={{ padding:'12px 20px', borderTop:'1px solid var(--gray-200)', display:'flex', justifyContent:'flex-end', gap:8 }}>
          <button onClick={onClose} style={{ padding:'7px 16px', border:'1px solid var(--gray-200)', borderRadius:8, fontSize:'0.82rem', color:'var(--gray-500)', background:'var(--surface)', cursor:'pointer' }}>キャンセル</button>
          <button onClick={handleSave} disabled={saving}
            style={{ padding:'7px 20px', border:'none', borderRadius:8, background:'var(--gray-500)', color:'#fff', fontSize:'0.82rem', fontWeight:700, cursor:'pointer' }}>
            {saving?'更新中…':'見送りにする'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── メイン ────────────────────────────────────────────────────
export default function CustomerList({ scope = 'all' }) {
  const navigate = useNavigate();
  const [deals, setDeals] = useState([]);
  const [kpi, setKpi] = useState(null);
  const [salesUsers, setSalesUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [q, setQ] = useState('');
  const [quickFilter, setQuickFilter] = useState('all');
  const [filterStages, setFilterStages] = useState(new Set());
  const [filterYomis, setFilterYomis] = useState(new Set());
  const [filterSales, setFilterSales] = useState('');
  const [showDormant, setShowDormant] = useState(false);
  const [dormantTarget, setDormantTarget] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [createForm, setCreateForm] = useState({
    name:'', nameShort:'', industry:'', prefecture:'', employeeCount:'',
    website:'', postalCode:'', address:'', inflowDate:'', inflowSource:'',
    inrevoPerson:'', businessDescription:'', serviceLpUrl1:'', serviceLpUrl2:'', memo:'',
  });
  const [creating, setCreating] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [syncDone, setSyncDone] = useState(false);
  const [totalCount, setTotalCount] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const sidebarRef = useRef(null);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [naToday, setNaToday] = useState(false);
  const todayYmdStr = new Date().toISOString().slice(0, 10);

  const buildQuery = (params = {}) => {
    const fs = (params.filterSales ?? filterSales);
    const isSelf = (params.quickFilter ?? quickFilter) === 'self';
    const naOnlyToday = params.naToday ?? naToday;
    return {
      scope,
      q: params.q ?? q,
      quickFilter: isSelf ? undefined : (params.quickFilter ?? quickFilter) === 'all' ? undefined : (params.quickFilter ?? quickFilter),
      salesUser: isSelf ? 'self_token' : fs,
      stage: [...(params.filterStages ?? filterStages)].join(','),
      yomi: [...(params.filterYomis ?? filterYomis)].join(','),
      showDormant: (params.showDormant ?? showDormant) ? '1' : undefined,
      // 担当者で絞ったときは NA順をデフォルトに
      sort: (fs || isSelf) ? 'na' : undefined,
      naToday: naOnlyToday ? '1' : undefined,
    };
  };

  const load = useCallback(async (params = {}) => {
    setLoading(true);
    try {
      const r = await api.crmDealsList(buildQuery(params));
      setDeals(r.deals || []);
      setKpi(r.kpi);
      setTotalCount(r.totalCount || 0);
      setHasMore(r.hasMore || false);
      setSalesUsers(r.salesUsers || []);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  }, [q, quickFilter, filterStages, filterYomis, filterSales, showDormant, scope, naToday]);

  const loadMore = async () => {
    setLoadingMore(true);
    try {
      const r = await api.crmDealsList({ ...buildQuery(), offset: deals.length });
      setDeals(prev => [...prev, ...(r.deals || [])]);
      setHasMore(r.hasMore || false);
    } catch (e) { console.error(e); }
    finally { setLoadingMore(false); }
  };

  useEffect(() => { load(); }, []);

  const handleQuickFilter = (key) => { setQuickFilter(key); load({ quickFilter: key }); };
  const toggleStage = (s) => {
    const n = new Set(filterStages); n.has(s)?n.delete(s):n.add(s);
    setFilterStages(n); load({ filterStages: n });
  };
  const toggleYomi = (y) => {
    const n = new Set(filterYomis); n.has(y)?n.delete(y):n.add(y);
    setFilterYomis(n); load({ filterYomis: n });
  };
  const clearFilters = () => { setFilterStages(new Set()); setFilterYomis(new Set()); setFilterSales(''); setQ(''); load({ filterStages:new Set(), q:'', filterSales:'', quickFilter:'all' }); setQuickFilter('all'); };
  const hasFilter = filterStages.size>0||filterYomis.size>0||filterSales||q;

  const handleKintoneSync = async () => {
    if (syncing) return;
    setSyncing(true); setSyncDone(false);
    try {
      await api.kintoneSync();
      const tick = setInterval(async () => {
        try { const st = await api.kintoneStatus(); if(!st.inProgress){ clearInterval(tick); setSyncDone(true); setSyncing(false); setTimeout(()=>{ load(); setSyncDone(false); },1500); } }
        catch { clearInterval(tick); setSyncing(false); }
      }, 2000);
    } catch { setSyncing(false); }
  };

  const handleCreate = async () => {
    if (!createForm.name.trim()) return;
    setCreating(true);
    try {
      const r = await api.crmCreateCustomer({
        name: createForm.name.trim(),
        nameShort: createForm.nameShort || null,
        industry: createForm.industry || null,
        prefecture: createForm.prefecture || null,
        employeeCount: createForm.employeeCount || null,
        website: createForm.website || null,
        postalCode: createForm.postalCode || null,
        address: createForm.address || null,
        inflowDate: createForm.inflowDate || null,
        inflowSource: createForm.inflowSource || null,
        inrevoPerson: createForm.inrevoPerson || null,
        businessDescription: createForm.businessDescription || null,
        serviceLpUrl1: createForm.serviceLpUrl1 || null,
        serviceLpUrl2: createForm.serviceLpUrl2 || null,
        memo: createForm.memo || null,
      });
      setShowCreateModal(false);
      navigate(`/crm/customers/${r.customer.id}`);
    } catch { alert('作成に失敗しました'); }
    finally { setCreating(false); }
  };

  // ステージ別件数
  const stageCounts = STAGES.reduce((acc, s) => {
    acc[s] = deals.filter(d => d.stage === s).length;
    return acc;
  }, {});

  // クイックフィルター件数
  const selfName = null; // TODO: ログインユーザー名と突合
  const qfCounts = {
    all:           deals.length,
    self:          deals.filter(d => d.sales_person_name?.includes('自分')).length, // placeholder
    high_priority: deals.filter(d => ['A 70％','S 90％'].includes(d.yomi) && d.status==='active').length,
    yomi_mgmt:     deals.filter(d => ['C 30％','B 50％','A 70％','S 90％'].includes(d.yomi) && d.status==='active').length,
    watch:         deals.filter(d => { const days=daysSince(d.updated_at); return days>=14&&d.status==='active'&&!['アポ化前'].includes(d.yomi); }).length,
  };

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%', background:'var(--surface-2)' }}>

      {/* ── ヘッダー ── */}
      <div style={{ background:'var(--surface)', borderBottom:'1px solid var(--gray-200)', padding:'10px 16px', flexShrink:0 }}>
        <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:10 }}>
          {/* KPI */}
          <div style={{ display:'flex', gap:8, flex:1, flexWrap:'wrap' }}>
            <KpiCard label="表示中" value={`${deals.length}件`} sub={`/ ${totalCount}件中`} />
            <KpiCard label="案件総額" value={fmtM(kpi?.totalAmount)} sub="進行中" color="#1e40af" bg="#eff6ff" />
            <KpiCard label="要対応" value={`${kpi?.alertCount||0}件`} sub="期限切れ or 停滞"
              color={kpi?.alertCount>0?'#dc2626':'var(--gray-400)'} highlight={kpi?.alertCount>0} />
          </div>
          {/* アクション */}
          <div style={{ display:'flex', gap:8, flexShrink:0 }}>
            <button onClick={handleKintoneSync} disabled={syncing}
              style={{ padding:'5px 12px', border:'1px solid var(--gray-200)', borderRadius:8, fontSize:'0.75rem', fontWeight:600,
                background:syncDone?'#f0fdf4':'var(--surface)', color:syncDone?'#059669':syncing?'var(--gray-400)':'var(--gray-500)', cursor:syncing?'default':'pointer', display:'flex', alignItems:'center', gap:4 }}>
              {syncDone?'✓':'⟳'} {syncDone?'同期完了':syncing?'同期中…':'kintone同期'}
            </button>
            <button onClick={() => setShowCreateModal(true)}
              style={{ padding:'6px 14px', background:'#1e40af', color:'#fff', border:'none', borderRadius:8, fontSize:'0.82rem', fontWeight:700, cursor:'pointer' }}>
              ＋ 顧客追加
            </button>
          </div>
        </div>

      </div>

      {/* ── メインエリア ── */}
      <div style={{ flex:1, display:'flex', overflow:'hidden' }}>

        {/* 左サイドバー */}
        <div style={{ width:sidebarOpen?200:0, flexShrink:0, overflowY:'auto', overflowX:'hidden',
          background:'var(--surface)', borderRight:'1px solid var(--gray-200)', transition:'width 0.2s', padding:sidebarOpen?'12px 0':0 }}>
          {sidebarOpen && (
            <div style={{ padding:'0 12px' }}>
              {/* 検索 */}
              <div style={{ position:'relative', marginBottom:14 }}>
                <input value={q} onChange={e=>setQ(e.target.value)} onKeyDown={e=>e.key==='Enter'&&load({q})}
                  placeholder="検索…" style={{ width:'100%', boxSizing:'border-box', padding:'6px 8px 6px 28px',
                    border:'1.5px solid var(--gray-200)', borderRadius:7, fontSize:'0.78rem', outline:'none', background:'var(--surface-2)' }} />
                <span style={{ position:'absolute', left:8, top:'50%', transform:'translateY(-50%)', color:'var(--gray-400)', fontSize:'0.75rem' }}>🔍</span>
              </div>

              {hasFilter && (
                <button onClick={clearFilters} style={{ width:'100%', marginBottom:12, padding:'4px 0', border:'1px solid var(--gray-200)', borderRadius:6, fontSize:'0.72rem', color:'var(--gray-500)', background:'var(--surface)', cursor:'pointer' }}>
                  フィルターをクリア
                </button>
              )}

              {/* ステージ */}
              <div style={{ marginBottom:16 }}>
                <div style={{ fontSize:'0.68rem', fontWeight:700, color:'var(--gray-400)', marginBottom:8, textTransform:'uppercase', letterSpacing:'0.05em' }}>ステージ</div>
                {STAGES.filter(s => s !== '見送り' || showDormant).map(s => {
                  const cfg = STAGE_CFG[s] || {};
                  const on = filterStages.has(s);
                  return (
                    <label key={s} style={{ display:'flex', alignItems:'center', gap:7, padding:'4px 0', cursor:'pointer' }}>
                      <input type="checkbox" checked={on} onChange={()=>toggleStage(s)} style={{ accentColor:cfg.color, width:13, height:13, cursor:'pointer' }} />
                      <span style={{ fontSize:'0.78rem', color:on?cfg.color:'var(--gray-700)', fontWeight:on?700:400, flex:1 }}>{s}</span>
                    </label>
                  );
                })}
                <label style={{ display:'flex', alignItems:'center', gap:7, padding:'4px 0', cursor:'pointer', marginTop:6, borderTop:'1px solid var(--gray-200)', paddingTop:8 }}>
                  <input type="checkbox" checked={showDormant} onChange={e=>{setShowDormant(e.target.checked);load({showDormant:e.target.checked});}} style={{ width:13, height:13, cursor:'pointer' }} />
                  <span style={{ fontSize:'0.75rem', color:'var(--gray-400)' }}>見送りを表示</span>
                </label>
              </div>

              {/* ヨミ */}
              <div style={{ marginBottom:16 }}>
                <div style={{ fontSize:'0.68rem', fontWeight:700, color:'var(--gray-400)', marginBottom:8, textTransform:'uppercase', letterSpacing:'0.05em' }}>ヨミ</div>
                {['S 90％','A 70％','B 50％','C 30％','D 15％','E 5％'].map(y => {
                  const cfg = YOMI_CFG[y] || {};
                  const on = filterYomis.has(y);
                  return (
                    <label key={y} style={{ display:'flex', alignItems:'center', gap:7, padding:'4px 0', cursor:'pointer' }}>
                      <input type="checkbox" checked={on} onChange={()=>toggleYomi(y)} style={{ accentColor:cfg.color, width:13, height:13, cursor:'pointer' }} />
                      <span style={{ fontSize:'0.78rem', color:on?cfg.color:'var(--gray-700)', fontWeight:on?700:400, flex:1 }}>{y}</span>
                    </label>
                  );
                })}
              </div>

              {/* 担当者 */}
              {salesUsers.length > 0 && (
                <div style={{ marginBottom:16 }}>
                  <div style={{ fontSize:'0.68rem', fontWeight:700, color:'var(--gray-400)', marginBottom:8, textTransform:'uppercase', letterSpacing:'0.05em' }}>担当者</div>
                  <select value={filterSales} onChange={e=>{setFilterSales(e.target.value);load({filterSales:e.target.value});}}
                    style={{ width:'100%', padding:'5px 8px', border:'1.5px solid var(--gray-200)', borderRadius:7, fontSize:'0.78rem', background:'var(--surface-2)', outline:'none' }}>
                    <option value="">全員</option>
                    {salesUsers.map(u=><option key={u} value={u}>{u}</option>)}
                  </select>
                  {filterSales && (
                    <button onClick={() => setNaToday(v => !v)}
                      title="NextActionが今日の案件のみ表示"
                      style={{ marginTop:6, width:'100%', padding:'5px 8px', borderRadius:7, fontSize:'0.74rem', fontWeight:700, cursor:'pointer',
                        border:`1px solid ${naToday?'#f59e0b':'var(--gray-200)'}`,
                        background: naToday?'#fef3c7':'var(--surface)',
                        color: naToday?'#92400e':'var(--gray-600)' }}>
                      📌 今日やることのみ {naToday ? 'ON' : 'OFF'}
                    </button>
                  )}
                </div>
              )}
            </div>
          )}
        </div>

        {/* トグルボタン */}
        <button onClick={()=>setSidebarOpen(v=>!v)}
          style={{ width:18, flexShrink:0, background:'var(--surface-2)', border:'none', borderRight:'1px solid var(--gray-200)', cursor:'pointer', color:'var(--gray-400)', fontSize:10, padding:0 }}>
          {sidebarOpen?'◀':'▶'}
        </button>

        {/* ── 案件リスト ── */}
        <div style={{ flex:1, overflowY:'auto', padding:'10px 12px' }}>
          {loading ? (
            <div style={{ textAlign:'center', padding:60, color:'var(--gray-400)' }}>読み込み中…</div>
          ) : deals.length === 0 ? (
            <div style={{ textAlign:'center', padding:60, color:'var(--gray-400)' }}>
              <div style={{ fontSize:'1.5rem', marginBottom:8 }}>📋</div>
              案件が見つかりません
            </div>
          ) : (
            <div style={{ display:'flex', flexDirection:'column', gap:4 }}>
              {deals.map(d => {
                const stageCfg = STAGE_CFG[d.stage] || {};
                const yomiCfg  = YOMI_CFG[d.yomi]  || {};
                const days = daysSince(d.updated_at);
                const stale = days >= 14 && d.status === 'active';
                const naDate = d.next_action_date ? String(d.next_action_date).slice(0,10) : '';
                const naIsToday = naDate === todayYmdStr;
                const overdue = naDate && naDate < todayYmdStr;
                const isDormant = d.status === 'dormant';
                const borderColor = naIsToday ? '#fcd34d' : overdue ? '#fca5a5' : stale ? '#fcd34d' : 'var(--gray-200)';
                const rowBg = naIsToday ? '#fffbeb' : 'var(--surface)';

                return (
                  <div key={d.id}
                    style={{ background:rowBg, borderRadius:10, border:`1px solid ${borderColor}`,
                      padding:'10px 14px', cursor:'pointer', display:'flex', alignItems:'center', gap:12,
                      opacity:isDormant?0.6:1, transition:'all 0.1s', boxShadow:'0 1px 2px rgba(0,0,0,0.03)' }}
                    onMouseEnter={e=>{e.currentTarget.style.borderColor='#c7d2fe';e.currentTarget.style.boxShadow='0 2px 8px rgba(99,102,241,0.08)';}}
                    onMouseLeave={e=>{e.currentTarget.style.borderColor=borderColor;e.currentTarget.style.boxShadow='0 1px 2px rgba(0,0,0,0.03)';}}
                    onClick={() => navigate(`/crm/customers/${d.customer_id}`)}>

                    <Initial name={d.customer_name} />

                    {/* 会社・案件名 */}
                    <div style={{ flex:'0 0 200px', minWidth:0 }}>
                      <div style={{ fontWeight:700, fontSize:'0.85rem', color:'var(--gray-900)', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{d.customer_name}</div>
                      <div style={{ fontSize:'0.72rem', color:'var(--gray-500)', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', marginTop:1 }}>{d.name}</div>
                      <div style={{ fontSize:'0.65rem', color:'var(--gray-400)', marginTop:1 }}>
                        {[d.prefecture, d.industry].filter(Boolean).join(' · ')}
                      </div>
                    </div>

                    {/* ステージ */}
                    <div style={{ flex:'0 0 90px' }}>
                      <span style={{ fontSize:'0.72rem', fontWeight:700, padding:'3px 10px', borderRadius:99,
                        background:stageCfg.bg, color:stageCfg.color, border:`1px solid ${stageCfg.border}`, whiteSpace:'nowrap' }}>
                        {d.stage}
                      </span>
                    </div>

                    {/* 担当者 */}
                    <div style={{ flex:'0 0 72px', minWidth:0 }}>
                      {d.sales_person_name
                        ? <span style={{ fontSize:'0.72rem', color:'var(--gray-700)', fontWeight:600,
                            background:'var(--surface-2)', padding:'2px 7px', borderRadius:5, display:'inline-block',
                            overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', maxWidth:'100%' }}>
                            {d.sales_person_name.split(/[\s　]/)[0]}
                          </span>
                        : <span style={{ color:'var(--gray-200)', fontSize:'0.72rem' }}>—</span>}
                    </div>

                    {/* ヨミ */}
                    <div style={{ flex:'0 0 85px' }}>
                      {yomiCfg.color
                        ? <span style={{ fontSize:'0.7rem', fontWeight:700, padding:'2px 9px', borderRadius:99,
                            background:yomiCfg.bg, color:yomiCfg.color, whiteSpace:'nowrap' }}>{d.yomi}</span>
                        : <span style={{ color:'var(--gray-200)', fontSize:'0.7rem' }}>—</span>}
                    </div>

                    {/* 金額 */}
                    <div style={{ flex:'0 0 72px', textAlign:'right' }}>
                      <div style={{ fontSize:'0.8rem', fontWeight:700, color:'#1e40af' }}>{fmtM(d.initial_fee||d.monthly_fee)}</div>
                      {d.contract_type && <div style={{ fontSize:'0.62rem', color:'var(--gray-400)' }}>{d.contract_type.replace('一括払い','一括').replace('採用保証','一括')}</div>}
                    </div>

                    {/* NA表示 */}
                    <div style={{ flex:'0 0 160px', minWidth:0 }}>
                      {naDate ? (
                        <>
                          <div style={{ fontSize:'0.7rem', fontWeight:700, color: naIsToday?'#92400e':overdue?'#dc2626':'#0f172a' }}>
                            {naIsToday ? '📌 今日' : overdue ? `⏰ ${naDate.slice(5).replace('-','/')}` : naDate.slice(5).replace('-','/')}
                          </div>
                          {d.next_action_content && (
                            <div style={{ fontSize:'0.66rem', color:'#64748b', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }} title={d.next_action_content}>
                              {d.next_action_content.slice(0, 25)}{d.next_action_content.length > 25 ? '…' : ''}
                            </div>
                          )}
                        </>
                      ) : (
                        <span style={{ fontSize:'0.66rem', color:'var(--gray-300)' }}>NA未設定</span>
                      )}
                    </div>

                    {/* 最終更新 */}
                    <div style={{ flex:'0 0 50px', textAlign:'right' }}>
                      <span style={{ fontSize:'0.66rem', color:stale?'#ef4444':'var(--gray-400)', fontWeight:stale?700:400 }}>
                        {days===0?'今日':`${days}d`}
                      </span>
                    </div>

                    {/* 見送りボタン */}
                    {d.status === 'active' && (
                      <button onClick={e=>{e.stopPropagation();setDormantTarget(d);}}
                        style={{ flexShrink:0, background:'none', border:'1px solid var(--gray-200)', borderRadius:6, cursor:'pointer', color:'var(--gray-400)', fontSize:'0.65rem', padding:'3px 8px', whiteSpace:'nowrap' }}>
                        見送り
                      </button>
                    )}
                    {d.status === 'dormant' && (
                      <button onClick={async e=>{e.stopPropagation();await api.crmRevertDormant(d.id);load();}}
                        style={{ flexShrink:0, background:'none', border:'1px solid #86efac', borderRadius:6, cursor:'pointer', color:'#059669', fontSize:'0.65rem', padding:'3px 8px', whiteSpace:'nowrap' }}>
                        復活
                      </button>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* もっと読み込む */}
      {hasMore && (
        <div style={{ textAlign:'center', padding:'16px 0' }}>
          <button onClick={loadMore} disabled={loadingMore}
            style={{ padding:'8px 28px', border:'1px solid var(--gray-200)', borderRadius:8, background:'var(--surface)', color:'var(--gray-700)', fontSize:'0.85rem', fontWeight:600, cursor: loadingMore ? 'default' : 'pointer' }}>
            {loadingMore ? '読み込み中…' : `さらに読み込む（残り ${totalCount - deals.length}件）`}
          </button>
        </div>
      )}

      {/* 見送りモーダル */}
      {dormantTarget && (
        <DormantModal deal={dormantTarget} onClose={()=>setDormantTarget(null)} onDone={()=>{load();setDormantTarget(null);}} />
      )}

      {/* 顧客追加モーダル */}
      {showCreateModal && (
        <CustomerCreateModal
          form={createForm}
          setForm={setCreateForm}
          onClose={()=>setShowCreateModal(false)}
          onSubmit={handleCreate}
          creating={creating}
        />
      )}
    </div>
  );
}

// ─── 顧客追加モーダル（kintone①と同等の項目） ─────────────────────────
const INDUSTRIES = ['農業・林業','漁業','鉱業・採石業・砂利採取業','建設業','製造業','電気・ガス・熱供給・水道業','情報通信業','運輸業・郵便業','卸売業・小売業','金融業・保険業','不動産業・物品賃貸業','学術研究・専門・技術サービス業','宿泊業・飲食サービス業','生活関連サービス業・娯楽業','教育・学習支援業','医療・福祉','複合サービス事業','サービス業','公務','その他'];
const PREFECTURES = ['北海道','青森県','岩手県','宮城県','秋田県','山形県','福島県','茨城県','栃木県','群馬県','埼玉県','千葉県','東京都','神奈川県','新潟県','富山県','石川県','福井県','山梨県','長野県','岐阜県','静岡県','愛知県','三重県','滋賀県','京都府','大阪府','兵庫県','奈良県','和歌山県','鳥取県','島根県','岡山県','広島県','山口県','徳島県','香川県','愛媛県','高知県','福岡県','佐賀県','長崎県','熊本県','大分県','宮崎県','鹿児島県','沖縄県'];
const EMP_COUNTS = ['1-10','11-50','51-100','101-300','301-500','501-1000','1000-'];

function CustomerCreateModal({ form, setForm, onClose, onSubmit, creating }) {
  const F = (k) => (e) => setForm(p => ({ ...p, [k]: e.target.value }));
  const inputSx = { width:'100%', boxSizing:'border-box', padding:'7px 10px', border:'1.5px solid var(--gray-200)', borderRadius:8, fontSize:'0.82rem', outline:'none' };
  const labelSx = { display:'block', fontSize:'0.7rem', fontWeight:700, color:'var(--gray-700)', marginBottom:3 };

  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.5)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }} onClick={onClose}>
      <div style={{ background:'var(--surface)', borderRadius:14, width:'min(640px,94vw)', maxHeight:'90vh', overflow:'hidden', boxShadow:'0 20px 50px rgba(0,0,0,0.2)', display:'flex', flexDirection:'column' }} onClick={e=>e.stopPropagation()}>
        <div style={{ padding:'14px 20px', borderBottom:'1px solid var(--gray-200)', display:'flex', alignItems:'center', justifyContent:'space-between' }}>
          <div style={{ fontWeight:800, color:'var(--gray-900)' }}>顧客を追加</div>
          <div style={{ fontSize:'0.66rem', color:'var(--gray-400)' }}>kintone顧客情報 と同期項目</div>
        </div>
        <div style={{ padding:'16px 20px', display:'flex', flexDirection:'column', gap:10, overflowY:'auto' }}>
          {/* 基本 */}
          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10 }}>
            <div style={{ gridColumn:'1 / -1' }}>
              <label style={labelSx}>会社名 <span style={{ color:'#ef4444' }}>*</span></label>
              <input value={form.name} onChange={F('name')} placeholder="例: 株式会社○○" style={inputSx} />
            </div>
            <div>
              <label style={labelSx}>会社名（略称・株式会社抜き）</label>
              <input value={form.nameShort} onChange={F('nameShort')} placeholder="例: ○○" style={inputSx} />
            </div>
            <div>
              <label style={labelSx}>INREVO担当者</label>
              <input value={form.inrevoPerson} onChange={F('inrevoPerson')} placeholder="例: 外山 雄大" style={inputSx} />
            </div>
            <div>
              <label style={labelSx}>業種</label>
              <select value={form.industry} onChange={F('industry')} style={inputSx}>
                <option value="">—</option>
                {INDUSTRIES.map(v=><option key={v} value={v}>{v}</option>)}
              </select>
            </div>
            <div>
              <label style={labelSx}>都道府県</label>
              <select value={form.prefecture} onChange={F('prefecture')} style={inputSx}>
                <option value="">—</option>
                {PREFECTURES.map(v=><option key={v} value={v}>{v}</option>)}
              </select>
            </div>
            <div>
              <label style={labelSx}>従業員規模</label>
              <select value={form.employeeCount} onChange={F('employeeCount')} style={inputSx}>
                <option value="">—</option>
                {EMP_COUNTS.map(v=><option key={v} value={v}>{v}</option>)}
              </select>
            </div>
            <div>
              <label style={labelSx}>会社HP</label>
              <input value={form.website} onChange={F('website')} placeholder="https://" style={inputSx} />
            </div>
          </div>
          {/* 住所 */}
          <div style={{ display:'grid', gridTemplateColumns:'150px 1fr', gap:10 }}>
            <div>
              <label style={labelSx}>郵便番号</label>
              <input value={form.postalCode} onChange={F('postalCode')} placeholder="000-0000" style={inputSx} />
            </div>
            <div>
              <label style={labelSx}>会社住所</label>
              <input value={form.address} onChange={F('address')} placeholder="例: 東京都中央区..." style={inputSx} />
            </div>
          </div>
          {/* 流入 */}
          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10 }}>
            <div>
              <label style={labelSx}>流入日</label>
              <input type="date" value={form.inflowDate} onChange={F('inflowDate')} style={inputSx} />
            </div>
            <div>
              <label style={labelSx}>流入経路</label>
              <input value={form.inflowSource} onChange={F('inflowSource')} placeholder="例: レディクル / リファラル" style={inputSx} />
            </div>
          </div>
          {/* サービスLP */}
          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10 }}>
            <div>
              <label style={labelSx}>サービスLP URL ①</label>
              <input value={form.serviceLpUrl1} onChange={F('serviceLpUrl1')} placeholder="https://" style={inputSx} />
            </div>
            <div>
              <label style={labelSx}>サービスLP URL ②</label>
              <input value={form.serviceLpUrl2} onChange={F('serviceLpUrl2')} placeholder="https://" style={inputSx} />
            </div>
          </div>
          {/* 事業内容・メモ */}
          <div>
            <label style={labelSx}>事業内容</label>
            <textarea value={form.businessDescription} onChange={F('businessDescription')} rows={2}
              style={{ ...inputSx, fontFamily:'inherit', resize:'vertical' }} />
          </div>
          <div>
            <label style={labelSx}>メモ</label>
            <textarea value={form.memo} onChange={F('memo')} rows={2}
              style={{ ...inputSx, fontFamily:'inherit', resize:'vertical' }} />
          </div>
        </div>
        <div style={{ padding:'12px 20px', borderTop:'1px solid var(--gray-200)', display:'flex', justifyContent:'flex-end', gap:8 }}>
          <button onClick={onClose} style={{ padding:'7px 16px', border:'1px solid var(--gray-200)', borderRadius:8, fontSize:'0.82rem', color:'var(--gray-500)', background:'var(--surface)', cursor:'pointer' }}>キャンセル</button>
          <button onClick={onSubmit} disabled={!form.name.trim()||creating}
            style={{ padding:'7px 20px', border:'none', borderRadius:8, background:'#1e40af', color:'#fff', fontSize:'0.82rem', fontWeight:700, cursor:'pointer', opacity:!form.name.trim()||creating?0.6:1 }}>
            {creating?'作成中…':'✓ 作成'}
          </button>
        </div>
      </div>
    </div>
  );
}
