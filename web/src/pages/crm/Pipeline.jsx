import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

// モジュールレベルキャッシュ（ページ遷移後も保持）
let _dealsCache = null;

const YOMI_STAGES = [
  { key: 'アポ化前',      color: '#94a3b8', bg: '#f8fafc', dept: 'MK' },
  { key: 'アポ化済商談前', color: '#64748b', bg: '#f1f5f9', dept: 'BC' },
  { key: 'E 5％',        color: '#a8b5c8', bg: '#f8fafc', dept: 'BC' },
  { key: 'D 15％',       color: '#7c8fa6', bg: '#f0f4f8', dept: 'BC' },
  { key: 'C 30％',       color: '#3b9bdb', bg: '#eff8ff', dept: 'BC' },
  { key: 'B 50％',       color: '#2563eb', bg: '#eff6ff', dept: 'BC' },
  { key: 'A 70％',       color: '#1d4ed8', bg: '#eef2ff', dept: 'BC' },
  { key: 'S 90％',       color: '#7c3aed', bg: '#f5f3ff', dept: 'BC' },
  { key: '受注',          color: '#059669', bg: '#ecfdf5', dept: '受注' },
  { key: '失注',          color: '#dc2626', bg: '#fef2f2', dept: '失注' },
];

const STORAGE_KEY = 'pipeline_filters';
function loadFilters() {
  try {
    const s = localStorage.getItem(STORAGE_KEY);
    if (!s) return null;
    const p = JSON.parse(s);
    return { visibleStages: new Set(p.visibleStages), filterSales: p.filterSales || '' };
  } catch { return null; }
}
function saveFilters(visibleStages, filterSales) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify({ visibleStages: [...visibleStages], filterSales }));
}
const DEFAULT_VISIBLE = new Set(['D 15％','C 30％','B 50％','A 70％','S 90％']);

const STAGE_GROUPS = [
  { label: 'MK',    color: '#94a3b8', stages: ['アポ化前'] },
  { label: 'BC',    color: '#2563eb', stages: ['アポ化済商談前','E 5％','D 15％','C 30％','B 50％','A 70％','S 90％'] },
  { label: '受注',  color: '#059669', stages: ['受注'] },
  { label: '失注',  color: '#dc2626', stages: ['失注'] },
];

function fmt(n) {
  if (!n || Number(n) === 0) return null;
  const num = Number(n);
  if (num >= 1000000) return `¥${Math.round(num/10000).toLocaleString()}万`;
  return `¥${num.toLocaleString()}`;
}

function DealCard({ deal, navigate }) {
  const stage = YOMI_STAGES.find(s => s.key === deal.yomi);
  const color = stage?.color || '#94a3b8';
  const amount = deal.monthly_fee || deal.initial_fee;

  return (
    <div
      draggable
      onDragStart={e => e.dataTransfer.setData('dealId', deal.id)}
      onClick={() => navigate(`/crm/customers/${deal.customer_id}`)}
      style={{
        background: '#fff',
        borderRadius: 10,
        padding: '12px 14px',
        marginBottom: 8,
        cursor: 'grab',
        userSelect: 'none',
        boxShadow: '0 1px 3px rgba(0,0,0,0.07), 0 1px 2px rgba(0,0,0,0.05)',
        border: '1px solid #f1f5f9',
        transition: 'box-shadow 0.15s, transform 0.15s',
        position: 'relative',
        overflow: 'hidden',
      }}
      onMouseEnter={e => { e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.1)'; e.currentTarget.style.transform = 'translateY(-1px)'; }}
      onMouseLeave={e => { e.currentTarget.style.boxShadow = '0 1px 3px rgba(0,0,0,0.07), 0 1px 2px rgba(0,0,0,0.05)'; e.currentTarget.style.transform = ''; }}
    >
      <div style={{ position: 'absolute', top: 0, left: 0, width: 3, height: '100%', background: color, borderRadius: '10px 0 0 10px' }} />
      <div style={{ paddingLeft: 6 }}>
        <div style={{ fontWeight: 700, color: '#111827', fontSize: '0.85rem', marginBottom: 3, lineHeight: 1.4, wordBreak: 'break-all' }}>
          {deal.customer_name}
        </div>
        <div style={{ color: '#6b7280', fontSize: '0.75rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', marginBottom: 6 }}>
          {deal.name}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          {amount
            ? <span style={{ fontSize: '0.78rem', fontWeight: 700, color: color, background: color + '15', padding: '2px 7px', borderRadius: 99 }}>{fmt(amount)}</span>
            : <span />
          }
          {deal.sales_user_id && (
            <span style={{ fontSize: '0.68rem', color: '#9ca3af', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 70 }}>{deal.sales_user_id}</span>
          )}
        </div>
      </div>
    </div>
  );
}

export default function Pipeline({ embedded = false }) {
  const navigate = useNavigate();
  const [deals, setDeals] = useState(_dealsCache || []);
  const [loading, setLoading] = useState(!_dealsCache);
  const saved = loadFilters();
  const [visibleStages, setVisibleStages] = useState(saved?.visibleStages ?? DEFAULT_VISIBLE);
  const [filterSales, setFilterSales] = useState(saved?.filterSales ?? '');
  const [dragOverKey, setDragOverKey] = useState(null);
  const kanbanRef = useRef(null);

  const load = async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      const r = await api.crmDeals({ status: 'all', limit: 2000 });
      _dealsCache = r.deals || [];
      setDeals(_dealsCache);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  };

  useEffect(() => {
    // キャッシュがあれば即表示、バックグラウンドで更新
    if (_dealsCache) load(true);
    else load(false);
  }, []);

  const toggleStage = (key) => {
    setVisibleStages(prev => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      saveFilters(next, filterSales);
      return next;
    });
  };

  const handleSalesFilter = (val) => {
    setFilterSales(val);
    saveFilters(visibleStages, val);
  };

  const handleDrop = async (targetYomi, e) => {
    e.preventDefault();
    const dealId = e.dataTransfer.getData('dealId');
    setDragOverKey(null);
    if (!dealId) return;
    const deal = deals.find(d => d.id === dealId);
    if (!deal || deal.yomi === targetYomi) return;
    setDeals(prev => prev.map(d => d.id === dealId ? { ...d, yomi: targetYomi } : d));
    await api.crmUpdateDeal(dealId, { yomi: targetYomi }).catch(() => load());
  };

  const salesUsers = [...new Set(deals.map(d => d.sales_user_id).filter(Boolean))].sort();
  const filtered = deals.filter(d => !filterSales || d.sales_user_id === filterSales);
  const visibleList = YOMI_STAGES.filter(s => visibleStages.has(s.key));

  const stageDeals = (yomi) => filtered.filter(d => d.yomi === yomi);
  const stageTotal = (yomi) => stageDeals(yomi).reduce((s, d) => s + (Number(d.monthly_fee) || Number(d.initial_fee) || 0), 0);

  if (loading && deals.length === 0) return <div className="page-loading">読み込み中...</div>;

  const activeCount = filtered.filter(d => visibleStages.has(d.yomi)).length;
  const totalAmount = visibleList.reduce((s, stage) => s + stageTotal(stage.key), 0);

  return (
    <div style={{ padding: embedded ? '16px 20px' : '24px 28px', height: embedded ? '100%' : '100vh', display: 'flex', flexDirection: 'column', background: '#f8fafc', overflow: 'hidden' }}>
      {/* ヘッダー */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 20 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: '1.5rem', fontWeight: 800, color: '#111827', letterSpacing: '-0.02em' }}>パイプライン</h1>
          <p style={{ margin: '4px 0 0', color: '#6b7280', fontSize: '0.85rem' }}>商談をヨミ別に管理</p>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <div style={{ textAlign: 'right', marginRight: 8 }}>
            <div style={{ fontSize: '1.1rem', fontWeight: 800, color: '#1d4ed8' }}>{activeCount}件</div>
            {totalAmount > 0 && <div style={{ fontSize: '0.75rem', color: '#6b7280' }}>{fmt(totalAmount)}</div>}
          </div>
          <button className="btn-primary" onClick={() => navigate('/crm/customers')}>顧客一覧</button>
        </div>
      </div>

      {/* フィルター・ステージ制御 */}
      <div style={{ background: '#fff', borderRadius: 12, padding: '12px 16px', marginBottom: 20, boxShadow: '0 1px 3px rgba(0,0,0,0.06)', border: '1px solid #f1f5f9' }}>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
          <select value={filterSales} onChange={e => handleSalesFilter(e.target.value)}
            style={{ padding: '6px 10px', border: '1px solid #e5e7eb', borderRadius: 8, fontSize: '0.82rem', background: '#fff', color: '#374151' }}>
            <option value="">全担当者</option>
            {salesUsers.map(u => <option key={u} value={u}>{u}</option>)}
          </select>
          <div style={{ width: 1, height: 24, background: '#e5e7eb' }} />
          {STAGE_GROUPS.map(group => (
            <div key={group.label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              {group.stages.map(key => {
                const s = YOMI_STAGES.find(x => x.key === key);
                const on = visibleStages.has(key);
                return (
                  <button key={key} onClick={() => toggleStage(key)}
                    style={{ fontSize: '0.72rem', padding: '3px 8px', borderRadius: 99, border: 'none', cursor: 'pointer', fontWeight: on ? 700 : 400, transition: 'all 0.1s',
                      background: on ? s?.color : '#f1f5f9',
                      color: on ? '#fff' : '#9ca3af',
                    }}>
                    {key}
                  </button>
                );
              })}
            </div>
          ))}
        </div>
      </div>

      {/* カンバン - flex:1で残り高さを使い切り、横スクロールを常時表示 */}
      <div ref={kanbanRef} style={{ display: 'flex', gap: 12, flex: 1, overflowX: 'auto', overflowY: 'hidden', alignItems: 'stretch', paddingBottom: 4 }}>
        {visibleList.map(stage => {
          const cards = stageDeals(stage.key);
          const total = stageTotal(stage.key);
          const isDragOver = dragOverKey === stage.key;
          // 1ステージのみ → 全幅・2カラムグリッド。複数ステージ → 固定幅・1カラム
          const singleStage = visibleList.length === 1;
          const fewStages = visibleList.length <= 4;
          const isWide = singleStage;
          const colW = singleStage ? undefined : (fewStages ? undefined : 230);
          return (
            <div key={stage.key}
              onDragOver={e => { e.preventDefault(); setDragOverKey(stage.key); }}
              onDragLeave={e => { if (!e.currentTarget.contains(e.relatedTarget)) setDragOverKey(null); }}
              onDrop={e => handleDrop(stage.key, e)}
              style={{
                flex: fewStages ? '1 1 0' : undefined,
                flexShrink: fewStages ? undefined : 0,
                width: colW,
                minWidth: fewStages ? 200 : undefined,
                background: isDragOver ? stage.bg : '#fff',
                borderRadius: 12,
                display: 'flex',
                flexDirection: 'column',
                border: `2px solid ${isDragOver ? stage.color : '#f1f5f9'}`,
                padding: '0 0 8px',
                transition: 'all 0.15s',
                boxShadow: isDragOver ? `0 0 0 3px ${stage.color}30` : '0 1px 3px rgba(0,0,0,0.06)',
              }}>
              {/* カラムヘッダー */}
              <div style={{
                padding: '12px 14px 10px',
                borderBottom: `2px solid ${stage.color}20`,
                background: `linear-gradient(135deg, ${stage.color}12, ${stage.color}05)`,
                borderRadius: '10px 10px 0 0',
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                  <span style={{ fontWeight: 800, fontSize: '0.85rem', color: stage.color }}>{stage.key}</span>
                  <span style={{ fontWeight: 700, fontSize: '0.78rem', background: stage.color, color: '#fff', padding: '1px 7px', borderRadius: 99 }}>
                    {cards.length}
                  </span>
                </div>
                {total > 0 && (
                  <div style={{ fontSize: '0.72rem', color: stage.color, fontWeight: 600, opacity: 0.8 }}>{fmt(total)}</div>
                )}
              </div>
              {/* カード一覧 - 列内スクロール */}
              <div style={{
                flex: 1,
                overflowY: 'auto',
                padding: '8px 8px 8px',
                display: singleStage ? 'grid' : 'block',
                gridTemplateColumns: singleStage ? 'repeat(auto-fill, minmax(220px, 1fr))' : undefined,
                gap: singleStage ? '0 8px' : undefined,
                alignContent: 'start',
              }}>
                {cards.length === 0
                  ? <div style={{ gridColumn: '1/-1', color: '#d1d5db', fontSize: '0.75rem', textAlign: 'center', padding: '16px 0', fontStyle: 'italic' }}>なし</div>
                  : cards.map(deal => <DealCard key={deal.id} deal={deal} navigate={navigate} />)
                }
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
