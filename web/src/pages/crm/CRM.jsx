import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import CustomerList from './CustomerList';
import SalesPerformance from './SalesPerformance';
import CrmDashboard from './CrmDashboard';
import { api } from '../../api/client';

const TARGET_REPS  = ['山本 夏乃', '板金 慎太郎', '萩原 隼人', '藤原 一矢', '野村 尭弘'];
const ROLE_NAMES   = ['役職無し', 'Lead', 'Sub Manager', 'Sub Chief', 'Chief', 'Sub Expert', 'Expert'];
const ROLE_OPTIONS = ['', ...ROLE_NAMES];

const fmt = (n) => n ? `¥${Math.round(Number(n)).toLocaleString()}` : '—';
const YOMI_COLOR = { 'S 90％':'#7c3aed','A 70％':'#1d4ed8','B 50％':'#0891b2','C 30％':'#059669' };

// 表示から除外するスタッフ
const YOMI_EXCLUDED = ['外山 雄大', '添田 剛'];

const YOMI_CFG_PANEL = {
  'S 90％': { color:'#4f46e5', bg:'#ede9fe', border:'#c4b5fd' },
  'A 70％': { color:'#1d4ed8', bg:'#dbeafe', border:'#93c5fd' },
  'B 50％': { color:'#0891b2', bg:'#cffafe', border:'#67e8f9' },
  'C 30％': { color:'#059669', bg:'#d1fae5', border:'#6ee7b7' },
};

// ── 案件詳細モーダル ──────────────────────────────────────────
function DealDetailModal({ deal, onClose, onMemoSave }) {
  const [memo, setMemo] = useState(deal.sales_memo || '');
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const cfg = YOMI_CFG_PANEL[deal.yomi] || { color:'#64748b', bg:'#f8fafc', border:'#e2e8f0' };
  const amt = Number(deal.monthly_fee || deal.initial_fee || 0) * 1.1;
  const daysSince = Math.floor((Date.now() - new Date(deal.updated_at)) / 86400000);

  const handleSave = async () => {
    setSaving(true);
    try {
      await api.crmUpdateDeal(deal.id, { salesMemo: memo });
      onMemoSave(deal.id, memo);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (e) { console.error(e); }
    finally { setSaving(false); }
  };

  const openCustomer = () => {
    window.open(`/dashboard/crm/customers/${deal.customer_id}`, '_blank', 'noopener');
  };

  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.55)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }}
      onClick={onClose}>
      <div style={{ background:'#fff', borderRadius:16, width:'min(520px,92vw)', maxHeight:'82vh', overflow:'hidden', display:'flex', flexDirection:'column', boxShadow:'0 32px 64px rgba(0,0,0,0.25)' }}
        onClick={e => e.stopPropagation()}>

        {/* ヘッダー */}
        <div style={{ padding:'16px 20px', borderBottom:'1px solid #f1f5f9', display:'flex', justifyContent:'space-between', alignItems:'flex-start' }}>
          <div style={{ flex:1, minWidth:0 }}>
            <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:5, flexWrap:'wrap' }}>
              <span style={{ fontSize:'0.75rem', fontWeight:700, padding:'3px 10px', borderRadius:99, background:cfg.bg, color:cfg.color, border:`1px solid ${cfg.border}` }}>{deal.yomi}</span>
              {deal.contract_type && <span style={{ fontSize:'0.7rem', color:'#6366f1', background:'#eef2ff', borderRadius:5, padding:'2px 7px', fontWeight:600 }}>{deal.contract_type}</span>}
              {amt > 0 && <span style={{ fontSize:'0.7rem', fontWeight:700, color:'#059669' }}>{fmt(amt)}</span>}
              <span style={{ fontSize:'0.68rem', color: daysSince >= 14 ? '#dc2626' : '#94a3b8', marginLeft:'auto' }}>
                {daysSince === 0 ? '今日更新' : `${daysSince}日前更新`}
              </span>
            </div>
            <div style={{ fontWeight:800, fontSize:'1.05rem', color:'#0f172a', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{deal.customer_name}</div>
            <div style={{ fontSize:'0.73rem', color:'#94a3b8', marginTop:2, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{deal.name}</div>
          </div>
          <button onClick={onClose} style={{ width:28, height:28, borderRadius:8, background:'#f1f5f9', border:'none', cursor:'pointer', color:'#64748b', fontSize:16, flexShrink:0, marginLeft:10 }}>×</button>
        </div>

        <div style={{ overflowY:'auto', padding:'16px 20px', display:'flex', flexDirection:'column', gap:12 }}>
          {/* 次回アクション */}
          {deal.next_action_date && (
            <div style={{ padding:'10px 14px', background:'#fef9c3', borderRadius:10, border:'1px solid #fcd34d' }}>
              <div style={{ fontSize:'0.65rem', color:'#92400e', fontWeight:700, marginBottom:2 }}>次回アクション</div>
              <div style={{ fontSize:'0.85rem', fontWeight:700, color:'#78350f' }}>{deal.next_action_date}</div>
              {deal.next_action_content && <div style={{ fontSize:'0.78rem', color:'#92400e', marginTop:3 }}>{deal.next_action_content}</div>}
            </div>
          )}

          {/* 商談メモ */}
          <div>
            <div style={{ fontSize:'0.72rem', fontWeight:700, color:'#374151', marginBottom:6 }}>商談メモ</div>
            <textarea value={memo} onChange={e => setMemo(e.target.value)} rows={6}
              placeholder="メモを入力…"
              style={{ width:'100%', boxSizing:'border-box', padding:'10px 12px', border:'1.5px solid #e2e8f0', borderRadius:8, fontSize:'0.85rem', outline:'none', resize:'vertical', lineHeight:1.7 }}
              onFocus={e => e.target.style.borderColor='#6366f1'}
              onBlur={e => e.target.style.borderColor='#e2e8f0'} />
          </div>
        </div>

        <div style={{ padding:'12px 20px', borderTop:'1px solid #f1f5f9', display:'flex', gap:8, alignItems:'center' }}>
          <button onClick={openCustomer}
            style={{ display:'flex', alignItems:'center', gap:6, padding:'7px 14px', border:'1.5px solid #e2e8f0', borderRadius:8, background:'#fff', color:'#374151', fontSize:'0.8rem', fontWeight:600, cursor:'pointer' }}>
            <span style={{ fontSize:'0.85rem' }}>↗</span> 顧客詳細（別タブ）
          </button>
          <div style={{ marginLeft:'auto', display:'flex', alignItems:'center', gap:8 }}>
            {saved && <span style={{ fontSize:'0.75rem', color:'#059669', fontWeight:600 }}>保存しました</span>}
            <button onClick={handleSave} disabled={saving}
              style={{ padding:'7px 20px', background:saving?'#94a3b8':'#1e40af', color:'#fff', border:'none', borderRadius:8, fontSize:'0.82rem', fontWeight:700, cursor:'pointer' }}>
              {saving ? '保存中…' : 'メモを保存'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── ヨミ管理パネル ──────────────────────────────────────────
function YomiPanel({ full = false }) {
  const [yomiKanri, setYomiKanri] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedDeal, setSelectedDeal] = useState(null);
  const [memoCache, setMemoCache] = useState({});
  const [filterStaff, setFilterStaff] = useState('');

  useEffect(() => {
    Promise.all([api.crmYomiKanri(), api.crmMyInfo()])
      .then(([r, me]) => {
        setYomiKanri(r);
        const init = {};
        Object.values(r.byStaff).flat().forEach(d => { init[d.id] = d.sales_memo || ''; });
        setMemoCache(init);
        // 自分の名前がスタッフ一覧に含まれていればデフォルトフィルタにセット
        if (me.displayName) {
          const myName = me.displayName;
          const matched = Object.keys(r.byStaff).find(name =>
            name.includes(myName) || myName.includes(name.split('/')[0].trim())
          );
          if (matched) setFilterStaff(matched);
        }
      }).catch(() => {}).finally(() => setLoading(false));
  }, []);

  if (loading) return <div style={{ padding:40, textAlign:'center', color:'#94a3b8', fontSize:'0.85rem' }}>読み込み中…</div>;
  if (!yomiKanri) return null;

  const allEntries = Object.entries(yomiKanri.byStaff)
    .filter(([name, d]) => d.length > 0 && !YOMI_EXCLUDED.some(ex => name.includes(ex.split(' ')[0])))
    .sort((a,b) => b[1].length - a[1].length);

  const staffOptions = allEntries.map(([name]) => name);
  const entries = filterStaff ? allEntries.filter(([name]) => name === filterStaff) : allEntries;
  const totalFiltered = entries.reduce((s,[,d]) => s + d.length, 0);

  const DealCard = ({ d, compact = false }) => {
    const cfg = YOMI_CFG_PANEL[d.yomi] || { color:'#64748b', bg:'#f8fafc', border:'#e2e8f0' };
    const amt = Number(d.monthly_fee || d.initial_fee || 0) * 1.1;
    const memo = memoCache[d.id] ?? d.sales_memo ?? '';
    const daysSince = Math.floor((Date.now() - new Date(d.updated_at)) / 86400000);
    const stale = daysSince >= 14;
    const [localMemo, setLocalMemo] = useState(memo);
    const [memoFocused, setMemoFocused] = useState(false);
    const [savingInline, setSavingInline] = useState(false);

    const saveInlineMemo = async () => {
      if (localMemo === memo) { setMemoFocused(false); return; }
      setSavingInline(true);
      try {
        await api.crmUpdateDeal(d.id, { salesMemo: localMemo });
        setMemoCache(p => ({ ...p, [d.id]: localMemo }));
      } catch (e) { console.error(e); }
      finally { setSavingInline(false); setMemoFocused(false); }
    };

    const badges = (
      <div style={{ display:'flex', gap:5, marginTop: compact?3:4, alignItems:'center', flexWrap:'wrap' }}>
        <span style={{ fontSize: compact?'0.68rem':'0.73rem', fontWeight:700, padding: compact?'1px 7px':'2px 9px', borderRadius:99, background:cfg.bg, color:cfg.color, border:`1px solid ${cfg.border}` }}>{d.yomi}</span>
        {d.contract_type && <span style={{ fontSize:'0.68rem', color:'#6366f1', background:'#eef2ff', borderRadius:4, padding:'1px 6px', fontWeight:600 }}>{d.contract_type}</span>}
        {stale && <span style={{ fontSize:'0.65rem', color:'#dc2626', fontWeight:700 }}>{daysSince}日未更新</span>}
      </div>
    );

    const inlineMemo = (
      <div onClick={e => e.stopPropagation()} style={{ marginTop:7 }}>
        <textarea
          value={localMemo}
          onChange={e => setLocalMemo(e.target.value)}
          onFocus={() => setMemoFocused(true)}
          onBlur={saveInlineMemo}
          onKeyDown={e => { if (e.key === 'Escape') { setLocalMemo(memo); setMemoFocused(false); } }}
          placeholder="メモを入力…"
          rows={memoFocused ? 3 : 1}
          style={{ width:'100%', boxSizing:'border-box', padding:'5px 8px', fontSize:'0.75rem', border:`1px solid ${memoFocused?'#6366f1':'#e2e8f0'}`, borderRadius:6, outline:'none', resize:'none', lineHeight:1.5, color: localMemo?'#374151':'#94a3b8', background: memoFocused?'#fff':'#f8fafc', transition:'all 0.15s' }} />
        {memoFocused && (
          <div style={{ fontSize:'0.62rem', color:'#94a3b8', marginTop:2 }}>
            {savingInline ? '保存中…' : 'Enterで改行 / フォーカスを外すと保存'}
          </div>
        )}
      </div>
    );

    if (compact) {
      return (
        <div style={{ padding:'10px 14px', borderBottom:'1px solid #f8fafc' }}>
          <div style={{ display:'flex', alignItems:'flex-start', gap:8 }}>
            <div style={{ flex:1, minWidth:0 }}>
              <div onClick={() => setSelectedDeal(d)} style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', cursor:'pointer' }}>{d.customer_name}</div>
              {badges}
              {inlineMemo}
            </div>
            <div style={{ textAlign:'right', flexShrink:0 }}>
              {amt > 0 && <div style={{ fontSize:'0.78rem', fontWeight:700, color:'#059669' }}>{fmt(amt)}</div>}
            </div>
          </div>
        </div>
      );
    }

    return (
      <div style={{ padding:'14px 16px', borderBottom:'1px solid #f1f5f9' }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', gap:10 }}>
          <div style={{ flex:1, minWidth:0 }}>
            <div onClick={() => setSelectedDeal(d)} style={{ fontWeight:800, fontSize:'0.92rem', color:'#0f172a', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', cursor:'pointer' }}>{d.customer_name}</div>
            {badges}
            {inlineMemo}
          </div>
          <div style={{ textAlign:'right', flexShrink:0 }}>
            {amt > 0 && <div style={{ fontWeight:800, fontSize:'0.88rem', color:'#059669' }}>{fmt(amt)}</div>}
            <div onClick={() => setSelectedDeal(d)} style={{ fontSize:'0.65rem', color:'#94a3b8', marginTop:2, cursor:'pointer' }}>詳細 →</div>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%', background:'#f8fafc' }}>
      <div style={{ padding:'10px 20px', background:'#fff', borderBottom:'1px solid #e2e8f0', flexShrink:0, display:'flex', alignItems:'center', gap:12 }}>
        <div>
          <div style={{ fontWeight:700, fontSize:'0.92rem', color:'#0f172a' }}>ヨミ管理</div>
          <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>C以上 進行中 {totalFiltered}件</div>
        </div>
        <select value={filterStaff} onChange={e => setFilterStaff(e.target.value)}
          style={{ marginLeft:'auto', padding:'5px 10px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.8rem', background:'#fff', color:'#374151', outline:'none', cursor:'pointer' }}>
          <option value="">全員</option>
          {staffOptions.map(name => (
            <option key={name} value={name}>{name.split('/')[0].trim()}</option>
          ))}
        </select>
      </div>
      <div style={{ flex:1, overflowY:'auto', padding:'12px 20px' }}>
        {full ? (
          filterStaff ? (
            /* 担当者絞り込み時: 案件カードを全幅グリッドで展開 */
            <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(320px, 1fr))', gap:10 }}>
              {entries.flatMap(([, sDeals]) => sDeals).map(d => (
                <div key={d.id} style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden' }}>
                  <DealCard d={d} />
                </div>
              ))}
            </div>
          ) : (
          /* 全員表示: 担当者カードのグリッド */
          <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(360px, 1fr))', gap:14 }}>
            {entries.map(([staff, sDeals]) => (
              <div key={staff} style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden' }}>
                <div style={{ padding:'11px 16px', background:'#f8fafc', borderBottom:'1px solid #f1f5f9', display:'flex', alignItems:'center', gap:8 }}>
                  <span style={{ fontWeight:800, fontSize:'0.88rem', color:'#0f172a' }}>{staff.split('/')[0].trim()}</span>
                  <span style={{ fontSize:'0.72rem', background:'#e2e8f0', color:'#64748b', borderRadius:99, padding:'1px 8px', fontWeight:600 }}>{sDeals.length}件</span>
                  <span style={{ fontSize:'0.78rem', color:'#059669', marginLeft:'auto', fontWeight:700 }}>
                    {fmt(sDeals.reduce((s,d) => s + Number(d.monthly_fee||d.initial_fee||0)*1.1, 0))}
                  </span>
                </div>
                <div style={{ maxHeight:500, overflowY:'auto' }}>
                  {sDeals.map(d => <DealCard key={d.id} d={d} />)}
                </div>
              </div>
            ))}
          </div>
          )
        ) : (
          entries.map(([staff, sDeals]) => (
            <div key={staff} style={{ marginBottom:10, background:'#fff', borderRadius:10, border:'1px solid #e2e8f0', overflow:'hidden' }}>
              <div style={{ padding:'8px 14px', background:'#f8fafc', borderBottom:'1px solid #f1f5f9', display:'flex', alignItems:'center', gap:8 }}>
                <span style={{ fontWeight:700, fontSize:'0.82rem', color:'#374151' }}>{staff.split('/')[0].trim()}</span>
                <span style={{ fontSize:'0.7rem', color:'#94a3b8' }}>{sDeals.length}件</span>
                <span style={{ fontSize:'0.72rem', color:'#059669', marginLeft:'auto', fontWeight:600 }}>
                  {fmt(sDeals.reduce((s,d) => s + Number(d.monthly_fee||d.initial_fee||0)*1.1, 0))}
                </span>
              </div>
              {sDeals.map(d => <DealCard key={d.id} d={d} compact />)}
            </div>
          ))
        )}
      </div>

      {selectedDeal && (
        <DealDetailModal
          deal={{ ...selectedDeal, sales_memo: memoCache[selectedDeal.id] ?? selectedDeal.sales_memo }}
          onClose={() => setSelectedDeal(null)}
          onMemoSave={(id, memo) => setMemoCache(p => ({...p, [id]: memo}))}
        />
      )}
    </div>
  );
}

// ── カスタムフィールド設定 ────────────────────────────────────
const FIELD_TYPES = [
  { value:'text',     label:'テキスト' },
  { value:'textarea', label:'メモ（複数行）' },
  { value:'number',   label:'数値' },
  { value:'date',     label:'日付' },
  { value:'select',   label:'選択肢' },
  { value:'checkbox', label:'チェックボックス' },
];

function CustomFieldsManager() {
  const [entity, setEntity] = useState('customer');
  const [fields, setFields] = useState([]);
  const [loading, setLoading] = useState(true);
  const [newField, setNewField] = useState({ field_label:'', field_type:'text', options:'' });
  const [adding, setAdding] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const [editForm, setEditForm] = useState({});

  const load = async (e = entity) => {
    setLoading(true);
    try { const r = await api.crmCustomFields(e); setFields(r.fields || []); }
    finally { setLoading(false); }
  };

  useEffect(() => { load(entity); }, [entity]);

  const handleAdd = async () => {
    if (!newField.field_label.trim()) return;
    setAdding(true);
    try {
      const opts = newField.field_type === 'select'
        ? newField.options.split('\n').map(s => s.trim()).filter(Boolean)
        : [];
      await api.crmCreateCustomField({ entity_type: entity, field_label: newField.field_label, field_type: newField.field_type, options: opts });
      setNewField({ field_label:'', field_type:'text', options:'' });
      load(entity);
    } finally { setAdding(false); }
  };

  const handleDelete = async (id) => {
    if (!window.confirm('このフィールドを削除しますか？\n入力済みのデータは残りますが画面上から見えなくなります。')) return;
    await api.crmDeleteCustomField(id);
    load(entity);
  };

  const handleSaveEdit = async (id) => {
    const opts = editForm.field_type === 'select'
      ? (editForm.options_text || '').split('\n').map(s => s.trim()).filter(Boolean)
      : [];
    await api.crmUpdateCustomField(id, { field_label: editForm.field_label, field_type: editForm.field_type, options: opts });
    setEditingId(null);
    load(entity);
  };

  const handleMoveUp = async (idx) => {
    if (idx === 0) return;
    await api.crmUpdateCustomField(fields[idx].id, { sort_order: idx - 1 });
    await api.crmUpdateCustomField(fields[idx-1].id, { sort_order: idx });
    load(entity);
  };

  const inputStyle = { padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.82rem', outline:'none', background:'#fff' };

  return (
    <div>
      {/* エンティティ切り替え */}
      <div style={{ display:'flex', background:'#f1f5f9', borderRadius:8, padding:3, width:'fit-content', marginBottom:16, gap:2 }}>
        {[['customer','顧客フォーム'],['deal','案件フォーム']].map(([v,l]) => (
          <button key={v} onClick={() => setEntity(v)}
            style={{ padding:'4px 16px', borderRadius:6, border:'none', cursor:'pointer', fontSize:'0.8rem',
              fontWeight:entity===v?700:400, background:entity===v?'#fff':'transparent',
              color:entity===v?'#1e40af':'#64748b', boxShadow:entity===v?'0 1px 3px rgba(0,0,0,0.08)':'none' }}>
            {l}
          </button>
        ))}
      </div>

      {/* 既存フィールド一覧 */}
      {loading ? (
        <div style={{ color:'#94a3b8', fontSize:'0.82rem', padding:'8px 0' }}>読み込み中…</div>
      ) : fields.length === 0 ? (
        <div style={{ color:'#94a3b8', fontSize:'0.82rem', padding:'8px 0' }}>フィールドがありません</div>
      ) : (
        <div style={{ display:'flex', flexDirection:'column', gap:4, marginBottom:16 }}>
          {fields.map((f, idx) => (
            <div key={f.id} style={{ background:'#fff', border:'1px solid #e2e8f0', borderRadius:9, padding:'10px 14px', display:'flex', alignItems:'flex-start', gap:10 }}>
              {/* 並び替えボタン */}
              <div style={{ display:'flex', flexDirection:'column', gap:2, flexShrink:0, marginTop:2 }}>
                <button onClick={() => handleMoveUp(idx)} disabled={idx===0}
                  style={{ padding:'1px 5px', fontSize:'0.65rem', border:'1px solid #e2e8f0', borderRadius:4, background:'#fff', cursor:'pointer', opacity:idx===0?0.3:1 }}>▲</button>
                <button onClick={async()=>{ await api.crmUpdateCustomField(f.id,{sort_order:idx+1}); if(fields[idx+1]) await api.crmUpdateCustomField(fields[idx+1].id,{sort_order:idx}); load(entity); }}
                  disabled={idx===fields.length-1}
                  style={{ padding:'1px 5px', fontSize:'0.65rem', border:'1px solid #e2e8f0', borderRadius:4, background:'#fff', cursor:'pointer', opacity:idx===fields.length-1?0.3:1 }}>▼</button>
              </div>

              {editingId === f.id ? (
                <div style={{ flex:1, display:'flex', flexDirection:'column', gap:6 }}>
                  <input value={editForm.field_label} onChange={e => setEditForm(p=>({...p,field_label:e.target.value}))}
                    style={{ ...inputStyle, width:'100%', boxSizing:'border-box' }} />
                  <select value={editForm.field_type} onChange={e => setEditForm(p=>({...p,field_type:e.target.value}))}
                    style={{ ...inputStyle, width:160 }}>
                    {FIELD_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
                  </select>
                  {editForm.field_type === 'select' && (
                    <textarea value={editForm.options_text} onChange={e => setEditForm(p=>({...p,options_text:e.target.value}))}
                      rows={3} placeholder="選択肢を1行ずつ入力"
                      style={{ ...inputStyle, width:'100%', boxSizing:'border-box', resize:'vertical' }} />
                  )}
                  <div style={{ display:'flex', gap:6 }}>
                    <button onClick={() => handleSaveEdit(f.id)}
                      style={{ padding:'4px 14px', background:'#1e40af', color:'#fff', border:'none', borderRadius:6, fontSize:'0.78rem', fontWeight:600, cursor:'pointer' }}>保存</button>
                    <button onClick={() => setEditingId(null)}
                      style={{ padding:'4px 10px', border:'1px solid #e2e8f0', borderRadius:6, fontSize:'0.78rem', background:'#fff', cursor:'pointer' }}>キャンセル</button>
                  </div>
                </div>
              ) : (
                <>
                  <div style={{ flex:1 }}>
                    <span style={{ fontSize:'0.85rem', fontWeight:600, color:'#0f172a' }}>{f.field_label}</span>
                    <span style={{ fontSize:'0.7rem', color:'#94a3b8', marginLeft:8 }}>
                      {FIELD_TYPES.find(t=>t.value===f.field_type)?.label || f.field_type}
                    </span>
                    {f.field_type === 'select' && f.options?.length > 0 && (
                      <div style={{ fontSize:'0.68rem', color:'#94a3b8', marginTop:2 }}>
                        選択肢: {f.options.join(' / ')}
                      </div>
                    )}
                  </div>
                  <div style={{ display:'flex', gap:6, flexShrink:0 }}>
                    <button onClick={() => { setEditingId(f.id); setEditForm({ field_label:f.field_label, field_type:f.field_type, options_text:(f.options||[]).join('\n') }); }}
                      style={{ padding:'3px 10px', border:'1px solid #e2e8f0', borderRadius:6, fontSize:'0.75rem', background:'#fff', cursor:'pointer', color:'#374151' }}>編集</button>
                    <button onClick={() => handleDelete(f.id)}
                      style={{ padding:'3px 10px', border:'1px solid #fca5a5', borderRadius:6, fontSize:'0.75rem', background:'#fff', cursor:'pointer', color:'#dc2626' }}>削除</button>
                  </div>
                </>
              )}
            </div>
          ))}
        </div>
      )}

      {/* 新規追加フォーム */}
      <div style={{ background:'#f8fafc', border:'1px dashed #e2e8f0', borderRadius:10, padding:'14px 16px' }}>
        <div style={{ fontSize:'0.78rem', fontWeight:700, color:'#374151', marginBottom:10 }}>フィールドを追加</div>
        <div style={{ display:'flex', gap:8, flexWrap:'wrap', alignItems:'flex-start' }}>
          <input value={newField.field_label} onChange={e => setNewField(p=>({...p,field_label:e.target.value}))}
            placeholder="フィールド名（例: 決裁者名）"
            style={{ ...inputStyle, flex:'1 1 160px' }} />
          <select value={newField.field_type} onChange={e => setNewField(p=>({...p,field_type:e.target.value}))}
            style={{ ...inputStyle, flexShrink:0 }}>
            {FIELD_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
          </select>
          <button onClick={handleAdd} disabled={adding || !newField.field_label.trim()}
            style={{ padding:'6px 18px', background:'#1e40af', color:'#fff', border:'none', borderRadius:7, fontSize:'0.82rem', fontWeight:700, cursor:'pointer', opacity:!newField.field_label.trim()?0.5:1 }}>
            追加
          </button>
        </div>
        {newField.field_type === 'select' && (
          <textarea value={newField.options} onChange={e => setNewField(p=>({...p,options:e.target.value}))}
            rows={3} placeholder="選択肢を1行ずつ入力（例: 新卒採用&#10;中途採用）"
            style={{ ...inputStyle, width:'100%', boxSizing:'border-box', marginTop:8, resize:'vertical' }} />
        )}
      </div>
    </div>
  );
}

// ── 選択肢管理 ───────────────────────────────────────────────
const SYSTEM_FIELDS = [
  { name:'sales_person', label:'担当営業', defaults:[
    '藤原 一矢','野村 尭弘','山本 夏乃','板金 慎太郎','萩原 隼人','添田 剛',
    '丸山 彰太','坂本 綾音','鈴木 直輝','長門 絢美','早川 恭平','窪田 健斗',
    '南 晴仁','西木 有理','外山 雄大','Marketing',
  ] },
  { name:'contract_type', label:'契約形態（kintone:支払方式）', defaults:[
    '月額','月額（1st upsell）',
    '採用保証','採用保証（1st upsell）','採用保証（2st upsell）',
    '後払い（媒体費用INREVO持ち)）','後払い（媒体費用クライアント持ち）',
    '変動プラン',
  ] },
  { name:'employment_type', label:'採用形態', defaults:['新卒','中途','アルバイト/インターン','業務委託'] },
  { name:'inflow_source', label:'流入経路', defaults:[
    'UNLIMT','カイマク','GLOXY','BrainNew','株式会社cluein','十方株式会社','株式会社Any Arts',
    'コバプロ','zerowork','セールスボンド株式会社','株式会社Dairi','株式会社mizusashi',
    'マーケティングコミット','ウィロード','Oneissueテレアポ','フォーム','デジオン',
    'カイマクフォーム','ナーズ','株式会社Woltz','りんくといん','FB5000','お手紙',
    'ヒトトレ採用LP','ヒトトレ採用WP','コンバートルA（全訴求）','コンバートルB（月額）',
    'ITトレンド','プロベル','家城（リファラル）','プロパゲート','メルマガ',
    'AIチェックインLP','roomportLP','合同会社ホヌプロジェクト','RASHISA','デジマン',
    '合同会社こころぽーと','ヒトトレ研修LP','ヒトトレ集客LP','キャククル','ボクシル',
    'ミツモア','ピタリク','一括.jp','ストラテ','olly','レディクル','グラハム',
    'OneIssue','新通エスピー','リファラル','SONY','トビタジャパン株式会社','アスティーダ',
    'イシン株式会社','株式会社サイドバレイコンサルティブ','インキュベーターくまもと',
    'セミナー','エージェントテレアポ','企業LOG','代理店','交流会','CxOダイレクト',
    'ツミキテック','X','武井SNS(X:旧Twitter)','マーケメディア','エンパワーエックス',
    'BDRリスト','メタ広告','オンリーストーリー','Sales dex','アポレル','MC経由',
    '代理店開拓（自社アウトバウンド）','資料請求サイト','リスティング広告','TikTok',
  ] },
  { name:'industry', label:'業界（kintone:業界_0）', defaults:[
    '農業、林業','漁業','鉱業、採石業、砂利採取業','建設業','製造業',
    '電気・ガス・熱供給・水道業','情報通信業','運輸業、郵便業','卸売業・小売業',
    '金融業・保険業','不動産業、物品賃貸業','学術研究、専門・技術サービス業',
    '宿泊業、飲食サービス業','生活関連サービス業、娯楽業','教育、学習支援業',
    '医療、福祉','複合サービス事業','サービス業（他に分類されないもの）',
    '公務（他に分類されるものを除く）','分類不能の産業',
  ] },
];

function FieldOptionsManager() {
  const [optionsMap, setOptionsMap] = useState({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState({});
  const [notice, setNotice] = useState({});
  const [newOption, setNewOption] = useState({});

  useEffect(() => {
    api.crmFieldOptions().then(r => {
      const map = r.options || {};
      // デフォルト値でフォールバック
      const init = {};
      for (const f of SYSTEM_FIELDS) init[f.name] = map[f.name] || [...f.defaults];
      setOptionsMap(init);
    }).finally(() => setLoading(false));
  }, []);

  const handleSave = async (fieldName) => {
    setSaving(p => ({...p, [fieldName]: true}));
    try {
      await api.crmUpdateFieldOptions(fieldName, optionsMap[fieldName] || []);
      setNotice(p => ({...p, [fieldName]: '保存しました'}));
      setTimeout(() => setNotice(p => ({...p, [fieldName]: ''})), 2000);
    } catch { setNotice(p => ({...p, [fieldName]: 'エラー'})); }
    setSaving(p => ({...p, [fieldName]: false}));
  };

  const addOption = (fieldName) => {
    const v = (newOption[fieldName] || '').trim();
    if (!v) return;
    setOptionsMap(p => ({...p, [fieldName]: [...(p[fieldName]||[]), v]}));
    setNewOption(p => ({...p, [fieldName]: ''}));
  };

  const removeOption = (fieldName, idx) => {
    setOptionsMap(p => ({...p, [fieldName]: p[fieldName].filter((_,i) => i!==idx)}));
  };

  if (loading) return <div style={{ color:'#94a3b8', fontSize:'0.82rem' }}>読み込み中…</div>;

  const inputStyle = { padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.82rem', outline:'none' };

  return (
    <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:16 }}>
      {SYSTEM_FIELDS.map(f => (
        <div key={f.name} style={{ background:'#fff', border:'1px solid #e2e8f0', borderRadius:12, overflow:'hidden' }}>
          <div style={{ padding:'10px 16px', background:'#f8fafc', borderBottom:'1px solid #f1f5f9', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
            <span style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a' }}>{f.label}</span>
            <div style={{ display:'flex', gap:8, alignItems:'center' }}>
              {notice[f.name] && <span style={{ fontSize:'0.72rem', color:'#059669' }}>{notice[f.name]}</span>}
              <button onClick={() => handleSave(f.name)} disabled={saving[f.name]}
                style={{ padding:'3px 12px', background:'#1e40af', color:'#fff', border:'none', borderRadius:6, fontSize:'0.75rem', fontWeight:600, cursor:'pointer' }}>
                保存
              </button>
            </div>
          </div>
          <div style={{ padding:'10px 12px', display:'flex', flexDirection:'column', gap:5, maxHeight:200, overflowY:'auto' }}>
            {(optionsMap[f.name]||[]).map((opt, idx) => (
              <div key={idx} style={{ display:'flex', alignItems:'center', gap:6 }}>
                <input value={opt} onChange={e => setOptionsMap(p => ({...p, [f.name]: p[f.name].map((o,i)=>i===idx?e.target.value:o)}))}
                  style={{ ...inputStyle, flex:1 }} />
                <button onClick={() => removeOption(f.name, idx)}
                  style={{ padding:'3px 8px', border:'1px solid #fca5a5', borderRadius:5, background:'#fff', color:'#dc2626', fontSize:'0.72rem', cursor:'pointer', flexShrink:0 }}>
                  削除
                </button>
              </div>
            ))}
          </div>
          <div style={{ padding:'8px 12px', borderTop:'1px solid #f1f5f9', display:'flex', gap:6 }}>
            <input value={newOption[f.name]||''} onChange={e => setNewOption(p=>({...p,[f.name]:e.target.value}))}
              onKeyDown={e => e.key==='Enter' && addOption(f.name)}
              placeholder="追加…" style={{ ...inputStyle, flex:1 }} />
            <button onClick={() => addOption(f.name)}
              style={{ padding:'4px 12px', background:'#f1f5f9', border:'1px solid #e2e8f0', borderRadius:6, fontSize:'0.78rem', cursor:'pointer' }}>
              ＋
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}

// ── CRM設定（サイドバー型） ─────────────────────────────────────
function CrmSettings() {
  const [section, setSection] = useState('kpi');
  const [roleTargetRows, setRoleTargetRows] = useState([]);
  const [roleTargets, setRoleTargets]       = useState({});
  const [repRoles, setRepRoles]             = useState({});
  const [period, setPeriod]                 = useState({ prevStart:'', prevEnd:'', currStart:'', currEnd:'' });
  const [saving, setSaving]                 = useState(false);
  const [notice, setNotice]                 = useState('');

  useEffect(() => {
    Promise.all([api.crmRoleTargets(), api.crmRepRoles(), api.crmPeriodSettings()]).then(([rt, rr, ps]) => {
      const rows = rt.targets || [];
      const merged = ROLE_NAMES.map((name, i) => {
        const ex = rows.find(r => r.role_name === name);
        return { role_name: name, monthly_target: Number(ex?.monthly_target || 0), sort_order: i };
      });
      setRoleTargetRows(merged);
      const rtMap = {};
      for (const r of merged) rtMap[r.role_name] = r.monthly_target;
      setRoleTargets(rtMap);
      const rrMap = {};
      for (const r of (rr.repRoles || [])) rrMap[r.rep_name] = r;
      setRepRoles(rrMap);
      const s = ps.settings || {};
      setPeriod({ prevStart:s.prev_start?.split('T')[0]||'', prevEnd:s.prev_end?.split('T')[0]||'', currStart:s.curr_start?.split('T')[0]||'', currEnd:s.curr_end?.split('T')[0]||'' });
    });
  }, []);

  const getEffective = (rep) => {
    const r = repRoles[rep];
    if (r?.monthly_target_override) return Number(r.monthly_target_override);
    return roleTargets[r?.role_name || '役職無し'] || 0;
  };

  const setRepRole     = (rep, role) => setRepRoles(prev => ({ ...prev, [rep]: { ...(prev[rep]||{rep_name:rep}), role_name: role } }));
  const setPrevRepRole = (rep, role) => setRepRoles(prev => ({ ...prev, [rep]: { ...(prev[rep]||{rep_name:rep}), prev_role_name: role } }));
  const setOverride    = (rep, wan)  => setRepRoles(prev => ({ ...prev, [rep]: { ...(prev[rep]||{rep_name:rep}), monthly_target_override: wan===''?null:Number(wan)*10000 } }));
  const setRoleTarget = (roleName, wan) => {
    setRoleTargetRows(prev => prev.map(r => r.role_name===roleName ? {...r, monthly_target: Number(wan)*10000} : r));
    setRoleTargets(prev => ({...prev, [roleName]: Number(wan)*10000}));
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const repArr  = TARGET_REPS.map(rep => ({
        rep_name:rep,
        role_name:repRoles[rep]?.role_name||'',
        monthly_target_override:repRoles[rep]?.monthly_target_override||null,
        prev_role_name:repRoles[rep]?.prev_role_name||'',
        prev_monthly_target_override:repRoles[rep]?.prev_monthly_target_override||null,
      }));
      const roleArr = roleTargetRows.map((r,i) => ({ role_name:r.role_name, monthly_target:r.monthly_target, sort_order:i }));
      await Promise.all([api.crmRepRolesSave(repArr), api.crmRoleTargetsSave(roleArr), api.crmPeriodSettingsSave({ prevStart:period.prevStart, prevEnd:period.prevEnd, currStart:period.currStart, currEnd:period.currEnd })]);
      setNotice('保存しました'); setTimeout(() => setNotice(''), 2500);
    } catch { setNotice('保存に失敗しました'); setTimeout(() => setNotice(''), 3000); }
    setSaving(false);
  };

  const teamTotal = TARGET_REPS.reduce((s, rep) => s + getEffective(rep), 0);

  const SECTIONS = [
    { key:'kpi',     label:'役職・KPI目標', icon:'🎯' },
    { key:'period',  label:'集計期間',      icon:'📅' },
    { key:'fields',  label:'カスタムフィールド', icon:'📋' },
    { key:'options', label:'選択肢管理',    icon:'⚙️' },
  ];

  return (
    <div style={{ display:'flex', height:'100%', overflow:'hidden' }}>
      {/* サイドバー */}
      <div style={{ width:180, borderRight:'1px solid #e2e8f0', background:'#f8fafc', flexShrink:0, padding:'16px 8px', display:'flex', flexDirection:'column', gap:2 }}>
        {SECTIONS.map(s => (
          <button key={s.key} onClick={() => setSection(s.key)}
            style={{ display:'flex', alignItems:'center', gap:8, padding:'8px 12px', borderRadius:8, border:'none', cursor:'pointer', textAlign:'left', fontSize:'0.83rem',
              fontWeight: section===s.key?700:400,
              background: section===s.key?'#fff':'transparent',
              color: section===s.key?'#1e40af':'#374151',
              boxShadow: section===s.key?'0 1px 3px rgba(0,0,0,0.08)':'none' }}>
            <span>{s.icon}</span> {s.label}
          </button>
        ))}
      </div>

      {/* コンテンツ */}
      <div style={{ flex:1, overflow:'auto', padding:'24px 32px' }}>

        {section === 'period' && (<>
          <div style={{ fontWeight:700, fontSize:'0.95rem', color:'#0f172a', marginBottom:4 }}>集計期間</div>
          <div style={{ fontSize:'0.75rem', color:'#94a3b8', marginBottom:16 }}>前期・今期の開始日と終了日を設定します</div>
          <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden', maxWidth:480, marginBottom:20 }}>
            {[[['prevStart','前期 開始'],['prevEnd','前期 終了']],[['currStart','今期 開始'],['currEnd','今期 終了']]].map((row,ri) => (
              <div key={ri} style={{ display:'grid', gridTemplateColumns:'1fr 1fr', borderBottom:ri===0?'1px solid #f1f5f9':'none' }}>
                {row.map(([key,label]) => (
                  <div key={key} style={{ padding:'12px 18px', borderRight:key.endsWith('Start')?'1px solid #f1f5f9':'none' }}>
                    <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginBottom:4 }}>{label}</div>
                    <input type="date" value={period[key]||''} onChange={e => setPeriod(p => ({...p,[key]:e.target.value}))}
                      style={{ width:'100%', padding:'6px 8px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.85rem', outline:'none', boxSizing:'border-box' }} />
                  </div>
                ))}
              </div>
            ))}
          </div>
          <button onClick={handleSave} disabled={saving}
            style={{ padding:'8px 24px', background:saving?'#94a3b8':'#1e40af', color:'#fff', border:'none', borderRadius:8, fontSize:'0.85rem', fontWeight:700, cursor:'pointer' }}>
            {saving?'保存中…':'保存'}
          </button>
          {notice && <span style={{ marginLeft:12, fontSize:'0.8rem', color:notice.includes('失敗')?'#dc2626':'#059669', fontWeight:600 }}>{notice}</span>}
        </>)}

        {section === 'kpi' && (<>
          <div style={{ fontWeight:700, fontSize:'0.95rem', color:'#0f172a', marginBottom:4 }}>役職・KPI目標</div>
          <div style={{ fontSize:'0.75rem', color:'#94a3b8', marginBottom:20 }}>役職別目標は成績ページの昇降格ラインにも使用されます</div>
          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:20, maxWidth:800 }}>
            {/* 役職別目標 */}
            <div>
              <div style={{ fontWeight:600, fontSize:'0.82rem', color:'#374151', marginBottom:10 }}>役職別 月次目標</div>
              <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden' }}>
                {roleTargetRows.map((row,i) => (
                  <div key={row.role_name} style={{ display:'flex', alignItems:'center', gap:10, padding:'9px 14px', borderBottom:i<roleTargetRows.length-1?'1px solid #f8fafc':'none' }}>
                    <span style={{ flex:1, fontSize:'0.83rem', fontWeight:600, color:'#374151' }}>{row.role_name}</span>
                    <input type="number" min="0" step="10"
                      value={row.monthly_target>0?Math.round(row.monthly_target/10000):''}
                      onChange={e => setRoleTarget(row.role_name, e.target.value)} placeholder="0"
                      style={{ width:80, padding:'4px 8px', border:'1px solid #e2e8f0', borderRadius:6, fontSize:'0.85rem', textAlign:'right', outline:'none' }} />
                    <span style={{ fontSize:'0.72rem', color:'#64748b' }}>万/月</span>
                  </div>
                ))}
              </div>
            </div>
            {/* 担当者別 */}
            <div>
              <div style={{ fontWeight:600, fontSize:'0.82rem', color:'#374151', marginBottom:10 }}>担当者別 役職割り当て</div>
              <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden' }}>
                {TARGET_REPS.map((rep,i) => {
                  const [fam, given] = rep.split(/[\s　]/);
                  const roleName = repRoles[rep]?.role_name || '';
                  const overrideRaw = repRoles[rep]?.monthly_target_override;
                  const overrideWan = overrideRaw ? Math.round(Number(overrideRaw)/10000) : '';
                  const effective = getEffective(rep);
                  return (
                    <div key={rep} style={{ padding:'9px 14px', borderBottom:i<TARGET_REPS.length-1?'1px solid #f8fafc':'none' }}>
                      <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:4 }}>
                        <span style={{ width:22, height:22, borderRadius:5, background:'#eef2ff', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.62rem', fontWeight:800, color:'#4f46e5', flexShrink:0 }}>{fam?.[0]}</span>
                        <span style={{ fontWeight:700, color:'#4f46e5', fontSize:'0.72rem' }}>{fam}</span>
                        <span style={{ color:'#374151', fontSize:'0.82rem' }}>{given}</span>
                        {effective > 0 && <span style={{ marginLeft:'auto', fontSize:'0.72rem', fontWeight:700, color:'#1e40af' }}>{Math.round(effective/10000)}万</span>}
                      </div>
                      <div style={{ display:'flex', gap:6, alignItems:'center', flexWrap:'wrap' }}>
                        <div style={{ display:'flex', alignItems:'center', gap:4, flex:'1 1 120px' }}>
                          <span style={{ fontSize:'0.62rem', color:'#94a3b8', flexShrink:0 }}>今期</span>
                          <select value={roleName} onChange={e => setRepRole(rep, e.target.value)}
                            style={{ flex:1, padding:'3px 6px', border:'1px solid #e2e8f0', borderRadius:6, fontSize:'0.75rem', background:'#fff', outline:'none' }}>
                            {ROLE_OPTIONS.map(r => <option key={r} value={r}>{r||'— 未選択 —'}</option>)}
                          </select>
                        </div>
                        <div style={{ display:'flex', alignItems:'center', gap:4, flex:'1 1 120px' }}>
                          <span style={{ fontSize:'0.62rem', color:'#94a3b8', flexShrink:0 }}>前期</span>
                          <select value={repRoles[rep]?.prev_role_name||''} onChange={e => setPrevRepRole(rep, e.target.value)}
                            style={{ flex:1, padding:'3px 6px', border:'1px solid #e2e8f0', borderRadius:6, fontSize:'0.75rem', background:'#fff', outline:'none', color: repRoles[rep]?.prev_role_name ? '#0f172a' : '#94a3b8' }}>
                            {ROLE_OPTIONS.map(r => <option key={r} value={r}>{r||'— 今期と同じ —'}</option>)}
                          </select>
                        </div>
                        <input type="number" min="0" step="10" value={overrideWan}
                          onChange={e => setOverride(rep, e.target.value)} placeholder="手動"
                          style={{ width:52, padding:'3px 6px', border:'1px solid #e2e8f0', borderRadius:6, fontSize:'0.75rem', textAlign:'right', outline:'none' }} />
                        <span style={{ fontSize:'0.65rem', color:'#94a3b8' }}>万</span>
                      </div>
                    </div>
                  );
                })}
                <div style={{ display:'flex', justifyContent:'space-between', padding:'8px 14px', background:'#f8fafc', borderTop:'1px solid #f1f5f9' }}>
                  <span style={{ fontSize:'0.75rem', color:'#64748b', fontWeight:600 }}>合計</span>
                  <span style={{ fontSize:'0.85rem', fontWeight:800, color:teamTotal>0?'#1e40af':'#cbd5e1' }}>{teamTotal>0?`${Math.round(teamTotal/10000)}万`:'未設定'}</span>
                </div>
              </div>
            </div>
          </div>
          <div style={{ marginTop:20 }}>
            <button onClick={handleSave} disabled={saving}
              style={{ padding:'8px 24px', background:saving?'#94a3b8':'#1e40af', color:'#fff', border:'none', borderRadius:8, fontSize:'0.85rem', fontWeight:700, cursor:'pointer' }}>
              {saving?'保存中…':'保存'}
            </button>
            {notice && <span style={{ marginLeft:12, fontSize:'0.8rem', color:notice.includes('失敗')?'#dc2626':'#059669', fontWeight:600 }}>{notice}</span>}
          </div>
        </>)}

        {section === 'fields' && (<>
          <div style={{ fontWeight:700, fontSize:'0.95rem', color:'#0f172a', marginBottom:4 }}>カスタムフィールド</div>
          <div style={{ fontSize:'0.75rem', color:'#94a3b8', marginBottom:16 }}>顧客・案件フォームに独自項目を追加できます</div>
          <CustomFieldsManager />
        </>)}

        {section === 'options' && (<>
          <div style={{ fontWeight:700, fontSize:'0.95rem', color:'#0f172a', marginBottom:4 }}>選択肢管理</div>
          <div style={{ fontSize:'0.75rem', color:'#94a3b8', marginBottom:16 }}>フォームのプルダウン項目の選択肢を編集できます。編集後は「保存」を押してください。</div>
          <FieldOptionsManager />
        </>)}
      </div>
    </div>
  );
}

// 🔒 アクセス拒否表示
function AccessDenied({ message }) {
  return (
    <div style={{ flex:1, display:'flex', alignItems:'center', justifyContent:'center', flexDirection:'column', gap:10, color:'#94a3b8' }}>
      <div style={{ fontSize:'2rem' }}>🔒</div>
      <div style={{ fontWeight:700, fontSize:'0.95rem', color:'#374151' }}>アクセス権限がありません</div>
      <div style={{ fontSize:'0.82rem', color:'#94a3b8' }}>{message || '権限が必要です'}</div>
    </div>
  );
}

// ── メイン CRM ────────────────────────────────────────────────
export default function CRM() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get('tab') || 'customers';
  const setTab = (t) => setSearchParams({ tab: t }, { replace: true });
  const [access, setAccess] = useState(null); // null=loading

  useEffect(() => {
    api.crmMyAccess().then(setAccess).catch(() => setAccess({ tabs: {
      dashboard:   { visible: false }, customers: { visible: true, scope: 'all' },
      yomi:        { visible: false }, performance: { visible: false }, settings: { visible: false },
    }}));
  }, []);

  if (!access) return <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:'100%', color:'#94a3b8', fontSize:'0.85rem' }}>読み込み中…</div>;

  const tabDefs = [
    { key:'dashboard',   label:'ダッシュボード', msg:'管理者またはBC所属のみ' },
    { key:'customers',   label:'顧客一覧',       msg:'' },
    { key:'yomi',        label:'ヨミ管理',       msg:'BC所属のみ' },
    { key:'performance', label:'成績',           msg:'管理者またはBC管理職のみ' },
    { key:'settings',    label:'設定',           msg:'管理者またはBC管理職のみ' },
  ];

  // 顧客一覧は常に表示（ただし非BC/非adminはダッシュボードに飛ばさない）
  const visibleTabs = tabDefs.filter(t => t.key === 'customers' || access.tabs[t.key]?.visible);

  return (
    <div style={{ height:'100%', display:'flex', flexDirection:'column' }}>
      {/* タブバー */}
      <div style={{ display:'flex', borderBottom:'1px solid #e5e7eb', background:'#fff', paddingLeft:8, flexShrink:0, overflowX:'auto', WebkitOverflowScrolling:'touch' }}>
        {visibleTabs.map(t => (
          <button key={t.key} onClick={() => setTab(t.key)}
            style={{ padding:'10px 16px', border:'none', background:'none', cursor:'pointer', fontSize:'0.88rem', whiteSpace:'nowrap', flexShrink:0,
              fontWeight: tab===t.key?700:400, color:tab===t.key?'#1d4ed8':'#6b7280',
              borderBottom: tab===t.key?'2px solid #1d4ed8':'2px solid transparent', transition:'color 0.15s' }}>
            {t.label}
          </button>
        ))}
      </div>

      {/* タブコンテンツ */}
      <div style={{ flex:1, overflow:'hidden', display:'flex', flexDirection:'column' }}>
        {tab === 'dashboard' && (
          access.tabs.dashboard?.visible
            ? <div style={{ flex:1, overflow:'auto' }}><CrmDashboard scope={access.tabs.dashboard?.scope} /></div>
            : <AccessDenied message="管理者またはBC所属のみ閲覧できます" />
        )}
        {tab === 'customers'  && <div style={{ flex:1, overflow:'hidden', display:'flex', flexDirection:'column' }}><CustomerList /></div>}
        {tab === 'yomi' && (
          access.tabs.yomi?.visible
            ? <div style={{ flex:1, overflow:'hidden', display:'flex', flexDirection:'column' }}><YomiPanel full /></div>
            : <AccessDenied message="BC所属のみ閲覧できます" />
        )}
        {tab === 'performance' && (
          access.tabs.performance?.visible
            ? <div style={{ flex:1, overflow:'auto' }}><SalesPerformance embedded /></div>
            : <AccessDenied message="管理者またはBC管理職のみ閲覧できます" />
        )}
        {tab === 'settings' && (
          access.tabs.settings?.visible
            ? <CrmSettings />
            : <AccessDenied message="管理者またはBC管理職のみ閲覧できます" />
        )}
      </div>
    </div>
  );
}
