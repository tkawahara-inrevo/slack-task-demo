import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

const YOMI_CFG = {
  'S 90％':      { color:'#7c3aed', bg:'#ede9fe', dot:'#7c3aed' },
  'A 70％':      { color:'#1d4ed8', bg:'#dbeafe', dot:'#1d4ed8' },
  'B 50％':      { color:'#0891b2', bg:'#cffafe', dot:'#0891b2' },
  'C 30％':      { color:'#059669', bg:'#d1fae5', dot:'#059669' },
  'D 15％':      { color:'#64748b', bg:'#f1f5f9', dot:'#94a3b8' },
  'E 5％':       { color:'#94a3b8', bg:'#f8fafc', dot:'#cbd5e1' },
  'アポ化済商談前': { color:'#94a3b8', bg:'#f8fafc', dot:'#cbd5e1' },
  'アポ化前':    { color:'#cbd5e1', bg:'#f8fafc', dot:'#e2e8f0' },
  '受注':        { color:'#059669', bg:'#dcfce7', dot:'#059669' },
  '失注':        { color:'#dc2626', bg:'#fee2e2', dot:'#dc2626' },
};
const YOMI_ORDER = ['S 90％','A 70％','B 50％','C 30％','D 15％','E 5％','アポ化済商談前','アポ化前','受注','失注'];
const daysSince = dt => Math.floor((Date.now() - new Date(dt)) / 86400000);

function InitialCircle({ name }) {
  const colors = ['#6366f1','#0891b2','#059669','#d97706','#dc2626','#7c3aed','#ec4899'];
  const color = colors[(name?.charCodeAt(0) || 0) % colors.length];
  const ch = name?.replace(/^(株式会社|有限会社|合同会社|一般社団法人)\s*/,'').charAt(0) || '?';
  return (
    <div style={{ width:38, height:38, borderRadius:10, background:color+'18', border:`1.5px solid ${color}33`,
      display:'flex', alignItems:'center', justifyContent:'center', flexShrink:0,
      fontWeight:800, fontSize:'0.88rem', color }}>
      {ch}
    </div>
  );
}

function YomiBadge({ yomi }) {
  if (!yomi) return <span style={{ color:'#e2e8f0', fontSize:'0.72rem' }}>—</span>;
  const cfg = YOMI_CFG[yomi] || { color:'#94a3b8', bg:'#f8fafc' };
  return (
    <span style={{ display:'inline-flex', alignItems:'center', gap:5, fontSize:'0.72rem', fontWeight:700,
      padding:'3px 10px', borderRadius:99, background:cfg.bg, color:cfg.color, whiteSpace:'nowrap' }}>
      <span style={{ width:6, height:6, borderRadius:'50%', background:cfg.dot || cfg.color, flexShrink:0 }} />
      {yomi}
    </span>
  );
}

export default function CustomerList() {
  const navigate = useNavigate();
  const [customers, setCustomers] = useState([]);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(true);
  const [q, setQ] = useState('');
  const [filterSales, setFilterSales] = useState('');
  const [filterYomis, setFilterYomis] = useState(new Set());
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [showModal, setShowModal] = useState(false);
  const [form, setForm] = useState({ name:'', industry:'', prefecture:'', employeeCount:'', website:'', memo:'' });
  const [saving, setSaving] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [syncDone, setSyncDone] = useState(false);
  const inputRef = useRef(null);
  const LIMIT = 100;

  const load = async (search=q, off=offset, sales=filterSales, yomis=filterYomis) => {
    setLoading(true);
    try {
      const r = await api.crmCustomers(search, off, LIMIT, sales, [...yomis].join(','));
      setCustomers(r.customers || []);
      setTotal(r.total || 0);
      if (r.meta) setMeta(r.meta);
    } catch {}
    finally { setLoading(false); }
  };

  useEffect(() => { load('', 0); }, []);

  const handleCreate = async () => {
    if (!form.name.trim()) return;
    setSaving(true);
    try {
      const r = await api.crmCreateCustomer(form);
      setShowModal(false);
      setForm({ name:'', industry:'', prefecture:'', employeeCount:'', website:'', memo:'' });
      navigate(`/crm/customers/${r.customer.id}`);
    } catch { alert('作成に失敗しました'); }
    finally { setSaving(false); }
  };

  const handleKintoneSync = async () => {
    if (syncing) return;
    setSyncing(true); setSyncDone(false);
    try {
      await api.kintoneSync();
      const tick = setInterval(async () => {
        try {
          const st = await api.kintoneStatus();
          if (!st.inProgress) { clearInterval(tick); setSyncDone(true); setSyncing(false); setTimeout(() => { load(); setSyncDone(false); }, 1500); }
        } catch { clearInterval(tick); setSyncing(false); }
      }, 2000);
    } catch { setSyncing(false); }
  };

  const toggleYomi = y => {
    const n = new Set(filterYomis);
    n.has(y) ? n.delete(y) : n.add(y);
    setFilterYomis(n);
    setOffset(0); load(q, 0, filterSales, n);
  };

  const clearAll = () => { setQ(''); setFilterSales(''); setFilterYomis(new Set()); setOffset(0); load('', 0, '', new Set()); };
  const hasFilter = q || filterSales || filterYomis.size > 0;

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%', background:'#f1f5f9' }}>

      {/* ── ヘッダー ── */}
      <div style={{ padding:'14px 20px 10px', background:'#fff', borderBottom:'1px solid #e2e8f0', flexShrink:0 }}>
        <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:10 }}>
          <div>
            <span style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a' }}>顧客一覧</span>
            <span style={{ fontSize:'0.72rem', color:'#94a3b8', marginLeft:8, fontWeight:400 }}>{total.toLocaleString()}件</span>
          </div>
          <div style={{ marginLeft:'auto', display:'flex', gap:8 }}>
            <button onClick={handleKintoneSync} disabled={syncing}
              style={{ padding:'5px 12px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.75rem', fontWeight:600,
                background:syncDone?'#f0fdf4':'#fff', color:syncDone?'#059669':syncing?'#94a3b8':'#64748b',
                cursor:syncing?'default':'pointer', display:'flex', alignItems:'center', gap:4 }}>
              <span style={{ fontSize:'0.88rem' }}>{syncDone?'✓':'⟳'}</span>
              {syncDone?'同期完了':syncing?'同期中…':'kintone同期'}
            </button>
            <button onClick={() => setShowModal(true)}
              style={{ padding:'6px 16px', background:'#1e40af', color:'#fff', border:'none', borderRadius:8, fontSize:'0.82rem', fontWeight:700, cursor:'pointer', display:'flex', alignItems:'center', gap:4 }}>
              <span style={{ fontSize:'1rem', lineHeight:1 }}>＋</span> 顧客追加
            </button>
          </div>
        </div>

        {/* 検索バー */}
        <div style={{ display:'flex', gap:8, alignItems:'center', flexWrap:'wrap' }}>
          <div style={{ flex:'1 1 200px', maxWidth:320, position:'relative' }}>
            <svg style={{ position:'absolute', left:10, top:'50%', transform:'translateY(-50%)', color:'#94a3b8' }} width="14" height="14" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clipRule="evenodd" />
            </svg>
            <input ref={inputRef} type="text" value={q} placeholder="会社名・業界で検索…"
              onChange={e => setQ(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && (setOffset(0), load(q, 0))}
              style={{ width:'100%', boxSizing:'border-box', padding:'7px 10px 7px 32px',
                border:'1.5px solid #e2e8f0', borderRadius:9, fontSize:'0.82rem', outline:'none', background:'#f8fafc' }} />
          </div>
          <select value={filterSales} onChange={e => { setFilterSales(e.target.value); setOffset(0); load(q, 0, e.target.value); }}
            style={{ padding:'7px 10px', border:'1.5px solid #e2e8f0', borderRadius:9, fontSize:'0.8rem', background:'#f8fafc', color:'#374151', outline:'none' }}>
            <option value="">全担当者</option>
            {(meta?.salesUsers||[]).map(u => <option key={u} value={u}>{u}</option>)}
          </select>
          <button onClick={() => { setOffset(0); load(q, 0); }}
            style={{ padding:'7px 16px', background:'#1e40af', color:'#fff', border:'none', borderRadius:9, fontSize:'0.8rem', fontWeight:600, cursor:'pointer' }}>
            検索
          </button>
          {hasFilter && (
            <button onClick={clearAll}
              style={{ padding:'7px 12px', border:'1.5px solid #e2e8f0', borderRadius:9, fontSize:'0.78rem', color:'#64748b', background:'#fff', cursor:'pointer' }}>
              クリア
            </button>
          )}
        </div>

        {/* ヨミフィルター */}
        <div style={{ display:'flex', gap:4, flexWrap:'wrap', marginTop:8, alignItems:'center' }}>
          <span style={{ fontSize:'0.68rem', color:'#94a3b8', marginRight:2 }}>ヨミ</span>
          {YOMI_ORDER.map(y => {
            const cfg = YOMI_CFG[y] || {};
            const on = filterYomis.has(y);
            return (
              <button key={y} onClick={() => toggleYomi(y)}
                style={{ padding:'2px 10px', borderRadius:20, fontSize:'0.68rem', fontWeight:on?700:500, cursor:'pointer',
                  border:`1.5px solid ${on ? cfg.color : '#e2e8f0'}`,
                  background: on ? cfg.bg : '#fff', color: on ? cfg.color : '#94a3b8',
                  display:'flex', alignItems:'center', gap:4, transition:'all 0.1s' }}>
                {on && <span style={{ width:5, height:5, borderRadius:'50%', background:cfg.dot, flexShrink:0 }} />}
                {y}
              </button>
            );
          })}
        </div>
      </div>

      {/* ── リスト ── */}
      <div style={{ flex:1, overflowY:'auto', padding:'12px 16px' }}>
        {loading ? (
          <div style={{ textAlign:'center', padding:60, color:'#94a3b8', fontSize:'0.85rem' }}>読み込み中…</div>
        ) : customers.length === 0 ? (
          <div style={{ textAlign:'center', padding:60, color:'#94a3b8', fontSize:'0.85rem' }}>
            <div style={{ fontSize:'2rem', marginBottom:8 }}>🔍</div>
            顧客が見つかりません
          </div>
        ) : (
          <div style={{ display:'flex', flexDirection:'column', gap:2 }}>
            {customers.map(c => {
              const days = daysSince(c.updated_at);
              const stale = days >= 14;
              const salesName = c.latest_sales_person?.split(/[\s　]/)[0] || null;
              return (
                <div key={c.id} onClick={() => navigate(`/crm/customers/${c.id}`)}
                  style={{ background:'#fff', borderRadius:10, border:'1px solid #e2e8f0', padding:'10px 14px',
                    cursor:'pointer', display:'flex', alignItems:'center', gap:12, transition:'all 0.1s',
                    boxShadow:'0 1px 2px rgba(0,0,0,0.03)' }}
                  onMouseEnter={e => { e.currentTarget.style.borderColor='#c7d2fe'; e.currentTarget.style.boxShadow='0 2px 8px rgba(99,102,241,0.08)'; }}
                  onMouseLeave={e => { e.currentTarget.style.borderColor='#e2e8f0'; e.currentTarget.style.boxShadow='0 1px 2px rgba(0,0,0,0.03)'; }}>

                  <InitialCircle name={c.name} />

                  {/* 会社名・都道府県 */}
                  <div style={{ flex:'0 0 240px', minWidth:0 }}>
                    <div style={{ fontWeight:700, fontSize:'0.88rem', color:'#0f172a', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{c.name}</div>
                    <div style={{ fontSize:'0.68rem', color:'#94a3b8', marginTop:1 }}>
                      {[c.prefecture, c.industry].filter(Boolean).join(' · ') || ' '}
                    </div>
                  </div>

                  {/* 担当者 */}
                  <div style={{ flex:'0 0 80px', minWidth:0 }}>
                    {salesName
                      ? <span style={{ fontSize:'0.75rem', fontWeight:600, color:'#374151', background:'#f1f5f9', padding:'2px 8px', borderRadius:5 }}>{salesName}</span>
                      : <span style={{ color:'#e2e8f0', fontSize:'0.75rem' }}>—</span>}
                  </div>

                  {/* 商談数 */}
                  <div style={{ flex:'0 0 60px', textAlign:'center' }}>
                    {c.deal_count > 0
                      ? <span style={{ fontWeight:700, fontSize:'0.82rem', color:'#1e40af' }}>{c.deal_count}<span style={{ fontSize:'0.62rem', color:'#94a3b8', marginLeft:2 }}>件</span></span>
                      : <span style={{ color:'#e2e8f0', fontSize:'0.75rem' }}>—</span>}
                  </div>

                  {/* ヨミ */}
                  <div style={{ flex:'1 1 130px' }}>
                    <YomiBadge yomi={c.latest_yomi} />
                  </div>

                  {/* 最終更新 */}
                  <div style={{ flex:'0 0 70px', textAlign:'right' }}>
                    <span style={{ fontSize:'0.7rem', color: stale?'#ef4444':'#94a3b8', fontWeight:stale?700:400 }}>
                      {days === 0 ? '今日' : `${days}日前`}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* ページネーション */}
        {total > LIMIT && (
          <div style={{ display:'flex', gap:8, justifyContent:'center', marginTop:16, alignItems:'center' }}>
            <button disabled={offset===0} onClick={() => { const o=Math.max(0,offset-LIMIT); setOffset(o); load(q,o); }}
              style={{ padding:'6px 18px', border:'1.5px solid #e2e8f0', borderRadius:8, fontSize:'0.8rem', cursor:offset===0?'default':'pointer', background:'#fff', color:'#374151', opacity:offset===0?0.4:1 }}>
              ← 前
            </button>
            <span style={{ fontSize:'0.78rem', color:'#64748b', minWidth:80, textAlign:'center' }}>
              {Math.floor(offset/LIMIT)+1} / {Math.ceil(total/LIMIT)}ページ
            </span>
            <button disabled={offset+LIMIT>=total} onClick={() => { const o=offset+LIMIT; setOffset(o); load(q,o); }}
              style={{ padding:'6px 18px', border:'1.5px solid #e2e8f0', borderRadius:8, fontSize:'0.8rem', cursor:offset+LIMIT>=total?'default':'pointer', background:'#fff', color:'#374151', opacity:offset+LIMIT>=total?0.4:1 }}>
              次 →
            </button>
          </div>
        )}
      </div>

      {/* ── 新規作成モーダル ── */}
      {showModal && (
        <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.5)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }}
          onClick={() => setShowModal(false)}>
          <div style={{ background:'#fff', borderRadius:16, width:'min(500px,92vw)', overflow:'hidden', boxShadow:'0 25px 50px rgba(0,0,0,0.25)' }}
            onClick={e => e.stopPropagation()}>
            <div style={{ padding:'16px 24px', borderBottom:'1px solid #f1f5f9', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
              <div style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a' }}>顧客を追加</div>
              <button onClick={() => setShowModal(false)} style={{ background:'#f1f5f9', border:'none', cursor:'pointer', color:'#64748b', fontSize:16, borderRadius:8, width:28, height:28 }}>×</button>
            </div>
            <div style={{ padding:'20px 24px', display:'flex', flexDirection:'column', gap:12 }}>
              {[
                { key:'name',          label:'会社名',   required:true, placeholder:'例: 株式会社サンプル' },
                { key:'industry',      label:'業界',     placeholder:'例: 製造業' },
                { key:'prefecture',    label:'都道府県', placeholder:'例: 東京都' },
                { key:'employeeCount', label:'従業員数', placeholder:'例: 51-100' },
                { key:'website',       label:'Webサイト',placeholder:'https://' },
              ].map(f => (
                <div key={f.key}>
                  <label style={{ display:'block', fontSize:'0.75rem', fontWeight:700, color:'#374151', marginBottom:4 }}>
                    {f.label}{f.required && <span style={{ color:'#ef4444', marginLeft:3 }}>*</span>}
                  </label>
                  <input type="text" value={form[f.key]} onChange={e => setForm(p => ({...p,[f.key]:e.target.value}))}
                    placeholder={f.placeholder}
                    style={{ width:'100%', boxSizing:'border-box', padding:'8px 12px', border:'1.5px solid #e2e8f0', borderRadius:8, fontSize:'0.85rem', outline:'none' }}
                    onFocus={e => e.target.style.borderColor='#6366f1'}
                    onBlur={e => e.target.style.borderColor='#e2e8f0'} />
                </div>
              ))}
              <div>
                <label style={{ display:'block', fontSize:'0.75rem', fontWeight:700, color:'#374151', marginBottom:4 }}>メモ</label>
                <textarea value={form.memo} onChange={e => setForm(p => ({...p,memo:e.target.value}))} rows={2}
                  style={{ width:'100%', resize:'vertical', boxSizing:'border-box', padding:'8px 12px', border:'1.5px solid #e2e8f0', borderRadius:8, fontSize:'0.85rem', outline:'none' }}
                  onFocus={e => e.target.style.borderColor='#6366f1'}
                  onBlur={e => e.target.style.borderColor='#e2e8f0'} />
              </div>
            </div>
            <div style={{ padding:'12px 24px', borderTop:'1px solid #f1f5f9', display:'flex', justifyContent:'flex-end', gap:8 }}>
              <button onClick={() => setShowModal(false)}
                style={{ padding:'8px 18px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.82rem', color:'#374151', background:'#fff', cursor:'pointer' }}>
                キャンセル
              </button>
              <button onClick={handleCreate} disabled={!form.name.trim()||saving}
                style={{ padding:'8px 22px', border:'none', borderRadius:8, background:'#1e40af', color:'#fff', fontSize:'0.82rem', fontWeight:700, cursor:'pointer', opacity:!form.name.trim()||saving?0.6:1 }}>
                {saving?'作成中…':'✓ 作成'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
