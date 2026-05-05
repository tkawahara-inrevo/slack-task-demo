import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../../api/client';
import { useBreakpoint } from '../../hooks/useWindowWidth';

const YOMI_CFG = {
  'S 90％': { color:'#7c3aed', bg:'#f5f3ff' },
  'A 70％': { color:'#1d4ed8', bg:'#eff6ff' },
  'B 50％': { color:'#0891b2', bg:'#ecfeff' },
  'C 30％': { color:'#059669', bg:'#f0fdf4' },
  'D 15％': { color:'#64748b', bg:'#f8fafc' },
  'E 5％':  { color:'#94a3b8', bg:'#f8fafc' },
  'アポ化済商談前': { color:'#94a3b8', bg:'#f8fafc' },
  'アポ化前': { color:'#cbd5e1', bg:'#f8fafc' },
  '受注':   { color:'#059669', bg:'#dcfce7' },
  '失注':   { color:'#dc2626', bg:'#fef2f2' },
};

const YOMI_ORDER = ['S 90％','A 70％','B 50％','C 30％','D 15％','E 5％','アポ化済商談前','アポ化前','受注','失注'];

const daysSince = (dt) => Math.floor((Date.now() - new Date(dt)) / 86400000);

export default function CustomerList() {
  const { isMobile } = useBreakpoint();
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
  const LIMIT = 100;

  const load = async (search=q, off=offset, sales=filterSales, yomis=filterYomis) => {
    setLoading(true);
    try {
      const yomiList = [...yomis].join(',');
      const r = await api.crmCustomers(search, off, LIMIT, sales, yomiList);
      setCustomers(r.customers || []);
      setTotal(r.total || 0);
      if (r.meta) setMeta(r.meta);
    } catch {}
    finally { setLoading(false); }
  };

  useEffect(() => { setOffset(0); load('', 0); }, []);

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
      let prog = 5;
      const tick = setInterval(async () => {
        prog = Math.min(prog + 8, 90);
        try {
          const st = await api.kintoneStatus();
          if (!st.inProgress) {
            clearInterval(tick);
            setSyncDone(true);
            setSyncing(false);
            setTimeout(() => { load(); setSyncDone(false); }, 1500);
          }
        } catch { clearInterval(tick); setSyncing(false); }
      }, 2000);
    } catch { setSyncing(false); }
  };

  const toggleYomi = (y) => {
    const n = new Set(filterYomis);
    n.has(y) ? n.delete(y) : n.add(y);
    setFilterYomis(n);
  };

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%', background:'#f8fafc' }}>

      {/* ヘッダー */}
      <div style={{ padding:'12px 20px', background:'#fff', borderBottom:'1px solid #e2e8f0', display:'flex', alignItems:'center', gap:10, flexShrink:0 }}>
        <div>
          <div style={{ fontWeight:800, fontSize:'0.95rem', color:'#0f172a' }}>顧客管理</div>
          <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>{total}件</div>
        </div>
        <div style={{ marginLeft:'auto', display:'flex', gap:8, alignItems:'center' }}>
          {/* kintone同期（暫定） */}
          <div style={{ display:'flex', flexDirection:'column', alignItems:'flex-end', gap:3 }}>
            <button onClick={handleKintoneSync} disabled={syncing}
              style={{ padding:'5px 12px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.75rem', fontWeight:600, cursor:syncing?'default':'pointer',
                background:syncDone?'#f0fdf4':syncing?'#f8fafc':'#fff', color:syncDone?'#059669':syncing?'#94a3b8':'#374151',
                display:'flex', alignItems:'center', gap:5 }}>
              <span style={{ fontSize:'0.82rem' }}>{syncDone?'✓':'⟳'}</span>
              {syncDone?'同期完了':syncing?'同期中…':'kintoneデータ取得'}
            </button>
          </div>
          <button onClick={() => setShowModal(true)}
            style={{ padding:'6px 16px', background:'#1e40af', color:'#fff', border:'none', borderRadius:8, fontSize:'0.82rem', fontWeight:700, cursor:'pointer' }}>
            ＋ 顧客追加
          </button>
        </div>
      </div>

      {/* 検索・フィルター */}
      <div style={{ padding:'10px 20px', background:'#fff', borderBottom:'1px solid #e2e8f0', flexShrink:0 }}>
        <div style={{ display:'flex', gap:8, marginBottom:8, alignItems:'center', flexWrap:'wrap' }}>
          <div style={{ flex:'1 1 200px', maxWidth:280, position:'relative' }}>
            <span style={{ position:'absolute', left:10, top:'50%', transform:'translateY(-50%)', color:'#94a3b8', fontSize:'0.85rem' }}>🔍</span>
            <input type="text" value={q} placeholder="会社名・業界で検索…"
              onChange={e => setQ(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && (setOffset(0), load(q, 0))}
              style={{ width:'100%', boxSizing:'border-box', padding:'6px 10px 6px 30px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.82rem', outline:'none' }} />
          </div>
          <select value={filterSales} onChange={e => { setFilterSales(e.target.value); setOffset(0); load(q, 0, e.target.value); }}
            style={{ padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.8rem', background:'#fff', color:'#374151', outline:'none' }}>
            <option value="">全担当者</option>
            {(meta?.salesUsers || []).map(u => <option key={u} value={u}>{u}</option>)}
          </select>
          <button onClick={() => (setOffset(0), load(q, 0))}
            style={{ padding:'6px 14px', background:'#1e40af', color:'#fff', border:'none', borderRadius:8, fontSize:'0.8rem', fontWeight:600, cursor:'pointer' }}>
            検索
          </button>
          {(q || filterSales || filterYomis.size > 0) && (
            <button onClick={() => { setQ(''); setFilterSales(''); setFilterYomis(new Set()); setOffset(0); load('', 0, '', new Set()); }}
              style={{ padding:'6px 12px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.8rem', color:'#64748b', background:'#fff', cursor:'pointer' }}>
              クリア
            </button>
          )}
        </div>
        {/* ヨミフィルター */}
        <div style={{ display:'flex', gap:5, flexWrap:'wrap', alignItems:'center' }}>
          <span style={{ fontSize:'0.7rem', color:'#94a3b8', marginRight:2 }}>ヨミ</span>
          {YOMI_ORDER.map(y => {
            const cfg = YOMI_CFG[y] || { color:'#94a3b8', bg:'#f8fafc' };
            const active = filterYomis.has(y);
            return (
              <button key={y} onClick={() => { toggleYomi(y); }}
                style={{ padding:'2px 9px', borderRadius:20, fontSize:'0.7rem', fontWeight:active?700:500, cursor:'pointer', border:`1.5px solid ${active?cfg.color:'#e2e8f0'}`,
                  background:active?cfg.bg:'#fff', color:active?cfg.color:'#94a3b8', transition:'all 0.1s' }}>
                {y}
              </button>
            );
          })}
        </div>
      </div>

      {/* テーブル */}
      <div style={{ flex:1, overflowY:'auto', padding:'12px 20px' }}>
        {loading ? (
          <div style={{ textAlign:'center', padding:40, color:'#94a3b8', fontSize:'0.85rem' }}>読み込み中…</div>
        ) : customers.length === 0 ? (
          <div style={{ textAlign:'center', padding:40, color:'#94a3b8', fontSize:'0.85rem' }}>顧客がありません</div>
        ) : isMobile ? (
            /* モバイル: カードリスト */
            <div style={{ display:'flex', flexDirection:'column', gap:6 }}>
              {customers.map(c => {
                const yomiCfg = YOMI_CFG[c.latest_yomi] || { color:'#94a3b8', bg:'#f8fafc' };
                const days = daysSince(c.updated_at);
                const stale = days >= 14;
                return (
                  <div key={c.id} onClick={() => navigate(`/crm/customers/${c.id}`)}
                    style={{ background:'#fff', borderRadius:10, border:'1px solid #e2e8f0', padding:'12px 14px', cursor:'pointer' }}>
                    <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', gap:8 }}>
                      <div style={{ flex:1, minWidth:0 }}>
                        <div style={{ fontWeight:700, color:'#0f172a', fontSize:'0.9rem', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{c.name}</div>
                        <div style={{ fontSize:'0.72rem', color:'#94a3b8', marginTop:2 }}>
                          {[c.prefecture, c.industry, c.latest_sales_person?.split(/[\s　]/)[0]].filter(Boolean).join(' · ')}
                        </div>
                      </div>
                      <div style={{ flexShrink:0, textAlign:'right' }}>
                        {c.latest_yomi && (
                          <span style={{ fontSize:'0.7rem', fontWeight:700, padding:'2px 8px', borderRadius:20, background:yomiCfg.bg, color:yomiCfg.color, whiteSpace:'nowrap', display:'block' }}>
                            {c.latest_yomi}
                          </span>
                        )}
                        <div style={{ fontSize:'0.65rem', color: stale ? '#dc2626' : '#94a3b8', marginTop:3, fontWeight: stale ? 700 : 400 }}>
                          {days === 0 ? '今日' : `${days}日前`}
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            /* デスクトップ: テーブル */
            <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden' }}>
              <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.82rem' }}>
                <thead>
                  <tr style={{ background:'#f8fafc', borderBottom:'1px solid #e2e8f0' }}>
                    {['会社名','担当者','業界','商談','最新ヨミ','最終更新'].map((h, i) => (
                      <th key={h} style={{ padding:'9px 14px', textAlign: i >= 2 ? 'center' : 'left', fontWeight:600, color:'#64748b', fontSize:'0.72rem', whiteSpace:'nowrap' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {customers.map(c => {
                    const yomiCfg = YOMI_CFG[c.latest_yomi] || { color:'#cbd5e1', bg:'transparent' };
                    const days = daysSince(c.updated_at);
                    const stale = days >= 14;
                    return (
                      <tr key={c.id} onClick={() => navigate(`/crm/customers/${c.id}`)}
                        style={{ borderBottom:'1px solid #f1f5f9', cursor:'pointer', transition:'background 0.1s' }}
                        onMouseEnter={e => e.currentTarget.style.background='#f8fafc'}
                        onMouseLeave={e => e.currentTarget.style.background=''}>
                        <td style={{ padding:'10px 14px' }}>
                          <div style={{ fontWeight:700, color:'#0f172a', fontSize:'0.85rem' }}>{c.name}</div>
                          {c.prefecture && <div style={{ fontSize:'0.68rem', color:'#94a3b8', marginTop:1 }}>{c.prefecture}</div>}
                        </td>
                        <td style={{ padding:'10px 14px' }}>
                          {c.latest_sales_person
                            ? <span style={{ fontSize:'0.75rem', color:'#374151' }}>{c.latest_sales_person.split(/[\s　]/)[0]}</span>
                            : <span style={{ color:'#cbd5e1', fontSize:'0.75rem' }}>—</span>}
                        </td>
                        <td style={{ padding:'10px 14px', textAlign:'center', color:'#64748b', fontSize:'0.78rem' }}>{c.industry || '—'}</td>
                        <td style={{ padding:'10px 14px', textAlign:'center' }}>
                          <span style={{ fontWeight:600, color: c.deal_count > 0 ? '#1e40af' : '#cbd5e1', fontSize:'0.82rem' }}>{c.deal_count || 0}</span>
                          <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>件</span>
                        </td>
                        <td style={{ padding:'10px 14px', textAlign:'center' }}>
                          {c.latest_yomi
                            ? <span style={{ fontSize:'0.72rem', fontWeight:700, padding:'2px 10px', borderRadius:20, background:yomiCfg.bg, color:yomiCfg.color, whiteSpace:'nowrap' }}>
                                {c.latest_yomi}
                              </span>
                            : <span style={{ color:'#cbd5e1', fontSize:'0.72rem' }}>—</span>}
                        </td>
                        <td style={{ padding:'10px 14px', textAlign:'center' }}>
                          <span style={{ fontSize:'0.72rem', color: stale ? '#dc2626' : '#94a3b8', fontWeight: stale ? 700 : 400 }}>
                            {days === 0 ? '今日' : `${days}日前`}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )
        }

        {/* ページネーション */}
        {total > LIMIT && (
          <div style={{ display:'flex', gap:8, justifyContent:'center', marginTop:14 }}>
            <button disabled={offset === 0}
              onClick={() => { const o = Math.max(0, offset-LIMIT); setOffset(o); load(q, o); }}
              style={{ padding:'6px 16px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.8rem', cursor:'pointer', background:'#fff', color:'#374151', opacity:offset===0?0.4:1 }}>
              ← 前
            </button>
            <span style={{ alignSelf:'center', fontSize:'0.78rem', color:'#64748b' }}>
              {Math.floor(offset/LIMIT)+1} / {Math.ceil(total/LIMIT)}ページ
            </span>
            <button disabled={offset+LIMIT >= total}
              onClick={() => { const o = offset+LIMIT; setOffset(o); load(q, o); }}
              style={{ padding:'6px 16px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.8rem', cursor:'pointer', background:'#fff', color:'#374151', opacity:offset+LIMIT>=total?0.4:1 }}>
              次 →
            </button>
          </div>
        )}
      </div>

      {/* 新規作成モーダル */}
      {showModal && (
        <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.5)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }}
          onClick={() => setShowModal(false)}>
          <div style={{ background:'#fff', borderRadius:16, width:'min(500px,92vw)', overflow:'hidden', boxShadow:'0 25px 50px rgba(0,0,0,0.25)' }}
            onClick={e => e.stopPropagation()}>
            <div style={{ padding:'16px 24px', borderBottom:'1px solid #f1f5f9', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
              <div>
                <div style={{ fontSize:'0.68rem', color:'#94a3b8', marginBottom:2 }}>新規作成</div>
                <div style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a' }}>顧客を追加</div>
              </div>
              <button onClick={() => setShowModal(false)} style={{ background:'none', border:'none', cursor:'pointer', color:'#94a3b8', fontSize:20 }}>×</button>
            </div>
            <div style={{ padding:'20px 24px' }}>
              {[
                { key:'name', label:'会社名', required:true, placeholder:'例: 株式会社サンプル' },
                { key:'industry', label:'業界', placeholder:'例: 製造業' },
                { key:'prefecture', label:'都道府県', placeholder:'例: 東京都' },
                { key:'employeeCount', label:'従業員数', placeholder:'例: 51-100' },
                { key:'website', label:'Webサイト', placeholder:'https://' },
              ].map(f => (
                <div key={f.key} style={{ marginBottom:12 }}>
                  <label style={{ display:'block', fontSize:'0.75rem', fontWeight:700, color:'#374151', marginBottom:4 }}>
                    {f.label}{f.required && <span style={{ color:'#ef4444', marginLeft:3 }}>*</span>}
                  </label>
                  <input type="text" value={form[f.key]} onChange={e => setForm(p => ({...p, [f.key]:e.target.value}))}
                    placeholder={f.placeholder}
                    style={{ width:'100%', boxSizing:'border-box', padding:'8px 12px', border:'1.5px solid #e2e8f0', borderRadius:8, fontSize:'0.85rem', outline:'none' }}
                    onFocus={e => e.target.style.borderColor='#6366f1'}
                    onBlur={e => e.target.style.borderColor='#e2e8f0'} />
                </div>
              ))}
              <div>
                <label style={{ display:'block', fontSize:'0.75rem', fontWeight:700, color:'#374151', marginBottom:4 }}>メモ</label>
                <textarea value={form.memo} onChange={e => setForm(p => ({...p, memo:e.target.value}))}
                  rows={2} style={{ width:'100%', resize:'vertical', boxSizing:'border-box', padding:'8px 12px', border:'1.5px solid #e2e8f0', borderRadius:8, fontSize:'0.85rem', outline:'none' }}
                  onFocus={e => e.target.style.borderColor='#6366f1'}
                  onBlur={e => e.target.style.borderColor='#e2e8f0'} />
              </div>
            </div>
            <div style={{ padding:'12px 24px', borderTop:'1px solid #f1f5f9', display:'flex', justifyContent:'flex-end', gap:8 }}>
              <button onClick={() => setShowModal(false)}
                style={{ padding:'8px 18px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.82rem', color:'#374151', background:'#fff', cursor:'pointer' }}>
                キャンセル
              </button>
              <button onClick={handleCreate} disabled={!form.name.trim() || saving}
                style={{ padding:'8px 22px', border:'none', borderRadius:8, background:'#1e40af', color:'#fff', fontSize:'0.82rem', fontWeight:700, cursor:'pointer', opacity:!form.name.trim()||saving?0.6:1 }}>
                {saving ? '作成中…' : '✓ 作成'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
