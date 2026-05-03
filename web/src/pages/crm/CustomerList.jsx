import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

const YOMI_COLOR = {
  'アポ化前': '#9ca3af', 'アポ化済商談前': '#6b7280',
  'E 5％': '#d1d5db', 'D 15％': '#a3b0c4',
  'C 30％': '#93c5fd', 'B 50％': '#60a5fa',
  'A 70％': '#3b82f6', 'S 90％': '#1d4ed8',
  '受注': '#10b981', '失注': '#ef4444',
};

export default function CustomerList() {
  const navigate = useNavigate();
  const [customers, setCustomers] = useState([]);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(true);
  const [q, setQ] = useState('');
  const [filterSales, setFilterSales] = useState('');
  const [filterYomis, setFilterYomis] = useState(new Set()); // チェックされたヨミ
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [showModal, setShowModal] = useState(false);
  const [form, setForm] = useState({ name: '', industry: '', prefecture: '', employeeCount: '', website: '', memo: '' });
  const [saving, setSaving] = useState(false);
  const LIMIT = 100;

  const load = async (search = q, off = offset, sales = filterSales, yomis = filterYomis) => {
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
      setForm({ name: '', industry: '', prefecture: '', employeeCount: '', website: '', memo: '' });
      navigate(`/crm/customers/${r.customer.id}`);
    } catch { alert('作成に失敗しました'); }
    finally { setSaving(false); }
  };

  return (
    <div className="rpo-page">
      <div className="rpo-header">
        <div>
          <h1 className="rpo-title">顧客管理</h1>
          <p className="rpo-subtitle">顧客・商談を一元管理</p>
        </div>
        <button className="btn-primary" onClick={() => setShowModal(true)}>＋ 顧客を追加</button>
      </div>

      {/* 検索・フィルター */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', gap: 8, marginBottom: 8, flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text" value={q} placeholder="会社名・業界で検索…"
            onChange={e => setQ(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && load(q, 0)}
            style={{ flex: '1 1 200px', maxWidth: 280, padding: '6px 10px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: '0.85rem' }}
          />
          <select value={filterSales} onChange={e => setFilterSales(e.target.value)}
            style={{ padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: '0.82rem' }}>
            <option value="">全担当者</option>
            {(meta?.salesUsers || []).map(u => <option key={u} value={u}>{u}</option>)}
          </select>
          <button className="btn-primary" onClick={() => { setOffset(0); load(q, 0); }}>検索</button>
          {(q || filterSales || filterYomis.size > 0) && (
            <button className="btn-secondary" onClick={() => {
              setQ(''); setFilterSales(''); setFilterYomis(new Set()); setOffset(0);
              load('', 0, '', new Set());
            }}>クリア</button>
          )}
          <span style={{ marginLeft: 'auto', fontSize: '0.8rem', color: '#9ca3af' }}>
            {total}件中 {Math.min(offset + 1, total)}〜{Math.min(offset + LIMIT, total)}件
          </span>
        </div>
        {/* ヨミフィルター（チェックボックス） */}
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
          <span style={{ fontSize: '0.75rem', color: '#9ca3af', whiteSpace: 'nowrap' }}>ヨミ：</span>
          {['アポ化前','アポ化済商談前','E 5％','D 15％','C 30％','B 50％','A 70％','S 90％','受注','失注'].map(y => (
            <label key={y} style={{ display: 'flex', alignItems: 'center', gap: 3, cursor: 'pointer', fontSize: '0.78rem', padding: '2px 6px', borderRadius: 4, background: filterYomis.has(y) ? '#eff6ff' : '#f9fafb', border: `1px solid ${filterYomis.has(y) ? '#93c5fd' : '#e5e7eb'}` }}>
              <input type="checkbox" checked={filterYomis.has(y)}
                onChange={() => setFilterYomis(prev => { const n = new Set(prev); n.has(y) ? n.delete(y) : n.add(y); return n; })}
                style={{ cursor: 'pointer' }} />
              {y}
            </label>
          ))}
        </div>
      </div>

      {loading ? (
        <div className="page-loading">読み込み中...</div>
      ) : customers.length === 0 ? (
        <p style={{ color: '#9ca3af', textAlign: 'center', padding: 32 }}>顧客がありません</p>
      ) : (
        <div style={{ border: '1px solid #e5e7eb', borderRadius: 8, overflow: 'hidden', background: '#fff' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
            <thead>
              <tr style={{ background: '#f9fafb' }}>
                {['会社名', '業界', '都道府県', '商談数', '最新ヨミ', '更新日'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {customers.map(c => (
                <tr key={c.id} onClick={() => navigate(`/crm/customers/${c.id}`)}
                  style={{ borderBottom: '1px solid #f3f4f6', cursor: 'pointer' }}
                  onMouseEnter={e => e.currentTarget.style.background = '#f9fafb'}
                  onMouseLeave={e => e.currentTarget.style.background = ''}>
                  <td style={{ padding: '10px 14px', fontWeight: 600, color: '#111827' }}>{c.name}</td>
                  <td style={{ padding: '10px 14px', color: '#6b7280' }}>{c.industry || '—'}</td>
                  <td style={{ padding: '10px 14px', color: '#6b7280' }}>{c.prefecture || '—'}</td>
                  <td style={{ padding: '10px 14px', color: '#6b7280' }}>{c.deal_count || 0}件</td>
                  <td style={{ padding: '10px 14px' }}>
                    {c.latest_yomi
                      ? <span style={{ fontSize: '0.78rem', fontWeight: 600, padding: '2px 8px', borderRadius: 999, background: (YOMI_COLOR[c.latest_yomi] || '#9ca3af') + '22', color: YOMI_COLOR[c.latest_yomi] || '#9ca3af' }}>{c.latest_yomi}</span>
                      : <span style={{ color: '#d1d5db' }}>—</span>}
                  </td>
                  <td style={{ padding: '10px 14px', color: '#9ca3af', fontSize: '0.78rem' }}>
                    {new Date(c.updated_at).toLocaleDateString('ja-JP')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* ページネーション */}
      {total > LIMIT && (
        <div style={{ display: 'flex', gap: 8, justifyContent: 'center', marginTop: 16 }}>
          <button className="btn-secondary" disabled={offset === 0}
            onClick={() => { const o = Math.max(0, offset - LIMIT); setOffset(o); load(q, o); }}>
            ← 前へ
          </button>
          <span style={{ alignSelf: 'center', fontSize: '0.82rem', color: '#6b7280' }}>
            {Math.floor(offset / LIMIT) + 1} / {Math.ceil(total / LIMIT)} ページ
          </span>
          <button className="btn-secondary" disabled={offset + LIMIT >= total}
            onClick={() => { const o = offset + LIMIT; setOffset(o); load(q, o); }}>
            次へ →
          </button>
        </div>
      )}

      {/* 新規作成モーダル */}
      {showModal && (
        <div className="modal-overlay" onClick={() => setShowModal(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()} style={{ maxWidth: 520, padding: 0, overflow: 'hidden' }}>
            <div style={{ padding: '16px 24px', borderBottom: '1px solid #f3f4f6', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                <div style={{ fontSize: '0.75rem', color: '#9ca3af', marginBottom: 2 }}>新規作成</div>
                <div style={{ fontWeight: 800, fontSize: '1rem', color: '#111827' }}>顧客を追加</div>
              </div>
              <button onClick={() => setShowModal(false)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#9ca3af', fontSize: 18, lineHeight: 1, padding: 4 }}>×</button>
            </div>
            <div style={{ padding: '20px 24px' }}>
              {[
                { key: 'name', label: '会社名', required: true, placeholder: '例: 株式会社サンプル' },
                { key: 'industry', label: '業界', placeholder: '例: 製造業' },
                { key: 'prefecture', label: '都道府県', placeholder: '例: 東京都' },
                { key: 'employeeCount', label: '従業員数', placeholder: '例: 51-100' },
                { key: 'website', label: 'Webサイト', placeholder: 'https://' },
              ].map(f => (
                <div key={f.key} style={{ marginBottom: 12 }}>
                  <label style={{ display: 'block', fontSize: '0.78rem', fontWeight: 700, color: '#374151', marginBottom: 5 }}>
                    {f.label}{f.required && <span style={{ color: '#ef4444', marginLeft: 3 }}>*</span>}
                  </label>
                  <input type="text" value={form[f.key]} onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))}
                    placeholder={f.placeholder}
                    style={{ width: '100%', boxSizing: 'border-box', padding: '9px 12px', border: '1.5px solid #e5e7eb', borderRadius: 8, fontSize: '0.88rem', outline: 'none' }}
                    onFocus={e => e.target.style.borderColor = '#6366f1'}
                    onBlur={e => e.target.style.borderColor = '#e5e7eb'}
                  />
                </div>
              ))}
              <div>
                <label style={{ display: 'block', fontSize: '0.78rem', fontWeight: 700, color: '#374151', marginBottom: 5 }}>メモ</label>
                <textarea value={form.memo} onChange={e => setForm(p => ({ ...p, memo: e.target.value }))}
                  rows={2} style={{ width: '100%', resize: 'vertical', boxSizing: 'border-box', padding: '9px 12px', border: '1.5px solid #e5e7eb', borderRadius: 8, fontSize: '0.88rem', outline: 'none' }}
                  onFocus={e => e.target.style.borderColor = '#6366f1'}
                  onBlur={e => e.target.style.borderColor = '#e5e7eb'}
                />
              </div>
            </div>
            <div style={{ padding: '12px 24px', borderTop: '1px solid #f3f4f6', display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
              <button style={{ padding: '8px 20px', border: '1.5px solid #e5e7eb', borderRadius: 8, background: '#fff', color: '#6b7280', fontSize: '0.85rem', fontWeight: 600, cursor: 'pointer' }}
                onClick={() => setShowModal(false)}>キャンセル</button>
              <button style={{ padding: '8px 22px', border: 'none', borderRadius: 8, background: '#1e293b', color: '#fff', fontSize: '0.85rem', fontWeight: 700, cursor: 'pointer' }}
                onClick={handleCreate} disabled={!form.name.trim() || saving}>
                {saving ? '作成中...' : '✓ 作成して閉じる'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
