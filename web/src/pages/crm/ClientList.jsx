import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

export default function ClientList() {
  const [clients, setClients] = useState([]);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState({ name: '', contactName: '', contactEmail: '', contactPhone: '', source: '', notes: '' });
  const [saving, setSaving] = useState(false);
  const navigate = useNavigate();

  const load = (q = search) => {
    api.crmClients(q ? { search: q } : {})
      .then(r => setClients(r.clients))
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  const handleSearch = (e) => {
    e.preventDefault();
    setLoading(true);
    load(search);
  };

  const handleCreate = async (e) => {
    e.preventDefault();
    if (!form.name.trim()) return;
    setSaving(true);
    try {
      const { client } = await api.crmCreateClient(form);
      navigate(`/crm/clients/${client.id}`);
    } catch (err) {
      alert('作成に失敗しました: ' + err.message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>顧客一覧</h1>
        <div className="header-right">
          <Link to="/" className="analytics-link">タスク</Link>
          <Link to="/crm/deals" className="analytics-link">案件</Link>
          <button className="admin-link" onClick={() => setShowCreate(true)}>＋ 顧客追加</button>
        </div>
      </header>

      <div style={{ padding: '16px 24px' }}>
        <form onSubmit={handleSearch} style={{ marginBottom: 16, display: 'flex', gap: 8 }}>
          <input
            className="filter-select"
            style={{ flex: 1, padding: '6px 12px' }}
            placeholder="会社名で検索..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
          <button type="submit" className="admin-link">検索</button>
        </form>

        {loading ? (
          <div className="loading">読み込み中...</div>
        ) : clients.length === 0 ? (
          <div style={{ color: '#888', padding: 24 }}>顧客がまだありません</div>
        ) : (
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '2px solid #eee', textAlign: 'left' }}>
                <th style={{ padding: '8px 12px' }}>会社名</th>
                <th style={{ padding: '8px 12px' }}>担当者</th>
                <th style={{ padding: '8px 12px' }}>メール</th>
                <th style={{ padding: '8px 12px' }}>電話</th>
                <th style={{ padding: '8px 12px' }}>問い合わせ元</th>
                <th style={{ padding: '8px 12px' }}>登録日</th>
              </tr>
            </thead>
            <tbody>
              {clients.map(c => (
                <tr key={c.id}
                  onClick={() => navigate(`/crm/clients/${c.id}`)}
                  style={{ borderBottom: '1px solid #f0f0f0', cursor: 'pointer' }}>
                  <td style={{ padding: '10px 12px', fontWeight: 600 }}>
                    <Link to={`/crm/clients/${c.id}`} onClick={e => e.stopPropagation()}>{c.name}</Link>
                  </td>
                  <td style={{ padding: '10px 12px' }}>{c.contact_name || '-'}</td>
                  <td style={{ padding: '10px 12px' }}>{c.contact_email || '-'}</td>
                  <td style={{ padding: '10px 12px' }}>{c.contact_phone || '-'}</td>
                  <td style={{ padding: '10px 12px' }}>{c.source || '-'}</td>
                  <td style={{ padding: '10px 12px', color: '#888', fontSize: 13 }}>
                    {new Date(c.created_at).toLocaleDateString('ja-JP')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {showCreate && (
        <div className="modal-overlay" onClick={() => setShowCreate(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()} style={{ maxWidth: 480 }}>
            <h2 style={{ marginBottom: 16 }}>顧客追加</h2>
            <form onSubmit={handleCreate}>
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', marginBottom: 4, fontWeight: 600 }}>会社名 *</label>
                <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }}
                  value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} required />
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', marginBottom: 4 }}>担当者名</label>
                <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }}
                  value={form.contactName} onChange={e => setForm(f => ({ ...f, contactName: e.target.value }))} />
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', marginBottom: 4 }}>メール</label>
                <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }} type="email"
                  value={form.contactEmail} onChange={e => setForm(f => ({ ...f, contactEmail: e.target.value }))} />
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', marginBottom: 4 }}>電話</label>
                <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }}
                  value={form.contactPhone} onChange={e => setForm(f => ({ ...f, contactPhone: e.target.value }))} />
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', marginBottom: 4 }}>問い合わせ元</label>
                <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }}
                  placeholder="例：HP、紹介、広告"
                  value={form.source} onChange={e => setForm(f => ({ ...f, source: e.target.value }))} />
              </div>
              <div style={{ marginBottom: 16 }}>
                <label style={{ display: 'block', marginBottom: 4 }}>メモ</label>
                <textarea className="filter-select" style={{ width: '100%', padding: '6px 10px', height: 80 }}
                  value={form.notes} onChange={e => setForm(f => ({ ...f, notes: e.target.value }))} />
              </div>
              <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                <button type="button" className="filter-clear-btn" onClick={() => setShowCreate(false)}>キャンセル</button>
                <button type="submit" className="admin-link" disabled={saving}>
                  {saving ? '作成中...' : '作成'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
