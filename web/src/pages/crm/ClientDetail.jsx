import { useEffect, useState } from 'react';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

const STAGES = [
  { value: 'mk', label: 'MK（アポ取り）' },
  { value: 'bc', label: 'BC（商談中）' },
  { value: 'contracted', label: '受注済' },
  { value: 'hr', label: 'HR分析中' },
  { value: 'direction', label: 'ディレクション' },
  { value: 'cs', label: 'CS（スカウト）' },
  { value: 'completed', label: '完了' },
  { value: 'lost', label: '失注' },
];

function stageLabel(v) {
  return STAGES.find(s => s.value === v)?.label || v;
}

export default function ClientDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [client, setClient] = useState(null);
  const [deals, setDeals] = useState([]);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState(false);
  const [form, setForm] = useState({});
  const [showDeal, setShowDeal] = useState(false);
  const [dealForm, setDealForm] = useState({ name: '', stage: 'mk', budget: '', notes: '' });
  const [saving, setSaving] = useState(false);

  const load = () => {
    api.crmClientDetail(id)
      .then(r => { setClient(r.client); setDeals(r.deals); setForm(r.client); })
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, [id]);

  const handleSave = async () => {
    setSaving(true);
    try {
      const { client: updated } = await api.crmUpdateClient(id, {
        name: form.name, contact_name: form.contact_name,
        contact_email: form.contact_email, contact_phone: form.contact_phone,
        source: form.source, notes: form.notes,
      });
      setClient(updated);
      setEditing(false);
    } catch (e) {
      alert('更新に失敗しました');
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!confirm(`「${client.name}」を削除しますか？関連する案件もすべて削除されます。`)) return;
    await api.crmDeleteClient(id);
    navigate('/crm/clients');
  };

  const handleCreateDeal = async (e) => {
    e.preventDefault();
    setSaving(true);
    try {
      const { deal } = await api.crmCreateDeal({
        clientId: id,
        name: dealForm.name,
        stage: dealForm.stage,
        budget: dealForm.budget ? Number(dealForm.budget) : null,
        notes: dealForm.notes,
      });
      navigate(`/crm/deals/${deal.id}`);
    } catch (e) {
      alert('作成に失敗しました');
    } finally {
      setSaving(false);
    }
  };

  if (loading) return <div className="loading">読み込み中...</div>;
  if (!client) return <div className="loading">顧客が見つかりません</div>;

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>
          <Link to="/crm/clients" style={{ color: '#888', fontWeight: 400, marginRight: 8 }}>顧客一覧</Link>
          / {client.name}
        </h1>
        <div className="header-right">
          <Link to="/crm/deals" className="analytics-link">案件一覧</Link>
          {!editing && <button className="analytics-link" onClick={() => setEditing(true)}>編集</button>}
          <button className="filter-clear-btn" onClick={handleDelete}>削除</button>
        </div>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: 24, padding: '16px 24px' }}>
        <main>
          {/* 顧客情報 */}
          <section style={{ background: '#fff', borderRadius: 8, padding: 20, marginBottom: 20, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <h2 style={{ marginBottom: 16, fontSize: 16 }}>基本情報</h2>
            {editing ? (
              <div>
                {[
                  { label: '会社名', key: 'name', required: true },
                  { label: '担当者名', key: 'contact_name' },
                  { label: 'メール', key: 'contact_email', type: 'email' },
                  { label: '電話', key: 'contact_phone' },
                  { label: '問い合わせ元', key: 'source' },
                ].map(f => (
                  <div key={f.key} style={{ marginBottom: 12 }}>
                    <label style={{ display: 'block', marginBottom: 4, fontWeight: f.required ? 600 : 400 }}>{f.label}</label>
                    <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }}
                      type={f.type || 'text'}
                      value={form[f.key] || ''}
                      onChange={e => setForm(v => ({ ...v, [f.key]: e.target.value }))} />
                  </div>
                ))}
                <div style={{ marginBottom: 16 }}>
                  <label style={{ display: 'block', marginBottom: 4 }}>メモ</label>
                  <textarea className="filter-select" style={{ width: '100%', padding: '6px 10px', height: 80 }}
                    value={form.notes || ''} onChange={e => setForm(v => ({ ...v, notes: e.target.value }))} />
                </div>
                <div style={{ display: 'flex', gap: 8 }}>
                  <button className="admin-link" onClick={handleSave} disabled={saving}>{saving ? '保存中...' : '保存'}</button>
                  <button className="filter-clear-btn" onClick={() => { setEditing(false); setForm(client); }}>キャンセル</button>
                </div>
              </div>
            ) : (
              <dl style={{ display: 'grid', gridTemplateColumns: '120px 1fr', rowGap: 10 }}>
                {[
                  ['担当者名', client.contact_name],
                  ['メール', client.contact_email],
                  ['電話', client.contact_phone],
                  ['問い合わせ元', client.source],
                  ['メモ', client.notes],
                ].map(([label, val]) => (
                  <>
                    <dt key={'dt-' + label} style={{ color: '#888', fontSize: 13 }}>{label}</dt>
                    <dd key={'dd-' + label} style={{ margin: 0 }}>{val || '-'}</dd>
                  </>
                ))}
              </dl>
            )}
          </section>

          {/* 案件一覧 */}
          <section style={{ background: '#fff', borderRadius: 8, padding: 20, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <h2 style={{ fontSize: 16, margin: 0 }}>案件</h2>
              <button className="admin-link" onClick={() => setShowDeal(true)}>＋ 案件追加</button>
            </div>
            {deals.length === 0 ? (
              <div style={{ color: '#888' }}>案件がまだありません</div>
            ) : (
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ borderBottom: '2px solid #eee', textAlign: 'left' }}>
                    <th style={{ padding: '6px 10px' }}>案件名</th>
                    <th style={{ padding: '6px 10px' }}>ステージ</th>
                    <th style={{ padding: '6px 10px' }}>予算</th>
                    <th style={{ padding: '6px 10px' }}>更新日</th>
                  </tr>
                </thead>
                <tbody>
                  {deals.map(d => (
                    <tr key={d.id} style={{ borderBottom: '1px solid #f0f0f0', cursor: 'pointer' }}
                      onClick={() => navigate(`/crm/deals/${d.id}`)}>
                      <td style={{ padding: '8px 10px', fontWeight: 600 }}>{d.name}</td>
                      <td style={{ padding: '8px 10px' }}>
                        <span className={`stage-badge stage-${d.stage}`}>{stageLabel(d.stage)}</span>
                      </td>
                      <td style={{ padding: '8px 10px' }}>{d.budget ? `¥${d.budget.toLocaleString()}` : '-'}</td>
                      <td style={{ padding: '8px 10px', color: '#888', fontSize: 13 }}>
                        {new Date(d.updated_at).toLocaleDateString('ja-JP')}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        </main>

        <aside>
          <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <h3 style={{ fontSize: 14, marginBottom: 12 }}>登録情報</h3>
            <div style={{ fontSize: 13, color: '#888' }}>
              <div>登録日：{new Date(client.created_at).toLocaleDateString('ja-JP')}</div>
              <div>更新日：{new Date(client.updated_at).toLocaleDateString('ja-JP')}</div>
            </div>
          </div>
        </aside>
      </div>

      {showDeal && (
        <div className="modal-overlay" onClick={() => setShowDeal(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()} style={{ maxWidth: 440 }}>
            <h2 style={{ marginBottom: 16 }}>案件追加</h2>
            <form onSubmit={handleCreateDeal}>
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', marginBottom: 4, fontWeight: 600 }}>案件名 *</label>
                <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }}
                  value={dealForm.name} onChange={e => setDealForm(f => ({ ...f, name: e.target.value }))} required />
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', marginBottom: 4 }}>ステージ</label>
                <select className="filter-select" style={{ width: '100%', padding: '6px 10px' }}
                  value={dealForm.stage} onChange={e => setDealForm(f => ({ ...f, stage: e.target.value }))}>
                  {STAGES.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
                </select>
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', marginBottom: 4 }}>予算（円）</label>
                <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }} type="number"
                  value={dealForm.budget} onChange={e => setDealForm(f => ({ ...f, budget: e.target.value }))} />
              </div>
              <div style={{ marginBottom: 16 }}>
                <label style={{ display: 'block', marginBottom: 4 }}>メモ</label>
                <textarea className="filter-select" style={{ width: '100%', padding: '6px 10px', height: 60 }}
                  value={dealForm.notes} onChange={e => setDealForm(f => ({ ...f, notes: e.target.value }))} />
              </div>
              <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                <button type="button" className="filter-clear-btn" onClick={() => setShowDeal(false)}>キャンセル</button>
                <button type="submit" className="admin-link" disabled={saving}>{saving ? '作成中...' : '作成'}</button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
