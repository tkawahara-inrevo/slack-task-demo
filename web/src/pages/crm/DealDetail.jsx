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

export default function DealDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [deal, setDeal] = useState(null);
  const [members, setMembers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState(false);
  const [form, setForm] = useState({});
  const [saving, setSaving] = useState(false);

  const load = () => {
    api.crmDealDetail(id)
      .then(r => { setDeal(r.deal); setMembers(r.members); setForm(r.deal); })
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, [id]);

  const handleSave = async () => {
    setSaving(true);
    try {
      const { deal: updated } = await api.crmUpdateDeal(id, {
        name: form.name, stage: form.stage,
        budget: form.budget ? Number(form.budget) : null,
        notes: form.notes,
      });
      setDeal(updated);
      setEditing(false);
    } catch (e) {
      alert('更新に失敗しました');
    } finally {
      setSaving(false);
    }
  };

  const handleStageChange = async (stage) => {
    try {
      const { deal: updated } = await api.crmUpdateDeal(id, { stage });
      setDeal(updated);
      setForm(f => ({ ...f, stage }));
    } catch (e) {
      alert('ステージ更新に失敗しました');
    }
  };

  const handleDelete = async () => {
    if (!confirm(`「${deal.name}」を削除しますか？`)) return;
    await api.crmDeleteDeal(id);
    navigate('/crm/deals');
  };

  if (loading) return <div className="loading">読み込み中...</div>;
  if (!deal) return <div className="loading">案件が見つかりません</div>;

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>
          <Link to="/crm/clients" style={{ color: '#888', fontWeight: 400 }}>顧客</Link>
          {' / '}
          <Link to={`/crm/clients/${deal.client_id}`} style={{ color: '#888', fontWeight: 400 }}>{deal.client_name}</Link>
          {' / '}{deal.name}
        </h1>
        <div className="header-right">
          <Link to="/crm/deals" className="analytics-link">案件一覧</Link>
          {!editing && <button className="analytics-link" onClick={() => setEditing(true)}>編集</button>}
          <button className="filter-clear-btn" onClick={handleDelete}>削除</button>
        </div>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 280px', gap: 24, padding: '16px 24px' }}>
        <main>
          {/* ステージ */}
          <section style={{ background: '#fff', borderRadius: 8, padding: 20, marginBottom: 20, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <h2 style={{ fontSize: 15, marginBottom: 12 }}>パイプラインステージ</h2>
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
              {STAGES.map(s => (
                <button key={s.value}
                  onClick={() => !editing && handleStageChange(s.value)}
                  style={{
                    padding: '6px 14px', borderRadius: 20, border: '2px solid',
                    borderColor: deal.stage === s.value ? '#1976d2' : '#ddd',
                    background: deal.stage === s.value ? '#1976d2' : '#fff',
                    color: deal.stage === s.value ? '#fff' : '#555',
                    cursor: 'pointer', fontWeight: deal.stage === s.value ? 600 : 400,
                    fontSize: 13,
                  }}>
                  {s.label}
                </button>
              ))}
            </div>
          </section>

          {/* 案件情報 */}
          <section style={{ background: '#fff', borderRadius: 8, padding: 20, marginBottom: 20, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <h2 style={{ fontSize: 15, marginBottom: 12 }}>案件情報</h2>
            {editing ? (
              <div>
                <div style={{ marginBottom: 12 }}>
                  <label style={{ display: 'block', marginBottom: 4, fontWeight: 600 }}>案件名</label>
                  <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }}
                    value={form.name || ''} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} />
                </div>
                <div style={{ marginBottom: 12 }}>
                  <label style={{ display: 'block', marginBottom: 4 }}>予算（円）</label>
                  <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }} type="number"
                    value={form.budget || ''} onChange={e => setForm(f => ({ ...f, budget: e.target.value }))} />
                </div>
                <div style={{ marginBottom: 16 }}>
                  <label style={{ display: 'block', marginBottom: 4 }}>メモ</label>
                  <textarea className="filter-select" style={{ width: '100%', padding: '6px 10px', height: 100 }}
                    value={form.notes || ''} onChange={e => setForm(f => ({ ...f, notes: e.target.value }))} />
                </div>
                <div style={{ display: 'flex', gap: 8 }}>
                  <button className="admin-link" onClick={handleSave} disabled={saving}>{saving ? '保存中...' : '保存'}</button>
                  <button className="filter-clear-btn" onClick={() => { setEditing(false); setForm(deal); }}>キャンセル</button>
                </div>
              </div>
            ) : (
              <dl style={{ display: 'grid', gridTemplateColumns: '100px 1fr', rowGap: 10 }}>
                <dt style={{ color: '#888', fontSize: 13 }}>予算</dt>
                <dd style={{ margin: 0 }}>{deal.budget ? `¥${deal.budget.toLocaleString()}` : '-'}</dd>
                <dt style={{ color: '#888', fontSize: 13 }}>メモ</dt>
                <dd style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{deal.notes || '-'}</dd>
              </dl>
            )}
          </section>
        </main>

        <aside>
          <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 4px rgba(0,0,0,0.08)', marginBottom: 16 }}>
            <h3 style={{ fontSize: 14, marginBottom: 12 }}>担当メンバー</h3>
            {members.length === 0 ? (
              <div style={{ color: '#aaa', fontSize: 13 }}>未設定</div>
            ) : (
              <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
                {members.map(m => (
                  <li key={m.user_id} style={{ padding: '4px 0', fontSize: 14, display: 'flex', justifyContent: 'space-between' }}>
                    <span>{m.displayName || m.user_id}</span>
                    <span style={{ fontSize: 12, color: '#888' }}>{m.role === 'admin' ? '管理者' : 'メンバー'}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <h3 style={{ fontSize: 14, marginBottom: 12 }}>情報</h3>
            <div style={{ fontSize: 13, color: '#888', lineHeight: 1.8 }}>
              <div>顧客：<Link to={`/crm/clients/${deal.client_id}`}>{deal.client_name}</Link></div>
              <div>ステージ：{stageLabel(deal.stage)}</div>
              <div>作成日：{new Date(deal.created_at).toLocaleDateString('ja-JP')}</div>
              <div>更新日：{new Date(deal.updated_at).toLocaleDateString('ja-JP')}</div>
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}
