import { useEffect, useState, useRef } from 'react';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

const STAGES = [
  { value: 'mk', label: 'MK（アポ取り）', color: '#6c8ebf' },
  { value: 'bc', label: 'BC（商談中）', color: '#d79b00' },
  { value: 'contracted', label: '受注済', color: '#00897b' },
  { value: 'hr', label: 'HR分析中', color: '#7b1fa2' },
  { value: 'direction', label: 'ディレクション', color: '#e65100' },
  { value: 'cs', label: 'CS（スカウト）', color: '#0277bd' },
  { value: 'completed', label: '完了', color: '#388e3c' },
  { value: 'lost', label: '失注', color: '#757575' },
];

const ACTIVITY_TYPES = [
  { value: 'note', label: 'メモ', icon: '📝' },
  { value: 'meeting', label: '面談・商談', icon: '🤝' },
  { value: 'call', label: '電話', icon: '📞' },
  { value: 'email', label: 'メール', icon: '✉️' },
  { value: 'stage_change', label: 'ステージ変更', icon: '🔄' },
  { value: 'document', label: '書類', icon: '📄' },
  { value: 'other', label: 'その他', icon: '💬' },
];

function stageLabel(v) { return STAGES.find(s => s.value === v)?.label || v; }
function stageColor(v) { return STAGES.find(s => s.value === v)?.color || '#888'; }
function actIcon(t) { return ACTIVITY_TYPES.find(a => a.value === t)?.icon || '💬'; }
function actLabel(t) { return ACTIVITY_TYPES.find(a => a.value === t)?.label || t; }

function timeAgo(dateStr) {
  const d = new Date(dateStr);
  const diff = Date.now() - d.getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'たった今';
  if (mins < 60) return `${mins}分前`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}時間前`;
  return d.toLocaleDateString('ja-JP');
}

export default function DealDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [editingInfo, setEditingInfo] = useState(false);
  const [infoForm, setInfoForm] = useState({});
  const [saving, setSaving] = useState(false);
  const [tab, setTab] = useState('activity'); // 'activity' | 'payments' | 'tasks' | 'members'

  // Activity
  const [actForm, setActForm] = useState({ activityType: 'note', content: '' });
  const [addingActivity, setAddingActivity] = useState(false);

  // Payment
  const [showPayment, setShowPayment] = useState(false);
  const [payForm, setPayForm] = useState({ label: '', amount: '', dueDate: '', notes: '' });

  // Member
  const [memberInput, setMemberInput] = useState('');
  const [allMembers, setAllMembers] = useState([]);

  const load = () => {
    Promise.all([api.crmDealFull(id), api.members()])
      .then(([full, mem]) => {
        setData(full);
        setInfoForm(full.deal);
        setAllMembers(mem.members || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, [id]);

  const handleSaveInfo = async () => {
    setSaving(true);
    try {
      const { deal } = await api.crmUpdateDeal(id, {
        name: infoForm.name, budget: infoForm.budget ? Number(infoForm.budget) : null, notes: infoForm.notes,
      });
      setData(d => ({ ...d, deal }));
      setEditingInfo(false);
    } catch { alert('更新に失敗しました'); }
    finally { setSaving(false); }
  };

  const handleStageChange = async (stage) => {
    try {
      const { deal } = await api.crmUpdateDeal(id, { stage });
      // auto-log stage change
      await api.crmAddActivity(id, {
        activityType: 'stage_change',
        content: `${stageLabel(data.deal.stage)} → ${stageLabel(stage)}`,
      });
      setData(d => ({ ...d, deal }));
      load(); // refresh activities
    } catch { alert('ステージ更新に失敗しました'); }
  };

  const handleAddActivity = async (e) => {
    e.preventDefault();
    if (!actForm.content.trim()) return;
    setAddingActivity(true);
    try {
      const { activity } = await api.crmAddActivity(id, actForm);
      setData(d => ({ ...d, activities: [activity, ...d.activities] }));
      setActForm(f => ({ ...f, content: '' }));
      load();
    } catch { alert('追加に失敗しました'); }
    finally { setAddingActivity(false); }
  };

  const handleDeleteActivity = async (actId) => {
    if (!confirm('削除しますか？')) return;
    await api.crmDeleteActivity(id, actId);
    setData(d => ({ ...d, activities: d.activities.filter(a => a.id !== actId) }));
  };

  const handleAddPayment = async (e) => {
    e.preventDefault();
    setSaving(true);
    try {
      const { payment } = await api.crmAddPayment(id, {
        label: payForm.label, amount: Number(payForm.amount),
        dueDate: payForm.dueDate || null, notes: payForm.notes,
      });
      setData(d => ({ ...d, payments: [...d.payments, payment] }));
      setShowPayment(false);
      setPayForm({ label: '', amount: '', dueDate: '', notes: '' });
    } catch { alert('追加に失敗しました'); }
    finally { setSaving(false); }
  };

  const handlePaymentStatus = async (payId, status, paidDate) => {
    const body = { status };
    if (paidDate) body.paid_date = paidDate;
    const { payments } = await api.crmUpdatePayment(id, payId, body);
    setData(d => ({ ...d, payments }));
  };

  const handleDeletePayment = async (payId) => {
    if (!confirm('削除しますか？')) return;
    await api.crmDeletePayment(id, payId);
    setData(d => ({ ...d, payments: d.payments.filter(p => p.id !== payId) }));
  };

  const handleAddMember = async (userId) => {
    await api.crmAddDealMember(id, userId, 'member');
    load();
  };

  const handleRemoveMember = async (userId) => {
    if (!confirm('メンバーを外しますか？')) return;
    await api.crmRemoveDealMember(id, userId);
    load();
  };

  const handleDelete = async () => {
    if (!confirm(`「${data.deal.name}」を削除しますか？`)) return;
    await api.crmDeleteDeal(id);
    navigate('/crm/deals');
  };

  if (loading) return <div className="loading">読み込み中...</div>;
  if (!data) return <div className="loading">案件が見つかりません</div>;

  const { deal, members, activities, payments, tasks } = data;
  const stageIdx = STAGES.findIndex(s => s.value === deal.stage);
  const totalPayments = payments.reduce((s, p) => s + (p.amount || 0), 0);
  const paidPayments = payments.filter(p => p.status === 'paid').reduce((s, p) => s + (p.amount || 0), 0);
  const memberIds = members.map(m => m.user_id);
  const availableToAdd = allMembers.filter(m => !memberIds.includes(m.assignee_id));

  return (
    <div className="dashboard" style={{ minHeight: '100vh', background: '#f8f9fa' }}>
      <header className="dashboard-header">
        <h1 style={{ fontSize: 17 }}>
          <Link to="/crm/clients" style={{ color: '#888', fontWeight: 400 }}>顧客</Link>
          {' / '}
          <Link to={`/crm/clients/${deal.client_id}`} style={{ color: '#888', fontWeight: 400 }}>{deal.client_name}</Link>
          {' / '}<span>{deal.name}</span>
        </h1>
        <div className="header-right">
          <Link to="/crm/deals" className="analytics-link">案件一覧</Link>
          <button className="filter-clear-btn" onClick={handleDelete}>削除</button>
        </div>
      </header>

      {/* ステージバー */}
      <div style={{ background: '#fff', borderBottom: '1px solid #eee', padding: '12px 24px', overflowX: 'auto' }}>
        <div style={{ display: 'flex', gap: 0, minWidth: 600 }}>
          {STAGES.filter(s => s.value !== 'lost').map((s, i) => {
            const active = deal.stage === s.value;
            const done = stageIdx > STAGES.indexOf(s);
            const isLast = i === STAGES.filter(s => s.value !== 'lost').length - 1;
            return (
              <button key={s.value}
                onClick={() => handleStageChange(s.value)}
                title={s.label}
                style={{
                  flex: 1, padding: '8px 4px', fontSize: 12, cursor: 'pointer',
                  background: active ? s.color : done ? '#e8f5e9' : '#f5f5f5',
                  color: active ? '#fff' : done ? '#388e3c' : '#555',
                  border: 'none', borderRight: isLast ? 'none' : '1px solid #ddd',
                  fontWeight: active ? 700 : 400,
                  transition: 'all 0.15s',
                }}>
                {s.label}
              </button>
            );
          })}
          <button
            onClick={() => handleStageChange('lost')}
            style={{
              padding: '8px 12px', fontSize: 12, cursor: 'pointer',
              background: deal.stage === 'lost' ? '#757575' : '#f5f5f5',
              color: deal.stage === 'lost' ? '#fff' : '#999',
              border: 'none', borderLeft: '2px solid #ddd', fontWeight: deal.stage === 'lost' ? 700 : 400,
            }}>失注</button>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: 20, padding: '20px 24px', maxWidth: 1200, margin: '0 auto' }}>
        <main>
          {/* タブ */}
          <div style={{ display: 'flex', gap: 0, borderBottom: '2px solid #eee', marginBottom: 16 }}>
            {[
              { key: 'activity', label: `活動（${activities.length}）` },
              { key: 'payments', label: `入金（${payments.length}）` },
              { key: 'tasks', label: `タスク（${tasks.length}）` },
              { key: 'members', label: `メンバー（${members.length}）` },
            ].map(t => (
              <button key={t.key}
                onClick={() => setTab(t.key)}
                style={{
                  padding: '8px 16px', border: 'none', background: 'none', cursor: 'pointer',
                  fontWeight: tab === t.key ? 700 : 400,
                  borderBottom: tab === t.key ? '2px solid #1976d2' : '2px solid transparent',
                  color: tab === t.key ? '#1976d2' : '#555',
                  marginBottom: -2,
                }}>
                {t.label}
              </button>
            ))}
          </div>

          {/* 活動タブ */}
          {tab === 'activity' && (
            <div>
              <form onSubmit={handleAddActivity} style={{ background: '#fff', borderRadius: 8, padding: 16, marginBottom: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
                <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>
                  {ACTIVITY_TYPES.filter(a => a.value !== 'stage_change').map(a => (
                    <button key={a.value} type="button"
                      onClick={() => setActForm(f => ({ ...f, activityType: a.value }))}
                      style={{
                        padding: '4px 10px', borderRadius: 16, border: '1px solid',
                        borderColor: actForm.activityType === a.value ? '#1976d2' : '#ddd',
                        background: actForm.activityType === a.value ? '#e3f2fd' : '#fff',
                        color: actForm.activityType === a.value ? '#1976d2' : '#555',
                        cursor: 'pointer', fontSize: 12,
                      }}>
                      {a.icon} {a.label}
                    </button>
                  ))}
                </div>
                <textarea
                  style={{ width: '100%', border: '1px solid #ddd', borderRadius: 6, padding: '8px 10px', resize: 'vertical', minHeight: 70, fontSize: 14, boxSizing: 'border-box' }}
                  placeholder="内容を入力..."
                  value={actForm.content}
                  onChange={e => setActForm(f => ({ ...f, content: e.target.value }))}
                />
                <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 8 }}>
                  <button type="submit" className="admin-link" disabled={addingActivity || !actForm.content.trim()}>
                    {addingActivity ? '追加中...' : '追加'}
                  </button>
                </div>
              </form>

              {activities.length === 0 ? (
                <div style={{ color: '#aaa', textAlign: 'center', padding: 24 }}>活動記録がありません</div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  {activities.map(a => (
                    <div key={a.id} style={{ background: '#fff', borderRadius: 8, padding: '12px 16px', boxShadow: '0 1px 3px rgba(0,0,0,0.06)', borderLeft: a.activity_type === 'stage_change' ? '3px solid #1976d2' : '3px solid #eee' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                          <span style={{ fontSize: 16 }}>{actIcon(a.activity_type)}</span>
                          <span style={{ fontSize: 12, color: '#888' }}>{actLabel(a.activity_type)}</span>
                          <span style={{ fontSize: 12, color: '#aaa' }}>·</span>
                          <span style={{ fontSize: 12, color: '#888' }}>{a.displayName}</span>
                          <span style={{ fontSize: 12, color: '#aaa' }}>·</span>
                          <span style={{ fontSize: 12, color: '#aaa' }}>{timeAgo(a.created_at)}</span>
                        </div>
                        {a.activity_type !== 'stage_change' && (
                          <button onClick={() => handleDeleteActivity(a.id)}
                            style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer', fontSize: 16, lineHeight: 1 }}>×</button>
                        )}
                      </div>
                      <div style={{ marginTop: 8, fontSize: 14, whiteSpace: 'pre-wrap', lineHeight: 1.6 }}>{a.content}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* 入金タブ */}
          {tab === 'payments' && (
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                <div style={{ fontSize: 14, color: '#555' }}>
                  合計: <strong>¥{totalPayments.toLocaleString()}</strong>
                  　入金済: <strong style={{ color: '#388e3c' }}>¥{paidPayments.toLocaleString()}</strong>
                  　残: <strong style={{ color: '#c62828' }}>¥{(totalPayments - paidPayments).toLocaleString()}</strong>
                </div>
                <button className="admin-link" onClick={() => setShowPayment(true)}>＋ 請求追加</button>
              </div>
              {payments.length === 0 ? (
                <div style={{ color: '#aaa', textAlign: 'center', padding: 24 }}>請求・入金記録がありません</div>
              ) : (
                <table style={{ width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
                  <thead>
                    <tr style={{ background: '#f5f5f5', textAlign: 'left' }}>
                      {['項目', '金額', '請求日', '入金日', 'ステータス', ''].map(h => (
                        <th key={h} style={{ padding: '8px 12px', fontSize: 13, fontWeight: 600 }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {payments.map(p => (
                      <tr key={p.id} style={{ borderBottom: '1px solid #f0f0f0' }}>
                        <td style={{ padding: '10px 12px' }}>{p.label}</td>
                        <td style={{ padding: '10px 12px', fontWeight: 600 }}>¥{p.amount.toLocaleString()}</td>
                        <td style={{ padding: '10px 12px', fontSize: 13, color: '#666' }}>{p.due_date ? new Date(p.due_date).toLocaleDateString('ja-JP') : '-'}</td>
                        <td style={{ padding: '10px 12px', fontSize: 13, color: '#666' }}>{p.paid_date ? new Date(p.paid_date).toLocaleDateString('ja-JP') : '-'}</td>
                        <td style={{ padding: '10px 12px' }}>
                          {p.status === 'paid' ? (
                            <span style={{ background: '#e8f5e9', color: '#388e3c', padding: '2px 8px', borderRadius: 12, fontSize: 12 }}>入金済</span>
                          ) : (
                            <button onClick={() => handlePaymentStatus(p.id, 'paid', new Date().toISOString().split('T')[0])}
                              style={{ background: '#fff3e0', color: '#e65100', padding: '2px 8px', borderRadius: 12, fontSize: 12, border: 'none', cursor: 'pointer' }}>
                              未入金
                            </button>
                          )}
                        </td>
                        <td style={{ padding: '10px 12px' }}>
                          <button onClick={() => handleDeletePayment(p.id)}
                            style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer', fontSize: 14 }}>削除</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              {showPayment && (
                <div className="modal-overlay" onClick={() => setShowPayment(false)}>
                  <div className="modal-content" onClick={e => e.stopPropagation()} style={{ maxWidth: 400 }}>
                    <h2 style={{ marginBottom: 16 }}>請求追加</h2>
                    <form onSubmit={handleAddPayment}>
                      {[
                        { label: '項目名', key: 'label', placeholder: '例：初回請求', required: true },
                        { label: '金額（円）', key: 'amount', type: 'number', required: true },
                        { label: '請求日', key: 'dueDate', type: 'date' },
                        { label: 'メモ', key: 'notes' },
                      ].map(f => (
                        <div key={f.key} style={{ marginBottom: 12 }}>
                          <label style={{ display: 'block', marginBottom: 4, fontWeight: f.required ? 600 : 400 }}>{f.label}</label>
                          <input className="filter-select" style={{ width: '100%', padding: '6px 10px' }}
                            type={f.type || 'text'} placeholder={f.placeholder} required={f.required}
                            value={payForm[f.key]} onChange={e => setPayForm(v => ({ ...v, [f.key]: e.target.value }))} />
                        </div>
                      ))}
                      <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
                        <button type="button" className="filter-clear-btn" onClick={() => setShowPayment(false)}>キャンセル</button>
                        <button type="submit" className="admin-link" disabled={saving}>{saving ? '追加中...' : '追加'}</button>
                      </div>
                    </form>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* タスクタブ */}
          {tab === 'tasks' && (
            <div>
              {tasks.length === 0 ? (
                <div style={{ color: '#aaa', textAlign: 'center', padding: 24 }}>紐づいたタスクがありません</div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {tasks.map(t => (
                    <div key={t.id} style={{ background: '#fff', borderRadius: 8, padding: '12px 16px', boxShadow: '0 1px 3px rgba(0,0,0,0.06)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <div>
                        <Link to={`/tasks/${t.id}`} style={{ fontWeight: 600, color: '#1976d2' }}>{t.title || '（タイトルなし）'}</Link>
                        <div style={{ fontSize: 12, color: '#888', marginTop: 4 }}>
                          {t.status} · {t.due_date ? new Date(t.due_date).toLocaleDateString('ja-JP') : '期限なし'}
                        </div>
                      </div>
                      <button onClick={() => api.crmRemoveDealTask(id, t.id).then(load)}
                        style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer' }}>外す</button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* メンバータブ */}
          {tab === 'members' && (
            <div>
              <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)', marginBottom: 16 }}>
                <h3 style={{ fontSize: 14, marginBottom: 12 }}>担当メンバー</h3>
                {members.length === 0 ? (
                  <div style={{ color: '#aaa' }}>未設定</div>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                    {members.map(m => (
                      <div key={m.user_id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '8px 0', borderBottom: '1px solid #f5f5f5' }}>
                        <div>
                          <span style={{ fontWeight: 600 }}>{m.displayName || m.user_id}</span>
                          <span style={{ fontSize: 12, color: '#888', marginLeft: 8 }}>{m.role === 'admin' ? '管理者' : 'メンバー'}</span>
                        </div>
                        {m.role !== 'admin' && (
                          <button onClick={() => handleRemoveMember(m.user_id)}
                            style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer', fontSize: 13 }}>外す</button>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </div>
              {availableToAdd.length > 0 && (
                <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
                  <h3 style={{ fontSize: 14, marginBottom: 12 }}>メンバー追加</h3>
                  <select className="filter-select" style={{ width: '100%', padding: '6px 10px', marginBottom: 8 }}
                    value={memberInput} onChange={e => setMemberInput(e.target.value)}>
                    <option value="">メンバーを選択...</option>
                    {availableToAdd.map(m => (
                      <option key={m.assignee_id} value={m.assignee_id}>{m.displayName}</option>
                    ))}
                  </select>
                  <button className="admin-link" disabled={!memberInput} onClick={() => { handleAddMember(memberInput); setMemberInput(''); }}>
                    追加
                  </button>
                </div>
              )}
            </div>
          )}
        </main>

        {/* サイドバー */}
        <aside style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          {/* 案件情報 */}
          <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
              <h3 style={{ fontSize: 14, margin: 0 }}>案件情報</h3>
              {!editingInfo && <button style={{ background: 'none', border: 'none', color: '#888', cursor: 'pointer', fontSize: 12 }} onClick={() => setEditingInfo(true)}>編集</button>}
            </div>
            {editingInfo ? (
              <div>
                <div style={{ marginBottom: 10 }}>
                  <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>案件名</label>
                  <input className="filter-select" style={{ width: '100%', padding: '5px 8px', fontSize: 13 }}
                    value={infoForm.name || ''} onChange={e => setInfoForm(f => ({ ...f, name: e.target.value }))} />
                </div>
                <div style={{ marginBottom: 10 }}>
                  <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>予算（円）</label>
                  <input className="filter-select" style={{ width: '100%', padding: '5px 8px', fontSize: 13 }} type="number"
                    value={infoForm.budget || ''} onChange={e => setInfoForm(f => ({ ...f, budget: e.target.value }))} />
                </div>
                <div style={{ marginBottom: 10 }}>
                  <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>メモ</label>
                  <textarea className="filter-select" style={{ width: '100%', padding: '5px 8px', fontSize: 13, height: 70 }}
                    value={infoForm.notes || ''} onChange={e => setInfoForm(f => ({ ...f, notes: e.target.value }))} />
                </div>
                <div style={{ display: 'flex', gap: 6 }}>
                  <button className="admin-link" style={{ fontSize: 12, padding: '4px 12px' }} onClick={handleSaveInfo} disabled={saving}>{saving ? '...' : '保存'}</button>
                  <button className="filter-clear-btn" style={{ fontSize: 12 }} onClick={() => { setEditingInfo(false); setInfoForm(deal); }}>キャンセル</button>
                </div>
              </div>
            ) : (
              <dl style={{ margin: 0, fontSize: 13 }}>
                <dt style={{ color: '#888', marginBottom: 2 }}>顧客</dt>
                <dd style={{ margin: '0 0 10px' }}><Link to={`/crm/clients/${deal.client_id}`}>{deal.client_name}</Link></dd>
                <dt style={{ color: '#888', marginBottom: 2 }}>ステージ</dt>
                <dd style={{ margin: '0 0 10px' }}>
                  <span style={{ background: stageColor(deal.stage), color: '#fff', padding: '2px 10px', borderRadius: 12, fontSize: 12 }}>
                    {stageLabel(deal.stage)}
                  </span>
                </dd>
                <dt style={{ color: '#888', marginBottom: 2 }}>予算</dt>
                <dd style={{ margin: '0 0 10px' }}>{deal.budget ? `¥${deal.budget.toLocaleString()}` : '-'}</dd>
                {deal.notes && <>
                  <dt style={{ color: '#888', marginBottom: 2 }}>メモ</dt>
                  <dd style={{ margin: '0 0 10px', whiteSpace: 'pre-wrap' }}>{deal.notes}</dd>
                </>}
              </dl>
            )}
          </div>

          {/* 入金サマリー */}
          {payments.length > 0 && (
            <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
              <h3 style={{ fontSize: 14, marginBottom: 12 }}>入金状況</h3>
              <div style={{ fontSize: 13 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                  <span style={{ color: '#888' }}>合計</span><strong>¥{totalPayments.toLocaleString()}</strong>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                  <span style={{ color: '#388e3c' }}>入金済</span><strong style={{ color: '#388e3c' }}>¥{paidPayments.toLocaleString()}</strong>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: '#c62828' }}>残</span><strong style={{ color: '#c62828' }}>¥{(totalPayments - paidPayments).toLocaleString()}</strong>
                </div>
                <div style={{ marginTop: 10, background: '#f5f5f5', borderRadius: 4, height: 6, overflow: 'hidden' }}>
                  <div style={{ height: '100%', background: '#388e3c', width: totalPayments ? `${(paidPayments / totalPayments) * 100}%` : '0%' }} />
                </div>
              </div>
            </div>
          )}

          {/* タイムスタンプ */}
          <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)', fontSize: 13, color: '#888' }}>
            <div>作成：{new Date(deal.created_at).toLocaleDateString('ja-JP')}</div>
            <div>更新：{new Date(deal.updated_at).toLocaleDateString('ja-JP')}</div>
          </div>
        </aside>
      </div>
    </div>
  );
}
