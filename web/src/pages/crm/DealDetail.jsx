import { useEffect, useState } from 'react';
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

const YOMI_OPTIONS = [
  'E（5%）', 'D（15%）', 'C（30%）', 'B（50%）', 'A（70%）', 'S（90%）',
  '受注', '失注', 'アポ化前', 'アポ化済',
];

const CONTRACT_TYPES = [
  '月額コンサルのみ', '月額実務のみ', '月額フルコミット',
  '後払い媒体費INREVO', '後払い媒体費クライアント',
  '採用保証分析付き', '採用保証人材紹介',
];

const PAYMENT_METHODS = ['月額', '後払い', '採用保証', '変動プラン'];
const APPT_TYPES = ['月額', '後払い', '採用保証', '不明'];
const HIRE_TYPES = ['新卒', '中途', '業務委託', 'アルバイト'];
const LOSS_REASONS = [
  'ニーズなし', '金額NG', '競合負け', '採用予定なし', '多忙',
  '人材紹介のみ', '時期が違う', '前払いNG', '企業年数', '外注意思なし', '上長NG',
];
const NEXT_ACTION_CONTENTS = ['面談CS', '架電', '商談', 'メール', 'その他'];
const POSITION_STATUSES = ['進行中', '達成', '停止', '完了'];
const ACTIVITY_RESULTS = ['', '有効会話', 'アポ獲得', 'コネクト', '不通'];

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
function fmtDate(d) { return d ? new Date(d).toLocaleDateString('ja-JP') : '-'; }
function fmtMoney(v) { return v != null && v !== '' ? `¥${Number(v).toLocaleString()}` : '-'; }

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

function evalCalc(expression, deal) {
  try {
    const keys = Object.keys(deal);
    const vals = keys.map(k => { const v = deal[k]; return typeof v === 'number' ? v : (Number(v) || 0); });
    // eslint-disable-next-line no-new-func
    const fn = new Function(...keys, `return (${expression})`);
    const result = fn(...vals);
    return isFinite(result) ? result : null;
  } catch {
    return null;
  }
}

function SectionHeader({ title }) {
  return (
    <div style={{ fontSize: 11, fontWeight: 700, color: '#888', textTransform: 'uppercase', letterSpacing: 0.5, marginBottom: 10, marginTop: 20, borderBottom: '1px solid #f0f0f0', paddingBottom: 4 }}>
      {title}
    </div>
  );
}

function FormRow({ label, children, required }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <label style={{ display: 'block', fontSize: 12, color: '#555', marginBottom: 4, fontWeight: required ? 600 : 400 }}>
        {label}{required && <span style={{ color: '#e74c3c', marginLeft: 2 }}>*</span>}
      </label>
      {children}
    </div>
  );
}

const inputSx = {
  width: '100%', padding: '6px 10px', border: '1px solid #ddd', borderRadius: 6,
  fontSize: 13, boxSizing: 'border-box', background: '#fff',
};

function buildInfoForm(deal) {
  const hc = deal.hearing_challenges || {};
  return {
    name: deal.name || '',
    budget: deal.budget ?? '',
    notes: deal.notes || '',
    yomi: deal.yomi || '',
    visibility: deal.visibility || 'all',
    inrevo_person: deal.inrevo_person || '',
    sales_person: deal.sales_person || '',
    acquisition_person: deal.acquisition_person || '',
    appointment_type: deal.appointment_type || '',
    contract_type: deal.contract_type || '',
    payment_method: deal.payment_method || '',
    hire_type: Array.isArray(deal.hire_type) ? deal.hire_type : [],
    first_meeting_date: deal.first_meeting_date ? String(deal.first_meeting_date).split('T')[0] : '',
    acquisition_date: deal.acquisition_date ? String(deal.acquisition_date).split('T')[0] : '',
    contract_approval_date: deal.contract_approval_date ? String(deal.contract_approval_date).split('T')[0] : '',
    contract_send_date: deal.contract_send_date ? String(deal.contract_send_date).split('T')[0] : '',
    order_date: deal.order_date ? String(deal.order_date).split('T')[0] : '',
    conclusion_date: deal.conclusion_date ? String(deal.conclusion_date).split('T')[0] : '',
    next_action_date: deal.next_action_date ? String(deal.next_action_date).split('T')[0] : '',
    next_action_content: deal.next_action_content || '',
    next_action_detail: deal.next_action_detail || '',
    loss_reason: deal.loss_reason || '',
    loss_reason_detail: deal.loss_reason_detail || '',
    bant_budget: deal.bant_budget || '',
    bant_budget_memo: deal.bant_budget_memo || '',
    bant_authority: deal.bant_authority || '',
    bant_authority_memo: deal.bant_authority_memo || '',
    bant_needs: deal.bant_needs || '',
    bant_needs_memo: deal.bant_needs_memo || '',
    bant_timeframe: deal.bant_timeframe || '',
    bant_timeframe_memo: deal.bant_timeframe_memo || '',
    initial_cost: deal.initial_cost ?? '',
    monthly_cost: deal.monthly_cost ?? '',
    unit_price: deal.unit_price ?? '',
    contract_months: deal.contract_months ?? '',
    guarantee_count: deal.guarantee_count ?? '',
    guarantee_salary: deal.guarantee_salary ?? '',
    rate: deal.rate ?? '',
    advance_payment: deal.advance_payment ?? '',
    antisocial_check: deal.antisocial_check || false,
    legal_check: deal.legal_check || false,
    contract_approval: deal.contract_approval || false,
    contract_sent: deal.contract_sent || false,
    hearing_collected: deal.hearing_collected || false,
    sales_memo: deal.sales_memo || '',
    invoice_to_name: deal.invoice_to_name || '',
    invoice_to_email: deal.invoice_to_email || '',
    invoice_cc_email: deal.invoice_cc_email || '',
    contract_to_name: deal.contract_to_name || '',
    contract_to_email: deal.contract_to_email || '',
    contract_cc_email: deal.contract_cc_email || '',
    hearing_challenges: {
      applications: hc.applications || '',
      quality: hc.quality || '',
      budget: hc.budget || '',
      notes: hc.notes || '',
    },
  };
}

export default function DealDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [showEditModal, setShowEditModal] = useState(false);
  const [infoForm, setInfoForm] = useState({});
  const [saving, setSaving] = useState(false);
  const [tab, setTab] = useState('activity');

  // Activity
  const [actForm, setActForm] = useState({ activityType: 'note', content: '', result: '' });
  const [addingActivity, setAddingActivity] = useState(false);

  // Payment
  const [showPayment, setShowPayment] = useState(false);
  const [payForm, setPayForm] = useState({ label: '', amount: '', direction: '入金', dueDate: '', notes: '', invoice_sent: false, incentive_amount: '' });

  // Deliverables
  const [showDeliverable, setShowDeliverable] = useState(false);
  const [dlvForm, setDlvForm] = useState({ title: '', description: '', dueDate: '' });

  // Positions
  const [showPosModal, setShowPosModal] = useState(false);
  const [editingPos, setEditingPos] = useState(null);
  const [posForm, setPosForm] = useState({ role_name: '', applications_target: '', applications_actual: '', hires_target: '', hires_actual: '', status: '進行中' });

  // Media plans
  const [showMediaModal, setShowMediaModal] = useState(false);
  const [editingMedia, setEditingMedia] = useState(null);
  const [mediaForm, setMediaForm] = useState({ media_name: '', position: '', hire_count: '', posting_cost: '', result_fee: '', margin: '', net_cost: '', total_cost: '' });

  // Members
  const [memberInput, setMemberInput] = useState('');
  const [allMembers, setAllMembers] = useState([]);

  const load = () => {
    Promise.all([api.crmDealFull(id), api.members()])
      .then(([full, mem]) => {
        setData(full);
        setInfoForm(buildInfoForm(full.deal));
        setAllMembers(mem.members || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, [id]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSaveInfo = async () => {
    setSaving(true);
    try {
      const numField = (v) => (v !== '' && v != null) ? Number(v) : null;
      const body = {
        ...infoForm,
        budget: numField(infoForm.budget),
        initial_cost: numField(infoForm.initial_cost),
        monthly_cost: numField(infoForm.monthly_cost),
        unit_price: numField(infoForm.unit_price),
        contract_months: numField(infoForm.contract_months),
        guarantee_count: numField(infoForm.guarantee_count),
        guarantee_salary: numField(infoForm.guarantee_salary),
        rate: numField(infoForm.rate),
        advance_payment: numField(infoForm.advance_payment),
      };
      const { deal } = await api.crmUpdateDeal(id, body);
      setData(d => ({ ...d, deal }));
      setShowEditModal(false);
    } catch { alert('更新に失敗しました'); }
    finally { setSaving(false); }
  };

  const handleStageChange = async (stage) => {
    try {
      const { deal } = await api.crmUpdateDeal(id, { stage });
      setData(d => ({ ...d, deal }));
      load();
    } catch { alert('ステージ更新に失敗しました'); }
  };

  const handleAddActivity = async (e) => {
    e.preventDefault();
    if (!actForm.content.trim()) return;
    setAddingActivity(true);
    try {
      await api.crmAddActivity(id, actForm);
      setActForm(f => ({ ...f, content: '', result: '' }));
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
      await api.crmAddPayment(id, {
        label: payForm.label,
        amount: Number(payForm.amount),
        direction: payForm.direction,
        dueDate: payForm.dueDate || null,
        notes: payForm.notes,
        invoice_sent: payForm.invoice_sent,
        incentive_amount: payForm.incentive_amount !== '' ? Number(payForm.incentive_amount) : null,
      });
      setShowPayment(false);
      setPayForm({ label: '', amount: '', direction: '入金', dueDate: '', notes: '', invoice_sent: false, incentive_amount: '' });
      load();
    } catch { alert('追加に失敗しました'); }
    finally { setSaving(false); }
  };

  const handlePaymentStatus = async (payId, status, paidDate) => {
    const body = { status };
    if (paidDate) body.paid_date = paidDate;
    await api.crmUpdatePayment(id, payId, body);
    load();
  };

  const handleDeletePayment = async (payId) => {
    if (!confirm('削除しますか？')) return;
    await api.crmDeletePayment(id, payId);
    setData(d => ({ ...d, payments: d.payments.filter(p => p.id !== payId) }));
  };

  const handleSavePos = async () => {
    setSaving(true);
    try {
      const body = {
        role_name: posForm.role_name,
        status: posForm.status,
        applications_target: posForm.applications_target !== '' ? Number(posForm.applications_target) : null,
        applications_actual: posForm.applications_actual !== '' ? Number(posForm.applications_actual) : null,
        hires_target: posForm.hires_target !== '' ? Number(posForm.hires_target) : null,
        hires_actual: posForm.hires_actual !== '' ? Number(posForm.hires_actual) : null,
      };
      if (editingPos) {
        await api.crmUpdateDealPosition(id, editingPos.id, body);
      } else {
        await api.crmAddDealPosition(id, body);
      }
      setShowPosModal(false); setEditingPos(null);
      setPosForm({ role_name: '', applications_target: '', applications_actual: '', hires_target: '', hires_actual: '', status: '進行中' });
      load();
    } catch { alert('保存に失敗しました'); }
    finally { setSaving(false); }
  };

  const handleDeletePos = async (posId) => {
    if (!confirm('削除しますか？')) return;
    await api.crmDeleteDealPosition(id, posId);
    load();
  };

  const handleSaveMedia = async () => {
    setSaving(true);
    try {
      const numField = (v) => v !== '' ? Number(v) : null;
      const body = {
        media_name: mediaForm.media_name,
        position: mediaForm.position,
        hire_count: numField(mediaForm.hire_count),
        posting_cost: numField(mediaForm.posting_cost),
        result_fee: numField(mediaForm.result_fee),
        margin: numField(mediaForm.margin),
        net_cost: numField(mediaForm.net_cost),
        total_cost: numField(mediaForm.total_cost),
      };
      if (editingMedia) {
        await api.crmUpdateDealMediaPlan(id, editingMedia.id, body);
      } else {
        await api.crmAddDealMediaPlan(id, body);
      }
      setShowMediaModal(false); setEditingMedia(null);
      setMediaForm({ media_name: '', position: '', hire_count: '', posting_cost: '', result_fee: '', margin: '', net_cost: '', total_cost: '' });
      load();
    } catch { alert('保存に失敗しました'); }
    finally { setSaving(false); }
  };

  const handleDeleteMedia = async (planId) => {
    if (!confirm('削除しますか？')) return;
    await api.crmDeleteDealMediaPlan(id, planId);
    load();
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

  const { deal, members, activities, payments, tasks, deliverables = [], positions = [], mediaplans = [], calcDefs = [] } = data;
  const stageIdx = STAGES.findIndex(s => s.value === deal.stage);

  const inPayments = payments.filter(p => (p.direction || '入金') === '入金');
  const outPayments = payments.filter(p => p.direction === '出金');
  const totalIn = inPayments.reduce((s, p) => s + (p.amount || 0), 0);
  const totalOut = outPayments.reduce((s, p) => s + (p.amount || 0), 0);
  const paidIn = inPayments.filter(p => p.status === 'paid').reduce((s, p) => s + (p.amount || 0), 0);

  const memberIds = members.map(m => m.user_id);
  const availableToAdd = allMembers.filter(m => !memberIds.includes(m.assignee_id));
  const bantCount = [deal.bant_budget, deal.bant_authority, deal.bant_needs, deal.bant_timeframe].filter(v => v === '確認済').length;

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
            const isLast = i === STAGES.filter(s2 => s2.value !== 'lost').length - 1;
            return (
              <button key={s.value} onClick={() => handleStageChange(s.value)} title={s.label}
                style={{
                  flex: 1, padding: '8px 4px', fontSize: 12, cursor: 'pointer',
                  background: active ? s.color : done ? '#e8f5e9' : '#f5f5f5',
                  color: active ? '#fff' : done ? '#388e3c' : '#555',
                  border: 'none', borderRight: isLast ? 'none' : '1px solid #ddd',
                  fontWeight: active ? 700 : 400, transition: 'all 0.15s',
                }}>
                {s.label}
              </button>
            );
          })}
          <button onClick={() => handleStageChange('lost')}
            style={{
              padding: '8px 12px', fontSize: 12, cursor: 'pointer',
              background: deal.stage === 'lost' ? '#757575' : '#f5f5f5',
              color: deal.stage === 'lost' ? '#fff' : '#999',
              border: 'none', borderLeft: '2px solid #ddd', fontWeight: deal.stage === 'lost' ? 700 : 400,
            }}>失注</button>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: 20, padding: '20px 24px', maxWidth: 1280, margin: '0 auto' }}>
        <main>
          {/* タブ */}
          <div style={{ display: 'flex', gap: 0, borderBottom: '2px solid #eee', marginBottom: 16, overflowX: 'auto' }}>
            {[
              { key: 'activity', label: `活動（${activities.length}）` },
              { key: 'payments', label: `入金（${payments.length}）` },
              { key: 'deliverables', label: `納品物（${deliverables.length}）` },
              { key: 'media', label: `媒体選定（${mediaplans.length}）` },
              { key: 'positions', label: `募集職種（${positions.length}）` },
              { key: 'tasks', label: `タスク（${tasks.length}）` },
              { key: 'members', label: `メンバー（${members.length}）` },
            ].map(t => (
              <button key={t.key} onClick={() => setTab(t.key)}
                style={{
                  padding: '8px 14px', border: 'none', background: 'none', cursor: 'pointer',
                  whiteSpace: 'nowrap', fontSize: 13,
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
                <div style={{ display: 'flex', gap: 6, marginBottom: 10, flexWrap: 'wrap' }}>
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
                <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginTop: 8, justifyContent: 'space-between' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <label style={{ fontSize: 12, color: '#666' }}>対応結果:</label>
                    <select value={actForm.result} onChange={e => setActForm(f => ({ ...f, result: e.target.value }))}
                      style={{ padding: '3px 8px', border: '1px solid #ddd', borderRadius: 6, fontSize: 12 }}>
                      {ACTIVITY_RESULTS.map(r => <option key={r} value={r}>{r || '（未選択）'}</option>)}
                    </select>
                  </div>
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
                    <div key={a.id} style={{
                      background: '#fff', borderRadius: 8, padding: '12px 16px',
                      boxShadow: '0 1px 3px rgba(0,0,0,0.06)',
                      borderLeft: a.activity_type === 'stage_change' ? '3px solid #1976d2' : '3px solid #eee',
                    }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                        <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
                          <span style={{ fontSize: 15 }}>{actIcon(a.activity_type)}</span>
                          <span style={{ fontSize: 12, color: '#888' }}>{actLabel(a.activity_type)}</span>
                          {a.result && (
                            <span style={{ fontSize: 11, background: '#e3f2fd', color: '#1976d2', padding: '1px 7px', borderRadius: 10 }}>{a.result}</span>
                          )}
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
                <div style={{ fontSize: 13, color: '#555' }}>
                  入金合計: <strong>¥{totalIn.toLocaleString()}</strong>
                  　入金済: <strong style={{ color: '#388e3c' }}>¥{paidIn.toLocaleString()}</strong>
                  {totalOut > 0 && <span>　出金: <strong style={{ color: '#c62828' }}>¥{totalOut.toLocaleString()}</strong></span>}
                </div>
                <button className="admin-link" onClick={() => setShowPayment(true)}>＋ 追加</button>
              </div>
              {payments.length === 0 ? (
                <div style={{ color: '#aaa', textAlign: 'center', padding: 24 }}>入出金記録がありません</div>
              ) : (
                <div style={{ overflowX: 'auto' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 3px rgba(0,0,0,0.08)', minWidth: 640 }}>
                    <thead>
                      <tr style={{ background: '#f5f5f5', textAlign: 'left' }}>
                        {['区分', '項目', '金額', 'インセン', '請求日', '請求書', '入金日', 'ステータス', ''].map(h => (
                          <th key={h} style={{ padding: '8px 10px', fontSize: 12, fontWeight: 600 }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {payments.map(p => (
                        <tr key={p.id} style={{ borderBottom: '1px solid #f0f0f0' }}>
                          <td style={{ padding: '10px 10px' }}>
                            <span style={{
                              padding: '2px 7px', borderRadius: 10, fontSize: 11,
                              background: (p.direction || '入金') === '入金' ? '#e8f5e9' : '#fce4ec',
                              color: (p.direction || '入金') === '入金' ? '#388e3c' : '#c62828',
                            }}>{p.direction || '入金'}</span>
                          </td>
                          <td style={{ padding: '10px 10px', fontSize: 13 }}>{p.label}</td>
                          <td style={{ padding: '10px 10px', fontWeight: 600, fontSize: 13 }}>¥{p.amount.toLocaleString()}</td>
                          <td style={{ padding: '10px 10px', fontSize: 12, color: '#666' }}>
                            {p.incentive_amount ? `¥${p.incentive_amount.toLocaleString()}` : '-'}
                          </td>
                          <td style={{ padding: '10px 10px', fontSize: 12, color: '#666' }}>{p.due_date ? new Date(p.due_date).toLocaleDateString('ja-JP') : '-'}</td>
                          <td style={{ padding: '10px 10px' }}>
                            {p.invoice_sent
                              ? <span style={{ fontSize: 11, color: '#388e3c' }}>✓ 済</span>
                              : <span style={{ fontSize: 11, color: '#bbb' }}>未</span>}
                          </td>
                          <td style={{ padding: '10px 10px', fontSize: 12, color: '#666' }}>{p.paid_date ? new Date(p.paid_date).toLocaleDateString('ja-JP') : '-'}</td>
                          <td style={{ padding: '10px 10px' }}>
                            {p.status === 'paid' ? (
                              <span style={{ background: '#e8f5e9', color: '#388e3c', padding: '2px 8px', borderRadius: 12, fontSize: 11 }}>入金済</span>
                            ) : (
                              <button onClick={() => handlePaymentStatus(p.id, 'paid', new Date().toISOString().split('T')[0])}
                                style={{ background: '#fff3e0', color: '#e65100', padding: '2px 8px', borderRadius: 12, fontSize: 11, border: 'none', cursor: 'pointer' }}>
                                未入金
                              </button>
                            )}
                          </td>
                          <td style={{ padding: '10px 10px' }}>
                            <button onClick={() => handleDeletePayment(p.id)}
                              style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer', fontSize: 12 }}>削除</button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {showPayment && (
                <div className="modal-overlay" onClick={() => setShowPayment(false)}>
                  <div className="modal-content" onClick={e => e.stopPropagation()} style={{ maxWidth: 440 }}>
                    <h2 style={{ marginBottom: 16 }}>入出金追加</h2>
                    <form onSubmit={handleAddPayment}>
                      <FormRow label="区分">
                        <div style={{ display: 'flex', gap: 8 }}>
                          {['入金', '出金'].map(d => (
                            <button key={d} type="button"
                              onClick={() => setPayForm(f => ({ ...f, direction: d }))}
                              style={{
                                padding: '5px 18px', borderRadius: 20, border: '1px solid',
                                borderColor: payForm.direction === d ? '#1976d2' : '#ddd',
                                background: payForm.direction === d ? '#e3f2fd' : '#fff',
                                color: payForm.direction === d ? '#1976d2' : '#555',
                                cursor: 'pointer', fontSize: 13,
                              }}>{d}</button>
                          ))}
                        </div>
                      </FormRow>
                      <FormRow label="項目名" required>
                        <input style={inputSx} placeholder="例：初回請求" required value={payForm.label} onChange={e => setPayForm(f => ({ ...f, label: e.target.value }))} />
                      </FormRow>
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                        <FormRow label="金額（円）" required>
                          <input style={inputSx} type="number" required value={payForm.amount} onChange={e => setPayForm(f => ({ ...f, amount: e.target.value }))} />
                        </FormRow>
                        <FormRow label="インセン金額（円）">
                          <input style={inputSx} type="number" value={payForm.incentive_amount} onChange={e => setPayForm(f => ({ ...f, incentive_amount: e.target.value }))} />
                        </FormRow>
                      </div>
                      <FormRow label="請求日">
                        <input style={inputSx} type="date" value={payForm.dueDate} onChange={e => setPayForm(f => ({ ...f, dueDate: e.target.value }))} />
                      </FormRow>
                      <FormRow label="">
                        <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, cursor: 'pointer' }}>
                          <input type="checkbox" checked={payForm.invoice_sent} onChange={e => setPayForm(f => ({ ...f, invoice_sent: e.target.checked }))} />
                          請求書発行済
                        </label>
                      </FormRow>
                      <FormRow label="メモ">
                        <input style={inputSx} value={payForm.notes} onChange={e => setPayForm(f => ({ ...f, notes: e.target.value }))} />
                      </FormRow>
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

          {/* 納品物タブ */}
          {tab === 'deliverables' && (
            <div>
              {deliverables.length > 0 && (() => {
                const done = deliverables.filter(d => d.status === 'done').length;
                const pct = Math.round((done / deliverables.length) * 100);
                return (
                  <div style={{ marginBottom: 16 }}>
                    <div style={{ background: '#e0e0e0', borderRadius: 4, height: 8, overflow: 'hidden' }}>
                      <div style={{ background: '#388e3c', width: `${pct}%`, height: '100%', transition: 'width 0.3s' }} />
                    </div>
                    <div style={{ fontSize: 12, color: '#666', textAlign: 'right', marginTop: 4 }}>{done} / {deliverables.length} 完了 ({pct}%)</div>
                  </div>
                );
              })()}
              {showDeliverable ? (
                <div style={{ background: '#fff', borderRadius: 8, padding: 14, boxShadow: '0 1px 3px rgba(0,0,0,0.08)', marginBottom: 14 }}>
                  <h4 style={{ fontSize: 13, marginBottom: 10 }}>納品物を追加</h4>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 8 }}>
                    <input placeholder="タイトル *" value={dlvForm.title} onChange={e => setDlvForm(f => ({ ...f, title: e.target.value }))}
                      style={{ padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6, fontSize: 13, gridColumn: '1/-1' }} />
                    <input placeholder="説明（任意）" value={dlvForm.description} onChange={e => setDlvForm(f => ({ ...f, description: e.target.value }))}
                      style={{ padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6, fontSize: 13 }} />
                    <input type="date" value={dlvForm.dueDate} onChange={e => setDlvForm(f => ({ ...f, dueDate: e.target.value }))}
                      style={{ padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6, fontSize: 13 }} />
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
                    <button onClick={() => { setShowDeliverable(false); setDlvForm({ title: '', description: '', dueDate: '' }); }}
                      style={{ padding: '6px 12px', border: '1px solid #ddd', borderRadius: 6, background: 'none', cursor: 'pointer', fontSize: 13 }}>キャンセル</button>
                    <button onClick={() => {
                      if (!dlvForm.title.trim()) return;
                      api.crmAddDeliverable(id, { title: dlvForm.title.trim(), description: dlvForm.description, dueDate: dlvForm.dueDate || undefined })
                        .then(() => { load(); setShowDeliverable(false); setDlvForm({ title: '', description: '', dueDate: '' }); });
                    }} style={{ padding: '6px 14px', background: '#1976d2', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer', fontSize: 13 }}>追加</button>
                  </div>
                </div>
              ) : (
                <button onClick={() => setShowDeliverable(true)}
                  style={{ marginBottom: 12, padding: '7px 14px', background: '#1976d2', color: '#fff', border: 'none', borderRadius: 7, cursor: 'pointer', fontSize: 13 }}>
                  ＋ 納品物を追加
                </button>
              )}
              {deliverables.length === 0 ? (
                <div style={{ color: '#aaa', textAlign: 'center', padding: 24 }}>納品物がありません</div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {deliverables.map(d => (
                    <div key={d.id} style={{
                      background: '#fff', borderRadius: 8, padding: '12px 16px',
                      boxShadow: '0 1px 3px rgba(0,0,0,0.06)',
                      display: 'flex', alignItems: 'center', gap: 12,
                      opacity: d.status === 'done' ? 0.7 : 1,
                    }}>
                      <input type="checkbox" checked={d.status === 'done'}
                        onChange={() => api.crmUpdateDeliverable(id, d.id, {
                          status: d.status === 'done' ? 'pending' : 'done',
                          completed_at: d.status === 'done' ? null : new Date().toISOString(),
                        }).then(load)}
                        style={{ width: 16, height: 16, cursor: 'pointer', flexShrink: 0 }} />
                      <div style={{ flex: 1 }}>
                        <div style={{ fontWeight: 600, fontSize: 13, textDecoration: d.status === 'done' ? 'line-through' : 'none' }}>{d.title}</div>
                        {d.description && <div style={{ fontSize: 12, color: '#666', marginTop: 2 }}>{d.description}</div>}
                        {d.due_date && (
                          <div style={{ fontSize: 11, color: new Date(d.due_date) < new Date() && d.status !== 'done' ? '#e74c3c' : '#888', marginTop: 2 }}>
                            期限: {new Date(d.due_date).toLocaleDateString('ja-JP')}
                          </div>
                        )}
                      </div>
                      {d.completed_at && <span style={{ fontSize: 11, color: '#388e3c' }}>✓ {new Date(d.completed_at).toLocaleDateString('ja-JP')}</span>}
                      <button onClick={() => { if (confirm('削除しますか？')) api.crmDeleteDeliverable(id, d.id).then(load); }}
                        style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer', fontSize: 14 }}>✕</button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* 媒体選定タブ */}
          {tab === 'media' && (
            <div>
              <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 12 }}>
                <button className="admin-link" onClick={() => {
                  setEditingMedia(null);
                  setMediaForm({ media_name: '', position: '', hire_count: '', posting_cost: '', result_fee: '', margin: '', net_cost: '', total_cost: '' });
                  setShowMediaModal(true);
                }}>＋ 追加</button>
              </div>
              {mediaplans.length === 0 ? (
                <div style={{ color: '#aaa', textAlign: 'center', padding: 24 }}>媒体選定データがありません</div>
              ) : (
                <div style={{ overflowX: 'auto' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 3px rgba(0,0,0,0.08)', minWidth: 700 }}>
                    <thead>
                      <tr style={{ background: '#f5f5f5' }}>
                        {['媒体', '採用ポジション', '採用人数', '掲載費用', '成果報酬', 'マージン', '実質費用', '合計費用', ''].map(h => (
                          <th key={h} style={{ padding: '8px 10px', fontSize: 12, fontWeight: 600, textAlign: 'left' }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {mediaplans.map(p => (
                        <tr key={p.id} style={{ borderBottom: '1px solid #f0f0f0' }}>
                          <td style={{ padding: '10px 10px', fontWeight: 600, fontSize: 13 }}>{p.media_name}</td>
                          <td style={{ padding: '10px 10px', fontSize: 13 }}>{p.position || '-'}</td>
                          <td style={{ padding: '10px 10px', fontSize: 13 }}>{p.hire_count != null ? `${p.hire_count}名` : '-'}</td>
                          <td style={{ padding: '10px 10px', fontSize: 13 }}>{p.posting_cost != null ? `¥${p.posting_cost.toLocaleString()}` : '-'}</td>
                          <td style={{ padding: '10px 10px', fontSize: 13 }}>{p.result_fee != null ? `¥${p.result_fee.toLocaleString()}` : '-'}</td>
                          <td style={{ padding: '10px 10px', fontSize: 13 }}>{p.margin != null ? `${p.margin}%` : '-'}</td>
                          <td style={{ padding: '10px 10px', fontSize: 13 }}>{p.net_cost != null ? `¥${p.net_cost.toLocaleString()}` : '-'}</td>
                          <td style={{ padding: '10px 10px', fontSize: 13, fontWeight: 600 }}>{p.total_cost != null ? `¥${p.total_cost.toLocaleString()}` : '-'}</td>
                          <td style={{ padding: '10px 10px' }}>
                            <div style={{ display: 'flex', gap: 6 }}>
                              <button onClick={() => {
                                setEditingMedia(p);
                                setMediaForm({ media_name: p.media_name || '', position: p.position || '', hire_count: p.hire_count ?? '', posting_cost: p.posting_cost ?? '', result_fee: p.result_fee ?? '', margin: p.margin ?? '', net_cost: p.net_cost ?? '', total_cost: p.total_cost ?? '' });
                                setShowMediaModal(true);
                              }} style={{ background: 'none', border: 'none', color: '#1976d2', cursor: 'pointer', fontSize: 12 }}>編集</button>
                              <button onClick={() => handleDeleteMedia(p.id)}
                                style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer', fontSize: 12 }}>削除</button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                    {mediaplans.length > 1 && (
                      <tfoot>
                        <tr style={{ background: '#f9f9f9', fontWeight: 700 }}>
                          <td colSpan={3} style={{ padding: '8px 10px', fontSize: 13 }}>合計</td>
                          <td style={{ padding: '8px 10px', fontSize: 13 }}>¥{mediaplans.reduce((s, p) => s + (p.posting_cost || 0), 0).toLocaleString()}</td>
                          <td style={{ padding: '8px 10px', fontSize: 13 }}>¥{mediaplans.reduce((s, p) => s + (p.result_fee || 0), 0).toLocaleString()}</td>
                          <td></td>
                          <td style={{ padding: '8px 10px', fontSize: 13 }}>¥{mediaplans.reduce((s, p) => s + (p.net_cost || 0), 0).toLocaleString()}</td>
                          <td style={{ padding: '8px 10px', fontSize: 13 }}>¥{mediaplans.reduce((s, p) => s + (p.total_cost || 0), 0).toLocaleString()}</td>
                          <td></td>
                        </tr>
                      </tfoot>
                    )}
                  </table>
                </div>
              )}
            </div>
          )}

          {/* 募集職種タブ */}
          {tab === 'positions' && (
            <div>
              <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 12 }}>
                <button className="admin-link" onClick={() => {
                  setEditingPos(null);
                  setPosForm({ role_name: '', applications_target: '', applications_actual: '', hires_target: '', hires_actual: '', status: '進行中' });
                  setShowPosModal(true);
                }}>＋ 追加</button>
              </div>
              {positions.length === 0 ? (
                <div style={{ color: '#aaa', textAlign: 'center', padding: 24 }}>募集職種データがありません</div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  {positions.map(pos => {
                    const appRate = pos.applications_target ? Math.round((pos.applications_actual || 0) / pos.applications_target * 100) : null;
                    const hireRate = pos.hires_target ? Math.round((pos.hires_actual || 0) / pos.hires_target * 100) : null;
                    return (
                      <div key={pos.id} style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 14 }}>
                          <div>
                            <span style={{ fontWeight: 700, fontSize: 15 }}>{pos.role_name}</span>
                            <span style={{
                              marginLeft: 10, fontSize: 11, padding: '2px 8px', borderRadius: 10,
                              background: pos.status === '達成' ? '#e8f5e9' : pos.status === '停止' ? '#fce4ec' : '#e3f2fd',
                              color: pos.status === '達成' ? '#388e3c' : pos.status === '停止' ? '#c62828' : '#1976d2',
                            }}>{pos.status}</span>
                          </div>
                          <div style={{ display: 'flex', gap: 8 }}>
                            <button onClick={() => {
                              setEditingPos(pos);
                              setPosForm({ role_name: pos.role_name || '', applications_target: pos.applications_target ?? '', applications_actual: pos.applications_actual ?? '', hires_target: pos.hires_target ?? '', hires_actual: pos.hires_actual ?? '', status: pos.status || '進行中' });
                              setShowPosModal(true);
                            }} style={{ background: 'none', border: 'none', color: '#1976d2', cursor: 'pointer', fontSize: 12 }}>編集</button>
                            <button onClick={() => handleDeletePos(pos.id)}
                              style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer', fontSize: 12 }}>削除</button>
                          </div>
                        </div>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
                          {[
                            { label: '応募数', target: pos.applications_target, actual: pos.applications_actual, rate: appRate },
                            { label: '採用数', target: pos.hires_target, actual: pos.hires_actual, rate: hireRate },
                          ].map(row => (
                            <div key={row.label}>
                              <div style={{ fontSize: 12, color: '#888', marginBottom: 6 }}>{row.label}</div>
                              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 13, marginBottom: 4 }}>
                                <span>目標: <strong>{row.target ?? '-'}</strong></span>
                                <span>実績: <strong>{row.actual ?? '-'}</strong></span>
                                {row.rate != null && <span style={{ color: row.rate >= 100 ? '#388e3c' : '#e65100', fontWeight: 600 }}>{row.rate}%</span>}
                              </div>
                              {row.rate != null && (
                                <div style={{ background: '#e0e0e0', borderRadius: 4, height: 6, overflow: 'hidden' }}>
                                  <div style={{ background: row.rate >= 100 ? '#388e3c' : '#1976d2', width: `${Math.min(row.rate, 100)}%`, height: '100%' }} />
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>
                    );
                  })}
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
                    {availableToAdd.map(m => <option key={m.assignee_id} value={m.assignee_id}>{m.displayName}</option>)}
                  </select>
                  <button className="admin-link" disabled={!memberInput} onClick={() => { handleAddMember(memberInput); setMemberInput(''); }}>追加</button>
                </div>
              )}
            </div>
          )}
        </main>

        {/* サイドバー */}
        <aside style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
              <h3 style={{ fontSize: 14, margin: 0 }}>案件情報</h3>
              <button style={{ background: 'none', border: 'none', color: '#1976d2', cursor: 'pointer', fontSize: 12 }}
                onClick={() => { setInfoForm(buildInfoForm(deal)); setShowEditModal(true); }}>編集</button>
            </div>
            <dl style={{ margin: 0, fontSize: 13 }}>
              <dt style={{ color: '#888', marginBottom: 2 }}>顧客</dt>
              <dd style={{ margin: '0 0 10px' }}><Link to={`/crm/clients/${deal.client_id}`}>{deal.client_name}</Link></dd>

              <dt style={{ color: '#888', marginBottom: 2 }}>ステージ / ヨミ</dt>
              <dd style={{ margin: '0 0 10px', display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                <span style={{ background: stageColor(deal.stage), color: '#fff', padding: '2px 10px', borderRadius: 12, fontSize: 12 }}>{stageLabel(deal.stage)}</span>
                {deal.yomi && <span style={{ background: '#fff3e0', color: '#e65100', padding: '2px 10px', borderRadius: 12, fontSize: 12, border: '1px solid #ffe0b2' }}>{deal.yomi}</span>}
              </dd>

              {(deal.inrevo_person || deal.sales_person || deal.acquisition_person) && (
                <>
                  <dt style={{ color: '#888', marginBottom: 4, marginTop: 10, fontSize: 11, fontWeight: 700, letterSpacing: 0.5, textTransform: 'uppercase' }}>担当者</dt>
                  {deal.inrevo_person && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>INREVO</span><span>{deal.inrevo_person}</span></dd>}
                  {deal.sales_person && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>営業</span><span>{deal.sales_person}</span></dd>}
                  {deal.acquisition_person && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>獲得者</span><span>{deal.acquisition_person}</span></dd>}
                </>
              )}

              {(deal.contract_type || deal.payment_method) && (
                <>
                  <dt style={{ color: '#888', marginBottom: 4, marginTop: 10, fontSize: 11, fontWeight: 700, letterSpacing: 0.5, textTransform: 'uppercase' }}>契約</dt>
                  {deal.contract_type && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>形態</span><span style={{ fontSize: 12 }}>{deal.contract_type}</span></dd>}
                  {deal.payment_method && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>支払</span><span style={{ fontSize: 12 }}>{deal.payment_method}</span></dd>}
                </>
              )}

              {(deal.budget || deal.initial_cost != null || deal.monthly_cost != null) && (
                <>
                  <dt style={{ color: '#888', marginBottom: 4, marginTop: 10, fontSize: 11, fontWeight: 700, letterSpacing: 0.5, textTransform: 'uppercase' }}>費用</dt>
                  {deal.budget && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>予算</span><span>{fmtMoney(deal.budget)}</span></dd>}
                  {deal.initial_cost != null && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>初期</span><span>{fmtMoney(deal.initial_cost)}</span></dd>}
                  {deal.monthly_cost != null && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>月額</span><span>{fmtMoney(deal.monthly_cost)}</span></dd>}
                  {deal.unit_price != null && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>単価</span><span>{fmtMoney(deal.unit_price)}</span></dd>}
                  {deal.contract_months != null && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>契約月数</span><span>{deal.contract_months}ヶ月</span></dd>}
                </>
              )}

              {calcDefs.length > 0 && (
                <>
                  <dt style={{ color: '#888', marginBottom: 4, marginTop: 10, fontSize: 11, fontWeight: 700, letterSpacing: 0.5, textTransform: 'uppercase' }}>計算フィールド</dt>
                  {calcDefs.map(def => {
                    const val = evalCalc(def.expression, deal);
                    return (
                      <dd key={def.id} style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}>
                        <span style={{ color: '#aaa', fontSize: 12 }}>{def.name}</span>
                        <span style={{ fontWeight: 600 }}>
                          {val != null ? (def.format === 'percent' ? `${val.toFixed(1)}%` : `¥${Math.round(val).toLocaleString()}`) : '-'}
                        </span>
                      </dd>
                    );
                  })}
                </>
              )}

              {(deal.order_date || deal.conclusion_date || deal.next_action_date) && (
                <>
                  <dt style={{ color: '#888', marginBottom: 4, marginTop: 10, fontSize: 11, fontWeight: 700, letterSpacing: 0.5, textTransform: 'uppercase' }}>重要日程</dt>
                  {deal.order_date && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>受注日</span><span style={{ fontSize: 12 }}>{fmtDate(deal.order_date)}</span></dd>}
                  {deal.conclusion_date && <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}><span style={{ color: '#aaa', fontSize: 12 }}>結論日</span><span style={{ fontSize: 12 }}>{fmtDate(deal.conclusion_date)}</span></dd>}
                  {deal.next_action_date && (
                    <dd style={{ margin: '0 0 4px', display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ color: '#aaa', fontSize: 12 }}>Next Action</span>
                      <span style={{ fontSize: 12, color: new Date(deal.next_action_date) < new Date() ? '#e74c3c' : '#333', fontWeight: 600 }}>{fmtDate(deal.next_action_date)}</span>
                    </dd>
                  )}
                  {deal.next_action_content && <dd style={{ margin: '0 0 4px', fontSize: 12, color: '#555' }}>→ {deal.next_action_content}</dd>}
                </>
              )}

              {bantCount > 0 && (
                <>
                  <dt style={{ color: '#888', marginBottom: 4, marginTop: 10, fontSize: 11, fontWeight: 700, letterSpacing: 0.5, textTransform: 'uppercase' }}>BANT ({bantCount}/4)</dt>
                  {[
                    { key: 'bant_budget', label: 'B' },
                    { key: 'bant_authority', label: 'A' },
                    { key: 'bant_needs', label: 'N' },
                    { key: 'bant_timeframe', label: 'T' },
                  ].map(b => (
                    <dd key={b.key} style={{ margin: '0 0 3px', display: 'flex', justifyContent: 'space-between', fontSize: 12 }}>
                      <span style={{ color: '#aaa' }}>{b.label}</span>
                      <span style={{ color: deal[b.key] === '確認済' ? '#388e3c' : '#ddd' }}>{deal[b.key] === '確認済' ? '✓' : '○'}</span>
                    </dd>
                  ))}
                </>
              )}

              {(deal.antisocial_check || deal.legal_check || deal.contract_approval || deal.contract_sent || deal.hearing_collected) && (
                <>
                  <dt style={{ color: '#888', marginBottom: 4, marginTop: 10, fontSize: 11, fontWeight: 700, letterSpacing: 0.5, textTransform: 'uppercase' }}>チェック済</dt>
                  {[
                    { key: 'antisocial_check', label: '反社' },
                    { key: 'legal_check', label: 'リーガル' },
                    { key: 'contract_approval', label: '契約稟議' },
                    { key: 'contract_sent', label: '契約書送付' },
                    { key: 'hearing_collected', label: 'ヒアリング' },
                  ].filter(c => deal[c.key]).map(c => (
                    <dd key={c.key} style={{ margin: '0 0 3px', fontSize: 12, color: '#388e3c' }}>✓ {c.label}</dd>
                  ))}
                </>
              )}

              {deal.loss_reason && (
                <>
                  <dt style={{ color: '#888', marginBottom: 2, marginTop: 10 }}>失注理由</dt>
                  <dd style={{ margin: '0 0 8px', fontSize: 12 }}>{deal.loss_reason}</dd>
                </>
              )}

              {deal.notes && (
                <>
                  <dt style={{ color: '#888', marginBottom: 2, marginTop: 10 }}>メモ</dt>
                  <dd style={{ margin: '0 0 8px', whiteSpace: 'pre-wrap', fontSize: 12, color: '#555' }}>{deal.notes}</dd>
                </>
              )}
            </dl>
          </div>

          {payments.length > 0 && (
            <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
              <h3 style={{ fontSize: 14, marginBottom: 12 }}>入金状況</h3>
              <div style={{ fontSize: 13 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                  <span style={{ color: '#888' }}>請求合計</span><strong>¥{totalIn.toLocaleString()}</strong>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                  <span style={{ color: '#388e3c' }}>入金済</span><strong style={{ color: '#388e3c' }}>¥{paidIn.toLocaleString()}</strong>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: totalOut > 0 ? 6 : 0 }}>
                  <span style={{ color: '#c62828' }}>残</span><strong style={{ color: '#c62828' }}>¥{(totalIn - paidIn).toLocaleString()}</strong>
                </div>
                {totalOut > 0 && <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: '#555' }}>出金合計</span><strong>¥{totalOut.toLocaleString()}</strong>
                </div>}
                <div style={{ marginTop: 10, background: '#f5f5f5', borderRadius: 4, height: 6, overflow: 'hidden' }}>
                  <div style={{ height: '100%', background: '#388e3c', width: totalIn ? `${(paidIn / totalIn) * 100}%` : '0%' }} />
                </div>
              </div>
            </div>
          )}

          <div style={{ background: '#fff', borderRadius: 8, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.08)', fontSize: 12, color: '#888' }}>
            <div>作成：{fmtDate(deal.created_at)}</div>
            <div>更新：{fmtDate(deal.updated_at)}</div>
          </div>
        </aside>
      </div>

      {/* ======== 編集モーダル ======== */}
      {showEditModal && (
        <div className="modal-overlay" onClick={() => setShowEditModal(false)}
          style={{ alignItems: 'flex-start', paddingTop: 32, overflow: 'auto' }}>
          <div className="modal-content" onClick={e => e.stopPropagation()}
            style={{ maxWidth: 700, width: '100%', maxHeight: '88vh', overflowY: 'auto' }}>
            <h2 style={{ marginBottom: 4 }}>案件情報を編集</h2>
            <p style={{ fontSize: 12, color: '#888', marginBottom: 4 }}>{deal.client_name} / {deal.name}</p>

            <SectionHeader title="基本情報" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <FormRow label="案件名" required>
                <input style={inputSx} value={infoForm.name} onChange={e => setInfoForm(f => ({ ...f, name: e.target.value }))} required />
              </FormRow>
              <FormRow label="ヨミ">
                <select style={inputSx} value={infoForm.yomi} onChange={e => setInfoForm(f => ({ ...f, yomi: e.target.value }))}>
                  <option value="">選択してください</option>
                  {YOMI_OPTIONS.map(y => <option key={y} value={y}>{y}</option>)}
                </select>
              </FormRow>
              <FormRow label="予算（円）">
                <input style={inputSx} type="number" value={infoForm.budget} onChange={e => setInfoForm(f => ({ ...f, budget: e.target.value }))} />
              </FormRow>
              <FormRow label="公開設定">
                <select style={inputSx} value={infoForm.visibility} onChange={e => setInfoForm(f => ({ ...f, visibility: e.target.value }))}>
                  <option value="all">全員に公開</option>
                  <option value="members">メンバーのみ</option>
                </select>
              </FormRow>
            </div>
            <FormRow label="メモ">
              <textarea style={{ ...inputSx, height: 70, resize: 'vertical' }} value={infoForm.notes} onChange={e => setInfoForm(f => ({ ...f, notes: e.target.value }))} />
            </FormRow>

            <SectionHeader title="担当者" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
              <FormRow label="INREVO担当者">
                <input style={inputSx} value={infoForm.inrevo_person} onChange={e => setInfoForm(f => ({ ...f, inrevo_person: e.target.value }))} />
              </FormRow>
              <FormRow label="担当営業">
                <input style={inputSx} value={infoForm.sales_person} onChange={e => setInfoForm(f => ({ ...f, sales_person: e.target.value }))} />
              </FormRow>
              <FormRow label="商談獲得者">
                <input style={inputSx} value={infoForm.acquisition_person} onChange={e => setInfoForm(f => ({ ...f, acquisition_person: e.target.value }))} />
              </FormRow>
            </div>

            <SectionHeader title="契約情報" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
              <FormRow label="契約形態">
                <select style={inputSx} value={infoForm.contract_type} onChange={e => setInfoForm(f => ({ ...f, contract_type: e.target.value }))}>
                  <option value="">選択</option>
                  {CONTRACT_TYPES.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
              </FormRow>
              <FormRow label="支払方式">
                <select style={inputSx} value={infoForm.payment_method} onChange={e => setInfoForm(f => ({ ...f, payment_method: e.target.value }))}>
                  <option value="">選択</option>
                  {PAYMENT_METHODS.map(m => <option key={m} value={m}>{m}</option>)}
                </select>
              </FormRow>
              <FormRow label="アポ獲得形態">
                <select style={inputSx} value={infoForm.appointment_type} onChange={e => setInfoForm(f => ({ ...f, appointment_type: e.target.value }))}>
                  <option value="">選択</option>
                  {APPT_TYPES.map(a => <option key={a} value={a}>{a}</option>)}
                </select>
              </FormRow>
            </div>
            <FormRow label="新卒・中途区分">
              <div style={{ display: 'flex', gap: 16 }}>
                {HIRE_TYPES.map(h => (
                  <label key={h} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, cursor: 'pointer' }}>
                    <input type="checkbox" checked={(infoForm.hire_type || []).includes(h)}
                      onChange={e => setInfoForm(f => ({ ...f, hire_type: e.target.checked ? [...(f.hire_type || []), h] : (f.hire_type || []).filter(x => x !== h) }))} />
                    {h}
                  </label>
                ))}
              </div>
            </FormRow>

            <SectionHeader title="費用" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 10 }}>
              {[
                { key: 'initial_cost', label: '初期費用（円）' },
                { key: 'monthly_cost', label: '月額費用（円）' },
                { key: 'unit_price', label: '1名単価（円）' },
                { key: 'contract_months', label: '契約月数' },
                { key: 'guarantee_count', label: '保証人数' },
                { key: 'guarantee_salary', label: '想定年収（円）' },
                { key: 'rate', label: '料率（%）' },
                { key: 'advance_payment', label: '前払い金額（円）' },
              ].map(f => (
                <FormRow key={f.key} label={f.label}>
                  <input style={inputSx} type="number" value={infoForm[f.key]} onChange={e => setInfoForm(v => ({ ...v, [f.key]: e.target.value }))} />
                </FormRow>
              ))}
            </div>

            <SectionHeader title="日程" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 10 }}>
              {[
                { key: 'first_meeting_date', label: '初回商談日' },
                { key: 'acquisition_date', label: '商談獲得日' },
                { key: 'order_date', label: '受注日' },
                { key: 'conclusion_date', label: '結論予定日' },
                { key: 'contract_approval_date', label: '契約稟議完了日' },
                { key: 'contract_send_date', label: '契約書送付日' },
              ].map(f => (
                <FormRow key={f.key} label={f.label}>
                  <input style={inputSx} type="date" value={infoForm[f.key]} onChange={e => setInfoForm(v => ({ ...v, [f.key]: e.target.value }))} />
                </FormRow>
              ))}
            </div>

            <SectionHeader title="Next Action" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <FormRow label="Next Action日">
                <input style={inputSx} type="date" value={infoForm.next_action_date} onChange={e => setInfoForm(f => ({ ...f, next_action_date: e.target.value }))} />
              </FormRow>
              <FormRow label="Next Action内容">
                <select style={inputSx} value={infoForm.next_action_content} onChange={e => setInfoForm(f => ({ ...f, next_action_content: e.target.value }))}>
                  <option value="">選択</option>
                  {NEXT_ACTION_CONTENTS.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
              </FormRow>
            </div>
            <FormRow label="Next Action詳細">
              <textarea style={{ ...inputSx, height: 60, resize: 'vertical' }} value={infoForm.next_action_detail} onChange={e => setInfoForm(f => ({ ...f, next_action_detail: e.target.value }))} />
            </FormRow>

            <SectionHeader title="BANT分析" />
            {[
              { conf: 'bant_budget', memo: 'bant_budget_memo', label: 'B（Budget）予算' },
              { conf: 'bant_authority', memo: 'bant_authority_memo', label: 'A（Authority）決裁者' },
              { conf: 'bant_needs', memo: 'bant_needs_memo', label: 'N（Needs）ニーズ' },
              { conf: 'bant_timeframe', memo: 'bant_timeframe_memo', label: 'T（Timeframe）時期' },
            ].map(b => (
              <div key={b.conf} style={{ marginBottom: 12, background: '#f9f9f9', borderRadius: 6, padding: 12 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontWeight: 600, fontSize: 13, cursor: 'pointer' }}>
                    <input type="checkbox" checked={infoForm[b.conf] === '確認済'}
                      onChange={e => setInfoForm(f => ({ ...f, [b.conf]: e.target.checked ? '確認済' : '' }))} />
                    {b.label}
                  </label>
                  <span style={{ fontSize: 11, color: infoForm[b.conf] === '確認済' ? '#388e3c' : '#aaa' }}>{infoForm[b.conf] === '確認済' ? '確認済' : '未確認'}</span>
                </div>
                <textarea placeholder="概要・詳細メモ" style={{ ...inputSx, height: 50, resize: 'vertical' }}
                  value={infoForm[b.memo] || ''} onChange={e => setInfoForm(f => ({ ...f, [b.memo]: e.target.value }))} />
              </div>
            ))}

            <SectionHeader title="ヒアリング課題" />
            {[
              { key: 'applications', label: '母集団形成' },
              { key: 'quality', label: '応募者の質' },
              { key: 'budget', label: '採用予算' },
              { key: 'notes', label: '留意事項' },
            ].map(h => (
              <FormRow key={h.key} label={h.label}>
                <textarea style={{ ...inputSx, height: 55, resize: 'vertical' }}
                  value={(infoForm.hearing_challenges || {})[h.key] || ''}
                  onChange={e => setInfoForm(f => ({ ...f, hearing_challenges: { ...(f.hearing_challenges || {}), [h.key]: e.target.value } }))} />
              </FormRow>
            ))}

            <SectionHeader title="失注情報" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <FormRow label="失注理由">
                <select style={inputSx} value={infoForm.loss_reason} onChange={e => setInfoForm(f => ({ ...f, loss_reason: e.target.value }))}>
                  <option value="">選択</option>
                  {LOSS_REASONS.map(r => <option key={r} value={r}>{r}</option>)}
                </select>
              </FormRow>
            </div>
            <FormRow label="失注理由詳細">
              <textarea style={{ ...inputSx, height: 60, resize: 'vertical' }} value={infoForm.loss_reason_detail} onChange={e => setInfoForm(f => ({ ...f, loss_reason_detail: e.target.value }))} />
            </FormRow>

            <SectionHeader title="チェックリスト" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
              {[
                { key: 'antisocial_check', label: '反社チェック' },
                { key: 'legal_check', label: 'リーガルチェック' },
                { key: 'contract_approval', label: '契約稟議完了' },
                { key: 'contract_sent', label: '契約書送付済' },
                { key: 'hearing_collected', label: 'ヒアリング回収' },
              ].map(c => (
                <label key={c.key} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, cursor: 'pointer', padding: '8px 10px', background: '#f9f9f9', borderRadius: 6 }}>
                  <input type="checkbox" checked={infoForm[c.key] || false} onChange={e => setInfoForm(f => ({ ...f, [c.key]: e.target.checked }))} />
                  {c.label}
                </label>
              ))}
            </div>

            <SectionHeader title="請求書・契約書送付先" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
              <FormRow label="請求先 氏名">
                <input style={inputSx} value={infoForm.invoice_to_name} onChange={e => setInfoForm(f => ({ ...f, invoice_to_name: e.target.value }))} />
              </FormRow>
              <FormRow label="請求先 メール">
                <input style={inputSx} type="email" value={infoForm.invoice_to_email} onChange={e => setInfoForm(f => ({ ...f, invoice_to_email: e.target.value }))} />
              </FormRow>
              <FormRow label="請求先 CC">
                <input style={inputSx} type="email" value={infoForm.invoice_cc_email} onChange={e => setInfoForm(f => ({ ...f, invoice_cc_email: e.target.value }))} />
              </FormRow>
              <FormRow label="契約書送付先 氏名">
                <input style={inputSx} value={infoForm.contract_to_name} onChange={e => setInfoForm(f => ({ ...f, contract_to_name: e.target.value }))} />
              </FormRow>
              <FormRow label="契約書送付先 メール">
                <input style={inputSx} type="email" value={infoForm.contract_to_email} onChange={e => setInfoForm(f => ({ ...f, contract_to_email: e.target.value }))} />
              </FormRow>
              <FormRow label="契約書送付先 CC">
                <input style={inputSx} type="email" value={infoForm.contract_cc_email} onChange={e => setInfoForm(f => ({ ...f, contract_cc_email: e.target.value }))} />
              </FormRow>
            </div>

            <SectionHeader title="営業メモ" />
            <FormRow label="">
              <textarea style={{ ...inputSx, height: 80, resize: 'vertical' }} value={infoForm.sales_memo} onChange={e => setInfoForm(f => ({ ...f, sales_memo: e.target.value }))} placeholder="営業メモ" />
            </FormRow>

            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 20, paddingTop: 16, borderTop: '1px solid #eee' }}>
              <button type="button" className="filter-clear-btn" onClick={() => setShowEditModal(false)}>キャンセル</button>
              <button type="button" className="admin-link" onClick={handleSaveInfo} disabled={saving}>{saving ? '保存中...' : '保存'}</button>
            </div>
          </div>
        </div>
      )}

      {/* ======== 媒体選定モーダル ======== */}
      {showMediaModal && (
        <div className="modal-overlay" onClick={() => setShowMediaModal(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()} style={{ maxWidth: 480 }}>
            <h2 style={{ marginBottom: 16 }}>{editingMedia ? '媒体選定を編集' : '媒体選定を追加'}</h2>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              <div style={{ gridColumn: '1/-1' }}>
                <FormRow label="媒体名" required>
                  <input style={inputSx} value={mediaForm.media_name} onChange={e => setMediaForm(f => ({ ...f, media_name: e.target.value }))} required />
                </FormRow>
              </div>
              <FormRow label="採用ポジション">
                <input style={inputSx} value={mediaForm.position} onChange={e => setMediaForm(f => ({ ...f, position: e.target.value }))} />
              </FormRow>
              <FormRow label="採用人数">
                <input style={inputSx} type="number" value={mediaForm.hire_count} onChange={e => setMediaForm(f => ({ ...f, hire_count: e.target.value }))} />
              </FormRow>
              <FormRow label="掲載費用（円）">
                <input style={inputSx} type="number" value={mediaForm.posting_cost} onChange={e => setMediaForm(f => ({ ...f, posting_cost: e.target.value }))} />
              </FormRow>
              <FormRow label="成果報酬費用（円）">
                <input style={inputSx} type="number" value={mediaForm.result_fee} onChange={e => setMediaForm(f => ({ ...f, result_fee: e.target.value }))} />
              </FormRow>
              <FormRow label="マージン（%）">
                <input style={inputSx} type="number" value={mediaForm.margin} onChange={e => setMediaForm(f => ({ ...f, margin: e.target.value }))} />
              </FormRow>
              <FormRow label="実質費用（円）">
                <input style={inputSx} type="number" value={mediaForm.net_cost} onChange={e => setMediaForm(f => ({ ...f, net_cost: e.target.value }))} />
              </FormRow>
              <FormRow label="合計費用（円）">
                <input style={inputSx} type="number" value={mediaForm.total_cost} onChange={e => setMediaForm(f => ({ ...f, total_cost: e.target.value }))} />
              </FormRow>
            </div>
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
              <button type="button" className="filter-clear-btn" onClick={() => setShowMediaModal(false)}>キャンセル</button>
              <button type="button" className="admin-link" onClick={handleSaveMedia} disabled={saving}>{saving ? '保存中...' : '保存'}</button>
            </div>
          </div>
        </div>
      )}

      {/* ======== 募集職種モーダル ======== */}
      {showPosModal && (
        <div className="modal-overlay" onClick={() => setShowPosModal(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()} style={{ maxWidth: 440 }}>
            <h2 style={{ marginBottom: 16 }}>{editingPos ? '募集職種を編集' : '募集職種を追加'}</h2>
            <FormRow label="募集職種名" required>
              <input style={inputSx} value={posForm.role_name} onChange={e => setPosForm(f => ({ ...f, role_name: e.target.value }))} required />
            </FormRow>
            <FormRow label="進捗ステータス">
              <select style={inputSx} value={posForm.status} onChange={e => setPosForm(f => ({ ...f, status: e.target.value }))}>
                {POSITION_STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </FormRow>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              <FormRow label="応募数 目標">
                <input style={inputSx} type="number" value={posForm.applications_target} onChange={e => setPosForm(f => ({ ...f, applications_target: e.target.value }))} />
              </FormRow>
              <FormRow label="応募数 実績">
                <input style={inputSx} type="number" value={posForm.applications_actual} onChange={e => setPosForm(f => ({ ...f, applications_actual: e.target.value }))} />
              </FormRow>
              <FormRow label="採用数 目標">
                <input style={inputSx} type="number" value={posForm.hires_target} onChange={e => setPosForm(f => ({ ...f, hires_target: e.target.value }))} />
              </FormRow>
              <FormRow label="採用数 実績">
                <input style={inputSx} type="number" value={posForm.hires_actual} onChange={e => setPosForm(f => ({ ...f, hires_actual: e.target.value }))} />
              </FormRow>
            </div>
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
              <button type="button" className="filter-clear-btn" onClick={() => setShowPosModal(false)}>キャンセル</button>
              <button type="button" className="admin-link" onClick={handleSavePos} disabled={saving}>{saving ? '保存中...' : '保存'}</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
