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

const INDUSTRIES = ['情報通信業','製造業','卸売業・小売業','金融業・保険業','不動産業、物品賃貸業','建設業','医療、福祉','教育、学習支援業','学術研究、専門・技術サービス業','生活関連サービス業、娯楽業','宿泊業、飲食サービス業','運輸業、郵便業','サービス業（他に分類されないもの）','農業、林業','漁業','鉱業、採石業、砂利採取業','電気・ガス・熱供給・水道業','複合サービス事業','公務（他に分類されるものを除く）','分類不能の産業'];
const PREFECTURES = ['北海道','青森県','岩手県','宮城県','秋田県','山形県','福島県','茨城県','栃木県','群馬県','埼玉県','千葉県','東京都','神奈川県','新潟県','富山県','石川県','福井県','山梨県','長野県','岐阜県','静岡県','愛知県','三重県','滋賀県','京都府','大阪府','兵庫県','奈良県','和歌山県','鳥取県','島根県','岡山県','広島県','山口県','徳島県','香川県','愛媛県','高知県','福岡県','佐賀県','長崎県','熊本県','大分県','宮崎県','鹿児島県','沖縄県'];
const EMPLOYEE_RANGES = ['1-10','11-50','51-100','101-300','301-500','501-1000','1000-','不明'];

function stageLabel(v) { return STAGES.find(s => s.value === v)?.label || v; }
function stageColor(v) { return STAGES.find(s => s.value === v)?.color || '#888'; }

function Field({ label, value, edit, children }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 3, marginBottom: 10 }}>
      <span style={{ fontSize: 11, color: '#888', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{label}</span>
      {edit ? children : <span style={{ fontSize: 14, color: '#333' }}>{value || <span style={{ color: '#bbb' }}>-</span>}</span>}
    </div>
  );
}

export default function ClientDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [client, setClient] = useState(null);
  const [deals, setDeals] = useState([]);
  const [contacts, setContacts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState('info'); // 'info' | 'contacts' | 'deals'
  const [editing, setEditing] = useState(false);
  const [form, setForm] = useState({});
  const [saving, setSaving] = useState(false);

  // deal creation
  const [showDeal, setShowDeal] = useState(false);
  const [dealForm, setDealForm] = useState({ name: '', stage: 'mk', budget: '', notes: '' });

  // contact creation/editing
  const [showContact, setShowContact] = useState(false);
  const [editingContact, setEditingContact] = useState(null);
  const [contactForm, setContactForm] = useState({ last_name: '', first_name: '', furigana: '', title: '', department: '', email: '', phone: '', notes: '', do_not_contact: false });

  const load = () => {
    Promise.all([
      api.crmClientDetail(id),
      api.crmClientContacts(id),
    ])
      .then(([r, cr]) => {
        setClient(r.client);
        setDeals(r.deals);
        setForm(r.client);
        setContacts(cr.contacts || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, [id]);

  const sf = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const handleSave = async () => {
    setSaving(true);
    try {
      const { client: updated } = await api.crmUpdateClient(id, {
        name: form.name, contact_name: form.contact_name,
        contact_email: form.contact_email, contact_phone: form.contact_phone,
        source: form.source, notes: form.notes,
        industry: form.industry, prefecture: form.prefecture,
        employee_range: form.employee_range, inrevo_person: form.inrevo_person,
        corporate_url: form.corporate_url, service_url1: form.service_url1,
        service_url2: form.service_url2,
        competition: Array.isArray(form.competition) ? form.competition : [],
      });
      setClient(updated);
      setForm(updated);
      setEditing(false);
    } catch { alert('更新に失敗しました'); }
    finally { setSaving(false); }
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
      const { deal } = await api.crmCreateDeal({ clientId: id, name: dealForm.name, stage: dealForm.stage, budget: dealForm.budget ? Number(dealForm.budget) : null, notes: dealForm.notes });
      navigate(`/crm/deals/${deal.id}`);
    } catch { alert('作成に失敗しました'); }
    finally { setSaving(false); }
  };

  const resetContactForm = () => setContactForm({ last_name: '', first_name: '', furigana: '', title: '', department: '', email: '', phone: '', notes: '', do_not_contact: false });

  const handleSaveContact = async () => {
    setSaving(true);
    try {
      if (editingContact) {
        const { contact } = await api.crmUpdateClientContact(id, editingContact.id, contactForm);
        setContacts(cs => cs.map(c => c.id === editingContact.id ? contact : c));
      } else {
        const { contact } = await api.crmAddClientContact(id, contactForm);
        setContacts(cs => [...cs, contact]);
      }
      setShowContact(false);
      setEditingContact(null);
      resetContactForm();
    } catch { alert('保存に失敗しました'); }
    finally { setSaving(false); }
  };

  const handleDeleteContact = async (cid) => {
    if (!confirm('担当者を削除しますか？')) return;
    await api.crmDeleteClientContact(id, cid);
    setContacts(cs => cs.filter(c => c.id !== cid));
  };

  const openEditContact = (c) => {
    setEditingContact(c);
    setContactForm({ last_name: c.last_name || '', first_name: c.first_name || '', furigana: c.furigana || '', title: c.title || '', department: c.department || '', email: c.email || '', phone: c.phone || '', notes: c.notes || '', do_not_contact: c.do_not_contact || false });
    setShowContact(true);
  };

  if (loading) return <div className="loading">読み込み中...</div>;
  if (!client) return <div className="loading">顧客が見つかりません</div>;

  const competition = Array.isArray(client.competition) ? client.competition : [];

  return (
    <div className="dashboard" style={{ minHeight: '100vh', background: '#f8f9fa' }}>
      <header className="dashboard-header">
        <h1 style={{ fontSize: 17 }}>
          <Link to="/crm/clients" style={{ color: '#888', fontWeight: 400 }}>顧客一覧</Link>
          {' / '}<span>{client.name}</span>
        </h1>
        <div className="header-right">
          <Link to="/crm/deals" className="analytics-link">案件一覧</Link>
          {!editing && <button className="analytics-link" onClick={() => setEditing(true)}>編集</button>}
          <button className="filter-clear-btn" style={{ color: '#e74c3c', border: '1px solid #e74c3c' }} onClick={handleDelete}>削除</button>
        </div>
      </header>

      {/* タブ */}
      <div style={{ background: '#fff', borderBottom: '1px solid #eee', padding: '0 24px', display: 'flex', gap: 0 }}>
        {[
          { key: 'info', label: '基本情報' },
          { key: 'contacts', label: `担当者（${contacts.length}）` },
          { key: 'deals', label: `案件（${deals.length}）` },
        ].map(t => (
          <button key={t.key} onClick={() => setTab(t.key)} style={{
            padding: '12px 20px', background: 'none', border: 'none', cursor: 'pointer',
            fontWeight: tab === t.key ? 700 : 400,
            color: tab === t.key ? '#1976d2' : '#666',
            borderBottom: tab === t.key ? '2px solid #1976d2' : '2px solid transparent',
            fontSize: 14, marginBottom: -1,
          }}>{t.label}</button>
        ))}
      </div>

      <div style={{ maxWidth: 1100, margin: '0 auto', padding: '20px 24px' }}>

        {/* ── 基本情報タブ ── */}
        {tab === 'info' && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 280px', gap: 20 }}>
            <div style={{ background: '#fff', borderRadius: 10, padding: 20, boxShadow: '0 1px 4px rgba(0,0,0,0.07)' }}>
              <h3 style={{ fontSize: 14, fontWeight: 700, color: '#666', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 16 }}>顧客情報</h3>

              {editing ? (
                <div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                    {[
                      { label: '会社名 *', key: 'name', span: true },
                      { label: '問い合わせ元（流入経路）', key: 'source', span: true },
                      { label: '担当者名', key: 'contact_name' },
                      { label: '電話', key: 'contact_phone' },
                      { label: 'メール', key: 'contact_email', type: 'email' },
                      { label: 'INREVO担当者', key: 'inrevo_person' },
                    ].map(f => (
                      <div key={f.key} style={{ gridColumn: f.span ? '1/-1' : undefined }}>
                        <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>{f.label}</label>
                        <input type={f.type || 'text'} value={form[f.key] || ''}
                          onChange={e => sf(f.key, e.target.value)}
                          style={{ width: '100%', padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6, fontSize: 13, boxSizing: 'border-box' }} />
                      </div>
                    ))}
                    <div>
                      <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>業界</label>
                      <select value={form.industry || ''} onChange={e => sf('industry', e.target.value)}
                        style={{ width: '100%', padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6, fontSize: 13 }}>
                        <option value="">選択してください</option>
                        {INDUSTRIES.map(i => <option key={i} value={i}>{i}</option>)}
                      </select>
                    </div>
                    <div>
                      <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>都道府県</label>
                      <select value={form.prefecture || ''} onChange={e => sf('prefecture', e.target.value)}
                        style={{ width: '100%', padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6, fontSize: 13 }}>
                        <option value="">選択してください</option>
                        {PREFECTURES.map(p => <option key={p} value={p}>{p}</option>)}
                      </select>
                    </div>
                    <div>
                      <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>従業員数</label>
                      <select value={form.employee_range || ''} onChange={e => sf('employee_range', e.target.value)}
                        style={{ width: '100%', padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6, fontSize: 13 }}>
                        <option value="">選択してください</option>
                        {EMPLOYEE_RANGES.map(r => <option key={r} value={r}>{r}名</option>)}
                      </select>
                    </div>
                    <div>
                      <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>競合</label>
                      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
                        {['研修','SaaS','採用'].map(c => (
                          <label key={c} style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 13 }}>
                            <input type="checkbox"
                              checked={(Array.isArray(form.competition) ? form.competition : []).includes(c)}
                              onChange={e => {
                                const cur = Array.isArray(form.competition) ? form.competition : [];
                                sf('competition', e.target.checked ? [...cur, c] : cur.filter(x => x !== c));
                              }} />{c}
                          </label>
                        ))}
                      </div>
                    </div>
                    {[
                      { label: 'コーポレートサイトURL', key: 'corporate_url', span: true },
                      { label: 'サービスLP URL①', key: 'service_url1' },
                      { label: 'サービスLP URL②', key: 'service_url2' },
                    ].map(f => (
                      <div key={f.key} style={{ gridColumn: f.span ? '1/-1' : undefined }}>
                        <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>{f.label}</label>
                        <input type="url" value={form[f.key] || ''} onChange={e => sf(f.key, e.target.value)}
                          style={{ width: '100%', padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6, fontSize: 13, boxSizing: 'border-box' }} />
                      </div>
                    ))}
                    <div style={{ gridColumn: '1/-1' }}>
                      <label style={{ display: 'block', fontSize: 12, color: '#888', marginBottom: 3 }}>メモ</label>
                      <textarea value={form.notes || ''} onChange={e => sf('notes', e.target.value)}
                        style={{ width: '100%', padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6, fontSize: 13, minHeight: 80, resize: 'vertical', fontFamily: 'inherit', boxSizing: 'border-box' }} />
                    </div>
                  </div>
                  <div style={{ display: 'flex', gap: 8, marginTop: 16 }}>
                    <button className="admin-link" onClick={handleSave} disabled={saving}>{saving ? '保存中...' : '保存'}</button>
                    <button className="filter-clear-btn" onClick={() => { setEditing(false); setForm(client); }}>キャンセル</button>
                  </div>
                </div>
              ) : (
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 0 }}>
                  <Field label="業界" value={client.industry} />
                  <Field label="都道府県" value={client.prefecture} />
                  <Field label="従業員数" value={client.employee_range ? `${client.employee_range}名` : null} />
                  <Field label="INREVO担当者" value={client.inrevo_person} />
                  <Field label="問い合わせ元" value={client.source} />
                  <Field label="競合" value={competition.length > 0 ? competition.join(' / ') : null} />
                  <Field label="担当者名" value={client.contact_name} />
                  <Field label="電話" value={client.contact_phone} />
                  <Field label="メール" value={client.contact_email} />
                  <div />
                  {client.corporate_url && (
                    <div style={{ gridColumn: '1/-1', marginBottom: 10 }}>
                      <span style={{ fontSize: 11, color: '#888', fontWeight: 600, textTransform: 'uppercase' }}>コーポレートサイト</span>
                      <div><a href={client.corporate_url} target="_blank" rel="noreferrer" style={{ fontSize: 13, color: '#1976d2' }}>{client.corporate_url}</a></div>
                    </div>
                  )}
                  {client.notes && (
                    <div style={{ gridColumn: '1/-1', marginBottom: 10 }}>
                      <span style={{ fontSize: 11, color: '#888', fontWeight: 600, textTransform: 'uppercase' }}>メモ</span>
                      <div style={{ fontSize: 14, color: '#333', whiteSpace: 'pre-wrap' }}>{client.notes}</div>
                    </div>
                  )}
                </div>
              )}
            </div>

            <aside>
              <div style={{ background: '#fff', borderRadius: 10, padding: 16, boxShadow: '0 1px 4px rgba(0,0,0,0.07)', marginBottom: 12 }}>
                <h3 style={{ fontSize: 13, fontWeight: 700, color: '#666', marginBottom: 10 }}>登録情報</h3>
                <div style={{ fontSize: 12, color: '#888', lineHeight: 1.8 }}>
                  <div>登録日：{new Date(client.created_at).toLocaleDateString('ja-JP')}</div>
                  <div>更新日：{new Date(client.updated_at).toLocaleDateString('ja-JP')}</div>
                </div>
              </div>
              <div style={{ background: '#fff', borderRadius: 10, padding: 16, boxShadow: '0 1px 4px rgba(0,0,0,0.07)' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
                  <h3 style={{ fontSize: 13, fontWeight: 700, color: '#666', margin: 0 }}>案件サマリー</h3>
                  <button className="admin-link" style={{ padding: '4px 10px', fontSize: 12 }} onClick={() => { setTab('deals'); setShowDeal(true); }}>＋</button>
                </div>
                {deals.length === 0 ? <div style={{ fontSize: 13, color: '#aaa' }}>案件なし</div> : deals.map(d => (
                  <div key={d.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '6px 0', borderBottom: '1px solid #f5f5f5', cursor: 'pointer' }}
                    onClick={() => navigate(`/crm/deals/${d.id}`)}>
                    <span style={{ fontSize: 13, color: '#333' }}>{d.name}</span>
                    <span style={{ fontSize: 11, background: stageColor(d.stage), color: '#fff', padding: '2px 8px', borderRadius: 10, flexShrink: 0 }}>{stageLabel(d.stage)}</span>
                  </div>
                ))}
              </div>
            </aside>
          </div>
        )}

        {/* ── 担当者タブ ── */}
        {tab === 'contacts' && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 12 }}>
              <button className="admin-link" onClick={() => { setEditingContact(null); resetContactForm(); setShowContact(true); }}>＋ 担当者を追加</button>
            </div>
            {contacts.length === 0 ? (
              <div style={{ textAlign: 'center', color: '#aaa', padding: 40 }}>担当者が登録されていません</div>
            ) : (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: 14 }}>
                {contacts.map(c => (
                  <div key={c.id} style={{ background: '#fff', borderRadius: 10, padding: 16, boxShadow: '0 1px 4px rgba(0,0,0,0.07)' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
                      <div>
                        <div style={{ fontWeight: 700, fontSize: 15 }}>{[c.last_name, c.first_name].filter(Boolean).join(' ') || '（名前未設定）'}</div>
                        {c.furigana && <div style={{ fontSize: 12, color: '#aaa' }}>{c.furigana}</div>}
                      </div>
                      <div style={{ display: 'flex', gap: 6 }}>
                        <button onClick={() => openEditContact(c)} style={{ background: 'none', border: '1px solid #ddd', borderRadius: 5, padding: '3px 8px', fontSize: 12, cursor: 'pointer' }}>編集</button>
                        <button onClick={() => handleDeleteContact(c.id)} style={{ background: 'none', border: '1px solid #fcc', borderRadius: 5, padding: '3px 8px', fontSize: 12, cursor: 'pointer', color: '#e74c3c' }}>削除</button>
                      </div>
                    </div>
                    <div style={{ fontSize: 13, color: '#555', lineHeight: 1.8 }}>
                      {c.title && <div>役職：{c.title}</div>}
                      {c.department && <div>部署：{c.department}</div>}
                      {c.email && <div>✉ <a href={`mailto:${c.email}`} style={{ color: '#1976d2' }}>{c.email}</a></div>}
                      {c.phone && <div>📞 {c.phone}</div>}
                      {c.do_not_contact && <div style={{ color: '#e74c3c', fontSize: 12, fontWeight: 600 }}>⚠ 営業禁止</div>}
                      {c.notes && <div style={{ marginTop: 6, fontSize: 12, color: '#888', whiteSpace: 'pre-wrap' }}>{c.notes}</div>}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* ── 案件タブ ── */}
        {tab === 'deals' && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 12 }}>
              <button className="admin-link" onClick={() => setShowDeal(true)}>＋ 案件追加</button>
            </div>
            {deals.length === 0 ? (
              <div style={{ textAlign: 'center', color: '#aaa', padding: 40 }}>案件がまだありません</div>
            ) : (
              <table className="crm-table" style={{ background: '#fff' }}>
                <thead>
                  <tr>
                    <th>案件名</th><th>ステージ</th><th>ヨミ</th><th>予算</th><th>担当営業</th><th>更新日</th>
                  </tr>
                </thead>
                <tbody>
                  {deals.map(d => (
                    <tr key={d.id} className="crm-row" onClick={() => navigate(`/crm/deals/${d.id}`)}>
                      <td style={{ fontWeight: 600 }}>{d.name}</td>
                      <td><span className="stage-badge" style={{ background: stageColor(d.stage) }}>{stageLabel(d.stage)}</span></td>
                      <td>{d.yomi || <span style={{ color: '#bbb' }}>-</span>}</td>
                      <td>{d.budget ? `¥${Number(d.budget).toLocaleString()}` : '-'}</td>
                      <td>{d.sales_person || '-'}</td>
                      <td style={{ color: '#888', fontSize: 12 }}>{new Date(d.updated_at).toLocaleDateString('ja-JP')}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>

      {/* 案件追加モーダル */}
      {showDeal && (
        <div className="crm-modal-overlay" onClick={() => setShowDeal(false)}>
          <div className="crm-modal" onClick={e => e.stopPropagation()}>
            <h2>案件追加</h2>
            <form onSubmit={handleCreateDeal}>
              <div className="crm-form-grid">
                <div className="crm-form-field crm-form-full">
                  <label>案件名 *</label>
                  <input value={dealForm.name} onChange={e => setDealForm(f => ({ ...f, name: e.target.value }))} required />
                </div>
                <div className="crm-form-field">
                  <label>ステージ</label>
                  <select value={dealForm.stage} onChange={e => setDealForm(f => ({ ...f, stage: e.target.value }))}>
                    {STAGES.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
                  </select>
                </div>
                <div className="crm-form-field">
                  <label>見込み予算（円）</label>
                  <input type="number" value={dealForm.budget} onChange={e => setDealForm(f => ({ ...f, budget: e.target.value }))} />
                </div>
                <div className="crm-form-field crm-form-full">
                  <label>メモ</label>
                  <textarea value={dealForm.notes} onChange={e => setDealForm(f => ({ ...f, notes: e.target.value }))} />
                </div>
              </div>
              <div className="crm-modal-actions">
                <button type="button" className="crm-btn-ghost" onClick={() => setShowDeal(false)}>キャンセル</button>
                <button type="submit" className="crm-btn" disabled={saving}>{saving ? '作成中...' : '作成'}</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* 担当者追加/編集モーダル */}
      {showContact && (
        <div className="crm-modal-overlay" onClick={() => { setShowContact(false); setEditingContact(null); }}>
          <div className="crm-modal" onClick={e => e.stopPropagation()}>
            <h2>{editingContact ? '担当者を編集' : '担当者を追加'}</h2>
            <div className="crm-form-grid">
              {[
                { label: '姓', key: 'last_name' }, { label: '名', key: 'first_name' },
                { label: 'ふりがな', key: 'furigana', span: true },
                { label: '役職', key: 'title' }, { label: '部署', key: 'department' },
                { label: 'メールアドレス', key: 'email', type: 'email', span: true },
                { label: '電話番号', key: 'phone', span: true },
              ].map(f => (
                <div key={f.key} className={`crm-form-field${f.span ? ' crm-form-full' : ''}`}>
                  <label>{f.label}</label>
                  <input type={f.type || 'text'} value={contactForm[f.key] || ''}
                    onChange={e => setContactForm(cf => ({ ...cf, [f.key]: e.target.value }))} />
                </div>
              ))}
              <div className="crm-form-field crm-form-full">
                <label>備考</label>
                <textarea value={contactForm.notes || ''} onChange={e => setContactForm(cf => ({ ...cf, notes: e.target.value }))} />
              </div>
              <div className="crm-form-field crm-form-full">
                <label style={{ display: 'flex', alignItems: 'center', gap: 8, flexDirection: 'row' }}>
                  <input type="checkbox" checked={contactForm.do_not_contact || false}
                    onChange={e => setContactForm(cf => ({ ...cf, do_not_contact: e.target.checked }))} />
                  営業禁止（この担当者への営業連絡は控える）
                </label>
              </div>
            </div>
            <div className="crm-modal-actions">
              <button className="crm-btn-ghost" onClick={() => { setShowContact(false); setEditingContact(null); }}>キャンセル</button>
              <button className="crm-btn" onClick={handleSaveContact} disabled={saving}>{saving ? '保存中...' : '保存'}</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
