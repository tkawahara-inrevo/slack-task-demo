import { useEffect, useState } from 'react';
import { api } from '../../api/client';

// Deal fields available for use in formulas
const AVAILABLE_FIELDS = [
  { key: 'budget', label: '予算' },
  { key: 'initial_cost', label: '初期費用' },
  { key: 'monthly_cost', label: '月額費用' },
  { key: 'unit_price', label: '単価' },
  { key: 'contract_months', label: '契約月数' },
  { key: 'guarantee_count', label: '保証人数' },
  { key: 'guarantee_salary', label: '想定年収' },
  { key: 'rate', label: '料率（%）' },
  { key: 'advance_payment', label: '前払い金額' },
];

const inputSx = {
  width: '100%', padding: '7px 10px', border: '1px solid #ddd', borderRadius: 6,
  fontSize: 13, boxSizing: 'border-box',
};

export default function FormulasAdmin() {
  const [defs, setDefs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [editing, setEditing] = useState(null);
  const [form, setForm] = useState({ name: '', expression: '', format: 'money', description: '' });
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState(null);

  const load = () => {
    api.crmCalcDefs()
      .then(d => setDefs(d.defs || []))
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  const openNew = () => {
    setEditing(null);
    setForm({ name: '', expression: '', format: 'money', description: '' });
    setTestResult(null);
    setShowModal(true);
  };

  const openEdit = (def) => {
    setEditing(def);
    setForm({ name: def.name, expression: def.expression, format: def.format || 'money', description: def.description || '' });
    setTestResult(null);
    setShowModal(true);
  };

  const handleSave = async () => {
    if (!form.name.trim() || !form.expression.trim()) return;
    setSaving(true);
    try {
      if (editing) {
        await api.crmUpdateCalcDef(editing.id, form);
      } else {
        await api.crmAddCalcDef(form);
      }
      setShowModal(false);
      load();
    } catch { alert('保存に失敗しました'); }
    finally { setSaving(false); }
  };

  const handleDelete = async (id) => {
    if (!confirm('この計算フィールド定義を削除しますか？')) return;
    await api.crmDeleteCalcDef(id);
    load();
  };

  const handleTest = () => {
    try {
      // Test with sample values
      const sample = { budget: 500000, initial_cost: 300000, monthly_cost: 100000, unit_price: 50000, contract_months: 6, guarantee_count: 3, guarantee_salary: 5000000, rate: 20, advance_payment: 100000 };
      const keys = Object.keys(sample);
      const vals = Object.values(sample);
      // eslint-disable-next-line no-new-func
      const fn = new Function(...keys, `return (${form.expression})`);
      const result = fn(...vals);
      setTestResult({ ok: true, value: result });
    } catch (e) {
      setTestResult({ ok: false, error: e.message });
    }
  };

  const insertField = (key) => {
    setForm(f => ({ ...f, expression: f.expression + key }));
  };

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 20 }}>計算フィールド管理</h2>
          <p style={{ margin: '4px 0 0', fontSize: 13, color: '#888' }}>案件詳細ページのサイドバーに表示する計算フィールドを定義します</p>
        </div>
        <button className="admin-link" onClick={openNew}>＋ 追加</button>
      </div>

      {loading ? (
        <div style={{ color: '#aaa', textAlign: 'center', padding: 40 }}>読み込み中...</div>
      ) : defs.length === 0 ? (
        <div style={{ background: '#fff', borderRadius: 8, padding: 40, textAlign: 'center', color: '#aaa', boxShadow: '0 1px 3px rgba(0,0,0,0.06)' }}>
          <div style={{ fontSize: 32, marginBottom: 12 }}>🧮</div>
          <div style={{ fontSize: 15, marginBottom: 8 }}>計算フィールドが未設定です</div>
          <div style={{ fontSize: 13, marginBottom: 16 }}>インセン金額・粗利率などの計算式を自由に設定できます</div>
          <button className="admin-link" onClick={openNew}>最初の計算フィールドを追加</button>
        </div>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
          <thead>
            <tr style={{ background: '#f5f5f5', textAlign: 'left' }}>
              {['フィールド名', '計算式', '表示形式', '説明', ''].map(h => (
                <th key={h} style={{ padding: '10px 14px', fontSize: 13, fontWeight: 600 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {defs.map(d => (
              <tr key={d.id} style={{ borderBottom: '1px solid #f0f0f0' }}>
                <td style={{ padding: '12px 14px', fontWeight: 600 }}>{d.name}</td>
                <td style={{ padding: '12px 14px' }}>
                  <code style={{ background: '#f5f5f5', padding: '2px 8px', borderRadius: 4, fontSize: 12, fontFamily: 'monospace' }}>{d.expression}</code>
                </td>
                <td style={{ padding: '12px 14px', fontSize: 13 }}>
                  <span style={{ background: d.format === 'percent' ? '#e3f2fd' : '#e8f5e9', color: d.format === 'percent' ? '#1976d2' : '#388e3c', padding: '2px 8px', borderRadius: 10, fontSize: 12 }}>
                    {d.format === 'percent' ? '%（パーセント）' : '¥（金額）'}
                  </span>
                </td>
                <td style={{ padding: '12px 14px', fontSize: 13, color: '#666' }}>{d.description || '-'}</td>
                <td style={{ padding: '12px 14px' }}>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <button onClick={() => openEdit(d)}
                      style={{ background: 'none', border: 'none', color: '#1976d2', cursor: 'pointer', fontSize: 13 }}>編集</button>
                    <button onClick={() => handleDelete(d.id)}
                      style={{ background: 'none', border: 'none', color: '#e74c3c', cursor: 'pointer', fontSize: 13 }}>削除</button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div style={{ marginTop: 24, background: '#f0f7ff', borderRadius: 8, padding: 16, border: '1px solid #bbdefb' }}>
        <h4 style={{ margin: '0 0 8px', fontSize: 13, color: '#1976d2' }}>使い方</h4>
        <div style={{ fontSize: 12, color: '#555', lineHeight: 1.7 }}>
          <div>• 計算式には以下のフィールド名が使えます: <code style={{ fontFamily: 'monospace', background: '#fff', padding: '1px 4px', borderRadius: 3 }}>{AVAILABLE_FIELDS.map(f => f.key).join(', ')}</code></div>
          <div>• 四則演算（+, -, *, /）や括弧が使えます</div>
          <div>• 例: <code style={{ fontFamily: 'monospace', background: '#fff', padding: '1px 4px', borderRadius: 3 }}>initial_cost + monthly_cost * contract_months</code>（売上合計）</div>
          <div>• 例: <code style={{ fontFamily: 'monospace', background: '#fff', padding: '1px 4px', borderRadius: 3 }}>(initial_cost - advance_payment) / initial_cost * 100</code>（残額率 → %形式で設定）</div>
        </div>
      </div>

      {showModal && (
        <div className="modal-overlay" onClick={() => setShowModal(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()} style={{ maxWidth: 520 }}>
            <h2 style={{ marginBottom: 16 }}>{editing ? '計算フィールドを編集' : '計算フィールドを追加'}</h2>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: 'block', fontSize: 12, color: '#555', marginBottom: 4, fontWeight: 600 }}>フィールド名 *</label>
              <input style={inputSx} placeholder="例：インセン金額、粗利率" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} />
            </div>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: 'block', fontSize: 12, color: '#555', marginBottom: 4, fontWeight: 600 }}>計算式 *</label>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginBottom: 6 }}>
                {AVAILABLE_FIELDS.map(f => (
                  <button key={f.key} type="button" onClick={() => insertField(f.key)}
                    style={{ padding: '2px 8px', fontSize: 11, border: '1px solid #ddd', borderRadius: 4, background: '#f5f5f5', cursor: 'pointer', fontFamily: 'monospace' }}>
                    {f.key}
                  </button>
                ))}
              </div>
              <textarea style={{ ...inputSx, height: 70, resize: 'vertical', fontFamily: 'monospace' }}
                placeholder="例: initial_cost + monthly_cost * contract_months"
                value={form.expression} onChange={e => setForm(f => ({ ...f, expression: e.target.value }))} />
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 6 }}>
                <button type="button" onClick={handleTest}
                  style={{ padding: '4px 12px', border: '1px solid #1976d2', borderRadius: 6, background: '#fff', color: '#1976d2', cursor: 'pointer', fontSize: 12 }}>
                  テスト実行
                </button>
                {testResult && (
                  testResult.ok ? (
                    <span style={{ fontSize: 12, color: '#388e3c' }}>
                      ✓ 結果: {form.format === 'percent' ? `${Number(testResult.value).toFixed(1)}%` : `¥${Math.round(testResult.value).toLocaleString()}`}
                      <span style={{ color: '#aaa', marginLeft: 6 }}>（サンプル値で計算）</span>
                    </span>
                  ) : (
                    <span style={{ fontSize: 12, color: '#e74c3c' }}>✗ エラー: {testResult.error}</span>
                  )
                )}
              </div>
            </div>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: 'block', fontSize: 12, color: '#555', marginBottom: 4, fontWeight: 600 }}>表示形式</label>
              <div style={{ display: 'flex', gap: 12 }}>
                {[{ value: 'money', label: '¥ 金額' }, { value: 'percent', label: '% パーセント' }].map(opt => (
                  <label key={opt.value} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, cursor: 'pointer' }}>
                    <input type="radio" name="format" value={opt.value} checked={form.format === opt.value} onChange={() => setForm(f => ({ ...f, format: opt.value }))} />
                    {opt.label}
                  </label>
                ))}
              </div>
            </div>

            <div style={{ marginBottom: 16 }}>
              <label style={{ display: 'block', fontSize: 12, color: '#555', marginBottom: 4 }}>説明（任意）</label>
              <input style={inputSx} placeholder="このフィールドの説明" value={form.description} onChange={e => setForm(f => ({ ...f, description: e.target.value }))} />
            </div>

            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
              <button type="button" className="filter-clear-btn" onClick={() => setShowModal(false)}>キャンセル</button>
              <button type="button" className="admin-link" onClick={handleSave} disabled={saving || !form.name.trim() || !form.expression.trim()}>
                {saving ? '保存中...' : '保存'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
