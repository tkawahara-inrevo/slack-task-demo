import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
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

function stageLabel(v) {
  return STAGES.find(s => s.value === v)?.label || v;
}
function stageColor(v) {
  return STAGES.find(s => s.value === v)?.color || '#888';
}

export default function DealList() {
  const [deals, setDeals] = useState([]);
  const [stageFilter, setStageFilter] = useState('');
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  const load = (stage = stageFilter) => {
    const params = stage ? { stage } : {};
    api.crmDeals(params)
      .then(r => setDeals(r.deals))
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  const handleStageChange = (v) => {
    setStageFilter(v);
    setLoading(true);
    load(v);
  };

  // パイプライン表示（ステージ別にグループ化）
  const grouped = STAGES.map(s => ({
    ...s,
    deals: deals.filter(d => d.stage === s.value),
  })).filter(s => !stageFilter || s.value === stageFilter);

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>案件パイプライン</h1>
        <div className="header-right">
          <Link to="/" className="analytics-link">タスク</Link>
          <Link to="/crm/clients" className="analytics-link">顧客一覧</Link>
        </div>
      </header>

      <div style={{ padding: '12px 24px 0' }}>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <button
            className={stageFilter === '' ? 'admin-link' : 'filter-clear-btn'}
            onClick={() => handleStageChange('')}
          >すべて（{deals.length}）</button>
          {STAGES.map(s => {
            const count = deals.filter(d => d.stage === s.value).length;
            return (
              <button key={s.value}
                style={{ borderLeft: `3px solid ${s.color}` }}
                className={stageFilter === s.value ? 'admin-link' : 'filter-clear-btn'}
                onClick={() => handleStageChange(s.value)}
              >{s.label}（{count}）</button>
            );
          })}
        </div>
      </div>

      {loading ? (
        <div className="loading">読み込み中...</div>
      ) : (
        <div style={{ padding: '16px 24px', display: 'flex', gap: 16, overflowX: 'auto' }}>
          {grouped.map(s => (
            <div key={s.value} style={{ minWidth: 240, flex: '0 0 240px' }}>
              <div style={{
                background: s.color, color: '#fff', padding: '8px 12px',
                borderRadius: '6px 6px 0 0', fontWeight: 600, fontSize: 13,
                display: 'flex', justifyContent: 'space-between'
              }}>
                <span>{s.label}</span>
                <span>{s.deals.length}</span>
              </div>
              <div style={{ background: '#f5f5f5', borderRadius: '0 0 6px 6px', padding: 8, minHeight: 80 }}>
                {s.deals.length === 0 ? (
                  <div style={{ color: '#bbb', fontSize: 12, padding: 8, textAlign: 'center' }}>なし</div>
                ) : s.deals.map(d => (
                  <div key={d.id}
                    onClick={() => navigate(`/crm/deals/${d.id}`)}
                    style={{
                      background: '#fff', borderRadius: 6, padding: '10px 12px', marginBottom: 8,
                      cursor: 'pointer', boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                      borderLeft: `3px solid ${s.color}`,
                    }}>
                    <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 4 }}>{d.name}</div>
                    <div style={{ fontSize: 12, color: '#888' }}>{d.client_name}</div>
                    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginTop: 4, alignItems: 'center' }}>
                      {d.yomi && <span style={{ fontSize: 10, background: '#fff3e0', color: '#e65100', padding: '1px 6px', borderRadius: 8, border: '1px solid #ffe0b2' }}>{d.yomi}</span>}
                      {d.budget && <span style={{ fontSize: 11, color: '#555' }}>¥{d.budget.toLocaleString()}</span>}
                    </div>
                    <div style={{ fontSize: 11, color: '#bbb', marginTop: 4 }}>
                      {new Date(d.updated_at).toLocaleDateString('ja-JP')}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
