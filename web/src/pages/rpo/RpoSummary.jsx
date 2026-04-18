import { useEffect, useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { api } from '../../api/client';

function formatYen(n) {
  if (n == null || isNaN(n)) return '—';
  return n.toLocaleString('ja-JP') + '円';
}

const STATUS_LABEL = { active: '進行中', paused: '一時停止', completed: '完了', cancelled: 'キャンセル' };
const STATUS_COLOR = { active: '#10b981', paused: '#f59e0b', completed: '#6b7280', cancelled: '#ef4444' };

export default function RpoSummary() {
  const navigate = useNavigate();
  const location = useLocation();
  // ClientListから渡されたチームコンテキスト (null = 全チーム表示から来た)
  const contextTeamId = location.state?.dashTeamId ?? undefined;
  const isAllTeamsContext = contextTeamId === null;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [teams, setTeams] = useState([]);
  const [access, setAccess] = useState(null);
  // 全チームコンテキストのときだけ手動フィルター可能
  const [filterTeamId, setFilterTeamId] = useState('');

  useEffect(() => {
    if (!isAllTeamsContext) return;
    api.rpoTeams().catch(() => ({ teams: [] })).then(r => setTeams(r.teams || []));
    api.rpoAccess().catch(() => ({})).then(r => setAccess(r));
  }, []);

  useEffect(() => {
    setLoading(true);
    // 特定チームから来た場合はそのチームで固定
    const dashTeamId = isAllTeamsContext ? (filterTeamId || undefined) : (contextTeamId || undefined);
    const params = dashTeamId ? { dashTeamId } : {};
    api.rpoSummary(params)
      .then(r => { setRows(r.rows || []); setLoading(false); })
      .catch(e => { setError(e.message); setLoading(false); });
  }, [filterTeamId]);

  const active = rows.filter(r => r.status === 'active');
  const others = rows.filter(r => r.status !== 'active');

  const totals = rows.reduce((acc, r) => {
    acc.contract    += Number(r.contract_amount) || 0;
    acc.mediaSpent  += Number(r.media_spent)     || 0;
    acc.accepted    += Number(r.accepted_count)  || 0;
    return acc;
  }, { contract: 0, mediaSpent: 0, accepted: 0 });
  totals.grossProfit = totals.contract - totals.mediaSpent;
  totals.costPerHire = totals.accepted > 0 ? Math.round(totals.mediaSpent / totals.accepted) : null;

  if (loading) return <div style={{ padding: '40px', textAlign: 'center' }}>読み込み中…</div>;
  if (error)   return <div style={{ padding: '40px', color: '#ef4444' }}>エラー: {error}</div>;

  return (
    <div className="rpo-summary-page">
      <div className="rpo-summary-header">
        <button className="btn-secondary small" onClick={() => navigate('/rpo')}>← 案件一覧</button>
        <h1 className="rpo-summary-title">全案件サマリー</h1>
        {isAllTeamsContext && teams.length > 0 && (
          <select
            value={filterTeamId}
            onChange={e => setFilterTeamId(e.target.value)}
            style={{ marginLeft: 'auto', padding: '6px 10px', border: '1px solid #d1d5db', borderRadius: '6px', fontSize: '0.875rem' }}
          >
            <option value="">全チーム</option>
            {teams.map(t => (
              <option key={t.id} value={t.id}>{t.name}</option>
            ))}
          </select>
        )}
      </div>

      {/* 合計カード */}
      <div className="revenue-cards" style={{ marginBottom: '28px' }}>
        <div className="revenue-card">
          <span className="revenue-card-label">総案件数</span>
          <span className="revenue-card-value">{rows.length}件（進行中 {active.length}件）</span>
        </div>
        <div className="revenue-card">
          <span className="revenue-card-label">受注金額 合計</span>
          <span className="revenue-card-value">{formatYen(totals.contract)}</span>
        </div>
        <div className="revenue-card">
          <span className="revenue-card-label">媒体費 合計</span>
          <span className="revenue-card-value" style={{ color: '#ef4444' }}>
            {totals.mediaSpent > 0 ? `▲${formatYen(totals.mediaSpent)}` : '—'}
          </span>
        </div>
        <div className="revenue-card" style={{ borderColor: totals.grossProfit >= 0 ? '#10b981' : '#ef4444' }}>
          <span className="revenue-card-label">粗利 合計</span>
          <span className="revenue-card-value" style={{ color: totals.grossProfit >= 0 ? '#10b981' : '#ef4444' }}>
            {totals.contract > 0 ? formatYen(totals.grossProfit) : '—'}
          </span>
        </div>
        <div className="revenue-card">
          <span className="revenue-card-label">内定承諾 合計</span>
          <span className="revenue-card-value">{totals.accepted}人</span>
        </div>
        <div className="revenue-card">
          <span className="revenue-card-label">平均採用単価</span>
          <span className="revenue-card-value">
            {totals.costPerHire !== null ? formatYen(totals.costPerHire) : '—'}
          </span>
        </div>
      </div>

      {/* 案件テーブル */}
      <div className="rpo-summary-table-wrap">
        <table className="rpo-summary-table">
          <thead>
            <tr>
              <th>案件名</th>
              <th>チーム</th>
              <th>ステータス</th>
              <th>受注金額</th>
              <th>媒体費</th>
              <th>粗利</th>
              <th>内定承諾</th>
              <th>採用単価</th>
              <th>採用目標</th>
            </tr>
          </thead>
          <tbody>
            {[...active, ...others].map(r => {
              const contract   = Number(r.contract_amount) || 0;
              const spent      = Number(r.media_spent)     || 0;
              const profit     = contract - spent;
              const accepted   = Number(r.accepted_count)  || 0;
              const costPerH   = accepted > 0 ? Math.round(spent / accepted) : null;
              return (
                <tr key={r.id} className="rpo-summary-row" onClick={() => navigate(`/rpo/${r.id}`)}>
                  <td>
                    <span className="rpo-summary-dot" style={{ background: colorFor(r.color) }} />
                    {r.name}
                  </td>
                  <td>{r.dash_team_name || '—'}</td>
                  <td>
                    <span className="rpo-summary-status" style={{ color: STATUS_COLOR[r.status] || '#6b7280' }}>
                      {STATUS_LABEL[r.status] || r.status}
                    </span>
                  </td>
                  <td className="num">{contract > 0 ? formatYen(contract) : '—'}</td>
                  <td className="num" style={{ color: spent > 0 ? '#ef4444' : undefined }}>
                    {spent > 0 ? `▲${formatYen(spent)}` : '—'}
                  </td>
                  <td className="num" style={{ color: contract > 0 ? (profit >= 0 ? '#10b981' : '#ef4444') : '#9ca3af' }}>
                    {contract > 0 ? formatYen(profit) : '—'}
                  </td>
                  <td className="num">{accepted}人</td>
                  <td className="num">{costPerH !== null ? formatYen(costPerH) : '—'}</td>
                  <td className="num">{r.hiring_target || '—'}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const COLOR_MAP = { Ocean: '#3b82f6', Emerald: '#10b981', Amber: '#f59e0b', Rose: '#ef4444', Violet: '#8b5cf6', Pink: '#ec4899', Teal: '#14b8a6', Slate: '#64748b' };
function colorFor(c) { return COLOR_MAP[c] || '#6b7280'; }
