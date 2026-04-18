import { useEffect, useState, useCallback, useRef } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { api } from '../../api/client';

function thisMonth() {
  return new Date().toISOString().slice(0, 7);
}
function fmtMonth(ym) {
  const [y, m] = ym.split('-');
  return `${y}年${Number(m)}月`;
}
function prevMonth(ym) {
  const [y, m] = ym.split('-').map(Number);
  return m === 1 ? `${y - 1}-12` : `${y}-${String(m - 1).padStart(2, '0')}`;
}
function nextMonth(ym) {
  const [y, m] = ym.split('-').map(Number);
  return m === 12 ? `${y + 1}-01` : `${y}-${String(m + 1).padStart(2, '0')}`;
}

export default function RpoWorkload() {
  const navigate = useNavigate();
  const location = useLocation();
  const contextTeamId  = location.state?.dashTeamId ?? undefined;
  const isAllTeams     = contextTeamId === null; // 全チームから開いた

  const [yearMonth, setYearMonth] = useState(thisMonth);
  const [clients, setClients]     = useState([]);
  const [loading, setLoading]     = useState(true);
  const [teams, setTeams]         = useState([]);
  const [filterTeamId, setFilterTeamId] = useState('');
  const [edits, setEdits] = useState({});
  const saveTimers = useRef({});

  useEffect(() => {
    if (isAllTeams) {
      api.rpoTeams().then(r => setTeams(r.teams || [])).catch(() => {});
    }
    const params = isAllTeams
      ? (filterTeamId ? { teamId: filterTeamId } : {})
      : (contextTeamId ? { teamId: contextTeamId } : {});
    api.rpoClients(params)
      .then(r => setClients((r.clients || []).filter(c => c.status === 'active')))
      .finally(() => setLoading(false));
  }, []);

  // 全チームモードでフィルター変更時に再取得
  useEffect(() => {
    if (!isAllTeams) return;
    setLoading(true);
    const params = filterTeamId ? { teamId: filterTeamId } : {};
    api.rpoClients(params)
      .then(r => setClients((r.clients || []).filter(c => c.status === 'active')))
      .finally(() => setLoading(false));
  }, [filterTeamId]);

  const getVal = (client, field) => {
    const local = edits[client.id];
    if (local && field in local) return local[field];
    const wb = client.data?.workloadByMonth?.[yearMonth];
    if (field === 'hours')      return wb?.hours      ?? '';
    if (field === 'otherHours') return wb?.otherHours ?? '';
    return '';
  };

  const handleEdit = useCallback((clientId, field, value) => {
    setEdits(prev => ({ ...prev, [clientId]: { ...(prev[clientId] || {}), [field]: value } }));

    // デバウンス保存
    clearTimeout(saveTimers.current[clientId]);
    saveTimers.current[clientId] = setTimeout(async () => {
      setEdits(cur => {
        const patch = cur[clientId] || {};
        const client = clients.find(c => c.id === clientId);
        if (!client) return cur;
        const wb = client.data?.workloadByMonth?.[yearMonth] || {};
        api.rpoUpdateWorkloadHours(clientId, {
          yearMonth,
          hours:      'hours'      in patch ? Number(patch.hours)      : (wb.hours      || 0),
          otherHours: 'otherHours' in patch ? Number(patch.otherHours) : (wb.otherHours || 0),
        }).catch(() => {});
        return cur;
      });
    }, 600);
  }, [clients, yearMonth]);

  // 担当者ごとの集計（hrAssigneeNameから引く）
  const assignees = {};
  clients.forEach(c => {
    const name = c.data?.hrAssigneeName || '';
    if (!name) return;
    const wb = c.data?.workloadByMonth?.[yearMonth] || {};
    const h  = Number(edits[c.id]?.hours      ?? wb.hours      ?? 0);
    const oh = Number(edits[c.id]?.otherHours ?? wb.otherHours ?? 0);
    if (!assignees[name]) assignees[name] = { hours: 0, otherHours: 0 };
    assignees[name].hours      += h;
    assignees[name].otherHours += oh;
  });
  const assigneeNames = Object.keys(assignees).sort();

  if (loading) return <div style={{ padding: '40px', textAlign: 'center' }}>読み込み中…</div>;

  return (
    <div className="rpo-wl-page">
      <div className="rpo-wl-header">
        <button className="btn-secondary small" onClick={() => navigate('/rpo')}>← 案件一覧</button>
        <h1 className="rpo-summary-title">工数管理</h1>
        {isAllTeams && teams.length > 0 && (
          <select
            value={filterTeamId}
            onChange={e => setFilterTeamId(e.target.value)}
            style={{ padding: '6px 10px', border: '1px solid #d1d5db', borderRadius: '6px', fontSize: '0.875rem' }}
          >
            <option value="">全チーム</option>
            {teams.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        )}
        <div className="rpo-wl-month-nav">
          <button className="btn-secondary small" onClick={() => setYearMonth(prevMonth)}>‹</button>
          <span className="rpo-wl-month-label">{fmtMonth(yearMonth)}</span>
          <button className="btn-secondary small" onClick={() => setYearMonth(nextMonth)}>›</button>
          <button className="btn-secondary small" onClick={() => setYearMonth(thisMonth())}>今月</button>
        </div>
      </div>

      <div className="rpo-wl-layout">
        {/* 左：クライアント別工数入力 */}
        <div className="rpo-wl-table-wrap">
          <table className="rpo-wl-table">
            <thead>
              <tr>
                <th>クライアント</th>
                <th>HR担当者</th>
                <th>実務工数</th>
                <th>その他（MTG等）</th>
                <th>計</th>
              </tr>
            </thead>
            <tbody>
              {clients.map(c => {
                const h  = Number(getVal(c, 'hours'))      || 0;
                const oh = Number(getVal(c, 'otherHours')) || 0;
                return (
                  <tr key={c.id} className="rpo-wl-row">
                    <td>
                      <span className="rpo-wl-dot" style={{ background: colorFor(c.color) }} />
                      {c.name}
                    </td>
                    <td style={{ color: c.data?.hrAssigneeName ? '#111827' : '#9ca3af', fontSize: '0.85rem' }}>
                      {c.data?.hrAssigneeName || '未設定'}
                    </td>
                    <td>
                      <input
                        className="rpo-wl-input num"
                        type="number"
                        min="0"
                        step="0.5"
                        value={getVal(c, 'hours')}
                        placeholder="0"
                        onChange={e => handleEdit(c.id, 'hours', e.target.value)}
                      />
                    </td>
                    <td>
                      <input
                        className="rpo-wl-input num"
                        type="number"
                        min="0"
                        step="0.5"
                        value={getVal(c, 'otherHours')}
                        placeholder="0"
                        onChange={e => handleEdit(c.id, 'otherHours', e.target.value)}
                      />
                    </td>
                    <td className="rpo-wl-total">{(h + oh) || '—'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* 右：担当者別集計 */}
        <div className="rpo-wl-summary">
          <h3 className="rpo-wl-summary-title">工数チェック表</h3>
          {assigneeNames.length === 0 ? (
            <p className="empty-hint">案件にHR担当者を設定すると集計が表示されます</p>
          ) : (
            <table className="rpo-wl-check-table">
              <thead>
                <tr>
                  <th></th>
                  {assigneeNames.map(n => <th key={n}>{n}</th>)}
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td className="rpo-wl-check-label">実務工数</td>
                  {assigneeNames.map(n => (
                    <td key={n} className="rpo-wl-check-val">{assignees[n].hours || 0}</td>
                  ))}
                </tr>
                <tr>
                  <td className="rpo-wl-check-label">その他（MTG等）</td>
                  {assigneeNames.map(n => (
                    <td key={n} className="rpo-wl-check-val">{assignees[n].otherHours || 0}</td>
                  ))}
                </tr>
                <tr className="rpo-wl-check-total">
                  <td className="rpo-wl-check-label">計</td>
                  {assigneeNames.map(n => (
                    <td key={n} className="rpo-wl-check-val">
                      {assignees[n].hours + assignees[n].otherHours}
                    </td>
                  ))}
                </tr>
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}

const COLOR_MAP = { Ocean: '#3b82f6', Emerald: '#10b981', Amber: '#f59e0b', Rose: '#ef4444', Violet: '#8b5cf6', Pink: '#ec4899', Teal: '#14b8a6', Slate: '#64748b' };
function colorFor(c) { return COLOR_MAP[c] || '#6b7280'; }
