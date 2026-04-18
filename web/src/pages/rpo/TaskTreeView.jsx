// RPO タスクツリービュー: チーム → 案件 → タスク（workload_items）
import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

export default function TaskTreeView() {
  const navigate = useNavigate();
  const [teams, setTeams]       = useState([]);
  const [clients, setClients]   = useState([]);
  const [items, setItems]       = useState({}); // clientId → items[]
  const [loading, setLoading]   = useState(true);
  const [expanded, setExpanded] = useState({}); // teamId or clientId → bool
  const [monthFilter, setMonthFilter] = useState(currentYearMonth());

  useEffect(() => {
    Promise.all([
      api.rpoTeams().catch(() => ({ teams: [] })),
      api.rpoClients({ teamId: '' }).catch(() => ({ clients: [] })),
    ]).then(([teamsRes, clientsRes]) => {
      setTeams(teamsRes.teams || []);
      setClients(clientsRes.clients || []);
      setLoading(false);
    });
  }, []);

  const toggleTeam = (teamId) => {
    setExpanded(prev => ({ ...prev, [teamId]: !prev[teamId] }));
  };

  const toggleClient = async (clientId) => {
    const next = !expanded[clientId];
    setExpanded(prev => ({ ...prev, [clientId]: next }));
    if (next && !items[clientId]) {
      try {
        const r = await api.rpoWorkloadItems(clientId);
        setItems(prev => ({ ...prev, [clientId]: r.items || [] }));
      } catch { setItems(prev => ({ ...prev, [clientId]: [] })); }
    }
  };

  // 月フィルター: アイテムの due_date が monthFilter 月か未設定のものを表示
  const filteredItems = (clientId) => {
    const all = items[clientId] || [];
    if (!monthFilter) return all;
    return all.filter(item => {
      if (!item.due_date) return true;
      return item.due_date.slice(0, 7) === monthFilter;
    });
  };

  // クライアントをチームごとにグループ化
  const clientsByTeam = {};
  for (const c of clients) {
    const key = c.dash_team_id || '__none__';
    if (!clientsByTeam[key]) clientsByTeam[key] = [];
    clientsByTeam[key].push(c);
  }

  // 月次タスク生成
  const handleGenerateMonthly = async (clientId) => {
    try {
      const r = await api.rpoGenerateMonthlyTasksClient(clientId, monthFilter);
      if (r.skipped) { alert('すでに生成済みか、対象外の案件です'); return; }
      alert(`${r.created?.length || 0}件の月次タスクを生成しました`);
      // キャッシュ更新
      const r2 = await api.rpoWorkloadItems(clientId);
      setItems(prev => ({ ...prev, [clientId]: r2.items || [] }));
    } catch (err) { alert('生成失敗: ' + err.message); }
  };

  if (loading) return <div className="page-loading">読み込み中...</div>;

  return (
    <div className="rpo-page">
      <div className="rpo-header">
        <div>
          <button className="btn-back-inline" onClick={() => navigate('/rpo')}>← 案件一覧</button>
          <h1 className="rpo-title">タスクツリー</h1>
          <p className="rpo-subtitle">チーム → 案件 → タスクの階層ビュー</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <label style={{ fontSize: '0.85rem', color: '#6b7280' }}>月絞り込み</label>
          <input
            type="month"
            value={monthFilter}
            onChange={e => setMonthFilter(e.target.value)}
            style={{ padding: '0.3rem 0.5rem', border: '1px solid #e5e7eb', borderRadius: '6px', fontSize: '0.85rem' }}
          />
          <button
            className="btn-secondary small"
            onClick={async () => {
              const r = await api.rpoGenerateMonthlyTasks(monthFilter);
              const total = r.results?.filter(x => !x.skipped).reduce((s, x) => s + (x.created?.length || 0), 0) || 0;
              alert(`${total}件の月次タスクを生成しました`);
            }}
          >
            全案件 月次タスク生成
          </button>
        </div>
      </div>

      <div className="tree-view">
        {teams.map(team => {
          const teamClients = clientsByTeam[team.id] || [];
          if (!teamClients.length) return null;
          return (
            <div key={team.id} className="tree-team-node">
              <button
                className="tree-team-header"
                onClick={() => toggleTeam(team.id)}
              >
                <span className="tree-chevron">{expanded[team.id] ? '▼' : '▶'}</span>
                <span className="tree-team-name">{team.name}</span>
                <span className="tree-badge">{teamClients.length}案件</span>
              </button>

              {expanded[team.id] && (
                <div className="tree-clients">
                  {teamClients.map(client => (
                    <div key={client.id} className="tree-client-node">
                      <div className="tree-client-header">
                        <button
                          className="tree-client-toggle"
                          onClick={() => toggleClient(client.id)}
                        >
                          <span className="tree-chevron">{expanded[client.id] ? '▼' : '▶'}</span>
                          <span
                            className="tree-client-dot"
                            style={{ background: colorOf(client.color) }}
                          />
                          <span className="tree-client-name">{client.name}</span>
                          <span className={`rpo-status-badge ${client.status}`} style={{ fontSize: '0.7rem' }}>
                            {client.status === 'active' ? '進行中' : 'アーカイブ'}
                          </span>
                        </button>
                        <div className="tree-client-actions">
                          {client.plan === 'monthly' && expanded[client.id] && (
                            <button
                              className="btn-secondary xsmall"
                              onClick={() => handleGenerateMonthly(client.id)}
                              title="月次タスク生成"
                            >
                              月次タスク生成
                            </button>
                          )}
                          <button
                            className="btn-secondary xsmall"
                            onClick={() => navigate(`/rpo/${client.id}`)}
                          >
                            詳細
                          </button>
                        </div>
                      </div>

                      {expanded[client.id] && (
                        <div className="tree-items">
                          {!items[client.id] ? (
                            <div className="tree-loading">読み込み中...</div>
                          ) : filteredItems(client.id).length === 0 ? (
                            <div className="tree-empty">タスクなし</div>
                          ) : (
                            filteredItems(client.id).map(item => (
                              <div key={item.id} className="tree-item">
                                <span className={`tree-item-dot ${item.is_done ? 'done' : ''}`} />
                                <span className={`tree-item-title ${item.is_done ? 'done' : ''}`}>{item.title}</span>
                                {item.owner_display_name && (
                                  <span className="tree-item-owner">{item.owner_display_name}</span>
                                )}
                                {item.due_date && (
                                  <span className="tree-item-due">{formatDate(item.due_date)}</span>
                                )}
                              </div>
                            ))
                          )}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })}

        {/* チーム未割当の案件 */}
        {clientsByTeam['__none__']?.length > 0 && (
          <div className="tree-team-node">
            <button className="tree-team-header" onClick={() => toggleTeam('__none__')}>
              <span className="tree-chevron">{expanded['__none__'] ? '▼' : '▶'}</span>
              <span className="tree-team-name" style={{ color: '#9ca3af' }}>チーム未割当</span>
              <span className="tree-badge">{clientsByTeam['__none__'].length}案件</span>
            </button>
            {expanded['__none__'] && (
              <div className="tree-clients">
                {clientsByTeam['__none__'].map(client => (
                  <div key={client.id} className="tree-client-node">
                    <div className="tree-client-header">
                      <button className="tree-client-toggle" onClick={() => toggleClient(client.id)}>
                        <span className="tree-chevron">{expanded[client.id] ? '▼' : '▶'}</span>
                        <span className="tree-client-dot" style={{ background: colorOf(client.color) }} />
                        <span className="tree-client-name">{client.name}</span>
                      </button>
                      <button className="btn-secondary xsmall" onClick={() => navigate(`/rpo/${client.id}`)}>詳細</button>
                    </div>
                    {expanded[client.id] && (
                      <div className="tree-items">
                        {filteredItems(client.id).length === 0
                          ? <div className="tree-empty">タスクなし</div>
                          : filteredItems(client.id).map(item => (
                            <div key={item.id} className="tree-item">
                              <span className={`tree-item-dot ${item.is_done ? 'done' : ''}`} />
                              <span className={`tree-item-title ${item.is_done ? 'done' : ''}`}>{item.title}</span>
                              {item.owner_display_name && <span className="tree-item-owner">{item.owner_display_name}</span>}
                            </div>
                          ))
                        }
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

const COLOR_MAP = {
  Ocean:   '#3b82f6',
  Emerald: '#10b981',
  Amber:   '#f59e0b',
  Rose:    '#ef4444',
  Violet:  '#8b5cf6',
};
function colorOf(name) { return COLOR_MAP[name] || '#6b7280'; }

function formatDate(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleDateString('ja-JP', { month: 'short', day: 'numeric' });
}

function currentYearMonth() {
  return new Date().toISOString().slice(0, 7);
}
