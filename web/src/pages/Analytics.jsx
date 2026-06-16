import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api/client';

const STATUS_COLORS = {
  done: '#2ecc71',
  in_progress: '#3498db',
  pending: '#f39c12',
  cancelled: '#95a5a6',
  overdue: '#e74c3c',
};

function Bar({ value, max, color, label }) {
  const pct = max > 0 ? (value / max) * 100 : 0;
  return (
    <div className="bar-row">
      {label && <span className="bar-label">{label}</span>}
      <div className="bar-track">
        <div className="bar-fill" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="bar-value">{value}</span>
    </div>
  );
}

function ProgressRing({ rate }) {
  const r = 36;
  const c = 2 * Math.PI * r;
  const offset = c - (rate / 100) * c;
  const color = rate >= 80 ? '#2ecc71' : rate >= 50 ? '#f39c12' : '#e74c3c';
  return (
    <svg className="progress-ring" width="90" height="90" viewBox="0 0 90 90">
      <circle cx="45" cy="45" r={r} fill="none" stroke="#e9ecef" strokeWidth="8" />
      <circle
        cx="45" cy="45" r={r} fill="none"
        stroke={color} strokeWidth="8"
        strokeDasharray={c} strokeDashoffset={offset}
        strokeLinecap="round"
        transform="rotate(-90 45 45)"
      />
      <text x="45" y="45" textAnchor="middle" dominantBaseline="central"
        fontSize="16" fontWeight="700" fill={color}>
        {rate}%
      </text>
    </svg>
  );
}

export default function Analytics() {
  const [memberData, setMemberData] = useState([]);
  const [dueData, setDueData] = useState([]);
  const [projectData, setProjectData] = useState([]);
  const [members, setMembers] = useState([]);
  const [usergroups, setUsergroups] = useState([]);
  const [filter, setFilter] = useState({ assignee: '', usergroup: '' });
  const [loading, setLoading] = useState(true);
  const [filterLoading, setFilterLoading] = useState(false);

  useEffect(() => {
    Promise.all([
      api.analyticsMemberCompletion(),
      api.analyticsDueCompliance(),
      api.analyticsProjectProgress(),
      api.members(),
      api.usergroups(),
    ])
      .then(([m, d, p, mem, ug]) => {
        setMemberData(m.members || []);
        setDueData(d.weeks || []);
        setProjectData(p.projects || []);
        setMembers(mem.members || []);
        setUsergroups(ug.usergroups || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (loading) return;
    setFilterLoading(true);
    const params = {};
    if (filter.assignee) params.assignee = filter.assignee;
    if (filter.usergroup) params.usergroup = filter.usergroup;
    Promise.all([
      api.analyticsMemberCompletion(params),
      api.analyticsDueCompliance(params),
    ])
      .then(([m, d]) => {
        setMemberData(m.members || []);
        setDueData(d.weeks || []);
      })
      .catch(console.error)
      .finally(() => setFilterLoading(false));
  }, [filter]);

  if (loading) return <div className="loading">読み込み中...</div>;

  const maxMemberTotal = Math.max(1, ...memberData.map((m) => m.total));

  return (
    <div className="analytics-page">
      <div className="task-detail-nav">
        <Link to="/" className="back-btn">&larr; ダッシュボード</Link>
      </div>

      <div className="filter-panel" style={{ marginBottom: 24 }}>
        <div className="filter-panel-row">
          {usergroups.length > 0 && (
            <select
              className="filter-select"
              value={filter.usergroup}
              onChange={(e) => setFilter(f => ({ ...f, usergroup: e.target.value, assignee: '' }))}
            >
              <option value="">チーム：すべて</option>
              {usergroups.map((g) => (
                <option key={g.id} value={g.id}>@{g.handle}</option>
              ))}
            </select>
          )}
          <select
            className="filter-select"
            value={filter.assignee}
            onChange={(e) => setFilter(f => ({ ...f, assignee: e.target.value }))}
          >
            <option value="">担当者：全員</option>
            {members
              .filter((m) => {
                if (!filter.usergroup) return true;
                const ug = usergroups.find((g) => g.id === filter.usergroup);
                return ug?.memberIds?.includes(m.assignee_id);
              })
              .map((m) => (
                <option key={m.assignee_id} value={m.assignee_id}>{m.displayName}</option>
              ))}
          </select>
          {(filter.assignee || filter.usergroup) && (
            <button className="filter-clear-btn" onClick={() => setFilter({ assignee: '', usergroup: '' })}>
              クリア
            </button>
          )}
          {filterLoading && <span style={{ color: 'var(--gray-500)', fontSize: 13 }}>更新中...</span>}
        </div>
      </div>

      <h1>分析ダッシュボード</h1>

      {/* 🎯 タスク消化（監視対象） */}
      <TaskOpsSection members={members} />

      {/* メンバー別完了率 */}
      <section className="analytics-section">
        <h2>メンバー別完了率</h2>
        {memberData.length === 0 ? (
          <p className="empty-text">データがありません</p>
        ) : (
          <div className="member-completion-grid">
            {memberData.map((m) => (
              <div key={m.assignee_id} className="member-completion-card">
                <div className="mc-header">
                  <ProgressRing rate={m.completion_rate} />
                  <div className="mc-info">
                    <span className="mc-name">{m.displayName}</span>
                    <span className="mc-total">全 {m.total} タスク</span>
                  </div>
                </div>
                <div className="mc-bars">
                  <Bar value={m.done} max={maxMemberTotal} color={STATUS_COLORS.done} label="完了" />
                  <Bar value={m.in_progress} max={maxMemberTotal} color={STATUS_COLORS.in_progress} label="進行中" />
                  <Bar value={m.pending} max={maxMemberTotal} color={STATUS_COLORS.pending} label="保留" />
                  {m.overdue > 0 && (
                    <Bar value={m.overdue} max={maxMemberTotal} color={STATUS_COLORS.overdue} label="期限切れ" />
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      {/* 期限遵守率の推移 */}
      <section className="analytics-section">
        <h2>期限遵守率の推移（週別・過去12週）</h2>
        {dueData.length === 0 ? (
          <p className="empty-text">データがありません</p>
        ) : (
          <div className="compliance-chart">
            <div className="chart-y-axis">
              <span>100%</span>
              <span>75%</span>
              <span>50%</span>
              <span>25%</span>
              <span>0%</span>
            </div>
            <div className="chart-bars">
              {dueData.map((w, i) => {
                const rate = w.compliance_rate ?? 0;
                const color = rate >= 80 ? STATUS_COLORS.done : rate >= 50 ? STATUS_COLORS.pending : STATUS_COLORS.overdue;
                const weekLabel = w.week_start?.slice(5, 10).replace('-', '/');
                return (
                  <div key={i} className="chart-bar-col">
                    <div className="chart-bar-wrap">
                      <div
                        className="chart-bar"
                        style={{ height: `${rate}%`, background: color }}
                        title={`${rate}% (${w.on_time}/${w.with_due})`}
                      />
                    </div>
                    <span className="chart-bar-label">{weekLabel}</span>
                    <span className="chart-bar-rate">{w.compliance_rate !== null ? `${rate}%` : '-'}</span>
                  </div>
                );
              })}
            </div>
          </div>
        )}
        <div className="compliance-legend">
          <span className="legend-item"><span className="legend-dot" style={{ background: STATUS_COLORS.done }} /> 期限内完了</span>
          <span className="legend-item"><span className="legend-dot" style={{ background: STATUS_COLORS.overdue }} /> 期限超過</span>
        </div>
      </section>

      {/* プロジェクト別の進捗状況 */}
      <section className="analytics-section">
        <h2>プロジェクト別の進捗状況</h2>
        {projectData.length === 0 ? (
          <p className="empty-text">プロジェクトがありません</p>
        ) : (
          <div className="project-progress-list">
            {projectData.map((p) => (
              <Link key={p.project_id} to={`/projects/${p.project_id}`} className="project-progress-card">
                <div className="pp-header">
                  <span className="pp-name">{p.project_name}</span>
                  <span className="pp-rate">{p.progress_rate}%</span>
                </div>
                <div className="pp-bar-track">
                  {p.total > 0 && (
                    <>
                      <div className="pp-bar-seg" style={{ width: `${(p.done / p.total) * 100}%`, background: STATUS_COLORS.done }} title={`完了: ${p.done}`} />
                      <div className="pp-bar-seg" style={{ width: `${(p.in_progress / p.total) * 100}%`, background: STATUS_COLORS.in_progress }} title={`進行中: ${p.in_progress}`} />
                      <div className="pp-bar-seg" style={{ width: `${(p.pending / p.total) * 100}%`, background: STATUS_COLORS.pending }} title={`保留: ${p.pending}`} />
                      <div className="pp-bar-seg" style={{ width: `${(p.cancelled / p.total) * 100}%`, background: STATUS_COLORS.cancelled }} title={`キャンセル: ${p.cancelled}`} />
                    </>
                  )}
                </div>
                <div className="pp-stats">
                  <span>全 {p.total}</span>
                  <span style={{ color: STATUS_COLORS.done }}>完了 {p.done}</span>
                  <span style={{ color: STATUS_COLORS.in_progress }}>進行中 {p.in_progress}</span>
                  {p.overdue > 0 && <span style={{ color: STATUS_COLORS.overdue }}>期限切れ {p.overdue}</span>}
                </div>
              </Link>
            ))}
          </div>
        )}
        <div className="project-legend">
          <span className="legend-item"><span className="legend-dot" style={{ background: STATUS_COLORS.done }} /> 完了</span>
          <span className="legend-item"><span className="legend-dot" style={{ background: STATUS_COLORS.in_progress }} /> 進行中</span>
          <span className="legend-item"><span className="legend-dot" style={{ background: STATUS_COLORS.pending }} /> 保留</span>
          <span className="legend-item"><span className="legend-dot" style={{ background: STATUS_COLORS.cancelled }} /> キャンセル</span>
        </div>
      </section>
    </div>
  );
}

// ── 🎯 タスク消化（監視対象） ────────────────────────────
function TaskOpsSection({ members }) {
  const today = new Date().toISOString().slice(0, 10);
  const past30 = new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10);

  const STORAGE_KEY = 'analytics_task_ops_users';
  const [selectedUsers, setSelectedUsers] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved);
    } catch {}
    return [];
  });
  const [from, setFrom] = useState(past30);
  const [to, setTo]     = useState(today);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [showPicker, setShowPicker] = useState(false);
  const [query, setQuery] = useState('');

  const load = (uids, f, t) => {
    if (uids.length === 0) { setData(null); return; }
    setLoading(true);
    api.analyticsTaskOps({ users: uids.join(','), from: f, to: t })
      .then(setData).catch(() => setData(null)).finally(() => setLoading(false));
  };

  useEffect(() => { load(selectedUsers, from, to); }, []); // 初回

  const persistUsers = (uids) => {
    setSelectedUsers(uids);
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(uids)); } catch {}
    load(uids, from, to);
  };
  const addUser = (uid) => {
    if (selectedUsers.includes(uid)) return;
    persistUsers([...selectedUsers, uid]);
  };
  const removeUser = (uid) => persistUsers(selectedUsers.filter(u => u !== uid));

  const filteredMembers = members.filter(m => {
    if (selectedUsers.includes(m.assignee_id)) return false;
    if (!query) return true;
    return (m.displayName || '').toLowerCase().includes(query.toLowerCase());
  });

  // 合計
  const totals = data ? data.users.reduce((acc, u) => ({
    issued: acc.issued + u.issued,
    completed: acc.completed + u.completed,
    on_time: acc.on_time + u.on_time,
    with_due: acc.with_due + u.with_due,
    pending: acc.pending + u.pending,
    pending_overdue: acc.pending_overdue + u.pending_overdue,
  }), { issued: 0, completed: 0, on_time: 0, with_due: 0, pending: 0, pending_overdue: 0 }) : null;
  const totalOnTimeRate = totals && totals.with_due > 0
    ? Math.round((totals.on_time / totals.with_due) * 100) : null;

  // 日別チャート（最大数）
  const dailyMax = data?.daily?.length > 0
    ? Math.max(1, ...data.daily.map(d => Math.max(d.issued, d.done)))
    : 1;

  return (
    <section className="analytics-section" style={{ background: 'var(--surface)', padding: 18, borderRadius: 12, border: '1px solid var(--gray-200)', marginBottom: 24 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 14 }}>
        <h2 style={{ margin: 0, fontSize: 16, fontWeight: 800 }}>🎯 タスク消化（監視対象）</h2>
        <span style={{ fontSize: 11, color: 'var(--gray-500)' }}>
          見たいユーザーを選んで、期限順守率や発行数を確認できます。選択はブラウザに保存されます。
        </span>
      </div>

      {/* 監視対象ユーザー選択 */}
      <div style={{ marginBottom: 14 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 8 }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--gray-700)' }}>監視対象 ({selectedUsers.length}名)</span>
          <button onClick={() => setShowPicker(s => !s)}
            style={{ background: '#2563eb', color: '#fff', border: 'none', padding: '5px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, cursor: 'pointer' }}>
            ＋ 追加
          </button>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
          {selectedUsers.length === 0 && <span style={{ fontSize: 12, color: 'var(--gray-400)' }}>「＋ 追加」から見たいユーザーを選択してください</span>}
          {selectedUsers.map(uid => {
            const m = members.find(x => x.assignee_id === uid);
            return (
              <span key={uid} style={{ display: 'inline-flex', alignItems: 'center', gap: 4, background: '#eff6ff', color: '#1e40af', border: '1px solid #bfdbfe', borderRadius: 999, padding: '3px 10px', fontSize: 12, fontWeight: 600 }}>
                {m?.displayName || uid}
                <button onClick={() => removeUser(uid)} style={{ background: 'none', border: 'none', color: '#1e40af', cursor: 'pointer', padding: 0, fontSize: 14, lineHeight: 1 }}>×</button>
              </span>
            );
          })}
        </div>
        {showPicker && (
          <div style={{ marginTop: 8, padding: 10, background: 'var(--gray-50)', border: '1px solid var(--gray-200)', borderRadius: 8 }}>
            <input value={query} onChange={e => setQuery(e.target.value)} placeholder="名前で検索…"
              style={{ width: '100%', padding: '6px 10px', border: '1px solid var(--gray-300)', borderRadius: 6, fontSize: 13, marginBottom: 8 }} />
            <div style={{ maxHeight: 200, overflowY: 'auto' }}>
              {filteredMembers.slice(0, 50).map(m => (
                <button key={m.assignee_id} onClick={() => { addUser(m.assignee_id); }}
                  style={{ display: 'block', width: '100%', textAlign: 'left', padding: '6px 10px', background: '#fff', border: '1px solid var(--gray-100)', borderRadius: 4, marginBottom: 2, fontSize: 13, cursor: 'pointer' }}>
                  {m.displayName}
                </button>
              ))}
              {filteredMembers.length === 0 && <div style={{ padding: 12, textAlign: 'center', fontSize: 12, color: 'var(--gray-400)' }}>該当ユーザーなし</div>}
            </div>
          </div>
        )}
      </div>

      {/* 期間 */}
      <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 14 }}>
        <label style={{ fontSize: 12, color: 'var(--gray-600)', fontWeight: 600 }}>期間:</label>
        <input type="date" value={from} onChange={e => { setFrom(e.target.value); load(selectedUsers, e.target.value, to); }}
          style={{ padding: '4px 8px', border: '1px solid var(--gray-300)', borderRadius: 6, fontSize: 13 }} />
        <span style={{ fontSize: 12, color: 'var(--gray-500)' }}>〜</span>
        <input type="date" value={to} onChange={e => { setTo(e.target.value); load(selectedUsers, from, e.target.value); }}
          style={{ padding: '4px 8px', border: '1px solid var(--gray-300)', borderRadius: 6, fontSize: 13 }} />
        {loading && <span style={{ fontSize: 12, color: 'var(--gray-500)' }}>更新中…</span>}
      </div>

      {!data || data.users.length === 0 ? (
        <div style={{ padding: 24, textAlign: 'center', color: 'var(--gray-400)', fontSize: 13 }}>
          ユーザーを選択するとデータが表示されます
        </div>
      ) : (
        <>
          {/* サマリカード */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 10, marginBottom: 16 }}>
            <Card label="発行数" value={totals.issued} sub="（依頼者として作成）" color="#2563eb"/>
            <Card label="完了数" value={totals.completed} sub={`期限内 ${totals.on_time} / 遅延 ${totals.with_due - totals.on_time}`} color="#16a34a"/>
            <Card label="期限順守率" value={totalOnTimeRate != null ? `${totalOnTimeRate}%` : '—'} sub={`${totals.on_time} / ${totals.with_due}`} color={totalOnTimeRate >= 80 ? '#16a34a' : totalOnTimeRate >= 60 ? '#d97706' : '#dc2626'}/>
            <Card label="未完了" value={totals.pending} sub={`うち期限切れ ${totals.pending_overdue}`} color="#d97706"/>
          </div>

          {/* ユーザー別テーブル */}
          <div style={{ overflowX: 'auto', marginBottom: 16, background: '#fff', border: '1px solid var(--gray-200)', borderRadius: 10 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead style={{ background: 'var(--gray-50)' }}>
                <tr>
                  {['ユーザー', '発行', '完了', '期限内', '遅延', '順守率', '未完了', '期限切れ'].map(h => (
                    <th key={h} style={{ padding: '8px 12px', textAlign: 'left', fontWeight: 700, color: 'var(--gray-600)', fontSize: 11, borderBottom: '1px solid var(--gray-200)' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.users.map(u => (
                  <tr key={u.user_id} style={{ borderBottom: '1px solid var(--gray-100)' }}>
                    <td style={{ padding: '10px 12px', fontWeight: 700 }}>{u.display_name}</td>
                    <td style={{ padding: '10px 12px' }}>{u.issued}</td>
                    <td style={{ padding: '10px 12px' }}>{u.completed}</td>
                    <td style={{ padding: '10px 12px', color: '#16a34a' }}>{u.on_time}</td>
                    <td style={{ padding: '10px 12px', color: '#dc2626' }}>{u.late}</td>
                    <td style={{ padding: '10px 12px', fontWeight: 700, color: u.on_time_rate >= 80 ? '#16a34a' : u.on_time_rate >= 60 ? '#d97706' : '#dc2626' }}>
                      {u.on_time_rate != null ? `${u.on_time_rate}%` : '—'}
                    </td>
                    <td style={{ padding: '10px 12px' }}>{u.pending}</td>
                    <td style={{ padding: '10px 12px', color: u.pending_overdue > 0 ? '#dc2626' : 'var(--gray-400)' }}>{u.pending_overdue}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* 日別チャート */}
          {data.daily && data.daily.length > 0 && (
            <div style={{ marginBottom: 16, background: '#fff', border: '1px solid var(--gray-200)', borderRadius: 10, padding: 14 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--gray-600)', marginBottom: 8 }}>日別 発行 vs 完了</div>
              <div style={{ display: 'flex', alignItems: 'flex-end', gap: 2, height: 120, borderBottom: '1px solid var(--gray-200)', paddingBottom: 4 }}>
                {data.daily.map((d, i) => (
                  <div key={i} style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 1 }} title={`${d.date}: 発行${d.issued} / 完了${d.done}`}>
                    <div style={{ width: '100%', display: 'flex', gap: 1, alignItems: 'flex-end', height: '100%' }}>
                      <div style={{ flex: 1, background: '#bfdbfe', height: `${(d.issued / dailyMax) * 100}%`, minHeight: d.issued > 0 ? 2 : 0, borderRadius: '2px 2px 0 0' }} />
                      <div style={{ flex: 1, background: '#86efac', height: `${(d.done / dailyMax) * 100}%`, minHeight: d.done > 0 ? 2 : 0, borderRadius: '2px 2px 0 0' }} />
                    </div>
                  </div>
                ))}
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: 'var(--gray-400)', marginTop: 4 }}>
                <span>{data.from}</span>
                <span>{data.to}</span>
              </div>
              <div style={{ display: 'flex', gap: 12, marginTop: 6, fontSize: 11, color: 'var(--gray-600)' }}>
                <span><span style={{ display: 'inline-block', width: 10, height: 10, background: '#bfdbfe', borderRadius: 2, marginRight: 4 }}/>発行</span>
                <span><span style={{ display: 'inline-block', width: 10, height: 10, background: '#86efac', borderRadius: 2, marginRight: 4 }}/>完了</span>
              </div>
            </div>
          )}

          {/* 完了タイムライン */}
          {data.timeline && data.timeline.length > 0 && (
            <div style={{ background: '#fff', border: '1px solid var(--gray-200)', borderRadius: 10, padding: 14 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--gray-600)', marginBottom: 10 }}>
                完了タイムライン（最新 {data.timeline.length} 件）
              </div>
              <div style={{ maxHeight: 400, overflowY: 'auto' }}>
                {data.timeline.map(t => (
                  <div key={t.task_id} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 0', borderBottom: '1px solid var(--gray-100)', fontSize: 13 }}>
                    <span style={{ width: 18, color: t.on_time ? '#16a34a' : '#dc2626' }}>
                      {t.on_time ? '✅' : '⚠️'}
                    </span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontWeight: 600, color: 'var(--gray-800)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {t.title || '（タイトルなし）'}
                      </div>
                      <div style={{ fontSize: 11, color: 'var(--gray-500)' }}>
                        {t.assignee_name} ← {t.requester_name}
                        {t.due_date && `　・　期限: ${String(t.due_date).slice(0, 10)}`}
                      </div>
                    </div>
                    <span style={{ fontSize: 11, color: 'var(--gray-500)' }}>
                      {new Date(t.completed_at).toLocaleString('ja-JP', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </section>
  );
}

function Card({ label, value, sub, color }) {
  return (
    <div style={{ background: '#fff', border: '1px solid var(--gray-200)', borderTop: `3px solid ${color}`, borderRadius: 10, padding: '12px 14px' }}>
      <div style={{ fontSize: 11, color: 'var(--gray-500)', fontWeight: 600, marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 800, color: 'var(--gray-800)', lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ fontSize: 10, color: 'var(--gray-400)', marginTop: 4 }}>{sub}</div>}
    </div>
  );
}
