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
