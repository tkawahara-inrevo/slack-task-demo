import { useEffect, useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { api } from '../../api/client';

function formatYen(n) {
  if (n == null || isNaN(n)) return '—';
  return n.toLocaleString('ja-JP') + '円';
}

const PHASES = [
  { key: 'cr',    label: 'CR',    desc: '初回インタビュー', color: '#8b5cf6' },
  { key: 'st_an', label: 'ST/AN', desc: '分析・媒体調査',   color: '#f59e0b' },
  { key: 'dr',    label: 'DR',    desc: '最終判断・承認',   color: '#ef4444' },
  { key: 'cs_op', label: 'CS/OP', desc: '採用活動中',       color: '#3b82f6' },
];

const STALL_DAYS = 7;

function daysSince(iso) {
  if (!iso) return 0;
  return Math.floor((Date.now() - new Date(iso)) / 86400000);
}

export default function RpoSummary() {
  const navigate = useNavigate();
  const location = useLocation();
  const contextTeamId = location.state?.dashTeamId ?? undefined;
  const isAllTeamsContext = contextTeamId === null;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [teams, setTeams] = useState([]);
  const [filterTeamId, setFilterTeamId] = useState('');

  useEffect(() => {
    if (!isAllTeamsContext) return;
    api.rpoTeams().catch(() => ({ teams: [] })).then(r => setTeams(r.teams || []));
  }, []);

  useEffect(() => {
    setLoading(true);
    const dashTeamId = isAllTeamsContext ? (filterTeamId || undefined) : (contextTeamId || undefined);
    const params = dashTeamId ? { dashTeamId } : {};
    api.rpoSummary(params)
      .then(r => { setRows(r.rows || []); setLoading(false); })
      .catch(e => { setError(e.message); setLoading(false); });
  }, [filterTeamId]);

  const active = rows.filter(r => r.status === 'active');
  const archived = rows.filter(r => r.status === 'archived');

  const totals = active.reduce((acc, r) => {
    acc.contract   += Number(r.contract_amount) || 0;
    acc.mediaSpent += Number(r.media_spent)     || 0;
    acc.accepted   += Number(r.accepted_count)  || 0;
    return acc;
  }, { contract: 0, mediaSpent: 0, accepted: 0 });
  totals.grossProfit = totals.contract - totals.mediaSpent;
  totals.costPerHire = totals.accepted > 0 ? Math.round(totals.mediaSpent / totals.accepted) : null;

  // 滞留案件（STALL_DAYS日以上フェーズが変わっていない進行中）
  const stalled = active.filter(r => daysSince(r.updated_at) >= STALL_DAYS);

  // 担当者別集計
  const byAssignee = active.reduce((acc, r) => {
    const name = r.hr_assignee_name?.split('/')?.[0]?.trim() || '未割当';
    if (!acc[name]) acc[name] = { name, total: 0, byPhase: {} };
    acc[name].total++;
    const ph = r.phase || 'cr';
    acc[name].byPhase[ph] = (acc[name].byPhase[ph] || 0) + 1;
    return acc;
  }, {});
  const assigneeRows = Object.values(byAssignee).sort((a, b) => b.total - a.total);

  if (loading) return <div style={{ padding: '40px', textAlign: 'center' }}>読み込み中…</div>;
  if (error)   return <div style={{ padding: '40px', color: '#ef4444' }}>エラー: {error}</div>;

  const S = {
    page:    { padding: '20px 24px', background: 'var(--gray-50)', minHeight: '100%' },
    section: { background: 'var(--surface)', borderRadius: 10, border: '1px solid var(--gray-200)', padding: '16px 20px', marginBottom: 16 },
    h2:      { fontWeight: 700, fontSize: '0.88rem', color: 'var(--gray-700)', marginBottom: 12 },
    kpi:     { display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 },
    kpiCard: { background: 'var(--surface-2)', borderRadius: 8, padding: '10px 16px', minWidth: 120, flex: '1 1 120px' },
  };

  return (
    <div style={S.page}>
      {/* ヘッダー */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20, flexWrap: 'wrap' }}>
        <button className="btn-secondary small" onClick={() => navigate('/rpo')}>← 案件一覧</button>
        <h1 style={{ fontWeight: 700, fontSize: '1.1rem', color: 'var(--gray-800)', margin: 0 }}>RPO 上長向けサマリー</h1>
        {isAllTeamsContext && teams.length > 0 && (
          <select value={filterTeamId} onChange={e => setFilterTeamId(e.target.value)}
            style={{ marginLeft: 'auto', padding: '6px 10px', border: '1px solid var(--gray-300)', borderRadius: 6, fontSize: '0.875rem', background: 'var(--surface)' }}>
            <option value="">全チーム</option>
            {teams.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        )}
      </div>

      {/* KPIカード */}
      <div style={S.kpi}>
        {[
          { label: '進行中案件', value: `${active.length}件` },
          { label: '受注金額合計', value: formatYen(totals.contract) },
          { label: '媒体費合計',  value: totals.mediaSpent > 0 ? `▲${formatYen(totals.mediaSpent)}` : '—', red: true },
          { label: '粗利合計',    value: totals.contract > 0 ? formatYen(totals.grossProfit) : '—', green: totals.grossProfit >= 0 },
          { label: '内定承諾合計', value: `${totals.accepted}人` },
          { label: '平均採用単価', value: totals.costPerHire ? formatYen(totals.costPerHire) : '—' },
        ].map(k => (
          <div key={k.label} style={S.kpiCard}>
            <div style={{ fontSize: '0.72rem', color: 'var(--gray-500)', marginBottom: 4 }}>{k.label}</div>
            <div style={{ fontWeight: 700, fontSize: '1rem', color: k.red ? '#ef4444' : k.green ? '#10b981' : 'var(--gray-800)' }}>{k.value}</div>
          </div>
        ))}
      </div>

      {/* フェーズパイプライン */}
      <div style={S.section}>
        <div style={S.h2}>フェーズ別パイプライン</div>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
          {PHASES.map(ph => {
            const count = active.filter(r => (r.phase || 'cr') === ph.key).length;
            return (
              <div key={ph.key} style={{ flex: '1 1 120px', borderRadius: 8, overflow: 'hidden', border: `1px solid ${ph.color}30` }}>
                <div style={{ background: ph.color, color: '#fff', padding: '6px 12px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span style={{ fontWeight: 700, fontSize: '0.82rem' }}>{ph.label}</span>
                  <span style={{ fontWeight: 700, fontSize: '1.1rem' }}>{count}</span>
                </div>
                <div style={{ padding: '6px 12px', background: 'var(--surface-2)', fontSize: '0.72rem', color: 'var(--gray-500)' }}>{ph.desc}</div>
                <div style={{ padding: '4px 12px 8px', display: 'flex', flexDirection: 'column', gap: 2 }}>
                  {active.filter(r => (r.phase || 'cr') === ph.key).map(r => (
                    <div key={r.id} onClick={() => navigate(`/rpo/${r.id}`)}
                      style={{ fontSize: '0.75rem', color: 'var(--gray-700)', cursor: 'pointer', padding: '2px 0', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                      title={r.name}>
                      · {r.name}
                    </div>
                  ))}
                  {count === 0 && <div style={{ fontSize: '0.72rem', color: 'var(--gray-400)', padding: '4px 0' }}>なし</div>}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* 滞留アラート */}
      {stalled.length > 0 && (
        <div style={{ ...S.section, borderColor: '#fca5a5', background: '#fef2f2' }}>
          <div style={{ ...S.h2, color: '#dc2626' }}>⚠ 滞留案件（{STALL_DAYS}日以上更新なし）</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {stalled.map(r => {
              const days = daysSince(r.updated_at);
              const ph = PHASES.find(p => p.key === (r.phase || 'cr'));
              return (
                <div key={r.id} onClick={() => navigate(`/rpo/${r.id}`)}
                  style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 12px', background: 'var(--surface)', borderRadius: 6, cursor: 'pointer', border: '1px solid #fca5a5' }}>
                  <span style={{ width: 8, height: 8, borderRadius: '50%', background: colorFor(r.color), flexShrink: 0 }} />
                  <span style={{ fontWeight: 600, fontSize: '0.84rem', flex: 1 }}>{r.name}</span>
                  <span style={{ fontSize: '0.72rem', background: ph?.color || '#6b7280', color: '#fff', borderRadius: 4, padding: '1px 6px' }}>{ph?.label || r.phase}</span>
                  <span style={{ fontSize: '0.72rem', color: '#dc2626', fontWeight: 700 }}>{days}日経過</span>
                  {r.hr_assignee_name && <span style={{ fontSize: '0.72rem', color: 'var(--gray-500)' }}>{r.hr_assignee_name.split('/')[0].trim()}</span>}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* 担当者別集計 */}
      {assigneeRows.length > 0 && (
        <div style={S.section}>
          <div style={S.h2}>担当者別案件数</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {assigneeRows.map(a => (
              <div key={a.name} style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                <span style={{ fontWeight: 600, fontSize: '0.84rem', minWidth: 120, color: 'var(--gray-700)' }}>{a.name}</span>
                <span style={{ fontSize: '0.78rem', color: 'var(--gray-500)', marginRight: 8 }}>計{a.total}件</span>
                {PHASES.map(ph => {
                  const cnt = a.byPhase[ph.key] || 0;
                  if (!cnt) return null;
                  return (
                    <span key={ph.key} style={{ fontSize: '0.72rem', background: ph.color + '20', color: ph.color, border: `1px solid ${ph.color}50`, borderRadius: 4, padding: '1px 8px', fontWeight: 600 }}>
                      {ph.label}: {cnt}
                    </span>
                  );
                })}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 案件テーブル（進行中） */}
      <div style={S.section}>
        <div style={S.h2}>進行中案件一覧</div>
        <div style={{ overflowX: 'auto' }}>
          <table className="rpo-summary-table">
            <thead>
              <tr>
                <th>案件名</th>
                <th>フェーズ</th>
                <th>HR担当</th>
                <th>チーム</th>
                <th>受注金額</th>
                <th>媒体費</th>
                <th>粗利</th>
                <th>内定承諾</th>
                <th>応募者</th>
                <th>最終更新</th>
              </tr>
            </thead>
            <tbody>
              {active.map(r => {
                const contract = Number(r.contract_amount) || 0;
                const spent    = Number(r.media_spent)     || 0;
                const profit   = contract - spent;
                const accepted = Number(r.accepted_count)  || 0;
                const ph       = PHASES.find(p => p.key === (r.phase || 'cr'));
                const days     = daysSince(r.updated_at);
                return (
                  <tr key={r.id} className="rpo-summary-row" onClick={() => navigate(`/rpo/${r.id}`)}>
                    <td>
                      <span className="rpo-summary-dot" style={{ background: colorFor(r.color) }} />
                      {r.name}
                    </td>
                    <td>
                      <span style={{ fontSize: '0.72rem', background: ph?.color || '#6b7280', color: '#fff', borderRadius: 4, padding: '1px 6px', fontWeight: 600 }}>{ph?.label || r.phase}</span>
                    </td>
                    <td style={{ fontSize: '0.82rem' }}>{r.hr_assignee_name ? r.hr_assignee_name.split('/')[0].trim() : '—'}</td>
                    <td>{r.dash_team_name || '—'}</td>
                    <td className="num">{contract > 0 ? formatYen(contract) : '—'}</td>
                    <td className="num" style={{ color: spent > 0 ? '#ef4444' : undefined }}>{spent > 0 ? `▲${formatYen(spent)}` : '—'}</td>
                    <td className="num" style={{ color: contract > 0 ? (profit >= 0 ? '#10b981' : '#ef4444') : '#9ca3af' }}>{contract > 0 ? formatYen(profit) : '—'}</td>
                    <td className="num">{accepted}人</td>
                    <td className="num">{Number(r.total_applicants) || 0}人</td>
                    <td style={{ fontSize: '0.75rem', color: days >= STALL_DAYS ? '#dc2626' : 'var(--gray-500)', fontWeight: days >= STALL_DAYS ? 700 : 400 }}>
                      {days === 0 ? '今日' : `${days}日前`}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* 終了済み案件（折りたたみ） */}
      {archived.length > 0 && (
        <details style={S.section}>
          <summary style={{ cursor: 'pointer', fontWeight: 600, fontSize: '0.84rem', color: 'var(--gray-600)' }}>
            終了済み案件 ({archived.length}件)
          </summary>
          <div style={{ overflowX: 'auto', marginTop: 12 }}>
            <table className="rpo-summary-table">
              <thead>
                <tr><th>案件名</th><th>チーム</th><th>受注金額</th><th>内定承諾</th></tr>
              </thead>
              <tbody>
                {archived.map(r => (
                  <tr key={r.id} className="rpo-summary-row" onClick={() => navigate(`/rpo/${r.id}`)}>
                    <td><span className="rpo-summary-dot" style={{ background: colorFor(r.color) }} />{r.name}</td>
                    <td>{r.dash_team_name || '—'}</td>
                    <td className="num">{Number(r.contract_amount) > 0 ? formatYen(Number(r.contract_amount)) : '—'}</td>
                    <td className="num">{Number(r.accepted_count) || 0}人</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      )}
    </div>
  );
}

const COLOR_MAP = { Ocean: '#3b82f6', Emerald: '#10b981', Amber: '#f59e0b', Rose: '#ef4444', Violet: '#8b5cf6', Pink: '#ec4899', Teal: '#14b8a6', Slate: '#64748b' };
function colorFor(c) { return COLOR_MAP[c] || '#6b7280'; }
