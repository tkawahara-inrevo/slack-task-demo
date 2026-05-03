import { useEffect, useRef, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { api } from '../../api/client';
import { FunnelTab } from './FunnelTab';
import ApplicantTab from './ApplicantTab';

const TABS = [
  { id: 'dashboard',  label: 'ダッシュボード' },
  { id: 'kpi',        label: 'KPI' },
  { id: 'content',    label: '媒体・予算管理' },
  { id: 'applicants', label: '応募者' },
  { id: 'tasks',      label: 'タスク' },
  { id: 'documents',  label: '書類' },
  { id: 'funnel',     label: '歩留まり' },
];

const COLOR_MAP = {
  Ocean:   '#3b82f6',
  Emerald: '#10b981',
  Amber:   '#f59e0b',
  Rose:    '#ef4444',
  Violet:  '#8b5cf6',
  Pink:    '#ec4899',
  Teal:    '#14b8a6',
  Slate:   '#64748b',
};
const COLOR_OPTIONS = Object.entries(COLOR_MAP).map(([name, bg]) => ({ name, bg }));

export default function ClientDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [client, setClient] = useState(null);
  const [activeTab, setActiveTab] = useState('dashboard');
  const [mountedTabs, setMountedTabs] = useState(new Set(['dashboard']));
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [teamUsers, setTeamUsers] = useState([]);
  const [showColorPicker, setShowColorPicker] = useState(false);

  useEffect(() => {
    api.rpoClient(id)
      .then(async r => {
        setClient(r.client);
        // dash_team_id がセットされていればそのチーム、なければ自分のチームにフォールバック
        const teamId = r.client?.dash_team_id || await api.rpoAccess()
          .then(a => a.myTeams?.[0]?.id)
          .catch(() => null);
        if (teamId) {
          api.workloadUsers(teamId)
            .then(u => setTeamUsers((u.members || u.users || []).map(m => ({
              userId: m.user_id || m.userId,
              displayName: m.display_name || m.displayName,
            }))))
            .catch(() => {});
        }
      })
      .catch(() => navigate('/rpo'))
      .finally(() => setLoading(false));
  }, [id]);

  const updateData = async (patch) => {
    if (!client) return;
    const newData = { ...client.data, ...patch };
    const newClient = { ...client, data: newData };
    setClient(newClient); // 楽観的更新
    setSaving(true);
    try {
      await api.rpoUpdateClient(id, { data: newData });
    } catch (e) {
      alert('保存に失敗しました');
      // ロールバック
      setClient(client);
    } finally {
      setSaving(false);
    }
  };

  useEffect(() => {
    if (!showColorPicker) return;
    const close = () => setShowColorPicker(false);
    document.addEventListener('mousedown', close);
    return () => document.removeEventListener('mousedown', close);
  }, [showColorPicker]);

  const changeColor = async (colorName) => {
    setClient(prev => ({ ...prev, color: colorName }));
    setShowColorPicker(false);
    await api.rpoUpdateClient(id, { color: colorName }).catch(() => {});
  };

  const accentColor = COLOR_MAP[client?.color] || '#3b82f6';

  if (loading) return <div className="page-loading">読み込み中...</div>;
  if (!client) return null;

  return (
    <div className="rpo-detail-page">
      {/* ヘッダー */}
      <div className="rpo-detail-header" style={{ borderLeft: `4px solid ${accentColor}` }}>
        <button className="btn-back" onClick={() => navigate('/rpo')}>← 案件一覧</button>
        <div className="rpo-detail-title-row">
          <div style={{ position: 'relative' }}>
            <div
              className="rpo-detail-avatar"
              style={{ background: accentColor, cursor: 'pointer' }}
              title="カラーを変更"
              onClick={() => setShowColorPicker(v => !v)}
            >
              {client.name.charAt(0)}
            </div>
            {showColorPicker && (
              <div
                style={{ position: 'absolute', top: '100%', left: 0, zIndex: 200, background: 'white', border: '1px solid #e5e7eb', borderRadius: '10px', boxShadow: '0 4px 16px rgba(0,0,0,0.12)', padding: '10px', display: 'flex', gap: '8px', flexWrap: 'wrap', width: '160px', marginTop: '6px' }}
                onClick={e => e.stopPropagation()}
              >
                {COLOR_OPTIONS.map(c => (
                  <button
                    key={c.name}
                    onClick={() => changeColor(c.name)}
                    style={{ width: '28px', height: '28px', borderRadius: '50%', background: c.bg, border: client.color === c.name ? '3px solid #1f2937' : '2px solid transparent', cursor: 'pointer', outline: 'none' }}
                    title={c.name}
                  />
                ))}
              </div>
            )}
          </div>
          <div>
            <h1 className="rpo-detail-name">{client.name}</h1>
            <div className="rpo-detail-meta">
              {planLabel(client.plan)}
              {saving && <span className="saving-indicator"> 保存中...</span>}
            </div>
          </div>
        </div>

        {/* タブ */}
        <div className="rpo-tabs">
          {TABS.map(tab => (
            <button
              key={tab.id}
              className={`rpo-tab ${activeTab === tab.id ? 'active' : ''}`}
              style={activeTab === tab.id ? { borderBottom: `2px solid ${accentColor}`, color: accentColor } : {}}
              onClick={() => { setActiveTab(tab.id); setMountedTabs(prev => new Set([...prev, tab.id])); }}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* タブコンテンツ：一度マウントしたらアンマウントせずCSS hide */}
      <div className="rpo-detail-content">
        {mountedTabs.has('dashboard')  && <div style={{ display: activeTab === 'dashboard'  ? '' : 'none' }}><DashboardTab  client={client} onUpdate={updateData} accentColor={accentColor} teamUsers={teamUsers} /></div>}
        {mountedTabs.has('kpi')        && <div style={{ display: activeTab === 'kpi'        ? '' : 'none' }}><KpiTab        client={client} onUpdate={updateData} accentColor={accentColor} /></div>}
        {mountedTabs.has('content')    && <div style={{ display: activeTab === 'content'    ? '' : 'none' }}><ContentTab    client={client} onUpdate={updateData} accentColor={accentColor} /></div>}
        {mountedTabs.has('applicants') && <div style={{ display: activeTab === 'applicants' ? '' : 'none' }}><SheetsApplicantTab client={client} onUpdate={updateData} /></div>}
        {mountedTabs.has('tasks')      && <div style={{ display: activeTab === 'tasks'      ? '' : 'none' }}><TasksTab      client={client} /></div>}
        {mountedTabs.has('documents')  && <div style={{ display: activeTab === 'documents'  ? '' : 'none' }}><DocumentsTab  client={client} onUpdate={updateData} /></div>}
        {mountedTabs.has('funnel')     && <div style={{ display: activeTab === 'funnel'     ? '' : 'none' }}><FunnelTab     client={client} onUpdate={updateData} /></div>}
      </div>
    </div>
  );
}

// ─── チャートコンポーネント ──────────────────────────
function SimpleBarChart({ data, color }) {
  if (!data.length) return null;
  const maxVal = Math.max(...data.map(d => d.value), 1);
  const H = 160, barW = 36, gap = 24;
  const totalW = data.length * (barW + gap);
  return (
    <div style={{ overflowX: 'auto' }}>
      <svg width={totalW + 40} height={H + 50} style={{ overflow: 'visible' }}>
        {[0, 0.5, 1].map((r, i) => (
          <line key={i} x1="0" y1={H * (1 - r)} x2={totalW + 40} y2={H * (1 - r)}
            stroke="#e5e7eb" strokeDasharray="4 4" />
        ))}
        {data.map((d, i) => {
          const bh = Math.max(2, (d.value / maxVal) * H);
          const x = i * (barW + gap) + 20;
          return (
            <g key={i}>
              <rect x={x} y={H - bh} width={barW} height={bh} rx="4" fill={color} opacity="0.85" />
              <text x={x + barW / 2} y={H - bh - 6} textAnchor="middle"
                style={{ fontSize: 10, fontWeight: 700, fill: '#6b7280' }}>{d.value}</text>
              <foreignObject x={x - 5} y={H + 8} width={barW + 10} height={36}>
                <div style={{ fontSize: 10, textAlign: 'center', color: '#6b7280', lineHeight: 1.3 }}>{d.label}</div>
              </foreignObject>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function SimpleFunnelChart({ data }) {
  if (!data.length) return null;
  const maxVal = data[0].value || 1;
  const H = 200, W = 280;
  const colors = ['#6366f1','#8b5cf6','#a78bfa','#c4b5fd','#ddd6fe','#ede9fe','#f5f3ff'];
  return (
    <div style={{ display: 'flex', justifyContent: 'center' }}>
      <svg width={W} height={H} style={{ overflow: 'visible' }}>
        {data.map((d, i) => {
          const prev = i === 0 ? maxVal : data[i - 1].value;
          const w1 = (prev / maxVal) * W;
          const w2 = (d.value / maxVal) * W;
          const h  = H / data.length;
          const y  = i * h;
          const x1 = (W - w1) / 2, x2 = (W - w2) / 2;
          const pts = i === 0
            ? `${x2},${y + h} ${x2 + w2},${y + h} ${x2 + w2},${y} ${x2},${y}`
            : `${x2},${y + h} ${x2 + w2},${y + h} ${x1 + w1},${y} ${x1},${y}`;
          return (
            <g key={i}>
              <polygon points={pts} fill={colors[i % colors.length]} opacity="0.85" />
              <text x={W / 2} y={y + h / 2 + 5} textAnchor="middle"
                style={{ fontSize: 11, fontWeight: 700, fill: 'white' }}>
                {d.label}: {d.value}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

// ─── ダッシュボード ───────────────────────────────────
function DashboardTab({ client, onUpdate, accentColor, teamUsers = [] }) {
  const d    = client.data;
  const info = d.projectInfo || {};
  const kpi  = d.kpiData     || {};
  const allMedia = d.mediaStatus || [];
  const media = allMedia.filter(m => m.hiredCount > 0 || m.mediaCost > 0);

  const [acceptedCount, setAcceptedCount] = useState(0);
  useEffect(() => {
    api.rpoApplicants(client.id)
      .then(r => {
        const counts = (r.counts || []).reduce((m, c) => { m[c.status] = c.count; return m; }, {});
        setAcceptedCount(counts['内定承諾'] || 0);
      })
      .catch(() => {});
  }, [client.id]);

  const [kintoneResults, setKintoneResults] = useState(null); // null=未検索, []+=結果
  const [kintoneLoading, setKintoneLoading] = useState(false);
  const fetchKintone = async () => {
    setKintoneLoading(true);
    try {
      const r = await api.kintoneSearch(client.name);
      setKintoneResults(r.results || []);
    } catch { setKintoneResults([]); }
    finally { setKintoneLoading(false); }
  };
  const applyKintone = (rec) => {
    const amount  = Number(rec.data?.['見込売り上げ_税抜き']) || 0;
    const target  = Number(rec.data?.['数値_0']) || 0;
    onUpdate({ projectInfo: { ...info, contractAmount: amount, hiringTarget: target } });
    setKintoneResults(null);
  };

  // 予算計算
  const totalBudget    = Number(info.totalBudget)    || 0;
  const contractAmount = Number(info.contractAmount) || 0;
  const totalSpent  = allMedia.reduce((s, m) => s + (Number(m.mediaCost) || 0), 0);
  const remaining   = totalBudget - totalSpent;
  const usagePct    = totalBudget > 0 ? Math.min(100, (totalSpent / totalBudget) * 100) : 0;
  const budgetStatus = usagePct >= 100 ? 'danger' : usagePct >= 80 ? 'warning' : 'ok';
  const budgetStatusColor = { ok: '#10b981', warning: '#f59e0b', danger: '#ef4444' }[budgetStatus];

  // 売上計算（シンプル）
  const grossProfit  = contractAmount - totalSpent;

  // 期間別売り上げ計算（掲載期間順にソート）
  const hiringTarget = Number(info.hiringTarget) || 0;
  const revenuePerHead = hiringTarget > 0 ? contractAmount / hiringTarget : 0;
  const sortedMedia = [...allMedia]
    .filter(m => Number(m.mediaCost) > 0 || m.periodStart)
    .sort((a, b) => {
      if (!a.periodStart && !b.periodStart) return 0;
      if (!a.periodStart) return 1;
      if (!b.periodStart) return -1;
      return a.periodStart < b.periodStart ? -1 : 1;
    });
  let accCostPerHead = 0;
  let prevHired = 0;
  const revenuePeriods = sortedMedia.map(m => {
    const remaining = Math.max(1, hiringTarget - prevHired);
    const cost      = Number(m.mediaCost) || 0;
    const cpHead    = cost / remaining;
    accCostPerHead += cpHead;
    const rev = revenuePerHead - accCostPerHead;
    prevHired += Number(m.hiredCount) || 0;
    return { ...m, remaining, cpHead: Math.round(cpHead), accCost: Math.round(accCostPerHead), revenuePerHire: Math.round(rev) };
  });
  const latestRevenue = revenuePeriods.length > 0 ? revenuePeriods[revenuePeriods.length - 1].revenuePerHire : null;

  // KPIファネル用データ（値が0より大きいフェーズのみ）
  const funnelData = DEFAULT_PHASES
    .map(p => ({ label: p.label, value: kpi[p.id] || 0 }))
    .filter(d => d.value > 0);

  // 媒体別採用数チャート
  const mediaBarData = media.map(m => ({ label: m.name, value: m.hiredCount || 0 }))
    .filter(d => d.value > 0);

  return (
    <div className="tab-section">
      <div className="section-header">
        <h2 className="section-title">プロジェクト概要</h2>
        <button className="btn-secondary small" onClick={fetchKintone} disabled={kintoneLoading}
          style={{ fontSize: '0.78rem' }}>
          {kintoneLoading ? '検索中…' : '↻ kintoneと再同期'}
        </button>
      </div>

      {/* kintone検索結果 */}
      {kintoneResults !== null && (
        <div style={{ marginBottom: '12px', border: '1px solid #e5e7eb', borderRadius: '8px', overflow: 'hidden' }}>
          {kintoneResults.length === 0 ? (
            <p style={{ padding: '10px 14px', fontSize: '0.85rem', color: '#6b7280' }}>一致するkintoneレコードがありません</p>
          ) : kintoneResults.map(rec => (
            <div key={rec.record_id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '8px 14px', borderBottom: '1px solid #f3f4f6' }}>
              <div style={{ fontSize: '0.85rem' }}>
                <strong>{rec.company_name}</strong>
                <span style={{ marginLeft: '12px', color: '#6b7280' }}>
                  受注: {rec.data?.['見込売り上げ_税抜き'] ? Number(rec.data['見込売り上げ_税抜き']).toLocaleString() + '円' : '—'}
                  　採用: {rec.data?.['数値_0'] || '—'}名
                </span>
              </div>
              <button className="btn-primary small" onClick={() => applyKintone(rec)}>反映</button>
            </div>
          ))}
        </div>
      )}

      <div className="info-grid">
        <div className="info-field">
          <label className="info-label">HR担当者</label>
          {teamUsers.length > 0 ? (
            <select
              value={d.hrAssigneeId || ''}
              onChange={e => {
                const u = teamUsers.find(u => u.userId === e.target.value);
                onUpdate({ hrAssigneeId: u?.userId || '', hrAssigneeName: u?.displayName || '' });
              }}
              style={{ padding: '7px 10px', border: '1px solid #e5e7eb', borderRadius: '6px', fontSize: '0.875rem', background: 'white' }}
            >
              <option value="">未設定</option>
              {teamUsers.map(u => (
                <option key={u.userId} value={u.userId}>{u.displayName}</option>
              ))}
            </select>
          ) : (
            <div className="info-value" style={{ color: '#9ca3af', background: '#f9fafb', cursor: 'default' }}>
              {d.hrAssigneeName || '未設定'}
            </div>
          )}
        </div>
        <InfoField label="営業担当者" value={info.inrevoContact || '未設定'}
          onChange={v => onUpdate({ projectInfo: { ...info, inrevoContact: v } })} />
        <InfoField label="クライアント担当" value={info.clientContact || '未設定'}
          onChange={v => onUpdate({ projectInfo: { ...info, clientContact: v } })} />
        <InfoField label="採用目標人数" type="number" value={info.hiringTarget || ''}
          onChange={v => onUpdate({ projectInfo: { ...info, hiringTarget: Number(v) } })} />
        <div className="info-field">
          <label className="info-label">受注金額（円）</label>
          <div className="info-value" style={{ color: contractAmount > 0 ? '#111827' : '#9ca3af', cursor: 'default', background: '#f9fafb' }}>
            {contractAmount > 0 ? contractAmount.toLocaleString('ja-JP') + ' 円' : '—'}
          </div>
        </div>
        <InfoField label="メモ" multiline value={info.memo || ''}
          onChange={v => onUpdate({ projectInfo: { ...info, memo: v } })} />
      </div>

      {/* 予算管理 */}
      <h2 className="section-title" style={{ marginTop: '28px' }}>予算管理</h2>
      <div className="budget-section">
        <div className="budget-input-row">
          <label className="budget-input-label">総予算</label>
          <InfoField
            label=""
            type="number"
            value={info.totalBudget || ''}
            onChange={v => onUpdate({ projectInfo: { ...info, totalBudget: Number(v) || 0 } })}
          />
          <span className="budget-unit">円</span>
        </div>

        {totalBudget > 0 && (
          <div className="budget-summary-card">
            <div className="budget-bar-wrap">
              <div className="budget-bar-track">
                <div
                  className="budget-bar-fill"
                  style={{ width: `${usagePct}%`, background: budgetStatusColor }}
                />
              </div>
              <span className="budget-bar-pct" style={{ color: budgetStatusColor }}>
                {usagePct.toFixed(1)}%
              </span>
            </div>
            <div className="budget-numbers">
              <div className="budget-number-item">
                <span className="budget-number-label">使用済み</span>
                <span className="budget-number-value" style={{ color: budgetStatusColor }}>
                  {formatYen(totalSpent)}
                </span>
              </div>
              <div className="budget-number-item">
                <span className="budget-number-label">残額</span>
                <span className="budget-number-value" style={{ color: remaining >= 0 ? '#10b981' : '#ef4444' }}>
                  {formatYen(remaining)}
                </span>
              </div>
              <div className="budget-number-item">
                <span className="budget-number-label">総予算</span>
                <span className="budget-number-value">{formatYen(totalBudget)}</span>
              </div>
            </div>
            {allMedia.filter(m => Number(m.mediaCost) > 0).length > 0 && (
              <div className="budget-media-breakdown">
                {allMedia.filter(m => Number(m.mediaCost) > 0).map(m => (
                  <div key={m.id} className="budget-media-row">
                    <span className="budget-media-name">{m.name}</span>
                    <div className="budget-media-bar-wrap">
                      <div className="budget-media-bar-track">
                        <div
                          className="budget-media-bar-fill"
                          style={{
                            width: `${Math.min(100, (Number(m.mediaCost) / totalBudget) * 100)}%`,
                            background: accentColor,
                          }}
                        />
                      </div>
                    </div>
                    <span className="budget-media-cost">{formatYen(Number(m.mediaCost))}</span>
                    <span className="budget-media-pct">
                      ({((Number(m.mediaCost) / totalBudget) * 100).toFixed(1)}%)
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
        {totalBudget === 0 && (
          <p className="empty-hint">「総予算」を入力すると使用率・残額が表示されます。媒体コストは「求人・媒体」タブで入力してください。</p>
        )}
      </div>

      {/* 売上・収益 */}
      <h2 className="section-title" style={{ marginTop: '28px' }}>売上・収益</h2>
      <div className="revenue-cards">
        <div className="revenue-card">
          <span className="revenue-card-label">受注金額</span>
          <span className="revenue-card-value">{contractAmount > 0 ? formatYen(contractAmount) : '—'}</span>
        </div>
        <div className="revenue-card">
          <span className="revenue-card-label">媒体費合計</span>
          <span className="revenue-card-value" style={{ color: '#ef4444' }}>
            {totalSpent > 0 ? `▲${formatYen(totalSpent)}` : '—'}
          </span>
        </div>
        <div className="revenue-card" style={{ borderColor: grossProfit >= 0 ? '#10b981' : '#ef4444' }}>
          <span className="revenue-card-label">粗利</span>
          <span className="revenue-card-value" style={{ color: contractAmount > 0 ? (grossProfit >= 0 ? '#10b981' : '#ef4444') : '#9ca3af' }}>
            {contractAmount > 0 ? formatYen(grossProfit) : '—'}
          </span>
        </div>
        <div className="revenue-card">
          <span className="revenue-card-label">内定承諾者数</span>
          <span className="revenue-card-value">{acceptedCount}人</span>
        </div>
        <div className="revenue-card" style={{ borderColor: latestRevenue !== null && latestRevenue >= 0 ? '#10b981' : '#e5e7eb' }}>
          <span className="revenue-card-label">1名あたり売り上げ（最新）</span>
          <span className="revenue-card-value" style={{ color: latestRevenue !== null ? (latestRevenue >= 0 ? '#10b981' : '#ef4444') : '#9ca3af' }}>
            {latestRevenue !== null ? formatYen(latestRevenue) : '—'}
          </span>
        </div>
      </div>

      {/* 期間別売り上げ内訳 */}
      {revenuePeriods.length > 0 && contractAmount > 0 && hiringTarget > 0 && (
        <div style={{ marginTop: '20px' }}>
          <h3 className="chart-title" style={{ marginBottom: '10px' }}>期間別売り上げ内訳</h3>
          <p style={{ fontSize: '0.75rem', color: '#9ca3af', marginBottom: '10px' }}>
            計算式: (受注金額 ÷ 採用予定数) ー (各期間コスト ÷ 残り採用予定数) の累積
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table className="rev-period-table">
              <thead>
                <tr>
                  <th>媒体</th><th>掲載期間</th><th>費用</th><th>採用数</th>
                  <th>期間開始時の残り目標</th><th>1人あたりコスト</th><th>累積コスト/人</th><th>1名あたり売り上げ</th>
                </tr>
              </thead>
              <tbody>
                {revenuePeriods.map((m, i) => (
                  <tr key={m.id ?? i}>
                    <td>{m.name}</td>
                    <td style={{ whiteSpace: 'nowrap', color: '#6b7280', fontSize: '0.78rem' }}>
                      {m.periodStart || '—'}{m.periodEnd ? ` 〜 ${m.periodEnd}` : ''}
                    </td>
                    <td className="num">{formatYen(Number(m.mediaCost) || 0)}</td>
                    <td className="num">{m.hiredCount || 0}人</td>
                    <td className="num">{m.remaining}人</td>
                    <td className="num">{formatYen(m.cpHead)}</td>
                    <td className="num">{formatYen(m.accCost)}</td>
                    <td className="num" style={{ fontWeight: 700, color: m.revenuePerHire >= 0 ? '#10b981' : '#ef4444' }}>
                      {formatYen(m.revenuePerHire)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      {(revenuePeriods.length === 0 || !contractAmount || !hiringTarget) && (
        <p className="empty-hint" style={{ marginTop: '12px' }}>
          受注金額・採用予定人数を入力し、媒体・予算管理タブで媒体ごとに掲載期間を設定すると1名あたりの売り上げが計算されます。
        </p>
      )}

      {funnelData.length >= 2 && (
        <div className="chart-section">
          <h3 className="chart-title">選考ファネル</h3>
          <SimpleFunnelChart data={funnelData} />
        </div>
      )}

      {mediaBarData.length > 0 && (
        <div className="chart-section">
          <h3 className="chart-title">媒体別採用数</h3>
          <SimpleBarChart data={mediaBarData} color={accentColor} />
        </div>
      )}
    </div>
  );
}

// ─── KPI 逆算ロジック ────────────────────────────────
const DEFAULT_PHASES = [
  { id: 'applications',  label: '応募' },
  { id: 'screening',     label: '書類選考' },
  { id: 'interview1',    label: '一次面接' },
  { id: 'interview2',    label: '二次面接' },
  { id: 'finalInterview',label: '最終面接' },
  { id: 'offer',         label: '内定' },
  { id: 'accepted',      label: '内定承諾' },
];

const DEFAULT_RATES = {
  screening: 0.50, interview1: 0.60, interview2: 0.70,
  finalInterview: 0.80, offer: 0.60, accepted: 0.70,
};

function calcRequired(hiringTarget, rates, phases) {
  if (!phases.length) return {};
  const sorted = [...phases].sort((a, b) => b.order - a.order);
  const req = {};
  req[sorted[0].id] = hiringTarget;
  for (let i = 0; i < sorted.length - 1; i++) {
    const rate = rates[sorted[i].id] ?? 0.5;
    req[sorted[i + 1].id] = Math.ceil(req[sorted[i].id] / rate);
  }
  return req;
}

function phaseStatus(actual, required) {
  if (!required) return 'success';
  const r = actual / required;
  if (r >= 1)   return 'success';
  if (r >= 0.7) return 'warning';
  return 'danger';
}

// フェーズID → 応募者ステータスのマッピング
const PHASE_TO_STATUS = {
  applications:   '応募',
  screening:      '書類選考',
  interview1:     '一次面接',
  interview2:     '二次面接',
  finalInterview: '最終面接',
  offer:          '内定',
  accepted:       '内定承諾',
};

// ─── KPI ────────────────────────────────────────────
function KpiTab({ client, onUpdate, accentColor }) {
  const kpi      = client.data.kpiData    || {};
  const settings = client.data.kpiSettings || {};
  const phases   = (settings.selectionFlow || DEFAULT_PHASES).map((p, i) => ({ ...p, order: p.order ?? i + 1 }));
  const rates    = { ...DEFAULT_RATES, ...(settings.conversionRates || {}) };
  const hiringTarget = client.data.projectInfo?.hiringTarget || 0;
  const required = hiringTarget ? calcRequired(hiringTarget, rates, phases) : {};

  const [editRates,    setEditRates]    = useState(false);
  const [editPhases,   setEditPhases]   = useState(false);
  const [draftRates,   setDraftRates]   = useState(settings.conversionRates || {});
  const [draftPhases,  setDraftPhases]  = useState(phases);
  const [actualCounts, setActualCounts] = useState({});

  useEffect(() => {
    api.rpoApplicants(client.id)
      .then(r => {
        const counts = (r.counts || []).reduce((m, c) => { m[c.status] = c.count; return m; }, {});
        setActualCounts(counts);
      })
      .catch(() => {});
  }, [client.id]);

  const saveRates = () => {
    onUpdate({ kpiSettings: { ...settings, conversionRates: draftRates } });
    setEditRates(false);
  };

  const updateKpi = (id, value) =>
    onUpdate({ kpiData: { ...kpi, [id]: Number(value) || 0 } });

  // DBに保存済みのphaseOptionsからフェーズを設定（同期不要）
  const importPhasesFromSheet = async () => {
    const options = client.data?.phaseOptions || [];
    if (!options.length) {
      alert('応募者タブで「再同期」を実行してからもう一度お試しください。');
      return;
    }
    const newFlow = options.map((label, i) => ({
      id: `phase_${i}`,
      label,
      order: i + 1,
    }));
    onUpdate({ kpiSettings: { ...settings, selectionFlow: newFlow } });
  };

  const statusColors = { success: '#10b981', warning: '#f59e0b', danger: '#ef4444' };
  const statusLabels = { success: '順調', warning: '注意', danger: '遅延' };

  const summary = {
    success: phases.filter(p => phaseStatus(kpi[p.id] || 0, required[p.id]) === 'success').length,
    warning: phases.filter(p => phaseStatus(kpi[p.id] || 0, required[p.id]) === 'warning').length,
    danger:  phases.filter(p => phaseStatus(kpi[p.id] || 0, required[p.id]) === 'danger').length,
  };

  return (
    <div className="tab-section">
      <div className="section-header">
        <h2 className="section-title">選考フェーズ別KPI</h2>
        <div style={{ display: 'flex', gap: '8px' }}>
          <button className="btn-secondary small" onClick={importPhasesFromSheet}>
            スプシからフェーズ取得
          </button>
          <button className="btn-secondary small" onClick={() => { setDraftPhases(phases); setEditPhases(v => !v); }}>
            {editPhases ? 'キャンセル' : 'フェーズ編集'}
          </button>
          <button className="btn-secondary" onClick={() => setEditRates(v => !v)}>
            {editRates ? 'キャンセル' : '歩留まり率設定'}
          </button>
        </div>
      </div>

      {/* フェーズ編集モード */}
      {editPhases && (
        <div style={{ background: '#f9fafb', border: '1px solid #e5e7eb', borderRadius: '8px', padding: '12px 16px', marginBottom: '16px' }}>
          <p style={{ fontSize: '0.8rem', color: '#6b7280', marginBottom: '8px' }}>不要なフェーズを×で削除。上下の矢印で並び替え。</p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
            {draftPhases.map((p, i) => (
              <div key={p.id} style={{ display: 'flex', alignItems: 'center', gap: '6px', background: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', padding: '6px 10px' }}>
                <span style={{ flex: 1, fontSize: '0.875rem' }}>{p.label}</span>
                <button onClick={() => setDraftPhases(prev => { const a = [...prev]; [a[i-1], a[i]] = [a[i], a[i-1]]; return a; })} disabled={i === 0} style={{ border: 'none', background: 'none', cursor: 'pointer', color: '#6b7280', fontSize: '12px', padding: '2px 4px' }}>↑</button>
                <button onClick={() => setDraftPhases(prev => { const a = [...prev]; [a[i], a[i+1]] = [a[i+1], a[i]]; return a; })} disabled={i === draftPhases.length - 1} style={{ border: 'none', background: 'none', cursor: 'pointer', color: '#6b7280', fontSize: '12px', padding: '2px 4px' }}>↓</button>
                <button onClick={() => setDraftPhases(prev => prev.filter((_, j) => j !== i))} style={{ border: 'none', background: 'none', cursor: 'pointer', color: '#ef4444', fontSize: '14px', padding: '2px 4px' }}>×</button>
              </div>
            ))}
          </div>
          <button className="btn-primary small" style={{ marginTop: '10px' }} onClick={() => {
            const newFlow = draftPhases.map((p, i) => ({ ...p, order: i + 1 }));
            onUpdate({ kpiSettings: { ...settings, selectionFlow: newFlow } });
            setEditPhases(false);
          }}>保存</button>
        </div>
      )}

      {!editPhases && (
        <p style={{ fontSize: '0.78rem', color: '#6b7280', marginBottom: '8px' }}>
          フェーズ: {phases.map(p => p.label).join(' → ')}
        </p>
      )}

      {!hiringTarget && (
        <p className="empty-hint" style={{ color: '#f59e0b' }}>
          ダッシュボードタブで「採用目標人数」を設定すると逆算値が表示されます
        </p>
      )}

      {editRates && (
        <div className="kpi-rates-panel">
          <p className="kpi-rates-hint">各フェーズへの通過率（%）。空欄はデフォルト値を使用</p>
          <div className="kpi-rates-grid">
            {phases.slice(1).map(p => {
              const def = Math.round((DEFAULT_RATES[p.id] ?? 0.5) * 100);
              return (
                <div key={p.id} className="kpi-rate-field">
                  <label>{p.label}<span>（既定 {def}%）</span></label>
                  <div className="kpi-rate-input-row">
                    <input type="number" min="1" max="100"
                      placeholder={String(def)}
                      value={draftRates[p.id] != null ? Math.round(draftRates[p.id] * 100) : ''}
                      onChange={e => {
                        const v = Number(e.target.value);
                        if (e.target.value === '') {
                          const r = { ...draftRates }; delete r[p.id]; setDraftRates(r);
                        } else if (v > 0 && v <= 100) {
                          setDraftRates(r => ({ ...r, [p.id]: v / 100 }));
                        }
                      }}
                    />
                    <span>%</span>
                  </div>
                </div>
              );
            })}
          </div>
          <button className="btn-primary btn-sm" onClick={saveRates}>保存</button>
        </div>
      )}

      <div className="kpi-phases">
        {phases.map((p, idx) => {
          const statusName = PHASE_TO_STATUS[p.id] ?? p.label;
          const actual = actualCounts[statusName] || 0;
          const req    = required[p.id] || 0;
          const st     = phaseStatus(actual, req);
          const pct    = req > 0 ? Math.min(100, (actual / req) * 100) : 0;
          const diff   = actual - req;
          return (
            <div key={p.id} className={`kpi-phase-card kpi-${st}`}>
              <div className="kpi-phase-label">{p.label}</div>
              <div className="kpi-phase-values">
                <span className="kpi-phase-actual" style={{ color: statusColors[st] }}>{actual}</span>
                {req > 0 && <span className="kpi-phase-required">/ {req}</span>}
              </div>
              {req > 0 && (
                <div className="kpi-phase-bar">
                  <div className="kpi-phase-bar-fill" style={{ width: `${pct}%`, background: statusColors[st] }} />
                </div>
              )}
              {req > 0 && (
                <div className="kpi-phase-diff" style={{ color: statusColors[st] }}>
                  {diff >= 0 ? `+${diff}` : diff} {statusLabels[st]}
                </div>
              )}
              {idx < phases.length - 1 && <div className="kpi-phase-arrow">›</div>}
            </div>
          );
        })}
      </div>

      {hiringTarget > 0 && (
        <div className="kpi-summary">
          <div className="kpi-summary-card success"><span>{summary.success}</span><label>順調</label></div>
          <div className="kpi-summary-card warning"><span>{summary.warning}</span><label>注意</label></div>
          <div className="kpi-summary-card danger"><span>{summary.danger}</span><label>遅延</label></div>
        </div>
      )}
    </div>
  );
}

// ─── 媒体・予算管理 ─────────────────────────────────
function ContentTab({ client, onUpdate, accentColor }) {
  const media = client.data.mediaStatus || [];
  const [masters, setMasters]       = useState([]);
  const [showPicker, setShowPicker] = useState(false);
  const [newMasterName, setNewMasterName] = useState('');

  useEffect(() => {
    api.rpoMediaMasters()
      .then(r => setMasters(r.masters || []))
      .catch(() => {});
  }, []);

  const addFromMaster = (master) => {
    const newItem = { id: Date.now(), name: master.name, status: '運用中', mediaCost: 0, hiredCount: 0, memo: '' };
    onUpdate({ mediaStatus: [...media, newItem] });
  };

  const handleAddCustom = () => {
    if (!newMasterName.trim()) return;
    const newItem = { id: Date.now(), name: newMasterName.trim(), status: '運用中', mediaCost: 0, hiredCount: 0, memo: '' };
    onUpdate({ mediaStatus: [...media, newItem] });
    setNewMasterName('');
  };

  const handleAddMasterGlobal = async () => {
    if (!newMasterName.trim()) return;
    try {
      const r = await api.rpoCreateMediaMaster(newMasterName.trim());
      setMasters(prev => [...prev, r.master]);
      const newItem = { id: Date.now(), name: r.master.name, status: '運用中', mediaCost: 0, hiredCount: 0, memo: '' };
      onUpdate({ mediaStatus: [...media, newItem] });
      setNewMasterName('');
    } catch { alert('追加に失敗しました'); }
  };

  const handleDeleteMaster = async (master) => {
    if (!confirm(`「${master.name}」をマスタから削除しますか？`)) return;
    await api.rpoDeleteMediaMaster(master.id).catch(() => {});
    setMasters(prev => prev.filter(m => m.id !== master.id));
  };

  const updateMedia = (id, field, value) => {
    onUpdate({ mediaStatus: media.map(m => m.id === id ? { ...m, [field]: value } : m) });
  };

  const deleteMedia = (id) => {
    onUpdate({ mediaStatus: media.filter(m => m.id !== id) });
  };

  return (
    <div className="tab-section">
      <div className="section-header">
        <h2 className="section-title">媒体管理</h2>
        <button className="btn-secondary" onClick={() => setShowPicker(v => !v)}>
          {showPicker ? '閉じる' : '＋ 媒体を追加'}
        </button>
      </div>

      {/* 媒体選択ピッカー */}
      {showPicker && (
        <div className="media-picker">
          <p className="media-picker-hint">追加する媒体を選択（グレーはすでに追加済み）</p>
          <div className="media-picker-grid">
            {masters.map(m => {
              return (
                <div key={m.id} className="media-picker-chip">
                  <button
                    className="media-picker-chip-btn"
                    onClick={() => addFromMaster(m)}
                  >
                    {m.name}
                  </button>
                  <button
                    className="media-picker-delete"
                    title="マスタから削除"
                    onClick={() => handleDeleteMaster(m)}
                  >×</button>
                </div>
              );
            })}
          </div>
          <div className="media-picker-custom">
            <input
              type="text"
              placeholder="マスタにない媒体名を入力"
              value={newMasterName}
              onChange={e => setNewMasterName(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleAddCustom()}
            />
            <button className="btn-secondary small" onClick={handleAddCustom}>この案件だけに追加</button>
            <button className="btn-primary small" onClick={handleAddMasterGlobal}>マスタにも追加</button>
          </div>
        </div>
      )}

      {media.length === 0 ? (
        <p className="empty-hint">媒体が登録されていません</p>
      ) : (
        <div className="media-table">
          <div className="media-header">
            <span>媒体名</span><span>ステータス</span><span>掲載開始</span><span>掲載終了</span><span>採用コスト（円）</span><span>採用数</span><span>メモ</span><span></span>
          </div>
          {media.map(m => (
            <div key={m.id} className="media-row">
              <span className="media-name-label">{m.name}</span>
              <select value={m.status} onChange={e => updateMedia(m.id, 'status', e.target.value)}>
                <option>運用中</option>
                <option>運用終了</option>
                <option>検討中</option>
              </select>
              <input type="date" value={m.periodStart || ''} onChange={e => updateMedia(m.id, 'periodStart', e.target.value)} />
              <input type="date" value={m.periodEnd   || ''} onChange={e => updateMedia(m.id, 'periodEnd',   e.target.value)} />
              <input type="number" value={m.mediaCost || 0} onChange={e => updateMedia(m.id, 'mediaCost', Number(e.target.value))} />
              <input type="number" value={m.hiredCount || 0} onChange={e => updateMedia(m.id, 'hiredCount', Number(e.target.value))} />
              <input value={m.memo || ''} onChange={e => updateMedia(m.id, 'memo', e.target.value)} placeholder="メモ" />
              <button className="btn-danger-sm" onClick={() => deleteMedia(m.id)}>削除</button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── タスク（workload items連携）────────────────────
function autoGrow(el) {
  if (!el) return;
  el.style.height = 'auto';
  el.style.height = el.scrollHeight + 'px';
}

// camelCase patch → snake_case state keys
function toSnake(patch) {
  const m = { isDone: 'is_done', dueDate: 'due_date', statusMemo: 'status_memo' };
  const r = {};
  for (const [k, v] of Object.entries(patch)) r[m[k] ?? k] = v;
  return r;
}

function TaskRow({ item, addingId, setAddingId, onSave, onDelete, todayStr }) {
  const [title,      setTitle]      = useState(item.title       ?? '');
  const [notes,      setNotes]      = useState(item.notes       ?? '');
  const [statusMemo, setStatusMemo] = useState(item.status_memo ?? '');
  const [dueDate,    setDueDate]    = useState(item.due_date    ?? '');
  const [isDone,     setIsDone]     = useState(!!item.is_done);

  // 最新値をrefで追跡（アンマウント時のフラッシュ用）
  const latestRef = useRef({ title, notes, statusMemo, dueDate, isDone });
  latestRef.current = { title, notes, statusMemo, dueDate, isDone };

  const timers = useRef({});
  const schedule = (key, apiPatch, delay = 500) => {
    clearTimeout(timers.current[key]);
    timers.current[key] = setTimeout(() => onSave(item.id, apiPatch), delay);
  };

  // アンマウント時: 未送信のデバウンスをフラッシュ
  useEffect(() => {
    return () => {
      Object.values(timers.current).forEach(clearTimeout);
      const { title, notes, statusMemo, dueDate, isDone } = latestRef.current;
      onSave(item.id, { title, notes: notes || null, statusMemo: statusMemo || null, dueDate: dueDate || null, isDone });
    };
  }, []);

  const isOverdue = !isDone && dueDate && dueDate < todayStr;

  return (
    <div className={`task-sheet-row ${isDone ? 'done' : ''} ${isOverdue ? 'overdue' : ''}`}>
      <div className="ts-col-done">
        <input type="checkbox" checked={isDone} onChange={e => {
          setIsDone(e.target.checked);
          onSave(item.id, { isDone: e.target.checked });
        }} />
      </div>
      <div className="ts-col-due">
        <input type="date" value={dueDate} onChange={e => {
          setDueDate(e.target.value);
          onSave(item.id, { dueDate: e.target.value || null });
        }} />
      </div>
      <div className="ts-col-title">
        <textarea
          ref={el => { if (el && item.id === addingId) { el.focus(); setAddingId(null); } autoGrow(el); }}
          value={title}
          placeholder="やることを入力…"
          onInput={e => autoGrow(e.target)}
          onChange={e => { setTitle(e.target.value); schedule('title', { title: e.target.value }); }}
        />
      </div>
      <div className="ts-col-notes">
        <textarea
          ref={el => autoGrow(el)}
          value={notes}
          onInput={e => autoGrow(e.target)}
          onChange={e => { setNotes(e.target.value); schedule('notes', { notes: e.target.value || null }); }}
        />
      </div>
      <div className="ts-col-status">
        <textarea
          ref={el => autoGrow(el)}
          value={statusMemo}
          onInput={e => autoGrow(e.target)}
          onChange={e => { setStatusMemo(e.target.value); schedule('statusMemo', { statusMemo: e.target.value || null }); }}
        />
      </div>
      <div className="ts-col-action">
        <button className="ts-delete-btn" onClick={() => onDelete(item.id)} title="削除">✕</button>
      </div>
    </div>
  );
}

function TasksTab({ client }) {
  const [items, setItems]       = useState([]);
  const [loading, setLoading]   = useState(true);
  const [addingId, setAddingId] = useState(null);
  const [filterStatus, setFilterStatus] = useState('all');
  const [filterText,   setFilterText]   = useState('');

  const sortItems = list => [...list].sort((a, b) => {
    if (!a.due_date && !b.due_date) return 0;
    if (!a.due_date) return 1;
    if (!b.due_date) return -1;
    return a.due_date < b.due_date ? -1 : 1;
  });

  useEffect(() => {
    api.rpoWorkloadItems(client.id)
      .then(r => setItems(sortItems(r.items || [])))
      .finally(() => setLoading(false));
  }, [client.id]);

  const saveItem = async (itemId, patch) => {
    // ローカルstateも更新（snake_caseに変換して反映）
    setItems(prev => sortItems(prev.map(i => i.id === itemId ? { ...i, ...toSnake(patch) } : i)));
    await api.updateWorkloadItem(itemId, patch).catch(() => {});
  };

  const handleAdd = async () => {
    try {
      const r = await api.rpoCreateWorkloadItem(client.id, {
        title: '', dashTeamId: client.dash_team_id,
        dueDate: null, notes: null, statusMemo: null,
      });
      setItems(prev => [...prev, r.item]);
      setAddingId(r.item.id);
    } catch { alert('作成に失敗しました'); }
  };

  const handleDelete = async (itemId) => {
    if (!window.confirm('このタスクを削除しますか？')) return;
    await api.rpoDeleteWorkloadItem(client.id, itemId).catch(() => {});
    setItems(prev => prev.filter(i => i.id !== itemId));
  };

  const todayStr = new Date().toISOString().split('T')[0];

  const displayItems = items.filter(item => {
    const isDone    = item.is_done;
    const isOverdue = !isDone && item.due_date && item.due_date < todayStr;
    if (filterStatus === 'active'  && (isDone || isOverdue)) return false;
    if (filterStatus === 'done'    && !isDone) return false;
    if (filterStatus === 'overdue' && !isOverdue) return false;
    if (filterText && !item.title?.toLowerCase().includes(filterText.toLowerCase())) return false;
    return true;
  });

  if (loading) return <div className="page-loading">読み込み中...</div>;

  return (
    <div className="tab-section task-sheet-section">
      <div className="section-header">
        <h2 className="section-title">タスク <span style={{ fontSize: '0.8rem', color: '#9ca3af', fontWeight: 400 }}>{items.length}件</span></h2>
        <button className="btn-primary small" onClick={handleAdd}>＋ タスク追加</button>
      </div>

      <div className="task-filter-bar">
        <div className="task-filter-tabs">
          {[['all','全て'], ['active','未完了'], ['done','完了'], ['overdue','期限切れ']].map(([v, label]) => (
            <button key={v} className={`task-filter-tab ${filterStatus === v ? 'active' : ''}`}
              onClick={() => setFilterStatus(v)}>{label}</button>
          ))}
        </div>
        <input className="task-filter-search" type="text" placeholder="タスク名で検索…"
          value={filterText} onChange={e => setFilterText(e.target.value)} />
      </div>

      <div className="task-sheet">
        <div className="task-sheet-header">
          <div className="ts-col-done"></div>
          <div className="ts-col-due">締切</div>
          <div className="ts-col-title">やること</div>
          <div className="ts-col-notes">補足</div>
          <div className="ts-col-status">状況詳細</div>
          <div className="ts-col-action"></div>
        </div>

        {displayItems.length === 0 ? (
          <div className="task-sheet-empty">{items.length === 0 ? '「＋ タスク追加」でタスクを作成できます' : 'フィルター条件に一致するタスクがありません'}</div>
        ) : displayItems.map(item => (
          <TaskRow
            key={item.id}
            item={item}
            addingId={addingId}
            setAddingId={setAddingId}
            onSave={saveItem}
            onDelete={handleDelete}
            todayStr={todayStr}
          />
        ))}
      </div>
    </div>
  );
}

// ─── 応募者（スプシ連携）─────────────────────────────
const SESSION_KEY = (id) => `rpo_sheets_${id}`;

function SheetsApplicantTab({ client, onUpdate }) {
  const [urlInput,     setUrlInput]     = useState(client.data?.sheetsUrl || '');
  const [syncing,      setSyncing]      = useState(false);
  const [sheets,       setSheets]       = useState([]);
  const [syncedAt,     setSyncedAt]     = useState(null);
  const [activeSheet,  setActiveSheet]  = useState(0);
  const [syncError,    setSyncError]    = useState(null);
  const [phaseOptions, setPhaseOptions] = useState([]);

  const applyResult = (r) => {
    const result = { sheets: r.sheets || [], syncedAt: r.syncedAt, phaseOptions: r.phaseOptions || [] };
    setSheets(result.sheets);
    setSyncedAt(result.syncedAt);
    setPhaseOptions(result.phaseOptions);
    setActiveSheet(0);
    try { sessionStorage.setItem(SESSION_KEY(client.id), JSON.stringify(result)); } catch {}
    // Reactのclient stateにも反映（KPIタブから参照できるよう）
    if (result.phaseOptions.length > 0) {
      onUpdate({ phaseOptions: result.phaseOptions });
    }
  };

  const syncSheets = async (url) => {
    setSyncing(true);
    setSyncError(null);
    try {
      const r = await api.rpsSheetsSync(client.id, url);
      applyResult(r);
    } catch (e) {
      const msg = e.message?.includes('apps_script_url_not_configured')
        ? 'Apps ScriptのURLが管理画面（管理 → 案件管理）に未登録です。'
        : e.message?.includes('no_applicant_sheets')
        ? '「応募者データ」シートが見つかりませんでした。'
        : e.message?.includes('invalid_sheets_url')
        ? 'スプレッドシートのURLが正しくありません。'
        : '同期に失敗しました: ' + e.message;
      setSyncError(msg);
    } finally {
      setSyncing(false);
    }
  };

  const handleSaveUrl = async () => {
    const url = urlInput.trim();
    if (!url) return;
    try { await api.rpsSaveSheetUrl(client.id, url); } catch { /* silent */ }
    await syncSheets(url);
  };

  // 初回マウント時: セッションキャッシュ優先、なければ自動同期
  useEffect(() => {
    try {
      const cached = sessionStorage.getItem(SESSION_KEY(client.id));
      if (cached) {
        const r = JSON.parse(cached);
        setSheets(r.sheets || []);
        setSyncedAt(r.syncedAt);
        setPhaseOptions(r.phaseOptions || []);
        return;
      }
    } catch {}
    if (client.data?.sheetsUrl) {
      syncSheets(client.data.sheetsUrl);
    }
  }, [client.id]);

  const fmtSync = (iso) => {
    if (!iso) return '';
    const d = new Date(iso);
    return `${d.getFullYear()}/${d.getMonth()+1}/${d.getDate()} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
  };

  return (
    <div className="tab-section">
      <div className="section-header">
        <h2 className="section-title">応募者データ（スプシ連携）</h2>
        {syncedAt && (
          <span style={{ fontSize: '0.75rem', color: '#9ca3af' }}>最終同期: {fmtSync(syncedAt)}</span>
        )}
      </div>

      <div className="sheets-url-row">
        <input
          type="url"
          className="sheets-url-input"
          value={urlInput}
          onChange={e => setUrlInput(e.target.value)}
          placeholder="https://docs.google.com/spreadsheets/d/…"
        />
        {urlInput.trim() !== (client.data?.sheetsUrl || '') && (
          <button className="btn-primary small" onClick={handleSaveUrl} disabled={syncing || !urlInput.trim()}>
            保存して同期
          </button>
        )}
        {urlInput.trim() === (client.data?.sheetsUrl || '') && client.data?.sheetsUrl && (
          <button className="btn-secondary small" onClick={() => syncSheets(client.data.sheetsUrl)} disabled={syncing}>
            {syncing ? '同期中…' : '再同期'}
          </button>
        )}
      </div>
      {syncError && <p className="sheets-error">{syncError}</p>}

      {syncing && <p style={{ color: '#6b7280', fontSize: '0.875rem', marginTop: '12px' }}>スプレッドシートを読み込み中…</p>}

      {sheets.length > 0 && (
        <>
          <div className="sheets-tab-bar">
            {sheets.map((s, i) => (
              <button key={i} className={`sheets-sheet-tab ${activeSheet === i ? 'active' : ''}`}
                onClick={() => setActiveSheet(i)}>
                {s.name}
              </button>
            ))}
          </div>
          <SheetTable rows={sheets[activeSheet]?.rows || []} />
        </>
      )}

      {phaseOptions.length > 0 && (
        <div style={{ marginTop: '8px', padding: '8px 12px', background: '#f9fafb', borderRadius: '6px', fontSize: '0.8rem', color: '#6b7280' }}>
          選考フェーズ: {phaseOptions.map(p => <span key={p} style={{ display: 'inline-block', background: '#e5e7eb', borderRadius: '4px', padding: '1px 6px', marginRight: '4px', marginBottom: '2px' }}>{p}</span>)}
        </div>
      )}

      {!sheets.length && !syncing && !syncError && !client.data?.sheetsUrl && (
        <p className="empty-hint" style={{ marginTop: '12px' }}>
          クライアントの管理スプレッドシートURLを入力して「保存して同期」を押してください。<br />
          「応募者データ(XX卒)」シートが自動検出されます。
        </p>
      )}
    </div>
  );
}

// ISO日時 or "YYYY/MM/DD" → "YYYY年MM月DD日" に変換、それ以外はそのまま
function fmtCellDate(v) {
  if (v == null || v === '') return '';
  const s = String(v);
  // ISO: 2026-02-20 or 2026-02-20T...
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[1]}年${iso[2]}月${iso[3]}日`;
  // スラッシュ区切り: 2026/02/20
  const slash = s.match(/^(\d{4})\/(\d{2})\/(\d{2})/);
  if (slash) return `${slash[1]}年${slash[2]}月${slash[3]}日`;
  return s;
}

function renderCell(v) {
  if (v === true || v === false) {
    return <input type="checkbox" checked={v} readOnly style={{ cursor: 'default' }} />;
  }
  return fmtCellDate(v);
}

function SheetTable({ rows }) {
  const [colFilters,  setColFilters]  = useState({});
  const [openFilter,  setOpenFilter]  = useState(null);
  const [hiddenCols,  setHiddenCols]  = useState(new Set());
  const [showColMenu, setShowColMenu] = useState(false);

  if (!rows.length) return <p className="empty-hint">データがありません</p>;
  const headers = rows[0];
  const nameColIdx = headers.findIndex(h => String(h) === '氏名');
  const body = rows.slice(1).filter(r =>
    nameColIdx >= 0
      ? (r[nameColIdx] !== '' && r[nameColIdx] != null)
      : r.some(c => c !== '' && c != null)
  );

  const hasFilter = Object.values(colFilters).some(v => v);
  const visibleCols = headers.map((_, i) => i).filter(i => !hiddenCols.has(i));

  // フィルター適用
  const filtered = body.filter(row =>
    Object.entries(colFilters).every(([ci, val]) => {
      if (!val) return true;
      return String(row[Number(ci)] ?? '').toLowerCase().includes(val.toLowerCase());
    })
  );

  const setColFilter = (ci, val) =>
    setColFilters(prev => ({ ...prev, [ci]: val }));

  const clearAll = () => { setColFilters({}); setOpenFilter(null); };

  // 各列のユニーク値（絞り込み候補）
  const uniqueVals = (ci) => {
    const set = new Set();
    body.forEach(row => { const v = String(row[ci] ?? ''); if (v) set.add(v); });
    return [...set].sort();
  };

  return (
    <div onClick={() => { setOpenFilter(null); setShowColMenu(false); }}>
      {/* ステータスバー */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '10px', padding: '4px 0 6px', fontSize: '0.8rem', color: '#6b7280' }}>
        <span>{filtered.length} / {body.length} 件</span>
        {hasFilter && (
          <button onClick={e => { e.stopPropagation(); clearAll(); }} style={{ fontSize: '0.78rem', color: '#ef4444', background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}>
            ✕ フィルタークリア
          </button>
        )}
        {/* 列の表示設定ボタン */}
        <div style={{ position: 'relative', marginLeft: 'auto' }}>
          <button
            onClick={e => { e.stopPropagation(); setShowColMenu(v => !v); }}
            style={{ fontSize: '0.78rem', padding: '3px 8px', border: '1px solid #d1d5db', borderRadius: '4px', background: hiddenCols.size ? '#eff6ff' : 'white', cursor: 'pointer', color: '#374151' }}
          >
            列 {hiddenCols.size > 0 ? `(${hiddenCols.size}列非表示)` : '表示設定'}
          </button>
          {showColMenu && (
            <div
              onClick={e => e.stopPropagation()}
              style={{ position: 'absolute', right: 0, top: '100%', zIndex: 200, background: 'white', border: '1px solid #d1d5db', borderRadius: '8px', boxShadow: '0 4px 16px rgba(0,0,0,0.12)', padding: '8px', minWidth: '200px', maxHeight: '320px', overflowY: 'auto' }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '6px' }}>
                <span style={{ fontSize: '0.78rem', color: '#6b7280', fontWeight: 600 }}>列の表示/非表示</span>
                <button onClick={() => setHiddenCols(new Set())} style={{ fontSize: '0.72rem', color: '#3b82f6', background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}>全て表示</button>
              </div>
              {headers.map((h, ci) => (
                <label key={ci} style={{ display: 'flex', alignItems: 'center', gap: '6px', padding: '3px 4px', cursor: 'pointer', fontSize: '0.82rem', borderRadius: '3px' }}>
                  <input
                    type="checkbox"
                    checked={!hiddenCols.has(ci)}
                    onChange={() => setHiddenCols(prev => {
                      const next = new Set(prev);
                      next.has(ci) ? next.delete(ci) : next.add(ci);
                      return next;
                    })}
                  />
                  {String(h) || `列${ci + 1}`}
                </label>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="sheet-table-wrap">
        <table className="sheet-table">
          <thead>
            <tr>
              {visibleCols.map(ci => {
                const active = !!colFilters[ci];
                return (
                  <th key={ci} style={{ position: 'relative' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <span style={{ flex: 1 }}>{String(headers[ci])}</span>
                      <button
                        onClick={e => { e.stopPropagation(); setOpenFilter(openFilter === ci ? null : ci); }}
                        style={{ background: active ? '#3b82f6' : '#e5e7eb', color: active ? 'white' : '#374151', border: 'none', borderRadius: '3px', cursor: 'pointer', fontSize: '10px', padding: '1px 4px', lineHeight: 1.4, flexShrink: 0 }}
                      >▼</button>
                    </div>
                    {openFilter === ci && (
                      <div onClick={e => e.stopPropagation()} style={{ position: 'absolute', top: '100%', left: 0, zIndex: 100, background: 'white', border: '1px solid #d1d5db', borderRadius: '6px', boxShadow: '0 4px 12px rgba(0,0,0,0.12)', padding: '8px', minWidth: '180px', maxWidth: '240px' }}>
                        <input autoFocus type="text" placeholder="テキストで絞り込み…" value={colFilters[ci] || ''}
                          onChange={e => setColFilter(ci, e.target.value)}
                          style={{ width: '100%', padding: '4px 8px', border: '1px solid #d1d5db', borderRadius: '4px', fontSize: '0.82rem', boxSizing: 'border-box', marginBottom: '6px' }} />
                        <div style={{ maxHeight: '160px', overflowY: 'auto' }}>
                          <div style={{ padding: '3px 6px', cursor: 'pointer', borderRadius: '3px', fontSize: '0.82rem', color: '#6b7280' }}
                            onClick={() => { setColFilter(ci, ''); setOpenFilter(null); }}>（全て表示）</div>
                          {uniqueVals(ci).map(val => (
                            <div key={val} style={{ padding: '3px 6px', cursor: 'pointer', borderRadius: '3px', fontSize: '0.82rem', background: colFilters[ci] === val ? '#eff6ff' : 'transparent' }}
                              onClick={() => { setColFilter(ci, val); setOpenFilter(null); }}>{fmtCellDate(val)}</div>
                          ))}
                        </div>
                      </div>
                    )}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {filtered.map((row, ri) => (
              <tr key={ri}>
                {visibleCols.map(ci => (
                  <td key={ci} style={colFilters[ci] ? { background: '#fefce8' } : {}}>
                    {renderCell(row[ci])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── 書類 ────────────────────────────────────────────
function DocumentsTab({ client, onUpdate }) {
  const docs = client.data.documents || [];
  const [driveUrl, setDriveUrl]   = useState(client.data?.driveFolder || '');
  const [driveFiles, setDriveFiles] = useState(null); // null=未取得
  const [driveLoading, setDriveLoading] = useState(false);
  const [driveError, setDriveError]   = useState(null);
  const [folderStack, setFolderStack] = useState([]); // breadcrumb用
  const [sheetLinked, setSheetLinked] = useState(null); // 自動検出された管理シートURL

  const fetchDrive = async (folderId) => {
    setDriveLoading(true);
    setDriveError(null);
    try {
      const r = await api.driveFiles(folderId);
      setDriveFiles(r.files || []);
    } catch (e) {
      setDriveError('ドライブの取得に失敗しました: ' + e.message);
    } finally {
      setDriveLoading(false);
    }
  };

  const handleSaveDriveUrl = async () => {
    const url = driveUrl.trim();
    onUpdate({ driveFolder: url });
    setFolderStack([]);
    setSheetLinked(null);
    if (url) {
      fetchDrive(url);
      // 管理シートを自動検出してsheetsUrlを更新
      try {
        const r = await api.rpoAutoLinkSheets(client.id, url);
        if (r.sheetsUrl) {
          setSheetLinked(r.sheetsUrl);
          onUpdate({ sheetsUrl: r.sheetsUrl });
        }
      } catch { /* 失敗しても無視 */ }
    } else {
      setDriveFiles(null);
    }
  };

  const openFolder = (file) => {
    setFolderStack(prev => [...prev, { id: client.data?.driveFolder || driveUrl, name: '← 戻る' }]);
    fetchDrive(file.id);
  };

  const goBack = () => {
    const prev = folderStack[folderStack.length - 1];
    setFolderStack(s => s.slice(0, -1));
    if (prev) fetchDrive(prev.id);
  };

  useEffect(() => {
    if (client.data?.driveFolder && driveFiles === null) {
      fetchDrive(client.data.driveFolder);
    }
  }, [client.id]);

  const addDoc = () => {
    const d = { id: Date.now(), name: '新規書類', url: '', memo: '' };
    onUpdate({ documents: [...docs, d] });
  };
  const updateDoc = (id, patch) => {
    onUpdate({ documents: docs.map(d => d.id === id ? { ...d, ...patch } : d) });
  };

  const fmtSize = (bytes) => {
    if (!bytes) return '';
    const kb = Number(bytes) / 1024;
    if (kb < 1024) return kb.toFixed(0) + ' KB';
    return (kb / 1024).toFixed(1) + ' MB';
  };
  const fmtDate = (iso) => {
    if (!iso) return '';
    return new Date(iso).toLocaleDateString('ja-JP', { month: 'short', day: 'numeric' });
  };

  return (
    <div className="tab-section">
      {/* Google Drive連携 */}
      <div className="section-header" style={{ marginBottom: '8px' }}>
        <h2 className="section-title">Googleドライブ</h2>
      </div>
      <div style={{ display: 'flex', gap: '8px', marginBottom: '12px' }}>
        <input
          type="url"
          value={driveUrl}
          onChange={e => setDriveUrl(e.target.value)}
          placeholder="https://drive.google.com/drive/folders/..."
          style={{ flex: 1, padding: '7px 10px', border: '1px solid #d1d5db', borderRadius: '6px', fontSize: '0.875rem' }}
        />
        <button className="btn-secondary small" onClick={handleSaveDriveUrl} disabled={driveLoading}>
          {driveLoading ? '取得中…' : '取得'}
        </button>
      </div>

      {driveError && <p style={{ color: '#ef4444', fontSize: '0.85rem', marginBottom: '8px' }}>{driveError}</p>}
      {sheetLinked && (
        <p style={{ color: '#10b981', fontSize: '0.8rem', marginBottom: '8px' }}>
          📊 管理シートを自動検出しました → 応募者管理タブで同期できます
        </p>
      )}

      {folderStack.length > 0 && (
        <button onClick={goBack} style={{ background: 'none', border: 'none', color: '#3b82f6', cursor: 'pointer', fontSize: '0.85rem', padding: '0 0 8px', display: 'block' }}>
          ← 戻る
        </button>
      )}

      {driveFiles !== null && (
        <div className="drive-file-list">
          {driveFiles.length === 0 ? (
            <p className="empty-hint">フォルダにファイルがありません</p>
          ) : driveFiles.map(f => (
            <div key={f.id} className="drive-file-row">
              <span className="drive-file-icon">{f.icon}</span>
              {f.isFolder ? (
                <button className="drive-file-name folder" onClick={() => openFolder(f)}>{f.name}</button>
              ) : (
                <a className="drive-file-name" href={f.webViewLink} target="_blank" rel="noreferrer">{f.name}</a>
              )}
              <span className="drive-file-meta">{fmtSize(f.size)}</span>
              <span className="drive-file-meta">{fmtDate(f.modifiedTime)}</span>
            </div>
          ))}
        </div>
      )}

      {/* 手動書類リスト */}
      <div className="section-header" style={{ marginTop: '28px', marginBottom: '8px' }}>
        <h2 className="section-title">書類メモ</h2>
        <button className="btn-secondary small" onClick={addDoc}>＋ 追加</button>
      </div>
      {docs.length === 0 ? (
        <p className="empty-hint">書類メモがありません</p>
      ) : (
        <div className="doc-list">
          {docs.map(d => (
            <div key={d.id} className="doc-item">
              <input value={d.name} onChange={e => updateDoc(d.id, { name: e.target.value })} placeholder="書類名" />
              <input value={d.url || ''} onChange={e => updateDoc(d.id, { url: e.target.value })} placeholder="URL（任意）" />
              <input value={d.memo || ''} onChange={e => updateDoc(d.id, { memo: e.target.value })} placeholder="メモ" />
              <button className="btn-danger-sm" onClick={() => onUpdate({ documents: docs.filter(x => x.id !== d.id) })}>削除</button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── 共通：編集可能フィールド ────────────────────────
function InfoField({ label, value, onChange, type = 'text', multiline = false }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);

  const commit = () => { onChange(draft); setEditing(false); };

  return (
    <div className="info-field">
      <label className="info-label">{label}</label>
      {editing ? (
        <div className="info-edit">
          {multiline
            ? <textarea value={draft} onChange={e => setDraft(e.target.value)} rows={3} />
            : <input type={type} value={draft} onChange={e => setDraft(e.target.value)} autoFocus />
          }
          <div className="info-edit-actions">
            <button className="btn-primary btn-sm" onClick={commit}>保存</button>
            <button className="btn-secondary btn-sm" onClick={() => { setDraft(value); setEditing(false); }}>キャンセル</button>
          </div>
        </div>
      ) : (
        <div className="info-value" onClick={() => { setDraft(value); setEditing(true); }}>
          {value || <span className="info-placeholder">クリックして編集</span>}
        </div>
      )}
    </div>
  );
}

function planLabel(plan) {
  return { monthly: '月額', guarantee: '採用保証' }[plan] || plan;
}

function formatYen(n) {
  return '¥' + Number(n).toLocaleString('ja-JP');
}
