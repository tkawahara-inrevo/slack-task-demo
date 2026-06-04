import { useEffect, useState } from 'react';
import { api } from '../../api/client';

const STATUS_LABEL = { pending: '未回答', answered: '回答済み', closed: 'クローズ' };
const STATUS_COLOR = { pending: '#dc2626', answered: '#2563eb', closed: '#6b7280' };
const STATUS_BG    = { pending: '#fef2f2', answered: '#eff6ff', closed: '#f3f4f6' };

const fmtYen = (n) => n != null ? `${Math.round(n / 10000)}万円` : '—';

export default function AnList() {
  const [requests, setRequests] = useState([]);
  const [loading, setLoading]   = useState(true);
  const [selected, setSelected] = useState(null);
  const [filterStatus, setFilterStatus] = useState('pending');
  const [answer, setAnswer]     = useState('');
  const [est, setEst]           = useState({ est_media_cost:'', est_unit_price:'', est_budget:'', est_hire_count:'', recommended_media:'' });
  const [saving, setSaving]     = useState(false);
  const [posting, setPosting]   = useState(false);
  const [view, setView]         = useState('list'); // 'list' | 'media' | 'master'
  // タブ切替時に右パネル閉じる
  useEffect(() => { if (view !== 'list') setSelected(null); }, [view]);
  const [rpoResults, setRpoResults] = useState(null);
  const [pastStudies, setPastStudies] = useState(null);
  const [mediaStats, setMediaStats] = useState([]);
  const [mediaFacets, setMediaFacets] = useState({ employment_types: [], job_types: [], priorities: [], requesters: [] });
  const [mediaFilters, setMediaFilters] = useState({ employment_type: '', job_type: '', priority: '', requester: '' });
  const [mediaLoading, setMediaLoading] = useState(false);
  const [expandedMedia, setExpandedMedia] = useState({});
  const [mediaDash, setMediaDash] = useState(null);
  const [unified, setUnified] = useState([]);
  const [unifiedCounts, setUnifiedCounts] = useState({ total: 0, slack_total: 0, kintone_total: 0 });
  const [searchQ, setSearchQ] = useState('');
  const [filterRequester, setFilterRequester] = useState('');
  const [filterPriority, setFilterPriority] = useState('');
  const [requesters, setRequesters] = useState([]);
  const [priorities, setPriorities] = useState([]);
  const [studyDetail, setStudyDetail] = useState(null);

  const load = async () => {
    setLoading(true);
    try {
      const apiStatus = filterStatus === 'pending' ? 'pending' : filterStatus === 'answered' ? 'done' : '';
      const r = await api.anUnified({ status: apiStatus, q: searchQ });
      let rows = r.rows || [];
      if (filterRequester) rows = rows.filter(x => x.requester === filterRequester);
      if (filterPriority)  rows = rows.filter(x => x.priority === filterPriority);
      // 全体から優先度の選択肢を抽出（filterPriority適用前のものを使う）
      const allRows = r.rows || [];
      const prios = [...new Set(allRows.map(x => x.priority).filter(Boolean))]
        .sort((a, b) => {
          const order = { '至急': 0, '高': 1, '中': 2, '低': 3 };
          return (order[a] ?? 99) - (order[b] ?? 99);
        });
      setPriorities(prios);
      setUnified(rows);
      setUnifiedCounts(r.counts || { total: 0 });
      if (r.facets?.requesters) setRequesters(r.facets.requesters);
    } catch { setUnified([]); }
    finally { setLoading(false); }
  };

  useEffect(() => { load(); }, [filterStatus, filterRequester, filterPriority]); // eslint-disable-line
  // 検索は debounce
  useEffect(() => { const t = setTimeout(() => load(), 300); return () => clearTimeout(t); }, [searchQ]); // eslint-disable-line

  const openDetail = async (row) => {
    setStudyDetail(null);
    setSelected({ ...row, _kind: 'study' });
    try {
      const r = await api.anStudyDetail(row.source_id);
      setStudyDetail({ study: r.study, media: r.media });
    } catch { setStudyDetail({ study: null, media: [] }); }
  };

  const loadMediaStats = (filters = mediaFilters) => {
    setMediaLoading(true);
    api.anMediaStats(filters).then(r => {
      setMediaStats(r.stats || []);
      if (r.facets) setMediaFacets(r.facets);
    }).catch(() => {}).finally(() => setMediaLoading(false));
  };

  useEffect(() => {
    if (view === 'media') {
      loadMediaStats(mediaFilters);
      if (mediaDash === null) api.anDashboardSummary().then(setMediaDash).catch(() => setMediaDash({}));
    }
  }, [mediaFilters, view]); // eslint-disable-line

  const saveAnswer = async () => {
    if (!selected) return;
    setSaving(true);
    try {
      const r = await api.anUpdate(selected.id, { answer, status: answer.trim() ? 'answered' : selected.status, ...est });
      setRequests(prev => prev.map(x => x.id === selected.id ? r.request : x));
      setSelected(r.request);
    } catch { alert('保存に失敗しました'); }
    finally { setSaving(false); }
  };

  const postToSlack = async () => {
    if (!selected) return;
    setPosting(true);
    try {
      await api.anPostToSlack(selected.id);
      const r = await api.anUpdate(selected.id, { status: 'answered' });
      setRequests(prev => prev.map(x => x.id === selected.id ? r.request : x));
      setSelected(r.request);
      alert('Slackに投稿しました');
    } catch (e) { alert('Slack投稿失敗: ' + e.message); }
    finally { setPosting(false); }
  };

  const fmtDate = (d) => d ? new Date(d).toLocaleDateString('ja-JP', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '—';

  const S = {
    page:   { display: 'flex', height: '100%', background: 'var(--gray-50)' },
    list:   { width: selected ? 360 : '100%', flexShrink: 0, borderRight: '1px solid var(--gray-200)', display: 'flex', flexDirection: 'column', background: 'var(--surface)', overflow: 'hidden' },
    detail: { flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' },
  };

  return (
    <div style={S.page}>
      {/* 左: 一覧 */}
      <div style={S.list}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--gray-200)', display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', gap: 2 }}>
            {[['list','AN依頼一覧'],['media','媒体実績DB'],['master','媒体マスタ']].map(([v,l]) => (
              <button key={v} onClick={() => setView(v)}
                style={{ padding: '3px 10px', fontSize: '0.78rem', fontWeight: 700, borderRadius: 5, border: 'none', cursor: 'pointer', background: view===v ? '#2563eb' : 'transparent', color: view===v ? '#fff' : 'var(--gray-500)' }}>
                {l}
              </button>
            ))}
          </div>
          <div style={{ display: 'flex', gap: 4, marginLeft: 'auto' }}>
            {[['', '全件'], ['pending', '未回答'], ['answered', '回答済み']].map(([v, l]) => (
              <button key={v} onClick={() => setFilterStatus(v)}
                style={{ padding: '3px 10px', fontSize: '0.75rem', fontWeight: 600, borderRadius: 5, border: '1px solid var(--gray-300)', cursor: 'pointer',
                  background: filterStatus === v ? '#2563eb' : 'var(--surface)', color: filterStatus === v ? '#fff' : 'var(--gray-600)' }}>
                {l}
              </button>
            ))}
          </div>
        </div>

        <div style={{ flex: 1, overflowY: 'auto' }}>
          {/* 媒体実績DBビュー */}
          {view === 'media' && (
            <MediaPerformanceView
              mediaStats={mediaStats}
              mediaFacets={mediaFacets}
              mediaFilters={mediaFilters}
              setMediaFilters={setMediaFilters}
              mediaLoading={mediaLoading}
              mediaDash={mediaDash}
              expandedMedia={expandedMedia}
              setExpandedMedia={setExpandedMedia}
            />
          )}
          {/* 媒体マスタビュー */}
          {view === 'master' && <MediaMasterView />}
          {/* AN依頼一覧ビュー（Slack依頼 + App221調査の統合） */}
          {view === 'list' && (
            <>
              <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--gray-200)', background: 'var(--surface-2)', display: 'flex', flexDirection: 'column', gap: 6 }}>
                <input value={searchQ} onChange={e => setSearchQ(e.target.value)} placeholder="会社名で検索…"
                  style={{ width: '100%', boxSizing: 'border-box', padding: '6px 10px', fontSize: '0.82rem', border: '1px solid var(--gray-300)', borderRadius: 6 }} />
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
                  <select value={filterRequester} onChange={e => setFilterRequester(e.target.value)}
                    style={{ width: '100%', boxSizing: 'border-box', padding: '6px 10px', fontSize: '0.78rem', border: '1px solid var(--gray-300)', borderRadius: 6, background: 'var(--surface)' }}>
                    <option value="">担当者: 全員</option>
                    {requesters.map(r => <option key={r} value={r}>{r}</option>)}
                  </select>
                  <select value={filterPriority} onChange={e => setFilterPriority(e.target.value)}
                    style={{ width: '100%', boxSizing: 'border-box', padding: '6px 10px', fontSize: '0.78rem', borderRadius: 6,
                      border: `1px solid ${filterPriority === '至急' ? '#dc2626' : filterPriority === '高' ? '#f59e0b' : 'var(--gray-300)'}`,
                      background: filterPriority === '至急' ? '#fef2f2' : filterPriority === '高' ? '#fff7ed' : 'var(--surface)',
                      color: filterPriority === '至急' ? '#dc2626' : filterPriority === '高' ? '#d97706' : 'var(--gray-700)',
                      fontWeight: filterPriority ? 700 : 400 }}>
                    <option value="">優先度: すべて</option>
                    {priorities.map(p => <option key={p} value={p}>{p}</option>)}
                  </select>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: '0.66rem', color: 'var(--gray-500)' }}>
                  <span>全{unifiedCounts.total}件</span>
                  {(filterRequester || filterPriority) && (
                    <button onClick={() => { setFilterRequester(''); setFilterPriority(''); }}
                      style={{ marginLeft: 'auto', fontSize: '0.66rem', padding: '2px 8px', borderRadius: 4, border: '1px dashed var(--gray-300)', background: 'transparent', color: 'var(--gray-500)', cursor: 'pointer' }}>
                      ✕ フィルタクリア
                    </button>
                  )}
                </div>
              </div>
              {loading ? <div style={{ padding: 24, textAlign: 'center', color: 'var(--gray-400)' }}>読み込み中...</div>
              : unified.length === 0 ? <div style={{ padding: 24, textAlign: 'center', color: 'var(--gray-400)' }}>該当なし</div>
              : (
                  <div style={{ padding: '8px 8px', background: '#f1f5f9', minHeight: '100%' }}>
                    {unified.map(row => {
                      const key = `study:${row.source_id}`;
                      const isSelectedRow = selected && selected.source_id===row.source_id;
                      const isDone = ['完了','対応済','クローズ'].includes(row.status);
                      const statusColor = isDone ? '#059669' : '#d97706';
                      const statusBg    = isDone ? '#f0fdf4' : '#fffbeb';
                      // 優先度色
                      const isUrgent = row.priority === '至急';
                      const accent = isUrgent ? '#dc2626' : row.priority === '高' ? '#f59e0b' : '#3b82f6';
                      return (
                        <div key={key} onClick={() => openDetail(row)}
                          style={{ marginBottom: 8, padding: '11px 12px 11px 14px', borderRadius: 10, cursor: 'pointer',
                            background: isSelectedRow ? '#eff6ff' : '#fff',
                            border: isSelectedRow ? '1.5px solid #2563eb' : '1px solid #e2e8f0',
                            borderLeft: `4px solid ${isSelectedRow ? '#2563eb' : accent}`,
                            boxShadow: isSelectedRow ? '0 2px 8px rgba(37,99,235,0.15)' : '0 1px 2px rgba(15,23,42,0.04)',
                            transition: 'all 0.12s ease' }}
                          onMouseEnter={e => {
                            if (!isSelectedRow) {
                              e.currentTarget.style.boxShadow = '0 2px 6px rgba(15,23,42,0.1)';
                              e.currentTarget.style.borderTopColor    = '#94a3b8';
                              e.currentTarget.style.borderRightColor  = '#94a3b8';
                              e.currentTarget.style.borderBottomColor = '#94a3b8';
                            }
                          }}
                          onMouseLeave={e => {
                            if (!isSelectedRow) {
                              e.currentTarget.style.boxShadow = '0 1px 2px rgba(15,23,42,0.04)';
                              e.currentTarget.style.borderTopColor    = '#e2e8f0';
                              e.currentTarget.style.borderRightColor  = '#e2e8f0';
                              e.currentTarget.style.borderBottomColor = '#e2e8f0';
                            }
                          }}>
                          {/* 1行目: 会社名 + 優先度 + ステータス */}
                          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 5 }}>
                            <span style={{ fontWeight: 700, fontSize: '0.86rem', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: '#0f172a' }}>
                              {row.company_name || '(会社名なし)'}
                            </span>
                            {row.priority && (
                              isUrgent ? (
                                <span style={{ fontSize: '0.64rem', fontWeight: 800, padding: '2px 8px', borderRadius: 99, background: '#dc2626', color: '#fff', letterSpacing: '0.04em', boxShadow: '0 1px 2px rgba(220,38,38,0.4)', flexShrink: 0 }}>
                                  🔥 至急
                                </span>
                              ) : (
                                <span style={{ fontSize: '0.64rem', fontWeight: 700, padding: '2px 8px', borderRadius: 99, background: row.priority === '高' ? '#fff7ed' : '#eff6ff', color: accent, border: `1px solid ${accent}40`, flexShrink: 0 }}>
                                  {row.priority}
                                </span>
                              )
                            )}
                            <span style={{ fontSize: '0.64rem', fontWeight: 700, padding: '2px 8px', borderRadius: 99, background: statusBg, color: statusColor, flexShrink: 0 }}>
                              {isDone ? '回答済' : (row.status || '未回答')}
                            </span>
                          </div>
                          {/* 2行目: 雇用形態 / 職種（メインメタ情報） */}
                          <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 5, flexWrap: 'wrap' }}>
                            {row.employment_type && (
                              <span style={{ fontSize: '0.7rem', fontWeight: 700, padding: '2px 8px', borderRadius: 5, background: '#ede9fe', color: '#6d28d9' }}>
                                {row.employment_type}
                              </span>
                            )}
                            {row.job_type && (
                              <span style={{ fontSize: '0.7rem', fontWeight: 700, padding: '2px 8px', borderRadius: 5, background: '#e0f2fe', color: '#0369a1' }}>
                                {row.job_type}
                              </span>
                            )}
                            {!row.employment_type && !row.job_type && (
                              <span style={{ fontSize: '0.7rem', color: '#cbd5e1' }}>—</span>
                            )}
                          </div>
                          {/* 3行目: 担当・媒体数・日付 */}
                          <div style={{ fontSize: '0.7rem', color: '#64748b', display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                            {row.requester && <span>👤 {row.requester}</span>}
                            {row.media_count > 0 && <span style={{ color: '#0284c7', background: '#f0f9ff', padding: '1px 6px', borderRadius: 4, fontWeight: 600 }}>📊 媒体{row.media_count}</span>}
                            <span style={{ marginLeft: 'auto', color: '#94a3b8' }}>{row.requested_at ? fmtDate(row.requested_at) : '—'}</span>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
            </>
          )}
        </div>
      </div>

      {/* 右: 詳細・編集 */}
      {selected && selected._kind === 'study' && (
        <StudyDetailPanel
          selected={selected}
          studyDetail={studyDetail}
          setStudyDetail={setStudyDetail}
          onClose={() => setSelected(null)}
          fmtYen={fmtYen}
          afterChange={() => load()}
        />
      )}

    </div>
  );
}

// ── 媒体実績DB（媒体ごとのROI＋紐づく案件）─────────────────────────
function MediaPerformanceView({ mediaStats, mediaFacets, mediaFilters, setMediaFilters, mediaLoading, mediaDash, expandedMedia, setExpandedMedia }) {
  const [casesCache, setCasesCache] = useState({}); // media_name -> cases[]
  const [casesLoading, setCasesLoading] = useState({});

  const loadCases = async (mediaName) => {
    if (casesCache[mediaName] || casesLoading[mediaName]) return;
    setCasesLoading(s => ({ ...s, [mediaName]: true }));
    try {
      const r = await api.anMediaCases({ media_name: mediaName, ...mediaFilters });
      setCasesCache(c => ({ ...c, [mediaName]: r.cases || [] }));
    } catch { setCasesCache(c => ({ ...c, [mediaName]: [] })); }
    finally { setCasesLoading(s => ({ ...s, [mediaName]: false })); }
  };

  // フィルタ変更でキャッシュクリア
  useEffect(() => { setCasesCache({}); }, [mediaFilters]);

  const toggleExpand = (name) => {
    setExpandedMedia(m => {
      const next = { ...m, [name]: !m[name] };
      if (next[name]) loadCases(name);
      return next;
    });
  };

  const accuracyColor = (p) => p == null ? '#94a3b8' : p >= 80 ? '#059669' : p >= 50 ? '#d97706' : '#dc2626';
  const accuracyBg    = (p) => p == null ? '#f1f5f9' : p >= 80 ? '#ecfdf5' : p >= 50 ? '#fffbeb' : '#fef2f2';

  const sorted = [...mediaStats].sort((a,b) => (b.cases || 0) - (a.cases || 0));

  return (
    <div style={{ padding: '14px 14px 24px', background: 'linear-gradient(180deg, #eef2ff 0%, #f8fafc 200px)', minHeight: '100%' }}>
      {/* KPI ヘッダー */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8, marginBottom: 12 }}>
        {[
          {
            label: '総調査件数',
            value: mediaDash?.kpi?.total_studies?.toLocaleString() || 0,
            sub: `延べ媒体スロット ${mediaDash?.kpi?.total_slots || 0}件`,
            grad: 'linear-gradient(135deg, #6366f1, #8b5cf6)',
            icon: '📚',
          },
          {
            label: '利用媒体数',
            value: mediaDash?.kpi?.unique_media?.toLocaleString() || 0,
            sub: 'ユニーク媒体数',
            grad: 'linear-gradient(135deg, #0ea5e9, #06b6d4)',
            icon: '📡',
          },
          {
            label: '平均予測精度',
            value: mediaDash?.kpi?.forecast_accuracy_pct != null ? `${mediaDash.kpi.forecast_accuracy_pct}%` : '—',
            sub: '実応募 / 予測応募',
            grad: mediaDash?.kpi?.forecast_accuracy_pct >= 80 ? 'linear-gradient(135deg, #10b981, #059669)'
                : mediaDash?.kpi?.forecast_accuracy_pct >= 50 ? 'linear-gradient(135deg, #f59e0b, #d97706)'
                : 'linear-gradient(135deg, #ef4444, #dc2626)',
            icon: '🎯',
          },
        ].map(k => (
          <div key={k.label} style={{ background: k.grad, borderRadius: 12, padding: '12px 14px', color: '#fff', boxShadow: '0 4px 12px rgba(99,102,241,0.15)', position: 'relative', overflow: 'hidden' }}>
            <div style={{ position: 'absolute', top: 4, right: 8, fontSize: '1.6rem', opacity: 0.25 }}>{k.icon}</div>
            <div style={{ fontSize: '0.66rem', opacity: 0.9, fontWeight: 600 }}>{k.label}</div>
            <div style={{ fontSize: '1.5rem', fontWeight: 800, marginTop: 2 }}>{k.value}</div>
            <div style={{ fontSize: '0.62rem', opacity: 0.85, marginTop: 1 }}>{k.sub}</div>
          </div>
        ))}
      </div>

      {/* フィルタバー */}
      <div style={{ marginBottom: 12, padding: '10px 12px', background: '#fff', border: '1px solid #e0e7ff', borderRadius: 10, boxShadow: '0 1px 3px rgba(0,0,0,0.04)', display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 8 }}>
        {[
          ['employment_type', '雇用形態', mediaFacets.employment_types || [], '#6366f1'],
          ['job_type',        '職種',     mediaFacets.job_types || [],        '#0ea5e9'],
          ['priority',        '優先度',   mediaFacets.priorities || [],       '#f59e0b'],
          ['requester',       '依頼者',   mediaFacets.requesters || [],       '#10b981'],
        ].map(([key, label, opts, color]) => (
          <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <label style={{ fontSize: '0.66rem', color, fontWeight: 700, letterSpacing: '0.02em' }}>{label}</label>
            <select value={mediaFilters[key]} onChange={e => setMediaFilters(f => ({ ...f, [key]: e.target.value }))}
              style={{ padding: '5px 8px', fontSize: '0.78rem', border: `1px solid ${mediaFilters[key] ? color : '#e2e8f0'}`, borderRadius: 6, background: mediaFilters[key] ? `${color}10` : '#fff', color: '#0f172a', fontWeight: mediaFilters[key] ? 600 : 400 }}>
              <option value="">すべて</option>
              {opts.map(v => <option key={v} value={v}>{v}</option>)}
            </select>
          </div>
        ))}
        {Object.values(mediaFilters).some(Boolean) && (
          <button onClick={() => setMediaFilters({ employment_type:'', job_type:'', priority:'', requester:'' })}
            style={{ gridColumn: '1 / -1', padding: '5px', fontSize: '0.72rem', background: '#f1f5f9', border: 'none', borderRadius: 6, color: '#64748b', cursor: 'pointer', fontWeight: 600 }}>
            ✕ フィルタをクリア
          </button>
        )}
      </div>

      {/* 媒体カードリスト */}
      {mediaLoading && <div style={{ textAlign: 'center', color: '#94a3b8', padding: 30 }}>読み込み中…</div>}
      {!mediaLoading && sorted.length === 0 && <div style={{ textAlign: 'center', color: '#94a3b8', padding: 30 }}>該当する媒体がありません</div>}

      <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        {sorted.map(s => {
          const expanded = !!expandedMedia[s.media_name];
          const cases = casesCache[s.media_name];
          const loadingCases = casesLoading[s.media_name];
          const accColor = accuracyColor(s.forecast_accuracy_pct);
          const accBg = accuracyBg(s.forecast_accuracy_pct);
          return (
            <div key={s.media_name} style={{ background: '#fff', borderRadius: 12, boxShadow: expanded ? '0 4px 16px rgba(15,23,42,0.08)' : '0 1px 3px rgba(15,23,42,0.05)', border: '1px solid #e2e8f0', overflow: 'hidden', transition: 'box-shadow 0.15s' }}>
              {/* カードヘッダー（クリックで展開） */}
              <div onClick={() => toggleExpand(s.media_name)}
                style={{ padding: '12px 14px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 10, background: expanded ? 'linear-gradient(90deg, #f8fafc, #fff)' : '#fff', borderBottom: expanded ? '1px solid #e2e8f0' : 'none' }}>
                <div style={{ width: 32, height: 32, borderRadius: 8, background: 'linear-gradient(135deg, #6366f1, #8b5cf6)', color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 800, fontSize: '0.8rem', flexShrink: 0 }}>
                  {s.media_name.slice(0, 2)}
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 700, fontSize: '0.92rem', color: '#0f172a', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{s.media_name}</div>
                  <div style={{ fontSize: '0.7rem', color: '#64748b', marginTop: 2 }}>
                    <span style={{ color: '#6366f1', fontWeight: 700 }}>{s.cases}件</span> の調査実績
                  </div>
                </div>
                {/* 予測 vs 実績 ミニグラフ */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: '0.74rem', color: '#475569' }}>
                  <div style={{ textAlign: 'right' }}>
                    <div style={{ fontSize: '0.62rem', color: '#94a3b8' }}>予測</div>
                    <div style={{ fontWeight: 700, color: '#64748b' }}>{s.avg_expected ?? '—'}</div>
                  </div>
                  <span style={{ color: '#cbd5e1', fontSize: '1rem' }}>→</span>
                  <div style={{ textAlign: 'right' }}>
                    <div style={{ fontSize: '0.62rem', color: '#94a3b8' }}>実績</div>
                    <div style={{ fontWeight: 800, color: '#059669' }}>{s.avg_effective ?? '—'}</div>
                  </div>
                </div>
                {/* 精度バッジ */}
                <div style={{ background: accBg, color: accColor, padding: '6px 10px', borderRadius: 8, fontWeight: 800, fontSize: '0.86rem', minWidth: 56, textAlign: 'center', border: `1px solid ${accColor}30` }}>
                  {s.forecast_accuracy_pct != null ? `${s.forecast_accuracy_pct}%` : '—'}
                </div>
                <span style={{ color: '#94a3b8', fontSize: '0.8rem', marginLeft: 4 }}>{expanded ? '▼' : '▶'}</span>
              </div>

              {/* 展開: 紐づく案件リスト */}
              {expanded && (
                <div style={{ padding: '10px 14px 14px' }}>
                  <div style={{ fontSize: '0.72rem', color: '#475569', fontWeight: 700, marginBottom: 6, display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ width: 3, height: 12, background: '#6366f1', borderRadius: 2 }}></span>
                    紐づく案件
                    {cases && <span style={{ color: '#94a3b8', fontWeight: 500 }}>（{cases.length}件）</span>}
                  </div>
                  {loadingCases || !cases ? (
                    <div style={{ padding: 16, textAlign: 'center', color: '#94a3b8', fontSize: '0.78rem' }}>読み込み中…</div>
                  ) : cases.length === 0 ? (
                    <div style={{ padding: 16, textAlign: 'center', color: '#94a3b8', fontSize: '0.78rem' }}>—</div>
                  ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                      {/* ヘッダー行 */}
                      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr 90px 60px 60px 60px', gap: 8, padding: '4px 10px', fontSize: '0.62rem', color: '#94a3b8', fontWeight: 700, letterSpacing: '0.04em' }}>
                        <div>案件 / 雇用形態・職種</div>
                        <div>依頼者</div>
                        <div style={{ textAlign: 'right' }}>料金</div>
                        <div style={{ textAlign: 'right' }}>予測</div>
                        <div style={{ textAlign: 'right' }}>実応募</div>
                        <div style={{ textAlign: 'center' }}>効果率</div>
                      </div>
                      {cases.map((c, i) => (
                        <div key={c.record_id + '_' + i} style={{ display: 'grid', gridTemplateColumns: '2fr 1fr 90px 60px 60px 60px', gap: 8, alignItems: 'center', padding: '8px 10px', background: i % 2 === 0 ? '#f8fafc' : '#fff', borderRadius: 6, fontSize: '0.76rem', border: '1px solid #f1f5f9' }}>
                          <div style={{ overflow: 'hidden', minWidth: 0 }}>
                            <div style={{ fontWeight: 700, color: '#0f172a', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{c.company_name || '—'}</div>
                            <div style={{ display: 'flex', gap: 4, marginTop: 2, flexWrap: 'wrap' }}>
                              {c.employment_type && <span style={{ fontSize: '0.62rem', fontWeight: 600, padding: '1px 6px', borderRadius: 4, background: '#ede9fe', color: '#6d28d9' }}>{c.employment_type}</span>}
                              {c.job_type && <span style={{ fontSize: '0.62rem', fontWeight: 600, padding: '1px 6px', borderRadius: 4, background: '#e0f2fe', color: '#0369a1' }}>{c.job_type}</span>}
                              {!c.employment_type && !c.job_type && <span style={{ fontSize: '0.66rem', color: '#cbd5e1' }}>—</span>}
                            </div>
                          </div>
                          <div style={{ fontSize: '0.7rem', color: '#475569', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {c.requester ? `👤 ${c.requester}` : <span style={{ color: '#cbd5e1' }}>—</span>}
                          </div>
                          <div style={{ textAlign: 'right' }}>
                            <div style={{ fontWeight: 700, color: c.fee ? '#0f172a' : '#cbd5e1', fontSize: '0.78rem' }}>
                              {c.fee ? `¥${Math.round(c.fee/10000).toLocaleString()}万` : '—'}
                            </div>
                            {c.cost_category && <div style={{ fontSize: '0.58rem', color: '#94a3b8' }}>{c.cost_category}</div>}
                          </div>
                          <div style={{ textAlign: 'right', fontWeight: 700, color: c.expected_apps != null ? '#64748b' : '#cbd5e1' }}>
                            {c.expected_apps ?? '—'}
                          </div>
                          <div style={{ textAlign: 'right', fontWeight: 800, color: c.effective_apps != null ? '#059669' : '#cbd5e1' }}>
                            {c.effective_apps ?? '—'}
                          </div>
                          <div style={{ background: accuracyBg(c.accuracy_pct), color: accuracyColor(c.accuracy_pct), padding: '4px 6px', borderRadius: 6, fontWeight: 700, fontSize: '0.74rem', textAlign: 'center', border: c.accuracy_pct != null ? `1px solid ${accuracyColor(c.accuracy_pct)}30` : 'none' }}>
                            {c.accuracy_pct != null ? `${c.accuracy_pct}%` : '—'}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── 媒体マスタ（kintone App225）─────────────────────────
function MediaMasterView() {
  const [data, setData] = useState({ media: [], facets: {} });
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({ q: '', industry: '', job_type: '', area: '', hire_method: '', employment_type: '', min_score: 0 });
  const [selected, setSelected] = useState(null);
  const [syncing, setSyncing] = useState(false);

  const load = (f) => {
    setLoading(true);
    api.mediaMaster(f).then(setData).catch(() => setData({ media: [], facets: {} })).finally(() => setLoading(false));
  };
  useEffect(() => { load(filters); }, []); // eslint-disable-line
  const setF = (k, v) => { const n = { ...filters, [k]: v }; setFilters(n); load(n); };

  const doSync = async () => {
    if (!window.confirm('kintone App225 から媒体マスタを再同期しますか？')) return;
    setSyncing(true);
    try {
      const r = await api.mediaMasterSync();
      alert('同期完了: ' + r.upserted + '件');
      load(filters);
    } catch (e) { alert('失敗: ' + e.message); }
    finally { setSyncing(false); }
  };

  const SelectFacet = ({ label, k, options }) => (
    <select value={filters[k]} onChange={e => setF(k, e.target.value)}
      style={{ padding: '4px 8px', fontSize: '0.74rem', border: '1px solid var(--gray-300)', borderRadius: 6, background: 'var(--surface)', maxWidth: 150 }}>
      <option value="">{label}: 全て</option>
      {(options || []).map(o => <option key={o} value={o}>{o}</option>)}
    </select>
  );

  return (
    <div style={{ padding: 12 }}>
      <div style={{ background: 'var(--surface)', borderRadius: 8, border: '1px solid var(--gray-200)', padding: '10px 12px', marginBottom: 10, display: 'flex', flexDirection: 'column', gap: 8 }}>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <input value={filters.q} onChange={e => setF('q', e.target.value)} placeholder="媒体名・備考で検索…"
            style={{ flex: 1, padding: '6px 10px', border: '1px solid var(--gray-300)', borderRadius: 6, fontSize: '0.82rem' }} />
          <button onClick={async () => {
              const name = window.prompt('媒体名を入力してください');
              if (!name?.trim()) return;
              try {
                const r = await api.mediaMasterCreate({ name: name.trim() });
                setSelected(r.media);
                load(filters);
              } catch (e) { alert('追加失敗: ' + e.message); }
            }}
            style={{ padding: '5px 10px', fontSize: '0.72rem', fontWeight: 700, borderRadius: 6, border: '1px solid #67e8f9', background: '#ecfeff', color: '#0e7490', cursor: 'pointer' }}>
            ＋ 新規追加
          </button>
          <button onClick={doSync} disabled={syncing}
            style={{ padding: '5px 10px', fontSize: '0.72rem', fontWeight: 700, borderRadius: 6, border: '1px solid var(--gray-300)', background: 'var(--surface)', cursor: 'pointer' }}>
            {syncing ? '同期中…' : '🔄 kintone同期'}
          </button>
        </div>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          <SelectFacet label="業種" k="industry" options={data.facets.industries} />
          <SelectFacet label="職種" k="job_type" options={data.facets.job_types} />
          <SelectFacet label="エリア" k="area" options={data.facets.areas} />
          <SelectFacet label="採用手法" k="hire_method" options={data.facets.hire_methods} />
          <SelectFacet label="対象" k="employment_type" options={data.facets.employment_types} />
          <select value={filters.min_score} onChange={e => setF('min_score', Number(e.target.value))}
            style={{ padding: '4px 8px', fontSize: '0.74rem', border: '1px solid var(--gray-300)', borderRadius: 6, background: 'var(--surface)' }}>
            <option value="0">オススメ度: 全て</option>
            {[1,2,3,4,5].map(n => <option key={n} value={n}>★{n}以上</option>)}
          </select>
          {Object.values(filters).some(v => v && v !== 0) && (
            <button onClick={() => { const r = { q:'', industry:'', job_type:'', area:'', hire_method:'', employment_type:'', min_score:0 }; setFilters(r); load(r); }}
              style={{ padding: '4px 10px', fontSize: '0.7rem', borderRadius: 6, border: '1px dashed var(--gray-300)', background: 'transparent', color: 'var(--gray-500)', cursor: 'pointer' }}>
              フィルタクリア
            </button>
          )}
        </div>
        <div style={{ fontSize: '0.7rem', color: 'var(--gray-500)' }}>該当 {data.total ?? data.media.length}件</div>
      </div>

      {loading ? (
        <div style={{ textAlign: 'center', color: 'var(--gray-400)', padding: 24 }}>読み込み中…</div>
      ) : data.media.length === 0 ? (
        <div style={{ textAlign: 'center', color: 'var(--gray-400)', padding: 24 }}>該当する媒体がありません</div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(280px,1fr))', gap: 10 }}>
          {data.media.map(m => (
            <div key={m.record_id} onClick={() => setSelected(m)}
              style={{ background: 'var(--surface)', border: '1px solid var(--gray-200)', borderRadius: 10, padding: '10px 12px', cursor: 'pointer', transition: 'border-color 0.1s' }}
              onMouseEnter={e=>e.currentTarget.style.borderColor='#93c5fd'} onMouseLeave={e=>e.currentTarget.style.borderColor='var(--gray-200)'}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                <span style={{ fontWeight: 700, fontSize: '0.88rem', flex: 1, color: 'var(--gray-900)' }}>{m.name || '(無名)'}</span>
                {m.recommend_score > 0 && (
                  <span style={{ fontSize: '0.7rem', color: '#d97706', fontWeight: 700 }}>{'★'.repeat(m.recommend_score)}</span>
                )}
              </div>
              {m.service_type && <span style={{ display: 'inline-block', fontSize: '0.66rem', background: '#eef2ff', color: '#4f46e5', borderRadius: 4, padding: '1px 6px', marginRight: 4 }}>{m.service_type}</span>}
              {(m.hire_methods||[]).slice(0,2).map(h => <span key={h} style={{ display: 'inline-block', fontSize: '0.66rem', background: '#f0fdf4', color: '#15803d', borderRadius: 4, padding: '1px 6px', marginRight: 4 }}>{h}</span>)}
              <div style={{ fontSize: '0.72rem', color: 'var(--gray-500)', marginTop: 6 }}>
                {(m.areas||[]).join(' / ') || '—'}
              </div>
              {m.industries?.length > 0 && (
                <div style={{ fontSize: '0.66rem', color: 'var(--gray-400)', marginTop: 4 }}>
                  業種: {m.industries.slice(0,3).join(', ')}{m.industries.length>3?`…+${m.industries.length-3}`:''}
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {selected && (
        <MediaMasterDetail
          media={selected}
          onClose={() => setSelected(null)}
          onUpdate={(updated) => { setSelected(updated); load(filters); }}
          onDelete={() => { setSelected(null); load(filters); }}
        />
      )}
      {/* 旧表示（非表示） */}
      {false && selected && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.4)', zIndex: 1100, display: 'flex', justifyContent: 'flex-end' }} onClick={() => setSelected(null)}>
          <div style={{ width: 'min(520px,94vw)', height: '100%', background: 'var(--surface)', boxShadow: '-8px 0 32px rgba(0,0,0,0.2)', overflowY: 'auto' }} onClick={e => e.stopPropagation()}>
            <div style={{ padding: '14px 20px', borderBottom: '1px solid var(--gray-200)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                <div style={{ fontWeight: 800, fontSize: '1rem', color: 'var(--gray-900)' }}>{selected.name}</div>
                {selected.recommend_score > 0 && <div style={{ fontSize: '0.8rem', color: '#d97706', fontWeight: 700 }}>{'★'.repeat(selected.recommend_score)}</div>}
              </div>
              <button onClick={() => setSelected(null)} style={{ background: 'var(--surface-2)', border: 'none', width: 28, height: 28, borderRadius: 8, cursor: 'pointer', fontSize: 16 }}>×</button>
            </div>
            <div style={{ padding: '14px 20px', display: 'flex', flexDirection: 'column', gap: 12, fontSize: '0.82rem' }}>
              {selected.vendor_url && <div><a href={selected.vendor_url} target="_blank" rel="noreferrer" style={{ color: '#2563eb' }}>{selected.vendor_url}</a></div>}
              {[
                ['種別', selected.service_type],
                ['採用手法', (selected.hire_methods||[]).join(', ')],
                ['エリア', (selected.areas||[]).join(', ')],
                ['対象区分', (selected.employment_types||[]).join(', ')],
                ['利用者年齢層', (selected.age_targets||[]).join(', ')],
                ['業種', (selected.industries||[]).join(', ')],
                ['職種', (selected.job_types||[]).join(', ')],
                ['基本請求先', selected.basic_billing],
                ['ノルマ', selected.norma],
              ].filter(([,v]) => v).map(([k,v]) => (
                <div key={k} style={{ display: 'flex', gap: 12 }}>
                  <div style={{ width: 100, color: 'var(--gray-500)', fontSize: '0.72rem' }}>{k}</div>
                  <div style={{ flex: 1, color: 'var(--gray-900)', wordBreak: 'break-all' }}>{v}</div>
                </div>
              ))}
              {selected.notes && (
                <div>
                  <div style={{ fontSize: '0.74rem', fontWeight: 700, color: 'var(--gray-700)', marginBottom: 4 }}>媒体備考</div>
                  <pre style={{ fontSize: '0.78rem', whiteSpace: 'pre-wrap', background: 'var(--surface-2)', padding: '8px 10px', borderRadius: 6, margin: 0 }}>{selected.notes}</pre>
                </div>
              )}
              {selected.caution && (
                <div>
                  <div style={{ fontSize: '0.74rem', fontWeight: 700, color: '#dc2626', marginBottom: 4 }}>注意事項</div>
                  <pre style={{ fontSize: '0.78rem', whiteSpace: 'pre-wrap', background: '#fef2f2', padding: '8px 10px', borderRadius: 6, margin: 0, color: '#7f1d1d' }}>{selected.caution}</pre>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}


// 推奨媒体サジェスト（媒体マスタ＋過去AN調査）
function MediaSuggestButton({ onPick }) {
  const [open, setOpen] = useState(false);
  const [list, setList] = useState(null);
  const fmtYen = (n) => n != null ? '¥' + Math.round(n).toLocaleString() : '—';

  const toggle = async () => {
    const next = !open;
    setOpen(next);
    if (next && list === null) {
      try {
        const r = await api.mediaSuggest({});
        setList(r.suggestions || []);
      } catch { setList([]); }
    }
  };

  return (
    <div style={{ position: 'relative' }}>
      <button onClick={toggle}
        style={{ fontSize: '0.68rem', padding: '2px 8px', border: '1px solid #c7d2fe', background: '#eef2ff', color: '#4f46e5', borderRadius: 4, cursor: 'pointer', fontWeight: 700 }}>
        🔍 候補から選ぶ
      </button>
      {open && (
        <div style={{ position: 'absolute', top: 24, right: 0, width: 320, maxHeight: 400, overflowY: 'auto', background: '#fff', border: '1px solid var(--gray-200)', borderRadius: 8, boxShadow: '0 4px 12px rgba(0,0,0,0.1)', zIndex: 10, padding: 8 }}>
          {list === null ? (
            <div style={{ padding: 12, textAlign: 'center', color: 'var(--gray-400)', fontSize: '0.76rem' }}>読み込み中…</div>
          ) : list.length === 0 ? (
            <div style={{ padding: 12, textAlign: 'center', color: 'var(--gray-400)', fontSize: '0.76rem' }}>候補なし</div>
          ) : list.map(m => (
            <div key={m.record_id} onClick={() => { onPick(m.name); setOpen(false); }}
              style={{ padding: '6px 8px', borderBottom: '1px solid var(--gray-100)', cursor: 'pointer', borderRadius: 4 }}
              onMouseEnter={e=>e.currentTarget.style.background='#f8fafc'} onMouseLeave={e=>e.currentTarget.style.background='transparent'}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ fontWeight: 600, fontSize: '0.78rem', flex: 1 }}>{m.name}</span>
                {m.recommend_score > 0 && <span style={{ fontSize: '0.66rem', color: '#d97706', fontWeight: 700 }}>{'★'.repeat(m.recommend_score)}</span>}
              </div>
              <div style={{ fontSize: '0.66rem', color: 'var(--gray-500)', marginTop: 2, display: 'flex', gap: 8 }}>
                {m.past_cases > 0 && <span>過去{m.past_cases}件</span>}
                {m.avg_effective != null && <span>平均応募 {m.avg_effective}</span>}
                {m.forecast_accuracy_pct != null && <span>精度 {m.forecast_accuracy_pct}%</span>}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}


// ── AN案件詳細パネル（編集可・モダン） ─────────────────────────
function StudyDetailPanel({ selected, studyDetail, setStudyDetail, onClose, fmtYen, afterChange }) {
  const study = studyDetail?.study;
  const media = studyDetail?.media || [];
  const [savingStatus, setSavingStatus] = useState(false);
  // (mustOpen/notesOpen は InlineTextarea に置き換え、削除)
  const [editingMedia, setEditingMedia] = useState(null); // media row being edited

  if (!study) {
    return (
      <div style={S_panel.root}>
        <PanelHeader title={selected.company_name || '...'} status={selected.status} onClose={onClose} />
        <div style={{ padding: 40, textAlign: 'center', color: '#94a3b8' }}>読み込み中…</div>
      </div>
    );
  }

  const isDone = ['完了','対応済','クローズ'].includes(study.status);

  const updateStudy = async (patch) => {
    try {
      const r = await api.anStudyUpdate(study.record_id, patch);
      setStudyDetail(prev => ({ ...prev, study: r.study }));
      afterChange && afterChange();
    } catch (e) { alert('保存失敗: ' + e.message); }
  };

  const toggleDone = async () => {
    setSavingStatus(true);
    await updateStudy({ status: isDone ? '対応中' : '完了' });
    setSavingStatus(false);
  };


  const addMedia = async () => {
    const name = window.prompt('媒体名を入力（後で編集可）') || '';
    try {
      const r = await api.anStudyMediaAdd(study.record_id, { media_name: name.trim() || null });
      setStudyDetail(prev => ({ ...prev, media: [...(prev.media || []), r.slot] }));
    } catch (e) { alert('追加失敗: ' + e.message); }
  };

  return (
    <div style={S_panel.root}>
      {/* ヘッダー */}
      <div style={{ ...S_panel.header, background: isDone ? 'linear-gradient(135deg,#059669,#10b981)' : 'linear-gradient(135deg,#1e40af,#3b82f6)' }}>
        <button onClick={onClose} style={S_panel.backBtn}>←</button>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: '0.66rem', opacity: 0.85, letterSpacing: '0.04em', textTransform: 'uppercase' }}>AN案件</div>
          <div style={{ fontWeight: 800, fontSize: '1.05rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{study.company_name || '（会社名なし）'}</div>
        </div>
        <button onClick={toggleDone} disabled={savingStatus}
          style={{ background: 'rgba(255,255,255,0.2)', border: '1px solid rgba(255,255,255,0.3)', color: '#fff', fontWeight: 700, fontSize: '0.78rem', padding: '6px 12px', borderRadius: 8, cursor: 'pointer' }}>
          {savingStatus ? '...' : (isDone ? '完了 ✓' : '完了にする')}
        </button>
      </div>

      <div style={{ flex: 1, overflowY: 'auto', padding: '16px 20px', background: '#f8fafc' }}>
        {/* CRMサマリー（顧客＋過去案件） */}
        <CrmSummaryCard studyId={study.record_id} />

        {/* 案件情報グリッド */}
        <Card title="案件情報" accent="#3b82f6">
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(180px,1fr))', gap: 10 }}>
            {[
              ['依頼者', study.requester, '👤'],
              ['依頼日', study.request_date ? String(study.request_date).slice(0,10) : null, '📅'],
              ['優先度', study.priority, '🔥'],
              ['雇用形態', study.employment_type, '💼'],
              ['職種', study.job_type, '🎯'],
              ['対象区分', study.target_classification?.join(', '), '🏷'],
              ['勤務地', study.work_locations?.join(', '), '📍'],
              ['年収', [study.min_salary, study.max_salary].filter(Boolean).length > 0 ? [study.min_salary, study.max_salary].filter(Boolean).map(v => fmtYen(v)).join(' 〜 ') : null, '💰'],
              ['年間休日', study.annual_holidays, '🌿'],
            ].filter(([,v]) => v).map(([label, val, icon]) => (
              <div key={label} style={S_panel.statCell}>
                <div style={S_panel.statLabel}>{icon} {label}</div>
                <div style={S_panel.statVal}>{val}</div>
              </div>
            ))}
          </div>
          {(study.case_link || study.slack_link || study.jobform_url) && (
            <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid #e2e8f0', display: 'flex', gap: 12, flexWrap: 'wrap', fontSize: '0.74rem' }}>
              {/* case_link は URL の場合のみリンク表示、テキストならそのまま */}
              {study.case_link && /^https?:\/\//.test(study.case_link) ? (
                <a href={study.case_link} target="_blank" rel="noreferrer" style={S_panel.link}>📂 案件リンク</a>
              ) : study.case_link ? (
                <span style={{ color: '#64748b' }}>📂 {study.case_link}</span>
              ) : null}
              {study.slack_link && <a href={study.slack_link} target="_blank" rel="noreferrer" style={S_panel.link}>💬 Slack</a>}
              {study.jobform_url && /^https?:\/\//.test(study.jobform_url) ? (
                <a href={study.jobform_url} target="_blank" rel="noreferrer" style={S_panel.link}>📄 求人票</a>
              ) : null}
            </div>
          )}
        </Card>

        {/* MUST条件 - 直接編集 */}
        <Card title="MUST条件" accent="#dc2626">
          <InlineTextarea
            value={study.must_condition || ''}
            placeholder="MUST条件を入力（フォーカス外しで保存）"
            border="#fca5a5"
            onSave={async (v) => updateStudy({ must_condition: v })}
          />
        </Card>

        {/* その他特記事項 - 直接編集 */}
        <Card title="特記事項" accent="#7c3aed">
          <InlineTextarea
            value={study.other_notes || ''}
            placeholder="特記事項を入力（フォーカス外しで保存）"
            border="#c4b5fd"
            onSave={async (v) => updateStudy({ other_notes: v })}
          />
        </Card>

        {/* Slackスレッド */}
        {study.slack_message_ts && study.slack_channel_id && (
          <SlackThreadCard studyId={study.record_id} slackLink={study.slack_link} />
        )}

        {/* 調査媒体 */}
        <Card title={`調査媒体（${media.length}件）`} accent="#0891b2"
          action={<button onClick={addMedia} style={S_panel.addBtn}>＋ 媒体追加</button>}>
          {media.length === 0 ? (
            <div style={{ padding: 20, textAlign: 'center', color: '#94a3b8', fontSize: '0.82rem' }}>媒体未登録</div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {media.map(m => (
                <MediaSlotRow key={m.id || m.slot} m={m} fmtYen={fmtYen}
                  isEditing={editingMedia === (m.id || m.slot)}
                  onEdit={() => setEditingMedia(m.id || m.slot)}
                  onCancel={() => setEditingMedia(null)}
                  onSaved={(updated) => {
                    setStudyDetail(prev => ({ ...prev, media: prev.media.map(x => (x.id||x.slot) === (m.id||m.slot) ? updated : x) }));
                    setEditingMedia(null);
                  }}
                  onDeleted={() => {
                    setStudyDetail(prev => ({ ...prev, media: prev.media.filter(x => (x.id||x.slot) !== (m.id||m.slot)) }));
                    setEditingMedia(null);
                  }}
                />
              ))}
            </div>
          )}
        </Card>
      </div>
    </div>
  );
}

function PanelHeader({ title, status, onClose }) {
  return (
    <div style={S_panel.header}>
      <button onClick={onClose} style={S_panel.backBtn}>←</button>
      <span style={{ flex: 1, fontWeight: 800 }}>{title}</span>
      {status && <span>{status}</span>}
    </div>
  );
}

function Card({ title, accent, action, children }) {
  return (
    <div style={{ background: '#fff', borderRadius: 12, marginBottom: 12, boxShadow: '0 1px 3px rgba(0,0,0,0.04)', overflow: 'hidden', border: '1px solid #e2e8f0' }}>
      <div style={{ padding: '10px 14px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid #f1f5f9', background: '#fafbfc' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ width: 4, height: 16, background: accent, borderRadius: 2 }} />
          <span style={{ fontWeight: 800, fontSize: '0.86rem', color: '#0f172a' }}>{title}</span>
        </div>
        {action}
      </div>
      <div style={{ padding: '12px 14px' }}>
        {children}
      </div>
    </div>
  );
}

function MediaSlotRow({ m, fmtYen, isEditing, onEdit, onCancel, onSaved, onDeleted }) {
  const [form, setForm] = useState({
    media_name: m.media_name || '',
    cost_category: m.cost_category || '',
    fee: m.fee ?? '',
    duration: m.duration ?? '',
    expected_apps: m.expected_apps ?? '',
    effective_apps: m.effective_apps ?? '',
    reply_rate: m.reply_rate ?? '',
    responses: (m.responses || []).join(', '),
    status_tags: (m.status_tags || []).join(', '),
    note: m.note || '',
    an_assignee: m.an_assignee || '',
  });
  const [saving, setSaving] = useState(false);

  const save = async () => {
    setSaving(true);
    try {
      const r = await api.anStudyMediaUpdate(m.id, form);
      onSaved(r.slot);
    } catch (e) { alert('保存失敗: ' + e.message); }
    finally { setSaving(false); }
  };
  const del = async () => {
    if (!window.confirm(`媒体「${m.media_name || `スロット${m.slot}`}」を削除しますか？`)) return;
    try { await api.anStudyMediaDelete(m.id); onDeleted(); } catch (e) { alert('削除失敗: ' + e.message); }
  };

  if (isEditing) {
    const Fld = ({ k, label, type = 'text' }) => (
      <div>
        <label style={{ fontSize: '0.66rem', color: '#64748b', fontWeight: 600, display: 'block', marginBottom: 2 }}>{label}</label>
        <input type={type} value={form[k]} onChange={e => setForm(f => ({ ...f, [k]: e.target.value }))}
          style={{ width: '100%', boxSizing: 'border-box', padding: '5px 8px', border: '1px solid #cbd5e1', borderRadius: 5, fontSize: '0.78rem' }} />
      </div>
    );
    return (
      <div style={{ background: '#eff6ff', border: '1px solid #93c5fd', borderRadius: 10, padding: '12px 14px' }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 8, marginBottom: 8 }}>
          <Fld k="media_name" label="媒体名" />
          <Fld k="an_assignee" label="AN担当" />
          <Fld k="cost_category" label="費用区分" />
          <Fld k="fee" label="料金(円)" type="number" />
          <Fld k="duration" label="掲載期間(週)" type="number" />
          <Fld k="reply_rate" label="返信率(%)" type="number" />
          <Fld k="expected_apps" label="予測応募" type="number" />
          <Fld k="effective_apps" label="実応募" type="number" />
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 8 }}>
          <Fld k="responses" label="対応（カンマ区切り）" />
          <Fld k="status_tags" label="ステータス（カンマ区切り）" />
        </div>
        <div>
          <label style={{ fontSize: '0.66rem', color: '#64748b', fontWeight: 600, display: 'block', marginBottom: 2 }}>備考</label>
          <textarea value={form.note} onChange={e => setForm(f => ({ ...f, note: e.target.value }))} rows={2}
            style={{ width: '100%', boxSizing: 'border-box', padding: '5px 8px', border: '1px solid #cbd5e1', borderRadius: 5, fontSize: '0.78rem', resize: 'vertical' }} />
        </div>
        <div style={{ marginTop: 10, display: 'flex', gap: 6, justifyContent: 'flex-end' }}>
          <button onClick={del} style={{ ...S_panel.cancelBtn, color: '#dc2626', border: '1px solid #fca5a5' }}>削除</button>
          <button onClick={onCancel} style={S_panel.cancelBtn}>キャンセル</button>
          <button onClick={save} disabled={saving} style={S_panel.saveBtn}>{saving ? '保存中…' : '保存'}</button>
        </div>
      </div>
    );
  }

  return (
    <div style={{ background: '#f8fafc', borderRadius: 10, padding: '10px 12px', border: '1px solid #e2e8f0' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <span style={{ fontWeight: 800, fontSize: '0.92rem', color: '#0f172a' }}>{m.media_name || `スロット${m.slot}`}</span>
        {m.an_assignee && <span style={{ fontSize: '0.66rem', color: '#475569' }}>AN: {m.an_assignee}</span>}
        {m.status_tags?.length > 0 && m.status_tags.map(t => <span key={t} style={{ fontSize: '0.62rem', padding: '1px 6px', borderRadius: 99, background: '#eef2ff', color: '#4f46e5', fontWeight: 600 }}>{t}</span>)}
        <button onClick={onEdit} style={{ ...S_panel.editBtn, marginLeft: 'auto' }}>編集</button>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 6, fontSize: '0.74rem' }}>
        {[
          ['料金', m.fee != null ? fmtYen(m.fee) : null],
          ['掲載', m.duration ? `${m.duration}週` : null],
          ['予測応募', m.expected_apps],
          ['実応募', m.effective_apps],
          ['返信率', m.reply_rate != null ? `${m.reply_rate}%` : null],
          ['費用区分', m.cost_category],
        ].filter(([,v]) => v != null && v !== '').map(([k,v]) => (
          <div key={k} style={{ background: '#fff', borderRadius: 6, padding: '4px 8px', border: '1px solid #e2e8f0' }}>
            <div style={{ fontSize: '0.62rem', color: '#64748b' }}>{k}</div>
            <div style={{ fontWeight: 700, color: '#0f172a' }}>{v}</div>
          </div>
        ))}
      </div>
      {m.responses?.length > 0 && <div style={{ marginTop: 6, fontSize: '0.7rem', color: '#475569' }}>対応: {m.responses.join(', ')}</div>}
      {m.note && <div style={{ marginTop: 4, fontSize: '0.72rem', color: '#475569' }}>備考: {m.note}</div>}
    </div>
  );
}

// CRMサマリーカード（顧客情報＋関連案件、折りたたみ）
function CrmSummaryCard({ studyId }) {
  const [data, setData] = useState(null);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    api.anStudyCrmSummary(studyId)
      .then(r => { if (!cancelled) setData(r); })
      .catch(() => { if (!cancelled) setData({ customer: null, deals: [] }); });
    return () => { cancelled = true; };
  }, [studyId]);

  const fmtY = (n) => n ? `¥${Math.round(Number(n)/10000).toLocaleString()}万` : '—';
  const yomiColor = (y) => {
    if (!y) return '#94a3b8';
    if (y.startsWith('S')) return '#dc2626';
    if (y.startsWith('A')) return '#f59e0b';
    if (y.startsWith('B')) return '#3b82f6';
    if (y.startsWith('C')) return '#6366f1';
    return '#64748b';
  };
  const statusBg = (s) => ({ won:'#ecfdf5', lost:'#fef2f2', dormant:'#f3f4f6' }[s] || '#eff6ff');
  const statusColor = (s) => ({ won:'#059669', lost:'#dc2626', dormant:'#6b7280' }[s] || '#1d4ed8');
  const statusLabel = (s) => ({ won:'受注', lost:'失注', dormant:'見送り', active:'進行中' }[s] || s);

  if (data === null) {
    return (
      <div style={{ background:'#fff', borderRadius:12, marginBottom:12, border:'1px solid #e2e8f0', padding:'10px 14px', fontSize:'0.78rem', color:'#94a3b8' }}>
        CRM情報を読み込み中…
      </div>
    );
  }
  if (!data.customer) {
    return (
      <div style={{ background:'#fffbeb', borderRadius:12, marginBottom:12, border:'1px solid #fde68a', padding:'10px 14px', fontSize:'0.78rem', color:'#92400e' }}>
        ⚠️ CRM上に「{data.company_name || 'この会社'}」の顧客レコードが見つかりません
      </div>
    );
  }

  const c = data.customer;
  const dealsLatest = data.deals?.[0];

  return (
    <div style={{ background:'#fff', borderRadius:12, marginBottom:12, boxShadow:'0 1px 3px rgba(0,0,0,0.04)', border:'1px solid #e2e8f0', overflow:'hidden' }}>
      {/* ヘッダー（クリックで展開） */}
      <div onClick={() => setOpen(!open)}
        style={{ padding:'10px 14px', cursor:'pointer', display:'flex', alignItems:'center', gap:10, background:open?'linear-gradient(90deg,#eef2ff,#fff)':'#fafbfc', borderBottom: open?'1px solid #e2e8f0':'none' }}>
        <span style={{ width:4, height:16, background:'#6366f1', borderRadius:2 }} />
        <span style={{ fontWeight:800, fontSize:'0.86rem', color:'#0f172a' }}>📇 CRM情報</span>
        {c.inrevo_person && <span style={{ fontSize:'0.66rem', padding:'2px 8px', borderRadius:99, background:'#ede9fe', color:'#6d28d9', fontWeight:700 }}>👤 {c.inrevo_person}</span>}
        {c.industry && <span style={{ fontSize:'0.66rem', padding:'2px 8px', borderRadius:99, background:'#f1f5f9', color:'#475569' }}>{c.industry}</span>}
        {c.prefecture && <span style={{ fontSize:'0.66rem', padding:'2px 8px', borderRadius:99, background:'#f1f5f9', color:'#475569' }}>{c.prefecture}</span>}
        {c.employee_count && <span style={{ fontSize:'0.66rem', padding:'2px 8px', borderRadius:99, background:'#f1f5f9', color:'#475569' }}>{c.employee_count}名</span>}
        <span style={{ marginLeft:'auto', fontSize:'0.7rem', color:'#64748b' }}>案件 {data.deals.length}件</span>
        <span style={{ color:'#94a3b8', fontSize:'0.8rem' }}>{open ? '▼' : '▶'}</span>
      </div>

      {/* 直近案件（ヘッダー直下に常時表示で1件） */}
      {!open && dealsLatest && (
        <div style={{ padding:'8px 14px', borderTop:'1px dashed #f1f5f9', display:'flex', alignItems:'center', gap:8, fontSize:'0.76rem' }}>
          <span style={{ color:'#94a3b8', fontSize:'0.66rem' }}>最新案件:</span>
          <span style={{ fontWeight:700, color:'#0f172a', flex:1, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{dealsLatest.name}</span>
          {dealsLatest.yomi && <span style={{ fontSize:'0.64rem', fontWeight:700, padding:'1px 6px', borderRadius:4, color:yomiColor(dealsLatest.yomi), background:`${yomiColor(dealsLatest.yomi)}15` }}>{dealsLatest.yomi}</span>}
          <span style={{ fontSize:'0.64rem', fontWeight:700, padding:'1px 6px', borderRadius:4, background:statusBg(dealsLatest.status), color:statusColor(dealsLatest.status) }}>{statusLabel(dealsLatest.status)}</span>
          {dealsLatest.contract_type && <span style={{ fontSize:'0.64rem', color:'#64748b' }}>{dealsLatest.contract_type}</span>}
          {(dealsLatest.initial_fee || dealsLatest.monthly_fee) && <span style={{ fontWeight:700, color:'#059669', fontSize:'0.76rem' }}>{fmtY(dealsLatest.initial_fee || dealsLatest.monthly_fee)}</span>}
        </div>
      )}

      {/* 展開: 詳細 */}
      {open && (
        <div style={{ padding:'12px 14px' }}>
          {/* 顧客の基本情報 */}
          <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fit,minmax(220px,1fr))', gap:8, marginBottom:12 }}>
            {c.business_description && (
              <div style={{ gridColumn:'1 / -1', padding:'8px 10px', background:'#f8fafc', borderRadius:6, fontSize:'0.74rem', color:'#475569', whiteSpace:'pre-wrap', maxHeight:90, overflowY:'auto' }}>
                <div style={{ fontSize:'0.62rem', color:'#94a3b8', fontWeight:700, marginBottom:2 }}>事業内容</div>
                {c.business_description}
              </div>
            )}
            {c.address && (
              <div style={{ padding:'6px 10px', background:'#f8fafc', borderRadius:6, fontSize:'0.72rem' }}>
                <div style={{ fontSize:'0.62rem', color:'#94a3b8', fontWeight:700 }}>住所</div>
                <div style={{ color:'#0f172a' }}>{c.address}</div>
              </div>
            )}
            {c.inflow_source && (
              <div style={{ padding:'6px 10px', background:'#f8fafc', borderRadius:6, fontSize:'0.72rem' }}>
                <div style={{ fontSize:'0.62rem', color:'#94a3b8', fontWeight:700 }}>流入経路</div>
                <div style={{ color:'#0f172a' }}>{c.inflow_source}{c.inflow_date ? ` (${String(c.inflow_date).slice(0,10)})` : ''}</div>
              </div>
            )}
            {c.website && (
              <div style={{ padding:'6px 10px', background:'#f8fafc', borderRadius:6, fontSize:'0.72rem' }}>
                <div style={{ fontSize:'0.62rem', color:'#94a3b8', fontWeight:700 }}>HP</div>
                <a href={c.website} target="_blank" rel="noreferrer" style={{ color:'#2563eb', textDecoration:'none' }}>{c.website}</a>
              </div>
            )}
          </div>

          {/* CRMリンク */}
          <div style={{ marginBottom:12 }}>
            <a href={`/dashboard/crm/customers/${c.id}`} target="_blank" rel="noreferrer"
              style={{ display:'inline-block', fontSize:'0.74rem', padding:'4px 10px', background:'#eef2ff', color:'#4338ca', textDecoration:'none', borderRadius:6, fontWeight:700 }}>
              ↗ CRMで開く
            </a>
          </div>

          {/* 担当者 */}
          {data.contacts?.length > 0 && (
            <div style={{ marginBottom:12 }}>
              <div style={{ fontSize:'0.7rem', fontWeight:700, color:'#475569', marginBottom:4 }}>担当者</div>
              <div style={{ display:'flex', flexDirection:'column', gap:3 }}>
                {data.contacts.map((p, i) => (
                  <div key={i} style={{ padding:'4px 8px', background:'#fafbfc', borderRadius:5, fontSize:'0.72rem', display:'flex', gap:8, alignItems:'center' }}>
                    <span style={{ fontWeight:700, color:'#0f172a' }}>{[p.last_name, p.first_name].filter(Boolean).join(' ') || '—'}</span>
                    {p.position_title && <span style={{ color:'#64748b' }}>{p.position_title}</span>}
                    {p.department && <span style={{ color:'#94a3b8' }}>{p.department}</span>}
                    {p.email && <span style={{ color:'#2563eb', marginLeft:'auto', fontSize:'0.66rem' }}>{p.email}</span>}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* 案件履歴 */}
          {data.deals?.length > 0 ? (
            <div>
              <div style={{ fontSize:'0.7rem', fontWeight:700, color:'#475569', marginBottom:4 }}>案件履歴（最新{data.deals.length}件）</div>
              <div style={{ display:'flex', flexDirection:'column', gap:4 }}>
                {data.deals.map(d => (
                  <div key={d.id} style={{ padding:'6px 10px', background:'#fafbfc', borderRadius:6, border:'1px solid #f1f5f9' }}>
                    <div style={{ display:'flex', alignItems:'center', gap:6 }}>
                      <span style={{ fontWeight:700, fontSize:'0.78rem', color:'#0f172a', flex:1, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{d.name}</span>
                      {d.yomi && <span style={{ fontSize:'0.62rem', fontWeight:700, padding:'1px 6px', borderRadius:4, color:yomiColor(d.yomi), background:`${yomiColor(d.yomi)}15` }}>{d.yomi}</span>}
                      <span style={{ fontSize:'0.62rem', fontWeight:700, padding:'1px 6px', borderRadius:4, background:statusBg(d.status), color:statusColor(d.status) }}>{statusLabel(d.status)}</span>
                    </div>
                    <div style={{ fontSize:'0.68rem', color:'#64748b', marginTop:2, display:'flex', gap:8, flexWrap:'wrap' }}>
                      {d.contract_type && <span>📋 {d.contract_type}</span>}
                      {d.sales_person && <span>👤 {d.sales_person}</span>}
                      {(d.initial_fee || d.monthly_fee) && <span style={{ color:'#059669', fontWeight:700 }}>{fmtY(d.initial_fee || d.monthly_fee)}</span>}
                      {d.order_date && <span style={{ color:'#94a3b8' }}>受注 {String(d.order_date).slice(0,10)}</span>}
                      {d.first_meeting_date && !d.order_date && <span style={{ color:'#94a3b8' }}>初回 {String(d.first_meeting_date).slice(0,10)}</span>}
                    </div>
                    {/* BANT サマリー */}
                    {(d.bant_budget || d.bant_authority || d.bant_needs || d.bant_timeframe) && (
                      <div style={{ marginTop:4, display:'flex', gap:4, flexWrap:'wrap' }}>
                        {d.bant_budget    && <span style={{ fontSize:'0.6rem', padding:'1px 5px', borderRadius:3, background:'#f0f9ff', color:'#0369a1' }}>B:{d.bant_budget}</span>}
                        {d.bant_authority && <span style={{ fontSize:'0.6rem', padding:'1px 5px', borderRadius:3, background:'#f0fdf4', color:'#15803d' }}>A:{d.bant_authority}</span>}
                        {d.bant_needs     && <span style={{ fontSize:'0.6rem', padding:'1px 5px', borderRadius:3, background:'#fef3c7', color:'#92400e' }}>N:{d.bant_needs}</span>}
                        {d.bant_timeframe && <span style={{ fontSize:'0.6rem', padding:'1px 5px', borderRadius:3, background:'#fae8ff', color:'#7e22ce' }}>T:{d.bant_timeframe}</span>}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div style={{ fontSize:'0.72rem', color:'#94a3b8', padding:8 }}>関連案件なし</div>
          )}
        </div>
      )}
    </div>
  );
}

// Slackスレッドカード（読み込み＋返信）
function SlackThreadCard({ studyId, slackLink }) {
  const [messages, setMessages] = useState(null);
  const [reply, setReply] = useState('');
  const [posting, setPosting] = useState(false);

  const reload = async () => {
    try {
      const r = await api.anStudySlackThread(studyId);
      setMessages(r.messages || []);
    } catch { setMessages([]); }
  };
  useEffect(() => { reload(); /* eslint-disable-next-line */ }, [studyId]);

  const send = async () => {
    if (!reply.trim()) return;
    setPosting(true);
    try {
      await api.anStudySlackReply(studyId, reply.trim());
      setReply('');
      await reload();
    } catch (e) { alert('返信失敗: ' + e.message); }
    finally { setPosting(false); }
  };

  const fmtTs = (ts) => {
    if (!ts) return '';
    const d = new Date(Number(ts) * 1000);
    return d.toLocaleString('ja-JP', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  };

  return (
    <Card title={`Slackスレッド${messages ? `（${messages.length}件）` : ''}`} accent="#4a154b"
      action={slackLink && <a href={slackLink} target="_blank" rel="noreferrer" style={{ fontSize: '0.7rem', color: '#2563eb', textDecoration: 'none', fontWeight: 600 }}>↗ Slackで開く</a>}>
      {messages === null ? (
        <div style={{ padding: 16, textAlign: 'center', color: '#94a3b8', fontSize: '0.78rem' }}>読み込み中…</div>
      ) : messages.length === 0 ? (
        <div style={{ padding: 16, textAlign: 'center', color: '#94a3b8', fontSize: '0.78rem' }}>メッセージなし</div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8, maxHeight: 360, overflowY: 'auto' }}>
          {messages.map((m, i) => (
            <div key={i} style={{ background: i === 0 ? '#fffbeb' : '#f8fafc', borderRadius: 8, padding: '8px 10px', border: '1px solid ' + (i === 0 ? '#fde68a' : '#e2e8f0') }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                <span style={{ fontWeight: 700, fontSize: '0.76rem', color: '#0f172a' }}>{m.user_name || m.username || m.user || '(bot)'}</span>
                {i === 0 && <span style={{ fontSize: '0.62rem', padding: '1px 6px', borderRadius: 99, background: '#fde68a', color: '#92400e', fontWeight: 700 }}>親メッセージ</span>}
                <span style={{ marginLeft: 'auto', fontSize: '0.66rem', color: '#94a3b8' }}>{fmtTs(m.ts)}</span>
              </div>
              <div style={{ fontSize: '0.8rem', color: '#334155', whiteSpace: 'pre-wrap', lineHeight: 1.5 }}>{m.text}</div>
            </div>
          ))}
        </div>
      )}
      <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px dashed #e2e8f0' }}>
        <textarea value={reply} onChange={e => setReply(e.target.value)} rows={2}
          placeholder="スレッドへ返信（Slack に投稿されます）"
          style={{ width: '100%', boxSizing: 'border-box', padding: '6px 10px', border: '1px solid #cbd5e1', borderRadius: 6, fontSize: '0.8rem', resize: 'vertical' }} />
        <div style={{ marginTop: 4, display: 'flex', justifyContent: 'flex-end' }}>
          <button onClick={send} disabled={posting || !reply.trim()}
            style={{ fontSize: '0.74rem', fontWeight: 700, padding: '5px 14px', borderRadius: 6, border: 'none', background: '#4a154b', color: '#fff', cursor: posting || !reply.trim() ? 'not-allowed' : 'pointer', opacity: posting || !reply.trim() ? 0.5 : 1 }}>
            {posting ? '送信中…' : '返信する'}
          </button>
        </div>
      </div>
    </Card>
  );
}

// 直接編集可能なtextarea（フォーカス外しで自動保存）
function InlineTextarea({ value, placeholder, border, onSave }) {
  const [local, setLocal] = useState(value || '');
  const [focused, setFocused] = useState(false);
  const [saving, setSaving] = useState(false);
  useEffect(() => { setLocal(value || ''); }, [value]);
  const save = async () => {
    if (local === (value || '')) { setFocused(false); return; }
    setSaving(true);
    await onSave(local);
    setSaving(false);
    setFocused(false);
  };
  return (
    <div>
      <textarea value={local} onChange={e => setLocal(e.target.value)}
        onFocus={() => setFocused(true)} onBlur={save}
        placeholder={placeholder}
        rows={focused ? 8 : Math.max(5, Math.min(10, (local.split('\n').length || 1) + 1))}
        style={{ width: '100%', boxSizing: 'border-box', padding: '8px 10px',
          border: `1.5px solid ${focused ? border : '#e2e8f0'}`,
          borderRadius: 6, fontSize: '0.82rem', resize: 'vertical', lineHeight: 1.6,
          background: focused ? '#fff' : '#fafbfc', transition: 'border-color 0.15s' }} />
      {focused && (
        <div style={{ fontSize: '0.62rem', color: '#94a3b8', marginTop: 2 }}>
          {saving ? '保存中…' : 'フォーカスを外すと自動保存'}
        </div>
      )}
    </div>
  );
}

const S_panel = {
  root: { flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', background: '#f8fafc' },
  header: { padding: '14px 20px', color: '#fff', display: 'flex', alignItems: 'center', gap: 12 },
  backBtn: { background: 'rgba(255,255,255,0.2)', border: 'none', color: '#fff', width: 30, height: 30, borderRadius: 8, cursor: 'pointer', fontSize: '1rem' },
  statCell: { background: '#fafbfc', borderRadius: 8, padding: '6px 10px', border: '1px solid #f1f5f9' },
  statLabel: { fontSize: '0.65rem', color: '#64748b', marginBottom: 2 },
  statVal: { fontSize: '0.84rem', fontWeight: 700, color: '#0f172a', wordBreak: 'break-all' },
  link: { color: '#2563eb', textDecoration: 'none', fontWeight: 600 },
  preBox: { fontSize: '0.8rem', color: '#334155', whiteSpace: 'pre-wrap', margin: 0, lineHeight: 1.6 },
  editBtn: { fontSize: '0.7rem', padding: '3px 10px', borderRadius: 5, border: '1px solid #cbd5e1', background: '#fff', color: '#475569', cursor: 'pointer', fontWeight: 600 },
  cancelBtn: { fontSize: '0.7rem', padding: '3px 10px', borderRadius: 5, border: '1px solid #cbd5e1', background: '#fff', color: '#64748b', cursor: 'pointer' },
  saveBtn: { fontSize: '0.74rem', fontWeight: 700, padding: '5px 14px', borderRadius: 6, border: 'none', background: '#1e40af', color: '#fff', cursor: 'pointer', marginTop: 6 },
  addBtn: { fontSize: '0.7rem', padding: '3px 10px', borderRadius: 5, border: '1px solid #67e8f9', background: '#ecfeff', color: '#0e7490', cursor: 'pointer', fontWeight: 700 },
};


// ── 媒体マスタ 詳細＋編集 ─────────────────────────
function MediaMasterDetail({ media, onClose, onUpdate, onDelete }) {
  const [editing, setEditing] = useState(false);
  const [form, setForm] = useState({
    name: media.name || '',
    vendor_url: media.vendor_url || '',
    recommend_score: media.recommend_score || '',
    service_type: media.service_type || '',
    hire_methods: (media.hire_methods || []).join(', '),
    areas: (media.areas || []).join(', '),
    employment_types: (media.employment_types || []).join(', '),
    age_targets: (media.age_targets || []).join(', '),
    industries: (media.industries || []).join(', '),
    job_types: (media.job_types || []).join(', '),
    basic_billing: media.basic_billing || '',
    norma: media.norma || '',
    notes: media.notes || '',
    caution: media.caution || '',
  });
  const [saving, setSaving] = useState(false);

  const save = async () => {
    setSaving(true);
    try {
      const r = await api.mediaMasterUpdate(media.record_id, {
        ...form,
        recommend_score: form.recommend_score === '' ? null : Number(form.recommend_score),
      });
      onUpdate && onUpdate(r.media);
      setEditing(false);
    } catch (e) { alert('保存失敗: ' + e.message); }
    finally { setSaving(false); }
  };

  const del = async () => {
    if (!window.confirm(`媒体「${media.name}」を削除しますか？`)) return;
    try { await api.mediaMasterDelete(media.record_id); onDelete && onDelete(); }
    catch (e) { alert('削除失敗: ' + e.message); }
  };

  const FieldRow = ({ label, k, type = 'text' }) => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 3, marginBottom: 8 }}>
      <label style={{ fontSize: '0.66rem', color: '#64748b', fontWeight: 600 }}>{label}</label>
      <input type={type} value={form[k]} onChange={e => setForm(f => ({ ...f, [k]: e.target.value }))}
        style={{ padding: '6px 10px', border: '1px solid #cbd5e1', borderRadius: 6, fontSize: '0.82rem' }} />
    </div>
  );
  const AreaField = ({ label, k }) => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 3, marginBottom: 8 }}>
      <label style={{ fontSize: '0.66rem', color: '#64748b', fontWeight: 600 }}>{label}<span style={{ color: '#94a3b8', fontWeight: 400, marginLeft: 4 }}>(カンマ区切り)</span></label>
      <input value={form[k]} onChange={e => setForm(f => ({ ...f, [k]: e.target.value }))}
        style={{ padding: '6px 10px', border: '1px solid #cbd5e1', borderRadius: 6, fontSize: '0.82rem' }} />
    </div>
  );

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.4)', zIndex: 1100, display: 'flex', justifyContent: 'flex-end' }} onClick={onClose}>
      <div style={{ width: 'min(560px,94vw)', height: '100%', background: '#fff', boxShadow: '-8px 0 32px rgba(0,0,0,0.2)', overflowY: 'auto' }} onClick={e => e.stopPropagation()}>
        <div style={{ padding: '14px 20px', borderBottom: '1px solid #e2e8f0', display: 'flex', justifyContent: 'space-between', alignItems: 'center', background: '#fafbfc' }}>
          <div>
            <div style={{ fontWeight: 800, fontSize: '1rem', color: '#0f172a' }}>{media.name}</div>
            {media.recommend_score > 0 && <div style={{ fontSize: '0.8rem', color: '#d97706', fontWeight: 700 }}>{'★'.repeat(media.recommend_score)}</div>}
          </div>
          <div style={{ display: 'flex', gap: 4 }}>
            {!editing && <button onClick={() => setEditing(true)} style={{ fontSize: '0.72rem', padding: '4px 10px', borderRadius: 6, border: '1px solid #93c5fd', background: '#eff6ff', color: '#1d4ed8', fontWeight: 700, cursor: 'pointer' }}>編集</button>}
            {!editing && <button onClick={del} style={{ fontSize: '0.72rem', padding: '4px 10px', borderRadius: 6, border: '1px solid #fca5a5', background: '#fef2f2', color: '#dc2626', fontWeight: 700, cursor: 'pointer' }}>削除</button>}
            <button onClick={onClose} style={{ background: '#f1f5f9', border: 'none', width: 28, height: 28, borderRadius: 8, cursor: 'pointer', fontSize: 16 }}>×</button>
          </div>
        </div>

        <div style={{ padding: '14px 20px' }}>
          {editing ? (
            <>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                <FieldRow label="媒体名" k="name" />
                <FieldRow label="公式HP" k="vendor_url" />
                <FieldRow label="種別" k="service_type" />
                <FieldRow label="オススメ度 (1-5)" k="recommend_score" type="number" />
                <FieldRow label="基本請求先" k="basic_billing" />
                <FieldRow label="ノルマ" k="norma" />
              </div>
              <AreaField label="採用手法" k="hire_methods" />
              <AreaField label="エリア" k="areas" />
              <AreaField label="対象区分" k="employment_types" />
              <AreaField label="利用者年齢層" k="age_targets" />
              <AreaField label="業種" k="industries" />
              <AreaField label="職種" k="job_types" />
              <div style={{ display: 'flex', flexDirection: 'column', gap: 3, marginBottom: 8 }}>
                <label style={{ fontSize: '0.66rem', color: '#64748b', fontWeight: 600 }}>媒体備考</label>
                <textarea value={form.notes} onChange={e => setForm(f => ({ ...f, notes: e.target.value }))} rows={3}
                  style={{ padding: '6px 10px', border: '1px solid #cbd5e1', borderRadius: 6, fontSize: '0.82rem', resize: 'vertical' }} />
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 3, marginBottom: 8 }}>
                <label style={{ fontSize: '0.66rem', color: '#dc2626', fontWeight: 600 }}>注意事項</label>
                <textarea value={form.caution} onChange={e => setForm(f => ({ ...f, caution: e.target.value }))} rows={3}
                  style={{ padding: '6px 10px', border: '1px solid #fca5a5', borderRadius: 6, fontSize: '0.82rem', resize: 'vertical' }} />
              </div>
              <div style={{ marginTop: 10, display: 'flex', gap: 6, justifyContent: 'flex-end' }}>
                <button onClick={() => setEditing(false)} style={{ padding: '6px 14px', fontSize: '0.78rem', borderRadius: 6, border: '1px solid #cbd5e1', background: '#fff', cursor: 'pointer' }}>キャンセル</button>
                <button onClick={save} disabled={saving} style={{ padding: '6px 18px', fontSize: '0.78rem', fontWeight: 700, borderRadius: 6, border: 'none', background: '#1e40af', color: '#fff', cursor: 'pointer' }}>{saving ? '保存中…' : '保存'}</button>
              </div>
            </>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12, fontSize: '0.82rem' }}>
              {media.vendor_url && <div><a href={media.vendor_url} target="_blank" rel="noreferrer" style={{ color: '#2563eb' }}>{media.vendor_url}</a></div>}
              {[
                ['種別', media.service_type],
                ['採用手法', (media.hire_methods||[]).join(', ')],
                ['エリア', (media.areas||[]).join(', ')],
                ['対象区分', (media.employment_types||[]).join(', ')],
                ['利用者年齢層', (media.age_targets||[]).join(', ')],
                ['業種', (media.industries||[]).join(', ')],
                ['職種', (media.job_types||[]).join(', ')],
                ['基本請求先', media.basic_billing],
                ['ノルマ', media.norma],
              ].filter(([,v]) => v).map(([k,v]) => (
                <div key={k} style={{ display: 'flex', gap: 12 }}>
                  <div style={{ width: 100, color: '#64748b', fontSize: '0.72rem' }}>{k}</div>
                  <div style={{ flex: 1, color: '#0f172a', wordBreak: 'break-all' }}>{v}</div>
                </div>
              ))}
              {media.notes && (
                <div>
                  <div style={{ fontSize: '0.74rem', fontWeight: 700, color: '#475569', marginBottom: 4 }}>媒体備考</div>
                  <pre style={{ fontSize: '0.78rem', whiteSpace: 'pre-wrap', background: '#f8fafc', padding: '8px 10px', borderRadius: 6, margin: 0 }}>{media.notes}</pre>
                </div>
              )}
              {media.caution && (
                <div>
                  <div style={{ fontSize: '0.74rem', fontWeight: 700, color: '#dc2626', marginBottom: 4 }}>注意事項</div>
                  <pre style={{ fontSize: '0.78rem', whiteSpace: 'pre-wrap', background: '#fef2f2', padding: '8px 10px', borderRadius: 6, margin: 0, color: '#7f1d1d' }}>{media.caution}</pre>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
