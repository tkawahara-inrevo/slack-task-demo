import { useEffect, useState } from 'react';
import { api } from '../../api/client';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, LabelList } from 'recharts';

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
  const [mediaFacets, setMediaFacets] = useState({ industries: [], prefectures: [], hire_types: [], size_buckets: [] });
  const [mediaFilters, setMediaFilters] = useState({ industry: '', hire_type: '', prefecture: '', size_bucket: '' });
  const [mediaLoading, setMediaLoading] = useState(false);
  const [expandedMedia, setExpandedMedia] = useState({});
  const [mediaRoi, setMediaRoi] = useState(null);
  const [mediaDash, setMediaDash] = useState(null);
  const [unified, setUnified] = useState([]);
  const [unifiedCounts, setUnifiedCounts] = useState({ total: 0, slack_total: 0, kintone_total: 0 });
  const [searchQ, setSearchQ] = useState('');
  const [studyDetail, setStudyDetail] = useState(null);

  const load = async () => {
    setLoading(true);
    try {
      const apiStatus = filterStatus === 'pending' ? 'pending' : filterStatus === 'answered' ? 'done' : '';
      const r = await api.anUnified({ status: apiStatus, q: searchQ });
      setUnified(r.rows || []);
      setUnifiedCounts(r.counts || { total: 0, slack_total: 0, kintone_total: 0 });
    } catch { setUnified([]); }
    finally { setLoading(false); }
  };

  useEffect(() => { load(); }, [filterStatus]); // eslint-disable-line
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
      if (mediaRoi === null) api.anMediaRoi().then(r => setMediaRoi(r.rows || [])).catch(() => setMediaRoi([]));
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
          <button onClick={async () => {
              const daysStr = window.prompt('過去何日分を取り込みますか？（1〜365）', '90');
              if (!daysStr) return;
              const days = Math.max(1, Math.min(365, Number(daysStr) || 30));
              try {
                const r = await api.anBackfill(days);
                alert(`取り込み完了\nスキャン: ${r.scanned}件\n新規登録: ${r.inserted}件`);
                load();
              } catch (e) { alert('取り込み失敗: ' + e.message); }
            }}
            title="Slackチャンネルの過去メッセージから依頼を取り込む"
            style={{ padding: '3px 10px', fontSize: '0.74rem', fontWeight: 600, borderRadius: 5, border: '1px solid var(--gray-300)', background: 'var(--surface)', color: 'var(--gray-600)', cursor: 'pointer' }}>
            📥 Slackから取り込み
          </button>
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
            <div style={{ padding: '16px 20px', background: '#f8fafc', minHeight: '100%' }}>
              {/* KPIカード */}
              {mediaDash?.kpi && (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(160px,1fr))', gap: 10, marginBottom: 14 }}>
                  {[
                    { label: '総調査件数', value: mediaDash.kpi.total_studies?.toLocaleString() || 0, sub: `うち完了 ${mediaDash.kpi.done_studies || 0}`, color: '#3b82f6', icon: '📚' },
                    { label: '利用媒体数', value: mediaDash.kpi.unique_media?.toLocaleString() || 0, sub: `延べ ${mediaDash.kpi.total_slots || 0}件`, color: '#0891b2', icon: '📡' },
                    { label: '平均料金', value: mediaDash.kpi.avg_fee ? `¥${Math.round(mediaDash.kpi.avg_fee/10000).toLocaleString()}万` : '—', sub: '媒体スロット平均', color: '#059669', icon: '💰' },
                    { label: '予測精度', value: mediaDash.kpi.forecast_accuracy_pct != null ? `${mediaDash.kpi.forecast_accuracy_pct}%` : '—', sub: '実応募 / 予測応募', color: mediaDash.kpi.forecast_accuracy_pct >= 80 ? '#059669' : mediaDash.kpi.forecast_accuracy_pct >= 50 ? '#d97706' : '#dc2626', icon: '🎯' },
                  ].map(k => (
                    <div key={k.label} style={{ background: '#fff', borderRadius: 10, padding: '12px 14px', boxShadow: '0 1px 3px rgba(0,0,0,0.05)', border: '1px solid #e2e8f0', position: 'relative', overflow: 'hidden' }}>
                      <div style={{ position: 'absolute', top: 8, right: 10, fontSize: '1rem', opacity: 0.4 }}>{k.icon}</div>
                      <div style={{ fontSize: '0.66rem', color: '#64748b', fontWeight: 600 }}>{k.label}</div>
                      <div style={{ fontSize: '1.4rem', fontWeight: 800, color: k.color, marginTop: 2 }}>{k.value}</div>
                      <div style={{ fontSize: '0.62rem', color: '#94a3b8', marginTop: 1 }}>{k.sub}</div>
                    </div>
                  ))}
                </div>
              )}

              {/* 採用数 TOP10 媒体（バーチャート） */}
              {mediaDash?.top_media?.length > 0 && (
                <div style={{ background: '#fff', borderRadius: 10, padding: '14px 16px', boxShadow: '0 1px 3px rgba(0,0,0,0.05)', border: '1px solid #e2e8f0', marginBottom: 14 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 10 }}>
                    <span style={{ width: 4, height: 16, background: '#3b82f6', borderRadius: 2 }} />
                    <span style={{ fontWeight: 800, fontSize: '0.86rem', color: '#0f172a' }}>媒体別 累計実応募 TOP10</span>
                  </div>
                  <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={mediaDash.top_media} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                      <XAxis dataKey="media_name" tick={{ fontSize: 10, fill: '#64748b' }} interval={0} angle={-20} textAnchor="end" height={50} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fontSize: 10, fill: '#64748b' }} axisLine={false} tickLine={false} width={44} />
                      <Tooltip contentStyle={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, fontSize: 11 }} />
                      <Bar dataKey="total_effective" radius={[6, 6, 0, 0]} name="実応募合計">
                        <LabelList dataKey="total_effective" position="top" style={{ fontSize: 9, fill: '#64748b' }} />
                        {mediaDash.top_media.map((_, i) => <Cell key={i} fill={i===0?'#1d4ed8':i<3?'#3b82f6':'#93c5fd'} />)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}

              {/* 媒体ROI（kintone App221 のAN調査から集計） */}
              {mediaRoi && mediaRoi.length > 0 && (
                <div style={{ marginBottom: 12, padding: '10px 12px', background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 8 }}>
                  <div style={{ fontSize: '0.78rem', fontWeight: 700, color: '#92400e', marginBottom: 6 }}>
                    📈 媒体ROI（AN調査ベース・応募予測精度）
                  </div>
                  <div style={{ overflowX: 'auto' }}>
                    <table style={{ width: '100%', fontSize: '0.72rem', borderCollapse: 'collapse' }}>
                      <thead><tr style={{ color: 'var(--gray-500)', textAlign: 'left' }}>
                        <th style={{ padding: '4px 6px' }}>媒体</th>
                        <th style={{ padding: '4px 6px', textAlign: 'right' }}>調査件数</th>
                        <th style={{ padding: '4px 6px', textAlign: 'right' }}>平均料金</th>
                        <th style={{ padding: '4px 6px', textAlign: 'right' }}>予測/実応募(平均)</th>
                        <th style={{ padding: '4px 6px', textAlign: 'right' }}>予測精度</th>
                      </tr></thead>
                      <tbody>
                        {mediaRoi.slice(0, 20).map((r, i) => (
                          <tr key={i} style={{ borderTop: '1px solid var(--gray-100)' }}>
                            <td style={{ padding: '4px 6px', fontWeight: 600 }}>{r.media_name}</td>
                            <td style={{ padding: '4px 6px', textAlign: 'right' }}>{r.cases}</td>
                            <td style={{ padding: '4px 6px', textAlign: 'right' }}>{r.avg_fee ? fmtYen(r.avg_fee) : '—'}</td>
                            <td style={{ padding: '4px 6px', textAlign: 'right' }}>
                              {r.avg_expected ?? '—'} / <b style={{ color: '#059669' }}>{r.avg_effective ?? '—'}</b>
                            </td>
                            <td style={{ padding: '4px 6px', textAlign: 'right', fontWeight: 700, color: r.forecast_accuracy_pct >= 80 ? '#059669' : r.forecast_accuracy_pct >= 50 ? '#d97706' : '#dc2626' }}>
                              {r.forecast_accuracy_pct != null ? `${r.forecast_accuracy_pct}%` : '—'}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div style={{ fontSize: '0.66rem', color: 'var(--gray-400)', marginTop: 4 }}>
                    予測精度 = 実応募の平均 ÷ 予測応募の平均 × 100%
                  </div>
                </div>
              )}

              {/* フィルタバー */}
              <div style={{ marginBottom: 10, padding: '8px 10px', background: 'var(--surface)', border: '1px solid var(--gray-200)', borderRadius: 8, display: 'grid', gridTemplateColumns: 'repeat(2,1fr)', gap: 6 }}>
                {[
                  ['industry',    '業界',     mediaFacets.industries.map(v => [v,v])],
                  ['hire_type',   '雇用形態', mediaFacets.hire_types.map(v => [v,v])],
                  ['prefecture',  'エリア',   mediaFacets.prefectures.map(v => [v,v])],
                  ['size_bucket', '採用規模', mediaFacets.size_buckets],
                ].map(([key, label, opts]) => (
                  <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                    <label style={{ fontSize: '0.66rem', color: 'var(--gray-500)', fontWeight: 600 }}>{label}</label>
                    <select value={mediaFilters[key]} onChange={e => setMediaFilters(f => ({ ...f, [key]: e.target.value }))}
                      style={{ padding: '4px 6px', fontSize: '0.76rem', border: '1px solid var(--gray-300)', borderRadius: 5, background: 'var(--surface)' }}>
                      <option value="">すべて</option>
                      {opts.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
                    </select>
                  </div>
                ))}
                {Object.values(mediaFilters).some(Boolean) && (
                  <button onClick={() => setMediaFilters({ industry:'', hire_type:'', prefecture:'', size_bucket:'' })}
                    style={{ gridColumn: '1 / -1', padding: '4px', fontSize: '0.72rem', background: 'transparent', border: '1px dashed var(--gray-300)', borderRadius: 5, color: 'var(--gray-500)', cursor: 'pointer' }}>
                    フィルタをクリア
                  </button>
                )}
              </div>

              {mediaLoading && <div style={{ textAlign: 'center', color: 'var(--gray-400)', padding: 24 }}>読み込み中...</div>}
              {!mediaLoading && mediaStats.length === 0 && <div style={{ textAlign: 'center', color: 'var(--gray-400)', padding: 24 }}>該当なし</div>}
              {mediaStats.map(s => {
                const expanded = expandedMedia[s.media_name];
                return (
                <div key={s.media_name} style={{ marginBottom: 8, background: 'var(--surface)', border: '1px solid var(--gray-200)', borderRadius: 8, padding: '10px 12px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                    <span style={{ fontWeight: 700, fontSize: '0.88rem', flex: 1 }}>{s.media_name}</span>
                    <button onClick={() => setExpandedMedia(m => ({ ...m, [s.media_name]: !m[s.media_name] }))}
                      style={{ fontSize: '0.7rem', color: '#2563eb', background: 'transparent', border: 'none', cursor: 'pointer', fontWeight: 600 }}>
                      {expanded ? '▼ 内訳を閉じる' : '▶ 内訳を見る'}
                    </button>
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 6 }}>
                    {[
                      ['掲載回数', `${s.campaigns}回`],
                      ['採用数', `${s.total_hired}人`],
                      ['採用率', s.campaigns > 0 ? `${Math.round(s.success_campaigns/s.campaigns*100)}%` : '—'],
                      ['総費用', fmtYen(s.total_cost)],
                      ['採用単価', s.cost_per_hire ? fmtYen(s.cost_per_hire) : '—'],
                      ['平均費用', fmtYen(s.avg_cost)],
                    ].map(([label, val]) => (
                      <div key={label} style={{ background: 'var(--surface-2)', borderRadius: 6, padding: '5px 8px' }}>
                        <div style={{ fontSize: '0.68rem', color: 'var(--gray-500)' }}>{label}</div>
                        <div style={{ fontSize: '0.84rem', fontWeight: 600, color: 'var(--gray-800)' }}>{val}</div>
                      </div>
                    ))}
                  </div>

                  {expanded && (
                    <div style={{ marginTop: 8, paddingTop: 8, borderTop: '1px dashed var(--gray-200)', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                      <div>
                        <div style={{ fontSize: '0.7rem', fontWeight: 700, color: 'var(--gray-600)', marginBottom: 4 }}>業界別</div>
                        {(s.by_industry || []).length === 0 && <div style={{ fontSize: '0.72rem', color: 'var(--gray-400)' }}>—</div>}
                        {(s.by_industry || []).map((b, i) => (
                          <div key={i} style={{ display: 'flex', fontSize: '0.74rem', padding: '2px 0', borderBottom: '1px solid var(--gray-100)' }}>
                            <span style={{ flex: 1, color: 'var(--gray-700)' }}>{b.industry}</span>
                            <span style={{ color: '#15803d', fontWeight: 600, minWidth: 36, textAlign: 'right' }}>{b.hired}人</span>
                            <span style={{ color: 'var(--gray-500)', minWidth: 60, textAlign: 'right' }}>{fmtYen(b.cost)}</span>
                          </div>
                        ))}
                      </div>
                      <div>
                        <div style={{ fontSize: '0.7rem', fontWeight: 700, color: 'var(--gray-600)', marginBottom: 4 }}>雇用形態別</div>
                        {(s.by_hire_type || []).length === 0 && <div style={{ fontSize: '0.72rem', color: 'var(--gray-400)' }}>—</div>}
                        {(s.by_hire_type || []).map((b, i) => (
                          <div key={i} style={{ display: 'flex', fontSize: '0.74rem', padding: '2px 0', borderBottom: '1px solid var(--gray-100)' }}>
                            <span style={{ flex: 1, color: 'var(--gray-700)' }}>{b.hire_type}</span>
                            <span style={{ color: '#15803d', fontWeight: 600, minWidth: 36, textAlign: 'right' }}>{b.hired}人</span>
                            <span style={{ color: 'var(--gray-500)', minWidth: 60, textAlign: 'right' }}>{fmtYen(b.cost)}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              );})}
            </div>
          )}
          {/* 媒体マスタビュー */}
          {view === 'master' && <MediaMasterView />}
          {/* AN依頼一覧ビュー（Slack依頼 + App221調査の統合） */}
          {view === 'list' && (
            <>
              <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--gray-100)', background: 'var(--surface-2)' }}>
                <input value={searchQ} onChange={e => setSearchQ(e.target.value)} placeholder="会社名・担当で検索…"
                  style={{ width: '100%', boxSizing: 'border-box', padding: '6px 10px', fontSize: '0.82rem', border: '1px solid var(--gray-300)', borderRadius: 6 }} />
                <div style={{ marginTop: 4, fontSize: '0.66rem', color: 'var(--gray-500)' }}>
                  全{unifiedCounts.total}件（Slack {unifiedCounts.slack_total} / kintone調査 {unifiedCounts.kintone_total}）
                </div>
              </div>
              {loading ? <div style={{ padding: 24, textAlign: 'center', color: 'var(--gray-400)' }}>読み込み中...</div>
              : unified.length === 0 ? <div style={{ padding: 24, textAlign: 'center', color: 'var(--gray-400)' }}>該当なし</div>
              : (() => {
                  // 同会社のカウント
                  const companyCount = {};
                  unified.forEach(r => { if (r.company_name) companyCount[r.company_name] = (companyCount[r.company_name]||0)+1; });
                  return unified.map(row => {
                    const key = `study:${row.source_id}`;
                    const isSelectedRow = selected && selected.source_id===row.source_id;
                    const isDone = ['完了','対応済','クローズ'].includes(row.status);
                    const statusColor = isDone ? '#059669' : '#d97706';
                    const statusBg    = isDone ? '#f0fdf4' : '#fffbeb';
                    const sameCount = row.company_name ? companyCount[row.company_name] : 0;
                    return (
                      <div key={key} onClick={() => openDetail(row)}
                        style={{ padding: '8px 12px', borderBottom: '1px solid var(--gray-100)', cursor: 'pointer',
                          background: isSelectedRow ? '#eff6ff' : 'var(--surface)' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
                          <span style={{ fontWeight: 700, fontSize: '0.82rem', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--gray-900)' }}>
                            {row.company_name || '(会社名なし)'}
                            {sameCount > 1 && <span style={{ marginLeft: 5, fontSize: '0.66rem', color: 'var(--gray-500)', fontWeight: 600 }}>({sameCount}件)</span>}
                          </span>
                          <span style={{ fontSize: '0.66rem', fontWeight: 700, padding: '1px 6px', borderRadius: 99, background: statusBg, color: statusColor }}>
                            {isDone ? '回答済' : (row.status || '未回答')}
                          </span>
                        </div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--gray-500)', display: 'flex', gap: 6, alignItems: 'center' }}>
                          {row.requester && <span>👤 {row.requester}</span>}
                          {row.request_type && <span style={{ color: 'var(--gray-600)' }}>{row.request_type}</span>}
                          {row.priority && <span style={{ color: '#dc2626', fontWeight: 700 }}>優先:{row.priority}</span>}
                          {row.media_count > 0 && <span style={{ color: '#0284c7' }}>📊 媒体{row.media_count}</span>}
                          <span style={{ marginLeft: 'auto', color: 'var(--gray-400)' }}>{row.requested_at ? fmtDate(row.requested_at) : '—'}</span>
                        </div>
                      </div>
                    );
                  });
                })()}
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
  const [mustOpen, setMustOpen] = useState(false);
  const [mustVal, setMustVal] = useState('');
  const [notesOpen, setNotesOpen] = useState(false);
  const [notesVal, setNotesVal] = useState('');
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

  const saveMust = async () => { await updateStudy({ must_condition: mustVal }); setMustOpen(false); };
  const saveNotes = async () => { await updateStudy({ other_notes: notesVal }); setNotesOpen(false); };

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
              {study.case_link && <a href={study.case_link} target="_blank" rel="noreferrer" style={S_panel.link}>📂 案件リンク</a>}
              {study.slack_link && <a href={study.slack_link} target="_blank" rel="noreferrer" style={S_panel.link}>💬 Slack</a>}
              {study.jobform_url && <a href={study.jobform_url} target="_blank" rel="noreferrer" style={S_panel.link}>📄 求人票</a>}
            </div>
          )}
        </Card>

        {/* MUST条件 */}
        <Card title="MUST条件" accent="#dc2626"
          action={!mustOpen ? <button onClick={() => { setMustVal(study.must_condition || ''); setMustOpen(true); }} style={S_panel.editBtn}>編集</button>
                            : <button onClick={() => setMustOpen(false)} style={S_panel.cancelBtn}>キャンセル</button>}>
          {mustOpen ? (
            <>
              <textarea value={mustVal} onChange={e => setMustVal(e.target.value)} rows={5}
                style={{ width: '100%', boxSizing: 'border-box', padding: '8px 10px', border: '1.5px solid #fca5a5', borderRadius: 6, fontSize: '0.82rem', resize: 'vertical', lineHeight: 1.5 }} />
              <button onClick={saveMust} style={S_panel.saveBtn}>保存</button>
            </>
          ) : (
            <pre style={S_panel.preBox}>{study.must_condition || '（未入力）'}</pre>
          )}
        </Card>

        {/* その他特記事項 */}
        <Card title="特記事項" accent="#7c3aed"
          action={!notesOpen ? <button onClick={() => { setNotesVal(study.other_notes || ''); setNotesOpen(true); }} style={S_panel.editBtn}>編集</button>
                             : <button onClick={() => setNotesOpen(false)} style={S_panel.cancelBtn}>キャンセル</button>}>
          {notesOpen ? (
            <>
              <textarea value={notesVal} onChange={e => setNotesVal(e.target.value)} rows={5}
                style={{ width: '100%', boxSizing: 'border-box', padding: '8px 10px', border: '1.5px solid #c4b5fd', borderRadius: 6, fontSize: '0.82rem', resize: 'vertical', lineHeight: 1.5 }} />
              <button onClick={saveNotes} style={S_panel.saveBtn}>保存</button>
            </>
          ) : (
            <pre style={S_panel.preBox}>{study.other_notes || '（未入力）'}</pre>
          )}
        </Card>

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
