import { useState, useEffect, useCallback, useRef } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend, LineChart, Line,
} from 'recharts';
import { api } from '../../api/client';

const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316','#84cc16','#ec4899','#6366f1'];

function KpiCard({ label, value, sub, color = '#3b82f6' }) {
  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 20px', minWidth: 140 }}>
      <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: '1.8rem', fontWeight: 800, color }}>{value}</div>
      {sub && <div style={{ fontSize: '0.7rem', color: '#9ca3af', marginTop: 2 }}>{sub}</div>}
    </div>
  );
}

function SectionTitle({ children }) {
  return <h3 style={{ margin: '24px 0 12px', fontSize: '0.9rem', fontWeight: 700, color: '#374151', borderBottom: '2px solid #e5e7eb', paddingBottom: 6 }}>{children}</h3>;
}

function statusColor(s) {
  if (!s) return '#9ca3af';
  if (s.includes('入社')) return '#10b981';
  if (s.includes('内定')) return '#3b82f6';
  if (s.includes('辞退') || s.includes('不採用') || s.includes('中止')) return '#ef4444';
  if (s.includes('通過')) return '#8b5cf6';
  return '#f59e0b';
}

const CHART_TICK_STYLE = { fontSize: 11, fill: '#6b7280' };

function fmtDate(d) {
  if (!d) return '—';
  return new Date(d).toLocaleDateString('ja-JP', { year: 'numeric', month: '2-digit', day: '2-digit' });
}

function DrilldownPanel({ filter, from, to, onClose }) {
  const [applicants, setApplicants] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api.hrmosApplicants({ from, to, ...filter })
      .then(d => setApplicants(d.applicants || []))
      .catch(() => setApplicants([]))
      .finally(() => setLoading(false));
  }, [filter, from, to]);

  const filterLabel = Object.entries(filter).map(([k, v]) => ({
    label: { label:'ラベル', status:'ステータス', job_name:'求人', source:'応募経路' }[k] || k, v,
  }));

  return (
    <>
      {/* オーバーレイ */}
      <div onClick={onClose} style={{
        position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.35)', zIndex: 1000,
      }} />
      {/* パネル */}
      <div style={{
        position: 'fixed', top: 0, right: 0, bottom: 0, width: 640, maxWidth: '95vw',
        background: '#fff', zIndex: 1001, display: 'flex', flexDirection: 'column',
        boxShadow: '-4px 0 24px rgba(0,0,0,0.15)',
      }}>
        {/* ヘッダー */}
        <div style={{ padding: '16px 20px', borderBottom: '1px solid #e5e7eb', display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: 2 }}>
              {filterLabel.map(f => `${f.label}: ${f.v}`).join(' / ')}
            </div>
            <div style={{ fontWeight: 700, fontSize: '0.95rem' }}>
              {loading ? '読み込み中...' : `${applicants.length}件`}
            </div>
          </div>
          <button onClick={onClose} style={{
            background: 'none', border: 'none', fontSize: '1.2rem', cursor: 'pointer', color: '#6b7280', padding: '4px 8px',
          }}>✕</button>
        </div>

        {/* テーブル */}
        <div style={{ flex: 1, overflowY: 'auto' }}>
          {loading
            ? <div style={{ padding: 24, color: '#9ca3af', textAlign: 'center' }}>読み込み中...</div>
            : applicants.length === 0
            ? <div style={{ padding: 24, color: '#9ca3af', textAlign: 'center' }}>該当者なし</div>
            : (
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
                <thead>
                  <tr style={{ background: '#f8fafc', position: 'sticky', top: 0 }}>
                    {['氏名', '求人', '応募日', '応募経路', 'ラベル', 'ステータス', '内定日', '入社日'].map(h => (
                      <th key={h} style={{ padding: '8px 10px', textAlign: 'left', fontWeight: 600, color: '#374151', whiteSpace: 'nowrap', borderBottom: '1px solid #e5e7eb' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {applicants.map((a, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid #f3f4f6', background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                      <td style={{ padding: '7px 10px', whiteSpace: 'nowrap', fontWeight: 500 }}>{a.applicant_name || '—'}</td>
                      <td style={{ padding: '7px 10px', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                          title={a.job_name}>{a.job_name || '—'}</td>
                      <td style={{ padding: '7px 10px', whiteSpace: 'nowrap', color: '#6b7280' }}>{fmtDate(a.applied_date)}</td>
                      <td style={{ padding: '7px 10px', whiteSpace: 'nowrap' }}>{a.source || '—'}</td>
                      <td style={{ padding: '7px 10px' }}>
                        {a.label ? <span style={{ background: '#eff6ff', color: '#1d4ed8', borderRadius: 4, padding: '2px 6px', fontSize: '0.72rem', whiteSpace: 'nowrap' }}>{a.label}</span> : '—'}
                      </td>
                      <td style={{ padding: '7px 10px', whiteSpace: 'nowrap' }}>
                        <span style={{ color: statusColor(a.status), fontWeight: 600 }}>{a.status || '—'}</span>
                      </td>
                      <td style={{ padding: '7px 10px', whiteSpace: 'nowrap', color: '#6b7280' }}>{fmtDate(a.offer_date)}</td>
                      <td style={{ padding: '7px 10px', whiteSpace: 'nowrap', color: '#6b7280' }}>{fmtDate(a.join_date)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )
          }
        </div>
      </div>
    </>
  );
}

export default function HrmosRecruitment() {
  const [summary, setSummary] = useState(null);
  const [analytics, setAnalytics] = useState(null);
  const [loading, setLoading] = useState(false);
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState(null);
  const [sheetUrl, setSheetUrl] = useState('');
  const [error, setError] = useState('');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [dragging, setDragging] = useState(false);
  const [drilldown, setDrilldown] = useState(null); // { label? status? job_name? source? }
  const fileRef = useRef();

  const openDrilldown = (filterKey, value) => setDrilldown({ [filterKey]: value });

  const loadSummary = useCallback(async () => {
    try { setSummary(await api.hrmosSummary()); } catch {}
  }, []);

  const calcGranularity = (f, t) => {
    if (!f || !t) return 'month';
    const days = (new Date(t) - new Date(f)) / 86400000;
    if (days <= 10) return 'day';
    if (days <= 29) return '3day';
    if (days <= 89) return 'week';
    return 'month';
  };

  const fetchAnalytics = useCallback(async (fromDate, toDate) => {
    setLoading(true);
    setError('');
    try {
      const granularity = calcGranularity(fromDate, toDate);
      const data = await api.hrmosAnalytics({ from: fromDate || '', to: toDate || '', granularity });
      setAnalytics(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  const loadAnalytics = useCallback(() => fetchAnalytics(from, to), [from, to, fetchAnalytics]);

  useEffect(() => { loadSummary(); fetchAnalytics('', ''); }, []);

  const handleImport = async (file) => {
    if (!file) return;
    setImporting(true);
    setImportResult(null);
    try {
      const result = await api.hrmosImportCsv(file);
      setImportResult(result);
      await loadSummary();
      await loadAnalytics();
    } catch (e) {
      setImportResult({ ok: false, error: e.message });
    } finally {
      setImporting(false);
    }
  };

  const onFileChange = (e) => handleImport(e.target.files?.[0]);

  const handleSheetImport = async () => {
    if (!sheetUrl.trim()) return;
    setImporting(true);
    setImportResult(null);
    try {
      const result = await api.hrmosImportSheet(sheetUrl.trim());
      setImportResult(result);
      await loadSummary();
      await fetchAnalytics(from, to);
    } catch (e) {
      setImportResult({ ok: false, error: e.message });
    } finally {
      setImporting(false);
    }
  };

  const onDrop = (e) => {
    e.preventDefault();
    setDragging(false);
    const file = e.dataTransfer.files?.[0];
    if (file && file.name.endsWith('.csv')) handleImport(file);
  };


  return (
    <div style={{ maxWidth: 1100 }}>
      <div style={{ marginBottom: 24 }}>
        <h2 style={{ margin: 0, fontSize: '1.1rem', fontWeight: 800, color: '#111827' }}>HRMOS採用 ダッシュボード</h2>
        {summary && (
          <p style={{ margin: '4px 0 0', fontSize: '0.8rem', color: '#6b7280' }}>
            累計 {summary.total?.toLocaleString()} 件 ／ 最終取り込み: {fmtDate(summary.latest_import)}
            {summary.earliest_date && ` ／ データ期間: ${fmtDate(summary.earliest_date)} 〜 ${fmtDate(summary.latest_date)}`}
          </p>
        )}
      </div>

      {/* インポートセクション */}
      <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: 20, marginBottom: 24 }}>
        <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 14 }}>データ取り込み</div>

        {/* スプシURL取り込み */}
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: '0.8rem', color: '#6b7280', marginBottom: 6 }}>Google スプレッドシートから取り込む（推奨）</div>
          <div style={{ display: 'flex', gap: 8 }}>
            <input
              type="text"
              value={sheetUrl}
              onChange={e => setSheetUrl(e.target.value)}
              placeholder="https://docs.google.com/spreadsheets/d/..."
              style={{ flex: 1, border: '1px solid #d1d5db', borderRadius: 6, padding: '7px 12px', fontSize: '0.85rem' }}
            />
            <button
              onClick={handleSheetImport}
              disabled={importing || !sheetUrl.trim()}
              style={{
                background: importing ? '#9ca3af' : '#10b981', color: '#fff', border: 'none',
                borderRadius: 6, padding: '7px 16px', fontSize: '0.85rem', cursor: importing ? 'not-allowed' : 'pointer', fontWeight: 600, whiteSpace: 'nowrap',
              }}
            >
              {importing ? '取り込み中...' : 'スプシから取り込む'}
            </button>
          </div>
          <div style={{ fontSize: '0.75rem', color: '#9ca3af', marginTop: 4 }}>
            ※ スプレッドシートをサービスアカウントに共有する必要があります
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
          <div style={{ flex: 1, height: 1, background: '#e5e7eb' }} />
          <span style={{ color: '#9ca3af', fontSize: '0.8rem' }}>または</span>
          <div style={{ flex: 1, height: 1, background: '#e5e7eb' }} />
        </div>

        {/* CSVアップロード */}
        <div style={{ fontSize: '0.8rem', color: '#6b7280', marginBottom: 6 }}>CSVファイルをアップロード</div>
        <div
          onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
          onClick={() => fileRef.current?.click()}
          style={{
            border: `2px dashed ${dragging ? '#3b82f6' : '#d1d5db'}`,
            borderRadius: 8, padding: '20px', textAlign: 'center',
            background: dragging ? '#eff6ff' : '#f9fafb',
            cursor: 'pointer', transition: 'all 0.15s',
          }}
        >
          <input ref={fileRef} type="file" accept=".csv" style={{ display: 'none' }} onChange={onFileChange} />
          {importing
            ? <span style={{ color: '#3b82f6', fontWeight: 600 }}>取り込み中...</span>
            : <span style={{ color: '#6b7280', fontSize: '0.85rem' }}>CSVをドロップ、またはクリックして選択</span>
          }
        </div>

        {importResult && (
          <div style={{
            marginTop: 12, padding: '10px 14px', borderRadius: 6,
            background: importResult.ok ? '#f0fdf4' : '#fef2f2',
            border: `1px solid ${importResult.ok ? '#bbf7d0' : '#fecaca'}`,
            fontSize: '0.85rem', color: importResult.ok ? '#15803d' : '#dc2626',
            overflow: 'hidden',
          }}>
            {importResult.ok
              ? <>
                  {`✅ 取り込み完了: ${importResult.imported}件インポート、${importResult.skipped}件スキップ`}
                  {importResult.colsFound && Object.keys(importResult.colsFound).length === 0 && (
                    <div style={{ marginTop: 6, color: '#b45309', fontWeight: 600 }}>
                      ⚠️ 列が1つも検出されませんでした。CSVファイルのエンコーディングを確認してください（UTF-8推奨）。
                    </div>
                  )}
                  {importResult.colsFound && Object.keys(importResult.colsFound).length > 0 && (
                    <div style={{ marginTop: 4, fontSize: '0.78rem', color: '#166534' }}>
                      検出列: {Object.keys(importResult.colsFound).join(', ')}
                    </div>
                  )}
                </>
              : `❌ エラー: ${importResult.error}`}
            {importResult.errors?.length > 0 && (
              <div style={{ marginTop: 6, fontSize: '0.75rem', color: '#dc2626', maxHeight: 120, overflowY: 'auto', wordBreak: 'break-all' }}>
                {importResult.errors.slice(0, 5).map((e, i) => <div key={i}>行{e.line}: {e.error}</div>)}
              </div>
            )}
          </div>
        )}
      </div>

      {/* フィルター */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20, flexWrap: 'wrap' }}>
        <label style={{ fontSize: '0.85rem', fontWeight: 600, color: '#374151' }}>応募日</label>
        <input type="date" value={from} onChange={e => setFrom(e.target.value)}
          style={{ border: '1px solid #d1d5db', borderRadius: 6, padding: '5px 10px', fontSize: '0.85rem' }} />
        <span style={{ color: '#6b7280' }}>〜</span>
        <input type="date" value={to} onChange={e => setTo(e.target.value)}
          style={{ border: '1px solid #d1d5db', borderRadius: 6, padding: '5px 10px', fontSize: '0.85rem' }} />
        <button onClick={loadAnalytics}
          style={{ background: '#3b82f6', color: '#fff', border: 'none', borderRadius: 6, padding: '6px 16px', fontSize: '0.85rem', cursor: 'pointer', fontWeight: 600 }}>
          絞り込む
        </button>
        {(from || to) && (
          <button onClick={() => { setFrom(''); setTo(''); fetchAnalytics('', ''); }}
            style={{ background: 'none', border: '1px solid #d1d5db', borderRadius: 6, padding: '5px 12px', fontSize: '0.82rem', cursor: 'pointer', color: '#6b7280' }}>
            リセット
          </button>
        )}
      </div>

      {error && <div style={{ color: '#dc2626', marginBottom: 16, fontSize: '0.85rem' }}>{error}</div>}

      {loading && <div style={{ color: '#6b7280', marginBottom: 16 }}>読み込み中...</div>}

      {analytics && !loading && (
        <>
          {/* KPI */}
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 8 }}>
            <KpiCard label="総応募数" value={analytics.total.toLocaleString()} />
            <KpiCard label="求人数（応募あり）" value={analytics.uniqueJobs?.toLocaleString() ?? '—'} color="#8b5cf6" />
            {analytics.byStatus.slice(0, 3).map(r => (
              <KpiCard key={r.name} label={r.name} value={r.cnt.toLocaleString()}
                sub={`${analytics.total > 0 ? Math.round(r.cnt / analytics.total * 100) : 0}%`}
                color={statusColor(r.name)} />
            ))}
          </div>

          {/* 応募数推移（粒度自動） */}
          {analytics.trend.length > 0 && (
            <>
              <SectionTitle>
                応募数推移
                <span style={{ fontSize: '0.75rem', fontWeight: 400, color: '#9ca3af', marginLeft: 8 }}>
                  {{ day:'日別', '3day':'3日別', week:'週別', month:'月別' }[analytics.granularity] ?? '月別'}
                </span>
              </SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 8px' }}>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={analytics.trend} margin={{ top: 4, right: 20, left: 0, bottom: 4 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                    <XAxis dataKey="period" tick={CHART_TICK_STYLE} />
                    <YAxis tick={CHART_TICK_STYLE} allowDecimals={false} />
                    <Tooltip formatter={(v) => [`${v}件`, '応募数']} />
                    <Line type="monotone" dataKey="cnt" stroke="#3b82f6" strokeWidth={2} dot={analytics.trend.length < 30 ? { r: 3 } : false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </>
          )}

          {/* 求人別 + 応募経路別（円グラフ） */}
          <div style={{ display: 'grid', gridTemplateColumns: '3fr 2fr', gap: 20, marginTop: 4 }}>
            <div>
              <SectionTitle>求人別応募数（上位20件）<span style={{ fontSize: '0.72rem', color: '#9ca3af', fontWeight: 400 }}> クリックで一覧</span></SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 8px' }}>
                {analytics.byJob.length === 0
                  ? <div style={{ color: '#9ca3af', fontSize: '0.85rem', textAlign: 'center', padding: 20 }}>データなし</div>
                  : (
                    <ResponsiveContainer width="100%" height={Math.max(200, analytics.byJob.length * 28)}>
                      <BarChart data={analytics.byJob} layout="vertical" margin={{ top: 0, right: 20, left: 8, bottom: 0 }}
                        onClick={e => e?.activePayload?.[0] && openDrilldown('job_name', e.activePayload[0].payload.name)}
                        style={{ cursor: 'pointer' }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                        <XAxis type="number" tick={CHART_TICK_STYLE} allowDecimals={false} />
                        <YAxis type="category" dataKey="name" tick={CHART_TICK_STYLE} width={150} />
                        <Tooltip formatter={(v) => [`${v}件`]} cursor={{ fill: '#eff6ff' }} />
                        <Bar dataKey="cnt" fill="#3b82f6" radius={[0, 4, 4, 0]} name="応募数" />
                      </BarChart>
                    </ResponsiveContainer>
                  )
                }
              </div>
            </div>

            <div>
              <SectionTitle>応募経路別 <span style={{ fontSize: '0.72rem', color: '#9ca3af', fontWeight: 400 }}>クリックで一覧</span></SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: 16 }}>
                {analytics.bySource.length === 0
                  ? <div style={{ color: '#9ca3af', fontSize: '0.85rem', textAlign: 'center', padding: 20 }}>データなし</div>
                  : (
                    <ResponsiveContainer width="100%" height={Math.max(220, analytics.bySource.length * 22 + 60)}>
                      <PieChart>
                        <Pie data={analytics.bySource} dataKey="cnt" nameKey="name"
                          cx="50%" cy="45%" outerRadius={85}
                          label={({ name, percent }) => percent > 0.04 ? `${(percent * 100).toFixed(0)}%` : ''}
                          labelLine={false} style={{ cursor: 'pointer' }}
                          onClick={d => d?.name && openDrilldown('source', d.name)}>
                          {analytics.bySource.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                        </Pie>
                        <Tooltip formatter={(v, n) => [`${v}件`, n]} />
                        <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
                      </PieChart>
                    </ResponsiveContainer>
                  )
                }
              </div>
            </div>
          </div>

          {/* ラベル（横棒グラフ） + 選考ステータス */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
            <div>
              <SectionTitle>ラベル別 <span style={{ fontSize: '0.72rem', color: '#9ca3af', fontWeight: 400 }}>クリックで一覧</span></SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 8px' }}>
                {analytics.byLabel.length === 0
                  ? <div style={{ color: '#9ca3af', fontSize: '0.85rem', textAlign: 'center', padding: 16 }}>データなし</div>
                  : (
                    <ResponsiveContainer width="100%" height={Math.max(200, analytics.byLabel.length * 26)}>
                      <BarChart data={analytics.byLabel} layout="vertical" margin={{ top: 0, right: 20, left: 8, bottom: 0 }}
                        onClick={e => e?.activePayload?.[0] && openDrilldown('label', e.activePayload[0].payload.name)}
                        style={{ cursor: 'pointer' }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                        <XAxis type="number" tick={CHART_TICK_STYLE} allowDecimals={false} />
                        <YAxis type="category" dataKey="name" tick={CHART_TICK_STYLE} width={130} />
                        <Tooltip formatter={(v) => [`${v}件`]} cursor={{ fill: '#eff6ff' }} />
                        <Bar dataKey="cnt" radius={[0, 4, 4, 0]} name="件数">
                          {analytics.byLabel.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  )
                }
              </div>
            </div>

            <div>
              <SectionTitle>選考ステータス内訳 <span style={{ fontSize: '0.72rem', color: '#9ca3af', fontWeight: 400 }}>クリックで一覧</span></SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: 16 }}>
                {analytics.byStatus.length === 0
                  ? <div style={{ color: '#9ca3af', fontSize: '0.85rem', textAlign: 'center', padding: 16 }}>データなし</div>
                  : analytics.byStatus.map(r => (
                    <div key={r.name} onClick={() => openDrilldown('status', r.name)}
                      style={{ marginBottom: 8, cursor: 'pointer', padding: '4px 6px', borderRadius: 6, transition: 'background 0.1s' }}
                      onMouseEnter={e => e.currentTarget.style.background = '#f8fafc'}
                      onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                        <span style={{ fontSize: '0.8rem', color: '#374151' }}>{r.name}</span>
                        <span style={{ fontSize: '0.8rem', fontWeight: 600, color: statusColor(r.name) }}>{r.cnt}件</span>
                      </div>
                      <div style={{ height: 6, background: '#f3f4f6', borderRadius: 3 }}>
                        <div style={{
                          height: '100%', borderRadius: 3,
                          width: `${analytics.total > 0 ? Math.round(r.cnt / analytics.total * 100) : 0}%`,
                          background: statusColor(r.name), transition: 'width 0.5s ease',
                        }} />
                      </div>
                    </div>
                  ))
                }
              </div>
            </div>
          </div>
        </>
      )}

      {drilldown && (
        <DrilldownPanel filter={drilldown} from={from} to={to} onClose={() => setDrilldown(null)} />
      )}

      {analytics && analytics.total === 0 && !loading && (
        <div style={{ textAlign: 'center', padding: '60px 20px', color: '#9ca3af' }}>
          <div style={{ fontSize: '2rem', marginBottom: 12 }}>📋</div>
          <div style={{ fontWeight: 600, marginBottom: 6 }}>データがありません</div>
          <div style={{ fontSize: '0.85rem' }}>HRMOSからCSVをエクスポートして上記からインポートしてください</div>
        </div>
      )}
    </div>
  );
}
