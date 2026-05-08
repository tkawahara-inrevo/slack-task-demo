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

export default function HrmosRecruitment() {
  const [summary, setSummary] = useState(null);
  const [analytics, setAnalytics] = useState(null);
  const [loading, setLoading] = useState(false);
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState(null);
  const [error, setError] = useState('');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [dragging, setDragging] = useState(false);
  const fileRef = useRef();

  const loadSummary = useCallback(async () => {
    try { setSummary(await api.hrmosSummary()); } catch {}
  }, []);

  const fetchAnalytics = useCallback(async (fromDate, toDate) => {
    setLoading(true);
    setError('');
    try {
      const data = await api.hrmosAnalytics({ from: fromDate || undefined, to: toDate || undefined });
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

  const onDrop = (e) => {
    e.preventDefault();
    setDragging(false);
    const file = e.dataTransfer.files?.[0];
    if (file && file.name.endsWith('.csv')) handleImport(file);
  };

  const totalByStatus = analytics?.byStatus?.reduce((s, r) => s + r.cnt, 0) || 0;

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

      {/* CSV アップロード */}
      <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: 20, marginBottom: 24 }}>
        <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 12 }}>CSVインポート（HRMOSからエクスポートしたCSVをアップロード）</div>
        <div
          onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
          onClick={() => fileRef.current?.click()}
          style={{
            border: `2px dashed ${dragging ? '#3b82f6' : '#d1d5db'}`,
            borderRadius: 8, padding: '28px 20px', textAlign: 'center',
            background: dragging ? '#eff6ff' : '#f9fafb',
            cursor: 'pointer', transition: 'all 0.15s',
          }}
        >
          <input ref={fileRef} type="file" accept=".csv" style={{ display: 'none' }} onChange={onFileChange} />
          {importing
            ? <span style={{ color: '#3b82f6', fontWeight: 600 }}>取り込み中...</span>
            : <span style={{ color: '#6b7280', fontSize: '0.88rem' }}>CSVファイルをドロップ、またはクリックして選択</span>
          }
        </div>

        {importResult && (
          <div style={{
            marginTop: 12, padding: '10px 14px', borderRadius: 6,
            background: importResult.ok ? '#f0fdf4' : '#fef2f2',
            border: `1px solid ${importResult.ok ? '#bbf7d0' : '#fecaca'}`,
            fontSize: '0.85rem', color: importResult.ok ? '#15803d' : '#dc2626',
          }}>
            {importResult.ok
              ? `✅ 取り込み完了: ${importResult.imported}件インポート、${importResult.skipped}件スキップ`
              : `❌ エラー: ${importResult.error}`}
            {importResult.errors?.length > 0 && (
              <div style={{ marginTop: 6, fontSize: '0.78rem', color: '#dc2626' }}>
                {importResult.errors.map((e, i) => <div key={i}>行{e.line}: {e.error}</div>)}
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
            {analytics.byStatus.slice(0, 4).map(r => (
              <KpiCard key={r.name} label={r.name} value={r.cnt.toLocaleString()}
                sub={`${totalByStatus > 0 ? Math.round(r.cnt / analytics.total * 100) : 0}%`}
                color={statusColor(r.name)} />
            ))}
          </div>

          {/* 月次推移 */}
          {analytics.trend.length > 0 && (
            <>
              <SectionTitle>月次応募数推移</SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 8px' }}>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={analytics.trend} margin={{ top: 4, right: 20, left: 0, bottom: 4 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                    <XAxis dataKey="month" tick={CHART_TICK_STYLE} />
                    <YAxis tick={CHART_TICK_STYLE} allowDecimals={false} />
                    <Tooltip formatter={(v) => [`${v}件`, '応募数']} />
                    <Line type="monotone" dataKey="cnt" stroke="#3b82f6" strokeWidth={2} dot={{ r: 4 }} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </>
          )}

          {/* 求人別 + 応募経路別 */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginTop: 4 }}>
            <div>
              <SectionTitle>求人別応募数（上位20件）</SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 8px' }}>
                {analytics.byJob.length === 0
                  ? <div style={{ color: '#9ca3af', fontSize: '0.85rem', textAlign: 'center', padding: 20 }}>データなし</div>
                  : (
                    <ResponsiveContainer width="100%" height={Math.max(200, analytics.byJob.length * 28)}>
                      <BarChart data={analytics.byJob} layout="vertical" margin={{ top: 0, right: 20, left: 8, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                        <XAxis type="number" tick={CHART_TICK_STYLE} allowDecimals={false} />
                        <YAxis type="category" dataKey="name" tick={CHART_TICK_STYLE} width={140} />
                        <Tooltip formatter={(v) => [`${v}件`]} />
                        <Bar dataKey="cnt" fill="#3b82f6" radius={[0, 4, 4, 0]} name="応募数" />
                      </BarChart>
                    </ResponsiveContainer>
                  )
                }
              </div>
            </div>

            <div>
              <SectionTitle>応募経路別</SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 8px' }}>
                {analytics.bySource.length === 0
                  ? <div style={{ color: '#9ca3af', fontSize: '0.85rem', textAlign: 'center', padding: 20 }}>データなし</div>
                  : (
                    <ResponsiveContainer width="100%" height={Math.max(200, analytics.bySource.length * 28 + 40)}>
                      <BarChart data={analytics.bySource} layout="vertical" margin={{ top: 0, right: 20, left: 8, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                        <XAxis type="number" tick={CHART_TICK_STYLE} allowDecimals={false} />
                        <YAxis type="category" dataKey="name" tick={CHART_TICK_STYLE} width={120} />
                        <Tooltip formatter={(v) => [`${v}件`]} />
                        <Bar dataKey="cnt" radius={[0, 4, 4, 0]} name="件数">
                          {analytics.bySource.map((_, idx) => (
                            <Cell key={idx} fill={COLORS[idx % COLORS.length]} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  )
                }
              </div>
            </div>
          </div>

          {/* ラベル + 選考ステータス */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
            <div>
              <SectionTitle>ラベル別</SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: 16 }}>
                {analytics.byLabel.length === 0
                  ? <div style={{ color: '#9ca3af', fontSize: '0.85rem', textAlign: 'center', padding: 16 }}>データなし</div>
                  : (
                    <ResponsiveContainer width="100%" height={220}>
                      <PieChart>
                        <Pie data={analytics.byLabel} dataKey="cnt" nameKey="name"
                          cx="50%" cy="50%" outerRadius={80} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                          labelLine={false}>
                          {analytics.byLabel.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                        </Pie>
                        <Tooltip formatter={(v) => [`${v}件`]} />
                        <Legend wrapperStyle={{ fontSize: '0.78rem' }} />
                      </PieChart>
                    </ResponsiveContainer>
                  )
                }
              </div>
            </div>

            <div>
              <SectionTitle>選考ステータス内訳</SectionTitle>
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: 16 }}>
                {analytics.byStatus.length === 0
                  ? <div style={{ color: '#9ca3af', fontSize: '0.85rem', textAlign: 'center', padding: 16 }}>データなし</div>
                  : analytics.byStatus.map(r => (
                    <div key={r.name} style={{ marginBottom: 8 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                        <span style={{ fontSize: '0.8rem', color: '#374151' }}>{r.name}</span>
                        <span style={{ fontSize: '0.8rem', fontWeight: 600, color: statusColor(r.name) }}>{r.cnt}件</span>
                      </div>
                      <div style={{ height: 6, background: '#f3f4f6', borderRadius: 3 }}>
                        <div style={{
                          height: '100%', borderRadius: 3,
                          width: `${analytics.total > 0 ? Math.round(r.cnt / analytics.total * 100) : 0}%`,
                          background: statusColor(r.name),
                          transition: 'width 0.5s ease',
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
