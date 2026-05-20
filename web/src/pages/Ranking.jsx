import { useEffect, useRef, useState } from 'react';
import { api } from '../api/client';

const CATEGORIES = [
  { value: 'all',              label: '全部合算' },
  { value: 'chat',             label: '雑談' },
  { value: 'report_reply',     label: '日報返信（合算）' },
  { value: 'report_in_reply',  label: '出勤日報返信' },
  { value: 'report_out_reply', label: '退勤日報返信' },
];
const MEDALS = { 1: '🥇', 2: '🥈', 3: '🥉' };
const JOB_KEY = 'ranking_job';

function thisMonthRange() {
  const now = new Date();
  const y = now.getFullYear(), m = now.getMonth();
  const from = `${y}-${String(m + 1).padStart(2, '0')}-01`;
  const last = new Date(y, m + 1, 0).getDate();
  const to   = `${y}-${String(m + 1).padStart(2, '0')}-${String(last).padStart(2, '0')}`;
  return { from, to };
}
function lastMonthRange() {
  const now = new Date();
  const d = new Date(now.getFullYear(), now.getMonth(), 0);
  const from = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-01`;
  const to   = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  return { from, to };
}

export default function Ranking() {
  const { from: df, to: dt } = thisMonthRange();
  const [from, setFrom]         = useState(df);
  const [to, setTo]             = useState(dt);
  const [category, setCategory] = useState('all');
  const [limit, setLimit]       = useState('20');
  const [ranking, setRanking]   = useState([]);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState('');
  const [elapsed, setElapsed]   = useState(null);
  const [progress, setProgress] = useState(null); // { channels, done, current, threadsDone, threadsTotal, elapsedSec }
  const pollRef = useRef(null);

  const stopPoll = () => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
  };

  const startPolling = (jobId) => {
    stopPoll();
    pollRef.current = setInterval(async () => {
      try {
        const r = await api.rankingStatus(jobId);
        if (r.status === 'done') {
          stopPoll();
          localStorage.removeItem(JOB_KEY);
          setRanking(r.ranking || []);
          setElapsed(r.elapsed);
          setLoading(false);
          setProgress(null);
        } else if (r.status === 'error') {
          stopPoll();
          localStorage.removeItem(JOB_KEY);
          setError(r.error || '集計に失敗しました');
          setLoading(false);
          setProgress(null);
        } else if (r.status === 'running') {
          setProgress({ ...r.progress, elapsedSec: r.elapsed });
        }
      } catch (e) {
        stopPoll();
        localStorage.removeItem(JOB_KEY);
        setError(e.message || '集計に失敗しました');
        setLoading(false);
        setProgress(null);
      }
    }, 2000);
  };

  // ページ読み込み時: 処理中のジョブがあれば期間ごと復元
  useEffect(() => {
    const saved = localStorage.getItem(JOB_KEY);
    if (saved) {
      try {
        const { jobId, from: f, to: t, category: cat, limit: lim } = JSON.parse(saved);
        if (f) setFrom(f);
        if (t) setTo(t);
        if (cat) setCategory(cat);
        if (lim) setLimit(lim);
        setLoading(true);
        startPolling(jobId);
      } catch { localStorage.removeItem(JOB_KEY); }
    }
    return () => stopPoll();
  }, []);

  const load = async () => {
    stopPoll();
    setLoading(true);
    setError('');
    setElapsed(null);
    setRanking([]);
    setProgress(null);
    try {
      const { jobId } = await api.rankingStart({ from, to, category, limit });
      localStorage.setItem(JOB_KEY, JSON.stringify({ jobId, from, to, category, limit }));
      startPolling(jobId);
    } catch (e) {
      setError(e.message || '集計の開始に失敗しました');
      setLoading(false);
    }
  };

  return (
    <div className="rpo-page">
      <div className="rpo-header">
        <div>
          <h1 className="rpo-title">Slackランキング</h1>
          <p className="rpo-subtitle">雑談・日報返信の活動量をSlackから集計</p>
        </div>
      </div>

      {/* フィルター */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap', alignItems: 'flex-end' }}>
        <div style={{ display: 'flex', gap: 4 }}>
          <button className="btn-secondary" style={{ fontSize: 12 }} disabled={loading}
            onClick={() => { const r = thisMonthRange(); setFrom(r.from); setTo(r.to); }}>今月</button>
          <button className="btn-secondary" style={{ fontSize: 12 }} disabled={loading}
            onClick={() => { const r = lastMonthRange(); setFrom(r.from); setTo(r.to); }}>先月</button>
        </div>
        <input type="date" value={from} onChange={e => setFrom(e.target.value)} disabled={loading}
          style={{ padding: '5px 8px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: '0.82rem' }} />
        <span style={{ color: '#9ca3af', alignSelf: 'center' }}>〜</span>
        <input type="date" value={to} onChange={e => setTo(e.target.value)} disabled={loading}
          style={{ padding: '5px 8px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: '0.82rem' }} />
        <select value={category} onChange={e => setCategory(e.target.value)} disabled={loading}
          style={{ padding: '5px 8px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: '0.82rem' }}>
          {CATEGORIES.map(c => <option key={c.value} value={c.value}>{c.label}</option>)}
        </select>
        <select value={limit} onChange={e => setLimit(e.target.value)} disabled={loading}
          style={{ padding: '5px 8px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: '0.82rem' }}>
          {[10, 20, 50, 0].map(n => <option key={n} value={n}>{n === 0 ? '全員' : `上位${n}名`}</option>)}
        </select>
        <button className="btn-primary" onClick={load} disabled={loading} style={{ fontSize: '0.85rem', minWidth: 80 }}>
          {loading ? '集計中...' : '集計'}
        </button>
        {elapsed && !loading && <span style={{ fontSize: '0.78rem', color: '#9ca3af', alignSelf: 'center' }}>{elapsed}秒</span>}
      </div>

      {loading && (
        <div style={{ marginBottom: 16, padding: '14px 16px', background: '#f0f9ff', border: '1px solid #bae6fd', borderRadius: 8 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: progress ? 10 : 0 }}>
            <span style={{ fontSize: '1.1rem' }}>⏳</span>
            <div>
              <div style={{ fontSize: '0.88rem', fontWeight: 600, color: '#0369a1' }}>
                Slackからデータを取得中…
                {progress?.elapsedSec && <span style={{ fontWeight: 400, color: '#64748b', marginLeft: 8 }}>{progress.elapsedSec}秒経過</span>}
              </div>
              <div style={{ fontSize: '0.75rem', color: '#64748b', marginTop: 2 }}>別タブに移動しても処理は継続されます</div>
            </div>
          </div>
          {progress && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {/* チャンネル進捗 */}
              {(progress.channels || []).map(ch => {
                const done = (progress.done || []).includes(ch);
                const isCurrent = progress.current === ch;
                return (
                  <div key={ch} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span style={{ fontSize: 14 }}>{done ? '✅' : isCurrent ? '⚙️' : '⬜'}</span>
                    <span style={{ fontSize: '0.82rem', color: done ? '#16a34a' : isCurrent ? '#0369a1' : '#94a3b8', fontWeight: isCurrent ? 700 : 400 }}>
                      {ch}
                      {isCurrent && progress.threadsTotal > 0 && (
                        <span style={{ fontWeight: 400, color: '#64748b', marginLeft: 6 }}>
                          スレッド取得中 {progress.threadsDone}/{progress.threadsTotal}件
                        </span>
                      )}
                    </span>
                  </div>
                );
              })}
              {/* スレッド進捗バー */}
              {progress.current && progress.threadsTotal > 0 && (
                <div style={{ marginTop: 4 }}>
                  <div style={{ background: '#e0f2fe', borderRadius: 4, height: 6, overflow: 'hidden' }}>
                    <div style={{ background: '#0284c7', height: '100%', borderRadius: 4, transition: 'width 0.3s',
                      width: `${Math.round((progress.threadsDone / progress.threadsTotal) * 100)}%` }} />
                  </div>
                  <div style={{ fontSize: '0.7rem', color: '#64748b', marginTop: 3 }}>
                    {Math.round((progress.threadsDone / progress.threadsTotal) * 100)}%
                    （スレッド1件あたり約1.2秒かかります）
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {error && (
        <div style={{ padding: 12, background: '#fef2f2', border: '1px solid #fca5a5', borderRadius: 6, color: '#dc2626', fontSize: '0.85rem', marginBottom: 12 }}>
          {error}
        </div>
      )}

      {!loading && ranking.length === 0 && !error && (
        <p style={{ color: '#9ca3af', textAlign: 'center', padding: 32 }}>
          期間を設定して「集計」を押してください
        </p>
      )}

      {ranking.length > 0 && (() => {
        const showChat      = ['all', 'chat'].includes(category);
        const showReportIn  = ['all', 'report_in_reply', 'report_reply'].includes(category);
        const showReportOut = ['all', 'report_out_reply', 'report_reply'].includes(category);
        const showTotal     = category === 'all';
        const scoreOf = r => {
          if (category === 'chat') return Number(r.chat_count);
          if (category === 'report_in_reply') return Number(r.report_in_count);
          if (category === 'report_out_reply') return Number(r.report_out_count);
          if (category === 'report_reply') return Number(r.report_in_count) + Number(r.report_out_count);
          return Number(r.total);
        };
        return (
          <div style={{ border: '1px solid #e5e7eb', borderRadius: 8, overflow: 'hidden', background: '#fff' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
              <thead>
                <tr style={{ background: '#f9fafb' }}>
                  <th style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>順位</th>
                  <th style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>名前</th>
                  {showChat      && <th style={{ padding: '10px 14px', textAlign: 'right', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>雑談</th>}
                  {showReportIn  && <th style={{ padding: '10px 14px', textAlign: 'right', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>出勤返信</th>}
                  {showReportOut && <th style={{ padding: '10px 14px', textAlign: 'right', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>退勤返信</th>}
                  {showTotal     && <th style={{ padding: '10px 14px', textAlign: 'right', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>合計</th>}
                  {!showTotal    && <th style={{ padding: '10px 14px', textAlign: 'right', fontWeight: 600, color: '#1d4ed8', borderBottom: '1px solid #e5e7eb' }}>スコア</th>}
                </tr>
              </thead>
              <tbody>
                {ranking.map(r => {
                  const rank = Number(r.rank);
                  const isTop3 = rank <= 3;
                  const score = scoreOf(r);
                  return (
                    <tr key={r.user_id} style={{ borderBottom: '1px solid #f3f4f6', background: isTop3 ? '#fffbeb' : '' }}>
                      <td style={{ padding: '10px 14px', fontWeight: 700, color: isTop3 ? '#d97706' : '#9ca3af', width: 64 }}>
                        {MEDALS[rank] || `#${rank}`}
                      </td>
                      <td style={{ padding: '10px 14px', fontWeight: isTop3 ? 700 : 400, color: '#111827' }}>{r.user_name}</td>
                      {showChat      && <td style={{ padding: '10px 14px', textAlign: 'right', color: '#6b7280' }}>{Number(r.chat_count).toLocaleString()}</td>}
                      {showReportIn  && <td style={{ padding: '10px 14px', textAlign: 'right', color: '#6b7280' }}>{Number(r.report_in_count).toLocaleString()}</td>}
                      {showReportOut && <td style={{ padding: '10px 14px', textAlign: 'right', color: '#6b7280' }}>{Number(r.report_out_count).toLocaleString()}</td>}
                      {showTotal     && <td style={{ padding: '10px 14px', textAlign: 'right', fontWeight: 700, color: '#111827' }}>{Number(r.total).toLocaleString()}</td>}
                      {!showTotal    && <td style={{ padding: '10px 14px', textAlign: 'right', fontWeight: 700, color: '#1d4ed8', fontSize: '1rem' }}>{score.toLocaleString()}</td>}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        );
      })()}
    </div>
  );
}
