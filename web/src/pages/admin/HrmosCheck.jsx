import { useEffect, useMemo, useState } from 'react';
import { api } from '../../api/client';

const ISSUE_COLORS = {
  '退勤打刻漏れ':       { bg: '#fef3c7', fg: '#92400e' },
  '出勤打刻漏れ':       { bg: '#fee2e2', fg: '#991b1b' },
  '出退勤打刻なし':     { bg: '#fee2e2', fg: '#991b1b' },
  '休憩登録漏れ':       { bg: '#dbeafe', fg: '#1e40af' },
  '勤怠申請が未申請':   { bg: '#ede9fe', fg: '#5b21b6' },
};

const todayYm = () => new Date().toISOString().slice(0, 7);

export default function HrmosCheck() {
  const [month, setMonth] = useState(todayYm());
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState('');
  const [copied, setCopied] = useState(null);

  const load = (m) => {
    setLoading(true); setErr('');
    api.hrmosMonthlyCheck(m).then(setData).catch(e => setErr(e.message || '取得失敗')).finally(() => setLoading(false));
  };
  useEffect(() => { load(month); }, [month]);

  const summary = useMemo(() => {
    if (!data?.users) return { totalIssueDays: 0, byIssue: {} };
    const byIssue = {};
    let totalIssueDays = 0;
    for (const u of data.users) {
      for (const d of u.days) {
        totalIssueDays++;
        for (const i of d.issues) byIssue[i] = (byIssue[i] || 0) + 1;
      }
    }
    return { totalIssueDays, byIssue };
  }, [data]);

  // Slack投稿用フォーマット
  const buildSlackText = () => {
    if (!data?.users?.length) return '';
    const lines = [`【${month}月 勤怠不備について】`, ''];
    for (const u of data.users) {
      lines.push(`■ ${u.full_name}`);
      for (const d of u.days) {
        lines.push(`　${d.day_display}（${d.wday}）　${d.issues.join('、')}`);
      }
      lines.push('');
    }
    return lines.join('\n');
  };

  const copySlack = async () => {
    const text = buildSlackText();
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch { alert('コピー失敗'); }
  };

  return (
    <div style={{ padding: '20px 28px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 6 }}>
        <div style={{ fontWeight: 800, fontSize: '1.15rem', color: '#0f172a' }}>🕒 HRMOS 勤怠不備チェック</div>
        <input type="month" value={month} onChange={e => setMonth(e.target.value)}
          style={{ padding: '6px 10px', border: '1px solid #cbd5e1', borderRadius: 8, fontSize: '0.86rem', background: '#fff' }} />
        <button onClick={() => load(month)} disabled={loading}
          style={{ padding: '6px 14px', borderRadius: 8, border: '1px solid #cbd5e1', background: '#fff', color: '#475569', cursor: 'pointer', fontSize: '0.8rem' }}>
          ⟳ 再取得
        </button>
        {data?.users?.length > 0 && (
          <button onClick={copySlack}
            style={{ marginLeft: 'auto', padding: '7px 18px', borderRadius: 8, border: 'none', background: copied ? '#059669' : '#4a154b', color: '#fff', fontWeight: 700, fontSize: '0.82rem', cursor: 'pointer' }}>
            {copied ? '✓ コピーしました' : '📋 Slack投稿用テキストをコピー'}
          </button>
        )}
      </div>
      <div style={{ fontSize: '0.78rem', color: '#64748b', marginBottom: 16 }}>
        当日と未来日は対象外 / 公休・休暇は除外 / HRMOSの月次データから自動検知
      </div>

      {/* サマリーバッジ */}
      {data && (
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 18 }}>
          <span style={{ padding: '6px 14px', background: '#f1f5f9', borderRadius: 99, fontSize: '0.82rem', fontWeight: 700, color: '#0f172a' }}>
            👥 不備者 <b style={{ color: data.total_users > 0 ? '#dc2626' : '#059669' }}>{data.total_users}名</b>
          </span>
          <span style={{ padding: '6px 14px', background: '#f1f5f9', borderRadius: 99, fontSize: '0.82rem', fontWeight: 700, color: '#0f172a' }}>
            📅 不備日数 <b style={{ color: summary.totalIssueDays > 0 ? '#dc2626' : '#059669' }}>{summary.totalIssueDays}日</b>
          </span>
          {Object.entries(summary.byIssue).sort((a,b) => b[1] - a[1]).map(([k, v]) => {
            const c = ISSUE_COLORS[k] || { bg: '#f1f5f9', fg: '#475569' };
            return (
              <span key={k} style={{ padding: '6px 12px', background: c.bg, color: c.fg, borderRadius: 99, fontSize: '0.78rem', fontWeight: 700 }}>
                {k} {v}件
              </span>
            );
          })}
        </div>
      )}

      {loading && <div style={{ color: '#94a3b8', padding: 20 }}>HRMOS から取得中…</div>}
      {err && <div style={{ color: '#dc2626', padding: 16, background: '#fef2f2', borderRadius: 8 }}>エラー: {err}</div>}

      {data && data.users.length === 0 && !loading && (
        <div style={{ padding: 24, textAlign: 'center', color: '#059669', background: '#f0fdf4', borderRadius: 8, border: '1px solid #bbf7d0', fontWeight: 700 }}>
          🎉 {month} の勤怠不備はありません！素晴らしい
        </div>
      )}

      {/* ユーザー別リスト */}
      {data?.users?.map(u => (
        <section key={u.user_id} style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 12, padding: '14px 18px', marginBottom: 12, boxShadow: '0 1px 3px rgba(0,0,0,0.04)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10, paddingBottom: 8, borderBottom: '1px solid #f1f5f9' }}>
            <span style={{ fontWeight: 800, fontSize: '0.95rem', color: '#0f172a' }}>{u.full_name}</span>
            {u.number && <span style={{ fontSize: '0.7rem', color: '#94a3b8' }}>#{u.number}</span>}
            <span style={{ marginLeft: 'auto', padding: '2px 10px', background: u.days.length >= 3 ? '#fef2f2' : '#fffbeb', color: u.days.length >= 3 ? '#dc2626' : '#d97706', borderRadius: 99, fontSize: '0.78rem', fontWeight: 700 }}>
              {u.days.length}日
            </span>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {u.days.map(d => (
              <div key={d.day} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '6px 8px', borderRadius: 6, background: '#fafbfc' }}>
                <span style={{ minWidth: 56, fontSize: '0.82rem', color: '#0f172a', fontWeight: 700 }}>{d.day_display}</span>
                <span style={{ minWidth: 28, fontSize: '0.72rem', color: '#64748b' }}>{d.wday}</span>
                <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', flex: 1 }}>
                  {d.issues.map(i => {
                    const c = ISSUE_COLORS[i] || { bg: '#f1f5f9', fg: '#475569' };
                    return <span key={i} style={{ padding: '2px 8px', borderRadius: 99, fontSize: '0.7rem', fontWeight: 700, background: c.bg, color: c.fg }}>{i}</span>;
                  })}
                </div>
                <span style={{ fontSize: '0.66rem', color: '#94a3b8' }}>
                  {d.stamping_start_at || '--:--'} 〜 {d.stamping_end_at || '--:--'}
                  {d.total_break_time ? ` / 休憩 ${d.total_break_time}` : ''}
                </span>
              </div>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}
