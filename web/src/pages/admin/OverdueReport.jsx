import { useEffect, useState } from 'react';
import { api } from '../../api/client';

export default function OverdueReport() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState('');

  const load = () => {
    setLoading(true); setErr('');
    api.overdueReport()
      .then(setData)
      .catch(e => setErr(e.message || '取得失敗'))
      .finally(() => setLoading(false));
  };
  useEffect(() => { load(); }, []);

  const today = new Date().toISOString().slice(0, 10);

  return (
    <div style={{ padding: '20px 28px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 6 }}>
        <div style={{ fontWeight: 800, fontSize: '1.15rem', color: '#0f172a' }}>📊 期限切れタスク レポート</div>
        <button onClick={load} disabled={loading}
          style={{ fontSize: '0.74rem', padding: '4px 10px', borderRadius: 6, border: '1px solid #cbd5e1', background: '#fff', color: '#475569', cursor: 'pointer' }}>
          ⟳ 再取得
        </button>
      </div>
      <div style={{ fontSize: '0.78rem', color: '#64748b', marginBottom: 16 }}>
        基準日 <b>{today}</b> 時点で <b>未完了</b> かつ <b>期限が過去</b> のタスク
      </div>

      {/* ダウンロードボタン */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 20 }}>
        <a href="/api/dashboard/admin/overdue-report.xlsx" download
          style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '8px 18px', background: '#059669', color: '#fff', borderRadius: 8, textDecoration: 'none', fontWeight: 700, fontSize: '0.85rem' }}>
          📥 Excel (xlsx) ダウンロード
        </a>
        <a href="/api/dashboard/admin/overdue-report.csv" download
          style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '8px 18px', background: '#fff', color: '#475569', border: '1px solid #cbd5e1', borderRadius: 8, textDecoration: 'none', fontWeight: 700, fontSize: '0.85rem' }}>
          📄 CSV ダウンロード
        </a>
      </div>

      {loading && <div style={{ color: '#94a3b8', padding: 20 }}>読み込み中…</div>}
      {err && <div style={{ color: '#dc2626', padding: 16, background: '#fef2f2', borderRadius: 8 }}>エラー: {err}</div>}

      {data && (
        <>
          {/* 担当者サマリー */}
          <section style={{ marginBottom: 24 }}>
            <div style={{ fontWeight: 800, fontSize: '0.92rem', color: '#0f172a', marginBottom: 8 }}>
              👥 担当者サマリー（{data.summary.length}名）
            </div>
            {data.summary.length === 0 ? (
              <div style={{ padding: 20, textAlign: 'center', color: '#94a3b8', background: '#f0fdf4', borderRadius: 8, border: '1px solid #bbf7d0' }}>
                🎉 期限切れタスクなし！素晴らしい
              </div>
            ) : (
              <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 10, overflow: 'hidden' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.84rem' }}>
                  <thead>
                    <tr style={{ background: '#fef3c7' }}>
                      <th style={th}>担当者</th>
                      <th style={{ ...th, textAlign: 'right' }}>件数</th>
                      <th style={{ ...th, textAlign: 'center' }}>最古の期限</th>
                      <th style={{ ...th, textAlign: 'right' }}>最大超過</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.summary.map((s, i) => (
                      <tr key={s.assignee_user_id || s.assignee_name + i} style={{ borderTop: '1px solid #f1f5f9' }}>
                        <td style={td}>{s.assignee_name}</td>
                        <td style={{ ...td, textAlign: 'right', fontWeight: 700, color: s.count >= 5 ? '#dc2626' : s.count >= 3 ? '#d97706' : '#0f172a' }}>{s.count}件</td>
                        <td style={{ ...td, textAlign: 'center', color: '#64748b' }}>{String(s.oldest_due).slice(0,10)}</td>
                        <td style={{ ...td, textAlign: 'right', color: s.max_days_overdue >= 14 ? '#dc2626' : s.max_days_overdue >= 7 ? '#d97706' : '#475569' }}>{s.max_days_overdue}日</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          {/* タスク明細 */}
          {data.tasks.length > 0 && (
            <section>
              <div style={{ fontWeight: 800, fontSize: '0.92rem', color: '#0f172a', marginBottom: 8 }}>
                📝 タスク明細（{data.tasks.length}件）
              </div>
              <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 10, overflow: 'hidden', maxHeight: 600, overflowY: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
                  <thead style={{ position: 'sticky', top: 0, background: '#fafbfc', zIndex: 1 }}>
                    <tr>
                      <th style={th}>担当者</th>
                      <th style={th}>タスク</th>
                      <th style={{ ...th, textAlign: 'center' }}>期限</th>
                      <th style={{ ...th, textAlign: 'right' }}>超過</th>
                      <th style={th}>依頼者</th>
                      <th style={{ ...th, textAlign: 'center' }}>リンク</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.tasks.map((t, i) => (
                      <tr key={t.task_id + '_' + t.assignee_user_id + i} style={{ borderTop: '1px solid #f1f5f9' }}>
                        <td style={td}>{t.assignee_name}</td>
                        <td style={td}>{(t.title || '').slice(0, 60)}{(t.title || '').length > 60 ? '…' : ''}</td>
                        <td style={{ ...td, textAlign: 'center', color: '#64748b' }}>{String(t.due_date).slice(0,10)}</td>
                        <td style={{ ...td, textAlign: 'right', color: t.days_overdue >= 14 ? '#dc2626' : t.days_overdue >= 7 ? '#d97706' : '#475569', fontWeight: 600 }}>{t.days_overdue}日</td>
                        <td style={td}>{t.requester_name}</td>
                        <td style={{ ...td, textAlign: 'center' }}>
                          {t.permalink && <a href={t.permalink} target="_blank" rel="noreferrer" style={{ color: '#2563eb', textDecoration: 'none' }}>↗</a>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}
        </>
      )}
    </div>
  );
}

const th = { textAlign: 'left', padding: '8px 12px', fontSize: '0.72rem', fontWeight: 700, color: '#475569' };
const td = { padding: '6px 12px', color: '#0f172a' };
