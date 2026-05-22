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
  const [saving, setSaving]     = useState(false);
  const [posting, setPosting]   = useState(false);
  const [view, setView]         = useState('list'); // 'list' | 'media'
  const [rpoResults, setRpoResults] = useState(null);
  const [mediaStats, setMediaStats] = useState([]);
  const [mediaLoading, setMediaLoading] = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      const r = await api.anRequests(filterStatus || undefined);
      setRequests(r.requests || []);
    } catch { setRequests([]); }
    finally { setLoading(false); }
  };

  useEffect(() => { load(); }, [filterStatus]);

  const openDetail = (req) => {
    setSelected(req);
    setAnswer(req.answer || '');
    setRpoResults(null);
    api.anRpoResults(req.id).then(r => setRpoResults(r.results)).catch(() => {});
  };

  const loadMediaStats = () => {
    setMediaLoading(true);
    api.anMediaStats().then(r => setMediaStats(r.stats || [])).catch(() => {}).finally(() => setMediaLoading(false));
  };

  const saveAnswer = async () => {
    if (!selected) return;
    setSaving(true);
    try {
      const r = await api.anUpdate(selected.id, { answer, status: answer.trim() ? 'answered' : selected.status });
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
            {[['list','AN依頼一覧'],['media','媒体実績DB']].map(([v,l]) => (
              <button key={v} onClick={() => { setView(v); if(v==='media') loadMediaStats(); }}
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
            <div style={{ padding: 12 }}>
              {mediaLoading && <div style={{ textAlign: 'center', color: 'var(--gray-400)', padding: 24 }}>読み込み中...</div>}
              {!mediaLoading && mediaStats.length === 0 && <div style={{ textAlign: 'center', color: 'var(--gray-400)', padding: 24 }}>データなし</div>}
              {mediaStats.map(s => (
                <div key={s.media_name} style={{ marginBottom: 8, background: 'var(--surface)', border: '1px solid var(--gray-200)', borderRadius: 8, padding: '10px 12px' }}>
                  <div style={{ fontWeight: 700, fontSize: '0.88rem', marginBottom: 6 }}>{s.media_name}</div>
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
                </div>
              ))}
            </div>
          )}
          {/* AN依頼一覧ビュー */}
          {view === 'list' && loading && <div style={{ padding: 24, textAlign: 'center', color: 'var(--gray-400)' }}>読み込み中...</div>}
          {view === 'list' && !loading && requests.length === 0 && <div style={{ padding: 24, textAlign: 'center', color: 'var(--gray-400)' }}>該当なし</div>}
          {view === 'list' && requests.map(req => (
            <div key={req.id} onClick={() => openDetail(req)}
              style={{ padding: '10px 14px', borderBottom: '1px solid var(--gray-100)', cursor: 'pointer',
                background: selected?.id === req.id ? '#eff6ff' : 'var(--surface)' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                <span style={{ fontWeight: 600, fontSize: '0.84rem', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {req.company_name || '（会社名なし）'}
                </span>
                <span style={{ fontSize: '0.68rem', fontWeight: 700, padding: '1px 6px', borderRadius: 4,
                  background: STATUS_BG[req.status], color: STATUS_COLOR[req.status] }}>
                  {STATUS_LABEL[req.status] || req.status}
                </span>
              </div>
              <div style={{ fontSize: '0.72rem', color: 'var(--gray-500)', display: 'flex', gap: 8 }}>
                <span>{req.request_type || '—'}</span>
                {req.priority && <span style={{ color: '#dc2626', fontWeight: 700 }}>優先: {req.priority}</span>}
                <span style={{ marginLeft: 'auto' }}>{fmtDate(req.created_at)}</span>
              </div>
              {req.sales_person && <div style={{ fontSize: '0.72rem', color: 'var(--gray-400)', marginTop: 2 }}>担当: {req.sales_person}</div>}
            </div>
          ))}
        </div>
      </div>

      {/* 右: 詳細・回答入力 */}
      {selected && (
        <div style={S.detail}>
          <div style={{ padding: '12px 20px', borderBottom: '1px solid var(--gray-200)', display: 'flex', alignItems: 'center', gap: 10 }}>
            <button onClick={() => setSelected(null)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--gray-500)', fontSize: '1.1rem', padding: 4 }}>←</button>
            <span style={{ fontWeight: 700, fontSize: '0.95rem', flex: 1 }}>{selected.company_name || '（会社名なし）'}</span>
            <span style={{ fontSize: '0.72rem', fontWeight: 700, padding: '2px 8px', borderRadius: 4,
              background: STATUS_BG[selected.status], color: STATUS_COLOR[selected.status] }}>
              {STATUS_LABEL[selected.status]}
            </span>
          </div>

          <div style={{ flex: 1, overflowY: 'auto', padding: '16px 20px', display: 'flex', flexDirection: 'column', gap: 14 }}>
            {/* メタ情報 */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              {[
                ['担当営業', selected.sales_person],
                ['依頼種別', selected.request_type],
                ['優先度', selected.priority],
                ['受付日時', fmtDate(selected.created_at)],
                ['CRM案件', selected.deal_name || selected.customer_name],
              ].map(([label, val]) => val ? (
                <div key={label}>
                  <div style={{ fontSize: '0.7rem', color: 'var(--gray-500)', marginBottom: 2 }}>{label}</div>
                  <div style={{ fontSize: '0.84rem', fontWeight: 500, color: 'var(--gray-800)' }}>{val}</div>
                </div>
              ) : null)}
            </div>

            {/* 依頼詳細 */}
            {selected.detail && (
              <div>
                <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--gray-600)', marginBottom: 6 }}>依頼内容</div>
                <pre style={{ fontSize: '0.8rem', color: 'var(--gray-700)', background: 'var(--surface-2)', borderRadius: 8, padding: '10px 12px', whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0 }}>
                  {selected.detail}
                </pre>
              </div>
            )}

            {/* 元のSlackメッセージ（折りたたみ） */}
            {selected.raw_text && (
              <details>
                <summary style={{ fontSize: '0.75rem', color: 'var(--gray-400)', cursor: 'pointer' }}>Slack元メッセージを見る</summary>
                <pre style={{ fontSize: '0.75rem', color: 'var(--gray-600)', background: 'var(--gray-50)', borderRadius: 8, padding: '10px 12px', whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: '6px 0 0', maxHeight: 300, overflowY: 'auto' }}>
                  {selected.raw_text}
                </pre>
              </details>
            )}

            {/* RPO実績 */}
            {rpoResults && (
              <div style={{ background: '#f0fdf4', border: '1px solid #86efac', borderRadius: 8, padding: '10px 12px', marginBottom: 14 }}>
                <div style={{ fontWeight: 700, fontSize: '0.78rem', color: '#15803d', marginBottom: 8 }}>📊 この案件のRPO実績（{rpoResults.client_name}）</div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2,1fr)', gap: 6, marginBottom: 8 }}>
                  {[
                    ['採用目標', `${rpoResults.hiring_target}人`],
                    ['内定承諾', `${rpoResults.accepted_count}人`],
                    ['総応募数', `${rpoResults.total_applicants}人`],
                    ['媒体費合計', fmtYen(rpoResults.total_cost)],
                  ].map(([l,v]) => (
                    <div key={l} style={{ background: 'var(--surface)', borderRadius: 5, padding: '4px 8px' }}>
                      <div style={{ fontSize: '0.68rem', color: 'var(--gray-500)' }}>{l}</div>
                      <div style={{ fontSize: '0.84rem', fontWeight: 600 }}>{v}</div>
                    </div>
                  ))}
                </div>
                {rpoResults.media.length > 0 && (
                  <div>
                    <div style={{ fontSize: '0.72rem', color: '#15803d', fontWeight: 700, marginBottom: 4 }}>利用媒体</div>
                    {rpoResults.media.map((m, i) => (
                      <div key={i} style={{ display: 'flex', gap: 8, fontSize: '0.78rem', padding: '3px 0', borderBottom: '1px solid #bbf7d0' }}>
                        <span style={{ flex: 1, fontWeight: 500 }}>{m.name}</span>
                        <span style={{ color: 'var(--gray-600)' }}>{fmtYen(m.mediaCost)}</span>
                        <span style={{ color: '#15803d', fontWeight: 700 }}>{m.hiredCount || 0}人採用</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* 回答入力 */}
            <div>
              <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--gray-700)', marginBottom: 6 }}>調査結果・回答</div>
              <textarea value={answer} onChange={e => setAnswer(e.target.value)} rows={10}
                placeholder="調査結果を入力してください..."
                style={{ width: '100%', padding: '10px 12px', border: '1px solid var(--gray-300)', borderRadius: 8, fontSize: '0.84rem', resize: 'vertical', boxSizing: 'border-box', background: 'var(--surface)' }} />
            </div>
          </div>

          <div style={{ padding: '12px 20px', borderTop: '1px solid var(--gray-200)', display: 'flex', gap: 8 }}>
            <button onClick={saveAnswer} disabled={saving}
              style={{ flex: 1, padding: '8px', borderRadius: 8, border: '1px solid var(--gray-300)', background: 'var(--surface)', fontWeight: 600, fontSize: '0.84rem', cursor: saving ? 'default' : 'pointer', color: 'var(--gray-700)' }}>
              {saving ? '保存中...' : '💾 保存'}
            </button>
            <button onClick={postToSlack} disabled={posting || !answer.trim()}
              style={{ flex: 1, padding: '8px', borderRadius: 8, border: 'none',
                background: answer.trim() ? '#2563eb' : 'var(--gray-200)',
                color: answer.trim() ? '#fff' : 'var(--gray-400)',
                fontWeight: 600, fontSize: '0.84rem', cursor: (posting || !answer.trim()) ? 'default' : 'pointer' }}>
              {posting ? '投稿中...' : '📤 Slackに投稿'}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
