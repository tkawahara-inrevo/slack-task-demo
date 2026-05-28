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
  const [view, setView]         = useState('list'); // 'list' | 'media'
  const [rpoResults, setRpoResults] = useState(null);
  const [mediaStats, setMediaStats] = useState([]);
  const [mediaFacets, setMediaFacets] = useState({ industries: [], prefectures: [], hire_types: [], size_buckets: [] });
  const [mediaFilters, setMediaFilters] = useState({ industry: '', hire_type: '', prefecture: '', size_bucket: '' });
  const [mediaLoading, setMediaLoading] = useState(false);
  const [expandedMedia, setExpandedMedia] = useState({});

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
    setEst({
      est_media_cost: req.est_media_cost ?? '',
      est_unit_price: req.est_unit_price ?? '',
      est_budget: req.est_budget ?? '',
      est_hire_count: req.est_hire_count ?? '',
      recommended_media: req.recommended_media ?? '',
    });
    setRpoResults(null);
    api.anRpoResults(req.id).then(r => setRpoResults(r.results)).catch(() => {});
  };

  const loadMediaStats = (filters = mediaFilters) => {
    setMediaLoading(true);
    api.anMediaStats(filters).then(r => {
      setMediaStats(r.stats || []);
      if (r.facets) setMediaFacets(r.facets);
    }).catch(() => {}).finally(() => setMediaLoading(false));
  };

  useEffect(() => {
    if (view === 'media') loadMediaStats(mediaFilters);
  }, [mediaFilters, view]);

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
            {[['list','AN依頼一覧'],['media','媒体実績DB']].map(([v,l]) => (
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
            <div style={{ padding: 12 }}>
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
            {/* 依頼者情報（メタ） */}
            <div style={{ background: 'var(--surface-2)', borderRadius: 10, padding: '12px 14px' }}>
              <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--gray-700)', marginBottom: 8 }}>依頼者情報</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: '0.82rem' }}>
                {[
                  ['会社名', selected.company_name || '—'],
                  ['担当営業', selected.sales_person || '—'],
                  selected.mentor_name ? ['メンター', selected.mentor_name] : null,
                  selected.hire_type ? ['新卒/中途', selected.hire_type] : null,
                  selected.request_type ? ['依頼粒度', selected.request_type] : null,
                  selected.priority ? ['優先度', selected.priority] : null,
                  selected.kintone_url ? ['kintone', <a key="kk" href={selected.kintone_url} target="_blank" rel="noreferrer" style={{ color: '#2563eb', textDecoration: 'none' }}>{selected.kintone_url}</a>] : null,
                  ['受付日時', fmtDate(selected.created_at)],
                  selected.deal_name || selected.customer_name ? ['CRM案件', selected.deal_name || selected.customer_name] : null,
                ].filter(Boolean).map(([label, val]) => (
                  <div key={label} style={{ display: 'flex', gap: 10 }}>
                    <div style={{ color: 'var(--gray-500)', fontSize: '0.72rem', width: 90, flexShrink: 0 }}>{label}</div>
                    <div style={{ color: 'var(--gray-900)', fontWeight: 500, wordBreak: 'break-all', flex: 1 }}>{val}</div>
                  </div>
                ))}
              </div>
            </div>

            {/* ヒアリング21項目 */}
            {selected.hearing && (
              <div>
                <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--gray-700)', marginBottom: 6 }}>ヒアリング項目（21項目）</div>
                <pre style={{ fontSize: '0.8rem', color: 'var(--gray-700)', background: 'var(--surface)', border: '1px solid var(--gray-200)', borderRadius: 8, padding: '10px 12px', whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0, lineHeight: 1.6 }}>
                  {selected.hearing}
                </pre>
              </div>
            )}

            {/* 媒体予算 */}
            {selected.budget && (
              <div>
                <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--gray-700)', marginBottom: 6 }}>媒体予算</div>
                <pre style={{ fontSize: '0.8rem', color: 'var(--gray-700)', background: 'var(--surface)', border: '1px solid var(--gray-200)', borderRadius: 8, padding: '10px 12px', whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0 }}>
                  {selected.budget}
                </pre>
              </div>
            )}

            {/* 依頼詳細（自由文） */}
            {selected.detail_parsed && (
              <div>
                <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--gray-700)', marginBottom: 6 }}>依頼内容（自由文）</div>
                <pre style={{ fontSize: '0.8rem', color: 'var(--gray-700)', background: 'var(--surface)', border: '1px solid var(--gray-200)', borderRadius: 8, padding: '10px 12px', whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0 }}>
                  {selected.detail_parsed}
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

            {/* 見積もり（構造化） */}
            <div>
              <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--gray-700)', marginBottom: 6 }}>調査結果（見積もり）</div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2,1fr)', gap: 8 }}>
                {[
                  ['est_media_cost', '媒体費（円）', 'number'],
                  ['est_unit_price', '採用単価（円）', 'number'],
                  ['est_budget', '推奨予算（円）', 'number'],
                  ['est_hire_count', '想定採用人数', 'number'],
                ].map(([key, label, type]) => (
                  <div key={key}>
                    <label style={{ fontSize: '0.68rem', color: 'var(--gray-500)' }}>{label}</label>
                    <input type={type} value={est[key]} onChange={e => setEst(p => ({ ...p, [key]: e.target.value }))}
                      style={{ width: '100%', padding: '6px 8px', border: '1px solid var(--gray-300)', borderRadius: 6, fontSize: '0.82rem', boxSizing: 'border-box', textAlign: 'right', background: 'var(--surface)' }} />
                  </div>
                ))}
                <div style={{ gridColumn: '1 / -1' }}>
                  <label style={{ fontSize: '0.68rem', color: 'var(--gray-500)' }}>推奨媒体</label>
                  <input type="text" value={est.recommended_media} onChange={e => setEst(p => ({ ...p, recommended_media: e.target.value }))}
                    placeholder="例: Indeed + OfferBox"
                    style={{ width: '100%', padding: '6px 8px', border: '1px solid var(--gray-300)', borderRadius: 6, fontSize: '0.82rem', boxSizing: 'border-box', background: 'var(--surface)' }} />
                </div>
              </div>
            </div>

            {/* 回答入力（自由文） */}
            <div>
              <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--gray-700)', marginBottom: 6 }}>調査結果・回答（詳細）</div>
              <textarea value={answer} onChange={e => setAnswer(e.target.value)} rows={8}
                placeholder="調査結果の詳細・補足を入力してください..."
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
