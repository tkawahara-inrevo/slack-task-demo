import { useState, useEffect, useCallback } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Cell, LabelList, ReferenceLine,
} from 'recharts';
import { api } from '../../api/client';

const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316','#84cc16','#ec4899','#6366f1'];
const fmtM = n => { if (!n) return '—'; const m = Math.round(Number(n)); return m >= 10000 ? `${Math.round(m/10000).toLocaleString()}万` : m.toLocaleString(); };
const TICK = { fontSize: 11, fill: '#6b7280' };

const YOMI_COLOR = {
  'アポ化前':  '#94a3b8',
  'アポ取得済': '#3b82f6',
  '商談中':    '#f59e0b',
  '受注':      '#10b981',
};

const pad = n => String(n).padStart(2, '0');
const ymd = (d) => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
const endOfMonth = (y, m) => ymd(new Date(y, m, 0)); // month is 1-based

function getPreset(key) {
  const now = new Date();
  const y = now.getFullYear(), m = now.getMonth() + 1;
  switch (key) {
    case 'this_month':   return { from: `${y}-${pad(m)}-01`, to: endOfMonth(y, m) };
    case 'last_month': {
      const ly = m === 1 ? y-1 : y, lm = m === 1 ? 12 : m-1;
      return { from: `${ly}-${pad(lm)}-01`, to: endOfMonth(ly, lm) };
    }
    case 'last3': {
      const d3 = new Date(now); d3.setMonth(d3.getMonth()-3);
      return { from: ymd(d3), to: ymd(now) };
    }
    case 'this_year': return { from: `${y}-04-01`, to: `${y+1}-03-31` };
    default: return { from: '', to: '' };
  }
}

export default function LeadsDashboard() {
  const [data, setData]     = useState(null);
  const [loading, setLoading] = useState(false);
  const [from, setFrom]     = useState('');
  const [to, setTo]         = useState('');
  const [activePreset, setActivePreset] = useState(null);
  const [page, setPage]     = useState(1);

  const load = useCallback(async (f, t, p = 1) => {
    setLoading(true);
    try {
      const d = await api.crmLeadsDashboard({ from: f, to: t, page: p });
      setData(d);
      setPage(p);
    } catch (e) { console.error(e); }
    setLoading(false);
  }, []);

  useEffect(() => { load('', '', 1); }, []);

  const applyPreset = (key) => {
    const { from: f, to: t } = getPreset(key);
    setFrom(f); setTo(t); setActivePreset(key);
    load(f, t, 1);
  };

  const handleSearch = () => { setActivePreset(null); load(from, to, 1); };
  const handleReset  = () => { setFrom(''); setTo(''); setActivePreset(null); load('', '', 1); };

  const presets = [
    { key: 'this_month', label: '今月' },
    { key: 'last_month', label: '先月' },
    { key: 'last3',      label: '直近3ヶ月' },
    { key: 'this_year',  label: '今年度' },
  ];

  const total = data?.funnel?.reduce((s, r) => s + r.cnt, 0) || 0;

  return (
    <div style={{ padding: '0 0 32px' }}>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ margin: 0, fontSize: '1.1rem', fontWeight: 800, color: '#111827' }}>リード管理</h2>
        {data?.period && (
          <p style={{ margin: '3px 0 0', fontSize: '0.8rem', color: '#6b7280' }}>
            {(() => {
              const f = data.period.from, t = data.period.to;
              if (!f || !t) return null;
              const fd = new Date(f), td = new Date(t);
              const fmt = d => `${d.getFullYear()}/${d.getMonth()+1}/${d.getDate()}`;
              // 同月なら "2026年5月" 表記
              if (fd.getFullYear() === td.getFullYear() && fd.getMonth() === td.getMonth())
                return `${fd.getFullYear()}年${fd.getMonth()+1}月`;
              return `${fmt(fd)} 〜 ${fmt(td)}`;
            })()}
          </p>
        )}
      </div>

      {/* 期間フィルター */}
      <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '12px 16px', marginBottom: 20 }}>
        {/* クイック選択 */}
        <div style={{ display: 'flex', gap: 6, marginBottom: 10, flexWrap: 'wrap' }}>
          {presets.map(p => (
            <button key={p.key} onClick={() => applyPreset(p.key)} style={{
              padding: '4px 12px', borderRadius: 20, fontSize: '0.8rem', fontWeight: 600, cursor: 'pointer',
              background: activePreset === p.key ? '#3b82f6' : '#f1f5f9',
              color: activePreset === p.key ? '#fff' : '#374151',
              border: 'none',
            }}>{p.label}</button>
          ))}
        </div>
        {/* カスタム期間 */}
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <span style={{ fontSize: '0.8rem', color: '#6b7280' }}>カスタム</span>
          <input type="date" value={from} onChange={e => { setFrom(e.target.value); setActivePreset(null); }}
            style={{ border: '1px solid #d1d5db', borderRadius: 6, padding: '4px 8px', fontSize: '0.82rem' }} />
          <span style={{ color: '#9ca3af' }}>〜</span>
          <input type="date" value={to} onChange={e => { setTo(e.target.value); setActivePreset(null); }}
            style={{ border: '1px solid #d1d5db', borderRadius: 6, padding: '4px 8px', fontSize: '0.82rem' }} />
          <button onClick={handleSearch}
            style={{ background: '#3b82f6', color: '#fff', border: 'none', borderRadius: 6, padding: '5px 14px', fontSize: '0.82rem', cursor: 'pointer', fontWeight: 600 }}>
            適用
          </button>
          {(from || to || activePreset) && (
            <button onClick={handleReset}
              style={{ background: 'none', border: '1px solid #d1d5db', borderRadius: 6, padding: '4px 10px', fontSize: '0.8rem', cursor: 'pointer', color: '#6b7280' }}>
              リセット
            </button>
          )}
        </div>
      </div>

      {loading && <div style={{ color: '#9ca3af', marginBottom: 16 }}>読み込み中...</div>}

      {data && !loading && (
        <>
          {/* ファネル KPI — 全て期間合計を分母にした割合 */}
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 20 }}>
            <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '14px 20px', minWidth: 150, flex: 1 }}>
              <div style={{ fontSize: '0.72rem', color: '#6b7280', marginBottom: 4 }}>期間内合計</div>
              <div style={{ fontSize: '2rem', fontWeight: 800, color: '#374151' }}>
                {(data.periodTotal || 0).toLocaleString()}
              </div>
            </div>
            {data.funnel.map((f) => {
              const label = f.label === 'リード（アポ化前）' ? 'アポ化前' : f.label;
              const rate = data.periodTotal > 0 ? Math.round(f.cnt / data.periodTotal * 100) : null;
              return (
                <div key={f.label} style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '14px 20px', minWidth: 150, flex: 1 }}>
                  <div style={{ fontSize: '0.72rem', color: '#6b7280', marginBottom: 4 }}>{label}</div>
                  <div style={{ fontSize: '2rem', fontWeight: 800, color: YOMI_COLOR[f.label] || '#374151' }}>
                    {f.cnt.toLocaleString()}
                  </div>
                  {rate !== null && (
                    <div style={{ fontSize: '0.72rem', color: '#9ca3af', marginTop: 2 }}>
                      {rate}%
                    </div>
                  )}
                </div>
              );
            })}
          </div>

          {/* 統計サマリー */}
          {data.stats && (
            <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 20 }}>
              {data.stats.diffPct !== null && (
                <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '12px 18px' }}>
                  <div style={{ fontSize: '0.72rem', color: '#6b7280', marginBottom: 2 }}>前期間比</div>
                  <div style={{ fontSize: '1.4rem', fontWeight: 800, color: data.stats.diffPct >= 0 ? '#16a34a' : '#dc2626' }}>
                    {data.stats.diffPct >= 0 ? '+' : ''}{data.stats.diffPct}%
                  </div>
                  <div style={{ fontSize: '0.7rem', color: '#9ca3af' }}>前期間 {data.stats.prev}件</div>
                </div>
              )}
              <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '12px 18px' }}>
                <div style={{ fontSize: '0.72rem', color: '#6b7280', marginBottom: 2 }}>月平均（直近12ヶ月）</div>
                <div style={{ fontSize: '1.4rem', fontWeight: 800, color: '#374151' }}>{data.stats.avg12}件/月</div>
              </div>
            </div>
          )}

          {/* 月次推移（常に直近12ヶ月） */}
          <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 12px', marginBottom: 20 }}>
            <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 4, paddingLeft: 8 }}>月次リード推移（直近12ヶ月）</div>
            <div style={{ fontSize: '0.72rem', color: '#9ca3af', paddingLeft: 8, marginBottom: 10 }}>点線: 12ヶ月平均 {data.stats?.avg12}件</div>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={data.trend} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis dataKey="month" tick={TICK} />
                <YAxis tick={TICK} allowDecimals={false} />
                <Tooltip formatter={v => [`${v}件`]} />
                {data.stats?.avg12 > 0 && (
                  <ReferenceLine y={data.stats.avg12} stroke="#f59e0b" strokeDasharray="4 3"
                    label={{ value: `平均 ${data.stats.avg12}`, fill: '#f59e0b', fontSize: 11, position: 'right' }} />
                )}
                <Line type="monotone" dataKey="cnt" stroke="#3b82f6" strokeWidth={2} dot={{ r: 3 }} name="リード数" />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* 流入経路（全件・数値常時表示） */}
          <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 12px', marginBottom: 20 }}>
            <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 12, paddingLeft: 8 }}>流入経路別（全件）</div>
            <ResponsiveContainer width="100%" height={Math.max(200, data.bySource.length * 26)}>
              <BarChart data={data.bySource} layout="vertical" margin={{ top: 0, right: 50, left: 4, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                <XAxis type="number" tick={TICK} allowDecimals={false} />
                <YAxis type="category" dataKey="source" tick={{ fontSize: 11, fill: '#6b7280' }} width={130} />
                <Tooltip formatter={v => [`${v}件`]} />
                <Bar dataKey="cnt" radius={[0, 4, 4, 0]} name="件数">
                  <LabelList dataKey="cnt" position="right" style={{ fontSize: 11, fill: '#374151' }} />
                  {data.bySource.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* アポ化/受注 流入経路 */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 20 }}>
            {[
              { title: 'アポ化につながった流入経路', key: 'appoBySource', color: '#3b82f6' },
              { title: '受注につながった流入経路', key: 'orderBySource', color: '#10b981' },
            ].map(({ title, key, color }) => (
              <div key={key} style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 12px' }}>
                <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 12, paddingLeft: 8 }}>{title}</div>
                {data[key]?.length === 0
                  ? <div style={{ color: '#9ca3af', fontSize: '0.8rem', textAlign: 'center', padding: 16 }}>データなし</div>
                  : (
                    <ResponsiveContainer width="100%" height={Math.max(160, (data[key]?.length || 0) * 26)}>
                      <BarChart data={data[key]} layout="vertical" margin={{ top: 0, right: 50, left: 4, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                        <XAxis type="number" tick={TICK} allowDecimals={false} />
                        <YAxis type="category" dataKey="source" tick={{ fontSize: 11, fill: '#6b7280' }} width={120} />
                        <Tooltip formatter={v => [`${v}件`]} />
                        <Bar dataKey="cnt" fill={color} radius={[0, 4, 4, 0]} name="件数">
                          <LabelList dataKey="cnt" position="right" style={{ fontSize: 11, fill: '#374151' }} />
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  )
                }
              </div>
            ))}
          </div>

          {/* リード一覧 */}
          <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, overflow: 'hidden' }}>
            <div style={{ padding: '12px 16px', borderBottom: '1px solid #f3f4f6', display: 'flex', alignItems: 'center', gap: 10 }}>
              <span style={{ fontWeight: 700, fontSize: '0.85rem' }}>リード一覧</span>
              <span style={{ fontSize: '0.75rem', color: '#9ca3af' }}>全{(data.pagination?.total || 0).toLocaleString()}件</span>
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
                <thead>
                  <tr style={{ background: '#f8fafc' }}>
                    {['流入日', '会社名', 'ヨミ', '流入経路', '担当者'].map(h => (
                      <th key={h} style={{ padding: '8px 12px', textAlign: 'left', fontWeight: 600, color: '#64748b', whiteSpace: 'nowrap', borderBottom: '1px solid #e5e7eb', fontSize: '0.72rem' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.recent.map((r, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid #f8fafc', background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap', color: '#6b7280' }}>{r.inflow_date?.slice(0,10) || '—'}</td>
                      <td style={{ padding: '7px 12px', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontWeight: 500 }}>{r.customer || '—'}</td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap' }}>
                        <span style={{ fontSize: '0.72rem', background: r.yomi === 'アポ化前' ? '#f1f5f9' : '#eff6ff', color: r.yomi === 'アポ化前' ? '#64748b' : '#3b82f6', borderRadius: 4, padding: '2px 6px' }}>
                          {r.yomi}
                        </span>
                      </td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap', color: '#374151' }}>{r.source || '—'}</td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap', color: '#374151' }}>{r.rep || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {/* ページング */}
            {data.pagination && data.pagination.total > data.pagination.limit && (
              <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', gap: 8, padding: '12px 16px', borderTop: '1px solid #f3f4f6' }}>
                <button onClick={() => load(from, to, page - 1)} disabled={page <= 1}
                  style={{ padding: '5px 12px', border: '1px solid #d1d5db', borderRadius: 6, cursor: page <= 1 ? 'not-allowed' : 'pointer', background: '#fff', color: page <= 1 ? '#d1d5db' : '#374151', fontSize: '0.8rem' }}>
                  ← 前
                </button>
                <span style={{ fontSize: '0.8rem', color: '#6b7280' }}>
                  {page} / {Math.ceil(data.pagination.total / data.pagination.limit)} ページ
                </span>
                <button onClick={() => load(from, to, page + 1)} disabled={page >= Math.ceil(data.pagination.total / data.pagination.limit)}
                  style={{ padding: '5px 12px', border: '1px solid #d1d5db', borderRadius: 6, cursor: page >= Math.ceil(data.pagination.total / data.pagination.limit) ? 'not-allowed' : 'pointer', background: '#fff', color: page >= Math.ceil(data.pagination.total / data.pagination.limit) ? '#d1d5db' : '#374151', fontSize: '0.8rem' }}>
                  次 →
                </button>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}
