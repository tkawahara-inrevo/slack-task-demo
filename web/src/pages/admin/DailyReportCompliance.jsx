import { useEffect, useState } from 'react';
import { api } from '../../api/client';

// 日報遵守率（月次）
// 集計ロジック:
//   - 勤務日数 = HRMOS の segment_display_title が「出勤系」の日数
//   - 提出日数 = hrmos_stamps (ok=true) があった日数 (bot処理 or skip 含む)

function todayYmd() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
}

export default function DailyReportCompliance() {
  const [month, setMonth] = useState(todayYmd());
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [sortKey, setSortKey] = useState('in_rate');
  const [detail, setDetail] = useState(null); // {user_id, type: 'in'|'out'}

  async function reload() {
    setLoading(true);
    try {
      const r = await api.dailyReportCompliance(month);
      setData(r);
    } catch (e) {
      alert('取得エラー: ' + e.message);
    } finally {
      setLoading(false);
    }
  }
  useEffect(() => { reload(); }, [month]); // eslint-disable-line

  function colorRate(rate) {
    if (rate == null) return '#9ca3af';
    if (rate >= 90) return '#059669';
    if (rate >= 70) return '#d97706';
    return '#dc2626';
  }
  function bgRate(rate) {
    if (rate == null) return 'transparent';
    if (rate >= 90) return '#ecfdf5';
    if (rate >= 70) return '#fffbeb';
    return '#fef2f2';
  }

  const members = (data?.members || []).slice().sort((a, b) => {
    const av = a[sortKey] ?? -1;
    const bv = b[sortKey] ?? -1;
    if (av !== bv) return av - bv;
    return (a.display_name || '').localeCompare(b.display_name || '');
  });

  return (
    <div style={{ padding: 20, maxWidth: 1100 }}>
      <h2 style={{ margin: '0 0 16px', fontSize: '1.2rem' }}>日報遵守率（月次）</h2>

      {/* コントロール */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: 16, flexWrap: 'wrap' }}>
        <label style={{ fontSize: 13 }}>
          月：
          <input type="month" value={month} onChange={e => setMonth(e.target.value)}
            style={{ marginLeft: 6, padding: '4px 8px', border: '1px solid #d1d5db', borderRadius: 4 }} />
        </label>
        <label style={{ fontSize: 13 }}>
          並び順：
          <select value={sortKey} onChange={e => setSortKey(e.target.value)}
            style={{ marginLeft: 6, padding: '4px 8px', border: '1px solid #d1d5db', borderRadius: 4 }}>
            <option value="in_rate">出勤遵守率（低い順）</option>
            <option value="out_rate">退勤遵守率（低い順）</option>
            <option value="work_days">勤務日数</option>
          </select>
        </label>
        {loading && <span style={{ fontSize: 12, color: '#6b7280' }}>読み込み中...</span>}
      </div>

      {/* 凡例 */}
      <div style={{ display: 'flex', gap: 12, fontSize: 11, color: '#6b7280', marginBottom: 8 }}>
        <span><span style={{ display: 'inline-block', width: 10, height: 10, background: '#ecfdf5', border: '1px solid #059669', verticalAlign: 'middle' }}></span> 90%以上</span>
        <span><span style={{ display: 'inline-block', width: 10, height: 10, background: '#fffbeb', border: '1px solid #d97706', verticalAlign: 'middle' }}></span> 70〜90%</span>
        <span><span style={{ display: 'inline-block', width: 10, height: 10, background: '#fef2f2', border: '1px solid #dc2626', verticalAlign: 'middle' }}></span> 70%未満</span>
      </div>

      {/* テーブル */}
      <table style={{ width: '100%', borderCollapse: 'collapse', background: '#fff' }}>
        <thead>
          <tr style={{ background: '#f9fafb', borderBottom: '2px solid #e5e7eb' }}>
            <th style={th}>名前</th>
            <th style={th}>勤務日</th>
            <th style={th} colSpan={3}>出勤日報</th>
            <th style={th} colSpan={3}>退勤日報</th>
          </tr>
          <tr style={{ background: '#f9fafb', borderBottom: '1px solid #e5e7eb', fontSize: 11, color: '#6b7280' }}>
            <th style={th}></th>
            <th style={th}></th>
            <th style={th}>提出</th>
            <th style={th}>未提出</th>
            <th style={th}>遵守率</th>
            <th style={th}>提出</th>
            <th style={th}>未提出</th>
            <th style={th}>遵守率</th>
          </tr>
        </thead>
        <tbody>
          {members.length === 0 && !loading && (
            <tr><td colSpan={8} style={{ padding: 20, textAlign: 'center', color: '#9ca3af' }}>データなし</td></tr>
          )}
          {members.map(m => {
            const inMissing = m.work_days - m.in_count;
            const outMissing = m.work_days - m.out_count;
            return (
              <tr key={m.user_id} style={{ borderBottom: '1px solid #f3f4f6' }}>
                <td style={td}>
                  {m.display_name}
                  {!m.hrmos_resolved && <span style={{ fontSize: 10, color: '#dc2626', marginLeft: 6 }}>(HRMOS未解決)</span>}
                </td>
                <td style={{ ...td, textAlign: 'center' }}>{m.work_days}</td>
                <td style={{ ...td, textAlign: 'center' }}>{m.in_count}</td>
                <td style={{ ...td, textAlign: 'center', color: inMissing > 0 ? '#dc2626' : '#9ca3af', cursor: inMissing > 0 ? 'pointer' : 'default' }}
                    onClick={() => inMissing > 0 && setDetail({ user: m, type: 'in' })}>
                  {inMissing > 0 ? `${inMissing} 件` : '—'}
                </td>
                <td style={{ ...td, textAlign: 'center', background: bgRate(m.in_rate), color: colorRate(m.in_rate), fontWeight: 700 }}>
                  {m.in_rate != null ? `${m.in_rate}%` : '—'}
                </td>
                <td style={{ ...td, textAlign: 'center' }}>{m.out_count}</td>
                <td style={{ ...td, textAlign: 'center', color: outMissing > 0 ? '#dc2626' : '#9ca3af', cursor: outMissing > 0 ? 'pointer' : 'default' }}
                    onClick={() => outMissing > 0 && setDetail({ user: m, type: 'out' })}>
                  {outMissing > 0 ? `${outMissing} 件` : '—'}
                </td>
                <td style={{ ...td, textAlign: 'center', background: bgRate(m.out_rate), color: colorRate(m.out_rate), fontWeight: 700 }}>
                  {m.out_rate != null ? `${m.out_rate}%` : '—'}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      {/* 未提出日詳細モーダル */}
      {detail && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 100 }}
          onClick={() => setDetail(null)}>
          <div style={{ background: 'white', borderRadius: 8, padding: 20, minWidth: 320, maxWidth: 500 }}
            onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 12px', fontSize: '1rem' }}>
              {detail.user.display_name} - {detail.type === 'in' ? '出勤' : '退勤'}日報 未提出日
            </h3>
            <div style={{ fontSize: 13, color: '#374151', maxHeight: 400, overflowY: 'auto' }}>
              {(detail.type === 'in' ? detail.user.in_missing_dates : detail.user.out_missing_dates).map(d => (
                <div key={d} style={{ padding: '6px 8px', borderBottom: '1px solid #f3f4f6' }}>{d}</div>
              ))}
            </div>
            <div style={{ marginTop: 12, textAlign: 'right' }}>
              <button onClick={() => setDetail(null)}
                style={{ padding: '6px 14px', background: '#6b7280', color: 'white', border: 'none', borderRadius: 4, cursor: 'pointer' }}>閉じる</button>
            </div>
          </div>
        </div>
      )}

      <div style={{ marginTop: 16, fontSize: 11, color: '#9ca3af' }}>
        ※ 勤務日 = HRMOSのsegmentが出勤系 / 提出 = 該当日にbot打刻処理あり (skip=手動打刻含む)
      </div>
    </div>
  );
}

const th = { padding: '8px 10px', textAlign: 'left', fontSize: 12, fontWeight: 600 };
const td = { padding: '8px 10px', fontSize: 13 };
