const STATUS_CONFIG = {
  in_progress: { label: '進行中', color: '#2196F3' },
  done:        { label: '完了',   color: '#4CAF50' },
  pending:     { label: '保留',   color: '#FF9800' },
  cancelled:   { label: 'キャンセル', color: '#9E9E9E' },
};

export default function StatusSummary({ summary, onFilter, activeStatus }) {
  const entries = Object.entries(STATUS_CONFIG)
    .map(([key, cfg]) => ({ key, ...cfg, count: summary?.[key] || 0 }));
  const total = entries.reduce((acc, e) => acc + e.count, 0);

  return (
    <div className="status-bar-widget">
      <div className="status-bar-total" onClick={() => onFilter('')} style={{ cursor: 'pointer' }}>
        <span className="status-bar-total-num">{total}</span>
        <span className="status-bar-total-label">件</span>
      </div>

      {total > 0 && (
        <div className="status-bar-track">
          {entries.filter(e => e.count > 0).map(e => (
            <div
              key={e.key}
              className="status-bar-segment"
              style={{ width: `${(e.count / total) * 100}%`, background: e.color }}
              title={`${e.label}: ${e.count}件`}
              onClick={() => onFilter(e.key)}
            />
          ))}
        </div>
      )}

      <div className="status-bar-legend">
        {entries.map(e => (
          <div
            key={e.key}
            className={`status-legend-item ${activeStatus === e.key ? 'active' : ''}`}
            onClick={() => onFilter(activeStatus === e.key ? '' : e.key)}
          >
            <span className="status-legend-dot" style={{ background: e.color }} />
            <span className="status-legend-label">{e.label}</span>
            <span className="status-legend-count">{e.count}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
