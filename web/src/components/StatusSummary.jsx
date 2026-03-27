const STATUS_CONFIG = {
  in_progress: { label: '進行中', color: '#2196F3', icon: '🔵' },
  done: { label: '完了', color: '#4CAF50', icon: '✅' },
  cancelled: { label: 'キャンセル', color: '#9E9E9E', icon: '⚪' },
  pending: { label: '保留', color: '#FF9800', icon: '🟡' },
};

export default function StatusSummary({ summary, onFilter }) {
  const entries = Object.entries(summary || {});
  const total = entries.reduce((acc, [, v]) => acc + v, 0);

  return (
    <div className="status-summary">
      <div className="status-card total" onClick={() => onFilter('')}>
        <div className="status-count">{total}</div>
        <div className="status-label">全タスク</div>
      </div>
      {entries.map(([status, count]) => {
        const cfg = STATUS_CONFIG[status] || { label: status, color: '#666', icon: '⚪' };
        return (
          <div
            key={status}
            className="status-card"
            style={{ borderTopColor: cfg.color }}
            onClick={() => onFilter(status)}
          >
            <div className="status-count">{cfg.icon} {count}</div>
            <div className="status-label">{cfg.label}</div>
          </div>
        );
      })}
    </div>
  );
}
