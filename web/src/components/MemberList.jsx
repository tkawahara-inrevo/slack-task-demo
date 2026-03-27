export default function MemberList({ members, onSelect, selectedId }) {
  if (!members || !members.length) return <p className="empty">メンバーデータなし</p>;

  return (
    <div className="sidebar-member-list">
      {members.map((m) => (
        <div
          key={m.assignee_id}
          className={`sidebar-member-row ${selectedId === m.assignee_id ? 'selected' : ''}`}
          onClick={() => onSelect(m.assignee_id)}
        >
          <span className="sidebar-member-name">{m.displayName}</span>
          <div className="sidebar-member-stats">
            <span title="進行中" style={{ color: '#2196F3' }}>{m.in_progress}</span>
            <span title="完了" style={{ color: '#4CAF50' }}>{m.done}</span>
            {m.overdue > 0 && <span title="期限切れ" style={{ color: '#e74c3c', fontWeight: 600 }}>！{m.overdue}</span>}
          </div>
        </div>
      ))}
    </div>
  );
}
