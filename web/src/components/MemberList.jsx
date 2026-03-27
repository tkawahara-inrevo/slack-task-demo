export default function MemberList({ members, onSelect, selectedId }) {
  if (!members || !members.length) return <p className="empty">メンバーデータなし</p>;

  return (
    <div className="member-list">
      {members.map((m) => (
        <div
          key={m.assignee_id}
          className={`member-card ${selectedId === m.assignee_id ? 'selected' : ''}`}
          onClick={() => onSelect(m.assignee_id)}
        >
          <div className="member-name">{m.displayName}</div>
          <div className="member-stats">
            <span className="stat" title="進行中">🔵 {m.in_progress}</span>
            <span className="stat" title="完了">✅ {m.done}</span>
            {m.overdue > 0 && <span className="stat overdue" title="期限切れ">🔴 {m.overdue}</span>}
          </div>
        </div>
      ))}
    </div>
  );
}
