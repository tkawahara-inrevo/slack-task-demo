import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../api/client';

export default function TaskCreate() {
  const navigate = useNavigate();
  const [projects, setProjects] = useState([]);
  const [form, setForm] = useState({
    title: '',
    description: '',
    assignee_id: '',
    due_date: '',
    project_id: '',
  });
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.projects().then((r) => setProjects(r.projects)).catch(console.error);
  }, []);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!form.title.trim()) return;
    setSaving(true);
    try {
      const r = await api.taskCreate({
        title: form.title.trim(),
        description: form.description.trim() || form.title.trim(),
        assignee_id: form.assignee_id.trim() || undefined,
        due_date: form.due_date || undefined,
        project_id: form.project_id || undefined,
      });
      navigate(`/tasks/${r.task.id}`);
    } catch (e) {
      console.error(e);
      setSaving(false);
    }
  };

  return (
    <div className="task-create">
      <h1>タスク作成</h1>
      <form onSubmit={handleSubmit} className="task-create-form">
        <div className="form-group">
          <label>タイトル *</label>
          <input
            type="text"
            value={form.title}
            onChange={(e) => setForm({ ...form, title: e.target.value })}
            placeholder="タスクのタイトル"
            autoFocus
          />
        </div>

        <div className="form-group">
          <label>説明</label>
          <textarea
            rows={5}
            value={form.description}
            onChange={(e) => setForm({ ...form, description: e.target.value })}
            placeholder="タスクの詳細説明（任意）"
          />
        </div>

        <div className="form-row">
          <div className="form-group">
            <label>担当者 (Slack User ID)</label>
            <input
              type="text"
              value={form.assignee_id}
              onChange={(e) => setForm({ ...form, assignee_id: e.target.value })}
              placeholder="未指定なら自分"
            />
          </div>

          <div className="form-group">
            <label>期限</label>
            <input
              type="date"
              value={form.due_date}
              onChange={(e) => setForm({ ...form, due_date: e.target.value })}
            />
          </div>
        </div>

        {projects.length > 0 && (
          <div className="form-group">
            <label>プロジェクト</label>
            <select value={form.project_id} onChange={(e) => setForm({ ...form, project_id: e.target.value })}>
              <option value="">なし</option>
              {projects.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
            </select>
          </div>
        )}

        <div className="form-actions">
          <button type="submit" className="btn-primary" disabled={saving || !form.title.trim()}>
            {saving ? '作成中...' : 'タスクを作成'}
          </button>
          <button type="button" className="btn-ghost" onClick={() => navigate('/')}>キャンセル</button>
        </div>
      </form>
    </div>
  );
}
