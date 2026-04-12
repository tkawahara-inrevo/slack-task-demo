import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api/client';

const DEFAULT_ITEM_COLOR = '#f97316';
const DOW_LABELS = ['日', '月', '火', '水', '木', '金', '土'];
// Weekly recurrence only Mon-Fri (indices 1-5)
const WEEKDAY_DOWS = [1, 2, 3, 4, 5];

function hexToRgba(hex, alpha) {
  const r = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  if (!r) return hex;
  return `rgba(${parseInt(r[1], 16)},${parseInt(r[2], 16)},${parseInt(r[3], 16)},${alpha})`;
}

function _dateKey(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

const _holidayCache = {};
function getJapaneseHolidays(year) {
  if (_holidayCache[year]) return _holidayCache[year];
  const set = new Set();
  const add = (m, d) => { if (d >= 1) set.add(`${year}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')}`); };
  const nthWd = (m, n, dow) => {
    const d = new Date(year, m - 1, 1);
    let c = 0;
    while (d.getMonth() === m - 1) { if (d.getDay() === dow && ++c === n) return d.getDate(); d.setDate(d.getDate() + 1); }
    return -1;
  };
  const vernal = Math.floor(20.8431 + 0.242194 * (year - 1980) - Math.floor((year - 1980) / 4));
  const autumn = Math.floor(23.2488 + 0.242194 * (year - 1980) - Math.floor((year - 1980) / 4));
  // Fixed
  add(1,1); add(2,11); add(2,23); add(3,vernal); add(4,29);
  add(5,3); add(5,4); add(5,5); add(8,11); add(9,autumn);
  add(11,3); add(11,23);
  // Floating
  add(1, nthWd(1,2,1)); add(7, nthWd(7,3,1)); add(9, nthWd(9,3,1)); add(10, nthWd(10,2,1));
  // Substitute holidays (振替休日): holiday on Sunday → next non-holiday Mon
  const base = new Set(set);
  for (const h of base) {
    const d = new Date(h);
    if (d.getDay() === 0) {
      const next = new Date(d);
      next.setDate(next.getDate() + 1);
      while (base.has(_dateKey(next))) next.setDate(next.getDate() + 1);
      set.add(_dateKey(next));
    }
  }
  // Sandwich holidays (国民の休日)
  const isLeap = (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
  const days = 365 + (isLeap ? 1 : 0);
  for (let i = 0; i < days; i++) {
    const d = new Date(year, 0, i + 1);
    const k = _dateKey(d);
    if (set.has(k) || d.getDay() === 0 || d.getDay() === 6) continue;
    const prev = new Date(d); prev.setDate(prev.getDate() - 1);
    const next = new Date(d); next.setDate(next.getDate() + 1);
    if (set.has(_dateKey(prev)) && set.has(_dateKey(next))) set.add(k);
  }
  _holidayCache[year] = set;
  return set;
}

function isWorkday(date) {
  const dow = date.getDay();
  if (dow === 0 || dow === 6) return false;
  return !getJapaneseHolidays(date.getFullYear()).has(_dateKey(date));
}

function matchesRecurrence(type, config, date) {
  if (type === 'daily') return isWorkday(date);
  if (type === 'weekly') return (config?.days || []).includes(date.getDay());
  if (type === 'monthly') return (config?.days || []).includes(date.getDate());
  return false;
}

function formatMonthLabel(monthKey) {
  const [year, month] = monthKey.split('-').map(Number);
  return `${year}年${month}月`;
}

function shiftMonth(monthKey, delta) {
  const [year, month] = monthKey.split('-').map(Number);
  const d = new Date(Date.UTC(year, month - 1 + delta, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}

function dateKey(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function monthKeyFromDate(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}

function getMonthDates(monthKey) {
  const [year, month] = monthKey.split('-').map(Number);
  const days = new Date(year, month, 0).getDate();
  return Array.from({ length: days }, (_, i) => new Date(year, month - 1, i + 1));
}

function getRollingDates(base = new Date(), length = 31) {
  const start = new Date(base.getFullYear(), base.getMonth(), base.getDate());
  return Array.from({ length }, (_, i) => {
    const d = new Date(start);
    d.setDate(start.getDate() + i);
    return d;
  });
}

function getRangeLabel(viewMode, monthKey, dates) {
  if (viewMode === 'month') return formatMonthLabel(monthKey);
  if (!dates.length) return '直近31日';
  const f = dates[0], l = dates[dates.length - 1];
  return `${f.getMonth() + 1}/${f.getDate()} - ${l.getMonth() + 1}/${l.getDate()}`;
}

function getRequiredMonthKeys(dates) {
  return [...new Set(dates.map(monthKeyFromDate))];
}

function buildCellsByItem(cells) {
  const map = {};
  for (const cell of cells || []) {
    const key = `${cell.month_key}-${String(cell.day_num).padStart(2, '0')}`;
    if (!map[cell.item_id]) map[cell.item_id] = {};
    map[cell.item_id][key] = Number(cell.intensity);
  }
  return map;
}

function mergeCellMaps(maps) {
  const merged = {};
  for (const cur of maps) {
    for (const [id, cellMap] of Object.entries(cur || {})) {
      if (!merged[id]) merged[id] = {};
      Object.assign(merged[id], cellMap);
    }
  }
  return merged;
}

function serializeCellsForMonth(cellMap = {}, monthKey) {
  return Object.entries(cellMap)
    .filter(([k, v]) => k.startsWith(`${monthKey}-`) && [1, 2].includes(Number(v)))
    .map(([k, v]) => ({ dayNum: Number(k.slice(-2)), intensity: Number(v) }))
    .sort((a, b) => a.dayNum - b.dayNum);
}

function uniqueItems(items = []) {
  const seen = new Map();
  for (const item of items) if (!seen.has(item.id)) seen.set(item.id, item);
  return [...seen.values()];
}

function getDateRange(displayDates, keyA, keyB) {
  const keys = displayDates.map(dateKey);
  const ia = keys.indexOf(keyA), ib = keys.indexOf(keyB);
  if (ia === -1 || ib === -1) return [];
  const [lo, hi] = ia <= ib ? [ia, ib] : [ib, ia];
  return keys.slice(lo, hi + 1);
}

// ─── Unified Gantt (Slack tasks + workload items) ────────────────────────────

const STATUS_DEF = {
  in_progress: { bar: '#3b82f6', text: '#1d4ed8', bg: '#dbeafe', label: '進行中' },
  done:        { bar: '#22c55e', text: '#15803d', bg: '#dcfce7', label: '完了' },
  pending:     { bar: '#f97316', text: '#c2410c', bg: '#ffedd5', label: '保留' },
  cancelled:   { bar: '#9ca3af', text: '#6b7280', bg: '#f3f4f6', label: 'キャンセル' },
};
const statusDef = (s) => STATUS_DEF[s] || { bar: '#93c5fd', text: '#3b82f6', bg: '#eff6ff', label: s || '不明' };

const GANTT_DAY_W = 28;
const DOW_JP = ['日', '月', '火', '水', '木', '金', '土'];

function stripSlack(text) {
  return (text || '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
}

function groupConsecutiveIndices(indices, dayW) {
  const bars = [];
  let rs = null, re = null;
  for (const i of indices) {
    if (rs === null) { rs = i; re = i; }
    else if (i === re + 1) { re = i; }
    else { bars.push({ left: rs * dayW, width: Math.max((re - rs + 1) * dayW - 2, 4) }); rs = i; re = i; }
  }
  if (rs !== null) bars.push({ left: rs * dayW, width: Math.max((re - rs + 1) * dayW - 2, 4) });
  return bars;
}

export default function WorkloadGantt() {
  const initialMonth = new Date().toISOString().slice(0, 7);
  const [teams, setTeams] = useState([]);
  const [selectedParentId, setSelectedParentId] = useState('');
  const [selectedTeamId, setSelectedTeamId] = useState('');
  const [monthKey, setMonthKey] = useState(initialMonth);
  const [viewMode, setViewMode] = useState('month');
  const [members, setMembers] = useState([]);
  const [items, setItems] = useState([]);
  const [cellsByItem, setCellsByItem] = useState({});
  const [loading, setLoading] = useState(false);
  const [categories, setCategories] = useState([]);
  const [catMgrOpen, setCatMgrOpen] = useState(false);
  const [catDraftName, setCatDraftName] = useState('');
  const [catDraftColor, setCatDraftColor] = useState('#6366f1');
  const [editingCatId, setEditingCatId] = useState('');
  const [allTasks, setAllTasks] = useState([]);

  // Editor state
  const [editorOpen, setEditorOpen] = useState(false);
  const [editorMode, setEditorMode] = useState('create');
  const [editorOwnerUserId, setEditorOwnerUserId] = useState('');
  const [editingItemId, setEditingItemId] = useState('');
  const [draftTitle, setDraftTitle] = useState('');
  const [draftCategory, setDraftCategory] = useState('');
  const [draftNotes, setDraftNotes] = useState('');
  const [draftColor, setDraftColor] = useState(DEFAULT_ITEM_COLOR);
  const [draftRecurrenceType, setDraftRecurrenceType] = useState('other');
  const [draftRecurrenceConfig, setDraftRecurrenceConfig] = useState({});
  const [draftStartDate, setDraftStartDate] = useState('');
  const [draftEndDate, setDraftEndDate] = useState('');

  const displayDates = useMemo(
    () => (viewMode === 'month' ? getMonthDates(monthKey) : getRollingDates(new Date(), 31)),
    [monthKey, viewMode],
  );
  const requiredMonthKeys = useMemo(() => getRequiredMonthKeys(displayDates), [displayDates]);
  const rangeLabel = useMemo(() => getRangeLabel(viewMode, monthKey, displayDates), [displayDates, monthKey, viewMode]);
  const todayKey = dateKey(new Date());
  const todayIdx = useMemo(() => displayDates.findIndex(d => dateKey(d) === todayKey), [displayDates, todayKey]);
  const totalWidth = displayDates.length * GANTT_DAY_W;

  // Load Slack tasks once
  useEffect(() => {
    api.tasks({ limit: 500 }).then(r => setAllTasks(r.tasks || [])).catch(console.error);
  }, []);

  const parentTeams = useMemo(() => teams.filter((t) => !t.parent_id), [teams]);
  const childrenOf = useMemo(() => {
    const map = {};
    for (const t of teams) {
      if (t.parent_id) {
        if (!map[t.parent_id]) map[t.parent_id] = [];
        map[t.parent_id].push(t);
      }
    }
    return map;
  }, [teams]);

  const loadTeams = useCallback(async () => {
    const res = await api.workloadTeams();
    const next = res.teams || [];
    setTeams(next);
    if (!next.length) { setMembers([]); setItems([]); setCellsByItem({}); setLoading(false); return; }
    if (!selectedTeamId) {
      // 最初の親チームを選択、子がいれば最初の子をアクティブに
      const parents = next.filter((t) => !t.parent_id);
      const first = parents[0] || next[0];
      const children = next.filter((t) => t.parent_id === first.id);
      setSelectedParentId(first.id);
      setSelectedTeamId(children[0]?.id || first.id);
    }
  }, [selectedTeamId]);

  // 親チーム変更時: 子があれば最初の子をアクティブに
  const handleParentChange = useCallback((parentId) => {
    setSelectedParentId(parentId);
    const children = childrenOf[parentId] || [];
    setSelectedTeamId(children[0]?.id || parentId);
  }, [childrenOf]);

  const loadCategories = useCallback(async (dashTeamId) => {
    if (!dashTeamId) return;
    const res = await api.workloadCategories(dashTeamId).catch(() => ({ categories: [] }));
    setCategories(res.categories || []);
  }, []);

  const loadBoard = useCallback(async (dashTeamId) => {
    if (!dashTeamId) return;
    setLoading(true);
    try {
      const [memberRes, ...dataRes] = await Promise.all([
        api.workloadUsers(dashTeamId),
        ...requiredMonthKeys.map((m) => api.workloadData(dashTeamId, m)),
      ]);
      setMembers(memberRes.members || []);
      setItems(uniqueItems(dataRes.flatMap((r) => r.items || [])));
      setCellsByItem(mergeCellMaps(dataRes.map((r) => buildCellsByItem(r.cells || []))));
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, [requiredMonthKeys]);

  useEffect(() => { loadTeams().catch(console.error); }, [loadTeams]);
  useEffect(() => {
    if (selectedTeamId) {
      loadBoard(selectedTeamId).catch(console.error);
      loadCategories(selectedTeamId).catch(console.error);
    }
  }, [selectedTeamId, loadBoard, loadCategories]);

  // Team member IDs for filtering Slack tasks to selected team
  const teamMemberIds = useMemo(() => new Set(members.map(m => m.user_id)), [members]);
  const tasksByAssignee = useMemo(() => {
    const g = {};
    for (const t of allTasks) {
      if (teamMemberIds.size > 0 && t.assignee_id && !teamMemberIds.has(t.assignee_id)) continue;
      const k = t.assignee_id || '__unassigned__';
      if (!g[k]) g[k] = [];
      g[k].push(t);
    }
    return g;
  }, [allTasks, teamMemberIds]);

  const itemsByOwner = useMemo(() => {
    const grouped = {};
    for (const item of items) {
      if (!grouped[item.owner_user_id]) grouped[item.owner_user_id] = [];
      grouped[item.owner_user_id].push(item);
    }
    return grouped;
  }, [items]);

  // Gantt bar helpers
  function getTaskBar(task) {
    const s = task.created_at ? new Date(task.created_at) : null;
    const e = task.due_date ? new Date(task.due_date) : null;
    if (!s && !e) return null;
    const effS = s || e, effE = e || s;
    const mStart = displayDates[0], mEnd = displayDates[displayDates.length - 1];
    if (effS > mEnd || effE < mStart) return null;
    const cS = effS < mStart ? mStart : effS;
    const cE = effE > mEnd ? mEnd : effE;
    const si = Math.max(0, Math.round((cS - mStart) / 86400000));
    const ei = Math.min(displayDates.length - 1, Math.round((cE - mStart) / 86400000));
    return {
      left: si * GANTT_DAY_W,
      width: Math.max((ei - si + 1) * GANTT_DAY_W - 2, 4),
      clippedLeft: effS < mStart,
      clippedRight: effE > mEnd,
    };
  }

  function getWorkloadBars(item) {
    const isRecurrence = item.recurrence_type && item.recurrence_type !== 'other';
    const indices = [];
    if (isRecurrence) {
      displayDates.forEach((d, i) => {
        if (matchesRecurrence(item.recurrence_type, item.recurrence_config, d)) indices.push(i);
      });
    } else {
      const cells = cellsByItem[item.id] || {};
      displayDates.forEach((d, i) => { if (cells[dateKey(d)]) indices.push(i); });
    }
    return groupConsecutiveIndices(indices, GANTT_DAY_W);
  }

  const itemPayload = (item) => ({
    dashTeamId: selectedTeamId,
    ownerUserId: item.owner_user_id,
    title: item.title,
    category: item.category,
    notes: item.notes,
    color: item.color,
    recurrenceType: item.recurrence_type,
    recurrenceConfig: item.recurrence_config,
    sortOrder: item.sort_order,
  });

  const openCreateModal = (ownerUserId) => {
    setEditorMode('create');
    setEditorOwnerUserId(ownerUserId);
    setEditingItemId('');
    setDraftTitle(''); setDraftCategory(''); setDraftNotes('');
    setDraftColor(DEFAULT_ITEM_COLOR);
    setDraftRecurrenceType('other'); setDraftRecurrenceConfig({});
    setDraftStartDate(''); setDraftEndDate('');
    setEditorOpen(true);
  };

  const openEditModal = (item) => {
    setEditorMode('edit');
    setEditorOwnerUserId(item.owner_user_id);
    setEditingItemId(item.id);
    setDraftTitle(item.title || '');
    setDraftCategory(item.category || '');
    setDraftNotes(item.notes || '');
    setDraftColor(item.color || DEFAULT_ITEM_COLOR);
    setDraftRecurrenceType(item.recurrence_type || 'other');
    setDraftRecurrenceConfig(item.recurrence_config || {});
    setDraftStartDate(''); setDraftEndDate('');
    setEditorOpen(true);
  };

  const closeEditor = () => {
    setEditorOpen(false);
    setDraftTitle(''); setDraftCategory(''); setDraftNotes('');
    setDraftColor(DEFAULT_ITEM_COLOR);
    setDraftRecurrenceType('other'); setDraftRecurrenceConfig({});
    setDraftStartDate(''); setDraftEndDate('');
  };

  const handleSubmitEditor = async () => {
    const title = draftTitle.trim();
    if (!title || !selectedTeamId || !editorOwnerUserId) return;
    const payload = {
      dashTeamId: selectedTeamId,
      ownerUserId: editorOwnerUserId,
      title,
      category: draftCategory.trim() || null,
      notes: draftNotes.trim() || null,
      color: draftColor,
      recurrenceType: draftRecurrenceType,
      recurrenceConfig: draftRecurrenceType !== 'other' ? draftRecurrenceConfig : null,
    };
    let savedId = editingItemId;
    if (editorMode === 'create') {
      const res = await api.createWorkloadItem(payload);
      savedId = res.item?.id;
    } else {
      const cur = items.find((it) => it.id === editingItemId);
      if (!cur) return;
      await api.updateWorkloadItem(editingItemId, { ...payload, sortOrder: cur.sort_order });
    }
    // For "期間指定" type with date range: auto-generate cells
    if (draftRecurrenceType === 'other' && draftStartDate && draftEndDate && savedId) {
      const monthCells = {};
      const s = new Date(draftStartDate), e = new Date(draftEndDate);
      for (const d = new Date(s); d <= e; d.setDate(d.getDate() + 1)) {
        const mk = monthKeyFromDate(d);
        if (!monthCells[mk]) monthCells[mk] = [];
        monthCells[mk].push({ dayNum: d.getDate(), intensity: 2 });
      }
      await Promise.all(
        Object.entries(monthCells).map(([mk, cells]) =>
          api.setWorkloadCells({ itemId: savedId, monthKey: mk, cells })
        )
      );
    }
    closeEditor();
    await loadBoard(selectedTeamId);
  };

  const handleDeleteItem = async (itemId) => {
    if (!window.confirm('この業務を削除しますか？')) return;
    await api.deleteWorkloadItem(itemId);
    await loadBoard(selectedTeamId);
  };

  const handleCopyPrevious = async () => {
    if (!selectedTeamId || viewMode !== 'month') return;
    await api.copyPreviousWorkloadMonth(selectedTeamId, monthKey);
    await loadBoard(selectedTeamId);
  };

  const handleSaveCategory = async () => {
    const name = catDraftName.trim();
    if (!name || !selectedTeamId) return;
    if (editingCatId) {
      await api.updateWorkloadCategory(editingCatId, { name, color: catDraftColor });
    } else {
      await api.createWorkloadCategory({ dashTeamId: selectedTeamId, name, color: catDraftColor });
    }
    setCatDraftName(''); setCatDraftColor('#6366f1'); setEditingCatId('');
    await loadCategories(selectedTeamId);
  };

  const handleDeleteCategory = async (id) => {
    if (!window.confirm('このカテゴリを削除しますか？')) return;
    await api.deleteWorkloadCategory(id);
    await loadCategories(selectedTeamId);
  };

  const toggleWeekday = (dow) => {
    const days = draftRecurrenceConfig.days || [];
    setDraftRecurrenceConfig({
      days: days.includes(dow) ? days.filter((d) => d !== dow) : [...days, dow].sort((a, b) => a - b),
    });
  };

  const toggleMonthDay = (dom) => {
    const days = draftRecurrenceConfig.days || [];
    setDraftRecurrenceConfig({
      days: days.includes(dom) ? days.filter((d) => d !== dom) : [...days, dom].sort((a, b) => a - b),
    });
  };

  const ROW_H = 36;
  const HEADER_H = 52;

  return (
    <div className="workload-page">
      <div className="page-header">
        <div>
          <h1>業務ガント</h1>
          <p className="page-subtitle">Slackタスクと繰り返し業務を一画面で管理</p>
        </div>
      </div>

      {/* ── Toolbar ── */}
      <div className="workload-toolbar">
        <select className="filter-select" value={selectedParentId} onChange={(e) => handleParentChange(e.target.value)}>
          {parentTeams.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
        </select>
        {(childrenOf[selectedParentId]?.length > 0) && (
          <select className="filter-select" value={selectedTeamId} onChange={(e) => setSelectedTeamId(e.target.value)}>
            {childrenOf[selectedParentId].map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        )}
        <div className="workload-view-toggle">
          <button type="button" className={viewMode === 'month' ? 'is-active' : ''} onClick={() => setViewMode('month')}>月表示</button>
          <button type="button" className={viewMode === 'rolling' ? 'is-active' : ''} onClick={() => setViewMode('rolling')}>直近31日</button>
        </div>
        {viewMode === 'month' ? (
          <div className="month-switcher">
            <button className="filter-clear-btn" onClick={() => setMonthKey(shiftMonth(monthKey, -1))}>前月</button>
            <span>{rangeLabel}</span>
            <button className="filter-clear-btn" onClick={() => setMonthKey(shiftMonth(monthKey, 1))}>次月</button>
          </div>
        ) : (
          <div className="month-switcher"><span>{rangeLabel}</span></div>
        )}
        <button type="button" className="filter-clear-btn" style={{ marginLeft: 'auto' }} onClick={() => setCatMgrOpen(v => !v)}>
          カテゴリ管理
        </button>
      </div>

      {loading ? (
        <p className="empty-text">読み込み中…</p>
      ) : members.length === 0 ? (
        <p className="empty-text">チームを選択してください</p>
      ) : (
        <div style={{ border: '1px solid var(--gray-200)', borderRadius: 8, overflow: 'hidden', background: '#fff' }}>
          <div style={{ display: 'flex' }}>

            {/* ── Left: fixed label column ── */}
            <div style={{ flexShrink: 0, width: 300, borderRight: '2px solid var(--gray-200)' }}>
              <div style={{ height: HEADER_H, borderBottom: '2px solid var(--gray-200)', display: 'flex', alignItems: 'center', padding: '0 16px', background: '#f8fafc', fontWeight: 600, fontSize: 12, color: 'var(--gray-500)' }}>
                担当者 / Slackタスク・業務
              </div>
              {members.map((member) => {
                const workItems = itemsByOwner[member.user_id] || [];
                const slackTasks = tasksByAssignee[member.user_id] || [];
                const name = (member.display_name || member.real_name || member.user_id).split('/')[0].trim();
                return (
                  <div key={member.user_id}>
                    <div style={{ height: ROW_H, padding: '0 16px', display: 'flex', alignItems: 'center', gap: 8, background: '#f1f5f9', borderBottom: '1px solid var(--gray-200)', fontWeight: 700, fontSize: 13, color: 'var(--gray-700)' }}>
                      {member.avatar_url
                        ? <img src={member.avatar_url} alt="" style={{ width: 22, height: 22, borderRadius: '50%', flexShrink: 0 }} />
                        : <div style={{ width: 22, height: 22, borderRadius: '50%', background: 'var(--primary-light)', flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 700, color: 'var(--primary)' }}>{name[0]?.toUpperCase()}</div>
                      }
                      <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>{name}</span>
                      <span style={{ fontSize: 11, color: 'var(--gray-400)', flexShrink: 0 }}>{workItems.length + slackTasks.length}件</span>
                    </div>
                    {/* Workload item rows */}
                    {workItems.map((item) => {
                      const isRecurrence = item.recurrence_type && item.recurrence_type !== 'other';
                      const catObj = item.category ? categories.find((c) => c.name === item.category) : null;
                      return (
                        <div key={item.id}
                          title={item.title + (item.notes ? ' — ' + item.notes : '')}
                          onClick={() => openEditModal(item)}
                          onMouseEnter={e => e.currentTarget.style.background = '#fafafa'}
                          onMouseLeave={e => e.currentTarget.style.background = ''}
                          style={{ height: ROW_H, padding: '0 16px', display: 'flex', alignItems: 'center', gap: 6, borderBottom: '1px solid var(--gray-100)', fontSize: 12, cursor: 'pointer' }}
                        >
                          <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: '50%', background: item.color || DEFAULT_ITEM_COLOR, flexShrink: 0 }} />
                          <span style={{ fontSize: 10, color: 'var(--gray-400)', flexShrink: 0 }}>{isRecurrence ? '↺' : '▬'}</span>
                          {catObj && <span style={{ fontSize: 9, fontWeight: 600, flexShrink: 0, color: catObj.color, background: catObj.color + '22', padding: '1px 5px', borderRadius: 6 }}>{catObj.name}</span>}
                          <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1, color: 'var(--gray-800)' }}>{item.title}</span>
                          <button onClick={e => { e.stopPropagation(); handleDeleteItem(item.id); }} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--gray-300)', fontSize: 14, lineHeight: 1, padding: 0, flexShrink: 0 }}>×</button>
                        </div>
                      );
                    })}
                    {/* Slack task rows */}
                    {slackTasks.map((task) => {
                      const sc = statusDef(task.status);
                      const title = stripSlack(task.title || '（タイトルなし）');
                      return (
                        <div key={task.id} style={{ height: ROW_H, padding: '0 16px', display: 'flex', alignItems: 'center', gap: 6, borderBottom: '1px solid var(--gray-100)', fontSize: 12 }}>
                          <span style={{ flexShrink: 0, fontSize: 9, fontWeight: 600, color: sc.text, background: sc.bg, padding: '2px 5px', borderRadius: 7, whiteSpace: 'nowrap' }}>{sc.label}</span>
                          <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1, color: task.status === 'done' || task.status === 'cancelled' ? 'var(--gray-400)' : 'var(--gray-800)', textDecoration: task.status === 'cancelled' ? 'line-through' : 'none' }}>
                            {title.length > 28 ? title.slice(0, 28) + '…' : title}
                          </span>
                        </div>
                      );
                    })}
                    {/* Add button row */}
                    <div style={{ height: ROW_H, padding: '0 16px', display: 'flex', alignItems: 'center', borderBottom: '2px solid var(--gray-200)', background: '#fafafa' }}>
                      <button onClick={() => openCreateModal(member.user_id)} style={{ background: 'none', border: '1px dashed var(--gray-300)', borderRadius: 6, fontSize: 11, color: 'var(--gray-400)', cursor: 'pointer', padding: '3px 10px', width: '100%', textAlign: 'left' }}>
                        ＋ 業務を追加
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* ── Right: scrollable Gantt ── */}
            <div style={{ flex: 1, overflowX: 'auto', minWidth: 0 }}>
              <div style={{ width: totalWidth, minWidth: '100%', position: 'relative' }}>
                {/* Date header */}
                <div style={{ display: 'flex', height: HEADER_H, position: 'sticky', top: 0, zIndex: 2, background: '#f8fafc', borderBottom: '2px solid var(--gray-200)' }}>
                  {displayDates.map((d, i) => {
                    const isToday = i === todayIdx;
                    const isWeekend = d.getDay() === 0 || d.getDay() === 6;
                    return (
                      <div key={i} style={{ width: GANTT_DAY_W, flexShrink: 0, borderRight: '1px solid var(--gray-100)', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', background: isToday ? '#dbeafe' : 'transparent', fontSize: 11 }}>
                        <span style={{ fontWeight: isToday ? 700 : 400, color: isWeekend ? 'var(--gray-400)' : 'var(--gray-700)' }}>{d.getDate()}</span>
                        <span style={{ fontSize: 10, color: d.getDay() === 0 ? '#ef4444' : d.getDay() === 6 ? '#3b82f6' : 'var(--gray-400)' }}>{DOW_JP[d.getDay()]}</span>
                      </div>
                    );
                  })}
                </div>
                {/* Per-member Gantt rows */}
                {members.map((member) => {
                  const workItems = itemsByOwner[member.user_id] || [];
                  const slackTasks = tasksByAssignee[member.user_id] || [];
                  const weekendBg = (i) => (displayDates[i]?.getDay() === 0 || displayDates[i]?.getDay() === 6)
                    ? <div key={i} style={{ position: 'absolute', top: 0, bottom: 0, left: i * GANTT_DAY_W, width: GANTT_DAY_W, background: 'rgba(0,0,0,0.025)', pointerEvents: 'none' }} />
                    : null;
                  const todayLine = todayIdx >= 0
                    ? <div style={{ position: 'absolute', top: 0, bottom: 0, left: todayIdx * GANTT_DAY_W + GANTT_DAY_W / 2, width: 1, background: '#3b82f6', opacity: 0.25 }} />
                    : null;
                  return (
                    <div key={member.user_id}>
                      <div style={{ height: ROW_H, borderBottom: '1px solid var(--gray-200)', background: '#f1f5f9', position: 'relative' }}>
                        {todayIdx >= 0 && <div style={{ position: 'absolute', top: 0, bottom: 0, left: todayIdx * GANTT_DAY_W + GANTT_DAY_W / 2, width: 2, background: '#3b82f6', opacity: 0.3 }} />}
                      </div>
                      {workItems.map((item) => {
                        const bars = getWorkloadBars(item);
                        return (
                          <div key={item.id} style={{ height: ROW_H, borderBottom: '1px solid var(--gray-100)', position: 'relative', background: '#fff' }}>
                            {displayDates.map((_, i) => weekendBg(i))}
                            {todayLine}
                            {bars.map((bar, bi) => (
                              <div key={bi} title={item.title} style={{ position: 'absolute', top: '50%', transform: 'translateY(-50%)', left: bar.left, width: bar.width, height: 18, borderRadius: 4, background: item.color || DEFAULT_ITEM_COLOR, opacity: 0.85 }} />
                            ))}
                          </div>
                        );
                      })}
                      {slackTasks.map((task) => {
                        const bar = getTaskBar(task);
                        const sc = statusDef(task.status);
                        return (
                          <div key={task.id} style={{ height: ROW_H, borderBottom: '1px solid var(--gray-100)', position: 'relative', background: '#fff' }}>
                            {displayDates.map((_, i) => weekendBg(i))}
                            {todayLine}
                            {bar && (
                              <div title={stripSlack(task.title || '')} style={{ position: 'absolute', top: '50%', transform: 'translateY(-50%)', left: bar.left, width: bar.width, height: 18, borderRadius: 4, background: sc.bar, opacity: task.status === 'cancelled' ? 0.35 : task.status === 'done' ? 0.65 : 1, display: 'flex', alignItems: 'center', overflow: 'hidden' }}>
                                {bar.clippedLeft && <div style={{ width: 0, height: 0, borderTop: '9px solid transparent', borderBottom: '9px solid transparent', borderRight: '6px solid rgba(0,0,0,0.3)', flexShrink: 0 }} />}
                                {bar.clippedRight && <div style={{ marginLeft: 'auto', width: 0, height: 0, borderTop: '9px solid transparent', borderBottom: '9px solid transparent', borderLeft: '6px solid rgba(0,0,0,0.3)', flexShrink: 0 }} />}
                              </div>
                            )}
                          </div>
                        );
                      })}
                      <div style={{ height: ROW_H, borderBottom: '2px solid var(--gray-200)', position: 'relative', background: '#fafafa' }}>
                        {todayLine}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

          </div>
        </div>
      )}


      {/* Category manager modal */}
      {catMgrOpen && (
        <div className="modal-overlay" onClick={() => setCatMgrOpen(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ maxWidth: 480 }}>
            <h3>カテゴリ管理</h3>

            <div style={{ marginBottom: 12 }}>
              {categories.length === 0 ? (
                <p style={{ color: 'var(--gray-400)', fontSize: 13 }}>カテゴリがまだありません</p>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {categories.map((cat) => (
                    <div key={cat.id} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      {editingCatId === cat.id ? (
                        <>
                          <input
                            type="text"
                            value={catDraftName}
                            onChange={(e) => setCatDraftName(e.target.value)}
                            style={{ flex: 1 }}
                          />
                          <input
                            type="color"
                            value={catDraftColor}
                            onChange={(e) => setCatDraftColor(e.target.value)}
                            style={{ width: 36, height: 30, cursor: 'pointer', border: '1px solid var(--gray-200)', borderRadius: 4, padding: 2 }}
                          />
                          <button className="btn-sm" onClick={handleSaveCategory}>保存</button>
                          <button className="btn-sm" onClick={() => { setEditingCatId(''); setCatDraftName(''); setCatDraftColor('#6366f1'); }}>キャンセル</button>
                        </>
                      ) : (
                        <>
                          <span style={{ width: 16, height: 16, borderRadius: '50%', background: cat.color, flexShrink: 0 }} />
                          <span style={{ flex: 1 }}>{cat.name}</span>
                          <button className="btn-sm" onClick={() => { setEditingCatId(cat.id); setCatDraftName(cat.name); setCatDraftColor(cat.color || '#6366f1'); }}>編集</button>
                          <button className="btn-sm btn-danger" onClick={() => handleDeleteCategory(cat.id)}>削除</button>
                        </>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>

            {!editingCatId && (
              <div style={{ borderTop: '1px solid var(--gray-100)', paddingTop: 12, display: 'flex', gap: 8, alignItems: 'center' }}>
                <input
                  type="text"
                  value={catDraftName}
                  onChange={(e) => setCatDraftName(e.target.value)}
                  placeholder="新しいカテゴリ名"
                  style={{ flex: 1 }}
                  onKeyDown={(e) => { if (e.key === 'Enter') handleSaveCategory(); }}
                />
                <input
                  type="color"
                  value={catDraftColor}
                  onChange={(e) => setCatDraftColor(e.target.value)}
                  style={{ width: 36, height: 30, cursor: 'pointer', border: '1px solid var(--gray-200)', borderRadius: 4, padding: 2 }}
                />
                <button className="btn-primary" onClick={handleSaveCategory} disabled={!catDraftName.trim()}>追加</button>
              </div>
            )}

            <div className="crm-modal-actions" style={{ marginTop: 16 }}>
              <button className="btn-secondary" onClick={() => { setCatMgrOpen(false); setCatDraftName(''); setCatDraftColor('#6366f1'); setEditingCatId(''); }}>閉じる</button>
            </div>
          </div>
        </div>
      )}

      {/* Editor modal */}
      {editorOpen && (
        <div className="modal-overlay" onClick={closeEditor}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ maxWidth: 560 }}>
            <h3>{editorMode === 'create' ? '業務を追加' : '業務を編集'}</h3>

            <div className="admin-form-row" style={{ flexDirection: 'column', alignItems: 'stretch' }}>
              <label htmlFor="wl-title">タイトル</label>
              <input id="wl-title" type="text" value={draftTitle} onChange={(e) => setDraftTitle(e.target.value)} placeholder="業務タイトルを入力" />
            </div>

            <div style={{ display: 'flex', gap: 12, marginTop: 12, alignItems: 'flex-end' }}>
              <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 4 }}>
                <label htmlFor="wl-category">カテゴリ</label>
                <select id="wl-category" value={draftCategory} onChange={(e) => setDraftCategory(e.target.value)}>
                  <option value="">なし</option>
                  {categories.map((cat) => (
                    <option key={cat.id} value={cat.name}>{cat.name}</option>
                  ))}
                </select>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                <label htmlFor="wl-color">色</label>
                <input
                  id="wl-color"
                  type="color"
                  value={draftColor}
                  onChange={(e) => setDraftColor(e.target.value)}
                  style={{ width: 48, height: 36, cursor: 'pointer', border: '1px solid var(--gray-200)', borderRadius: 6, padding: 2 }}
                />
              </div>
            </div>

            <div className="admin-form-row" style={{ flexDirection: 'column', alignItems: 'stretch', marginTop: 12 }}>
              <label htmlFor="wl-notes">補足</label>
              <textarea id="wl-notes" value={draftNotes} onChange={(e) => setDraftNotes(e.target.value)} placeholder="補足事項を入力" rows={2} style={{ width: '100%', resize: 'vertical' }} />
            </div>

            <div style={{ marginTop: 16 }}>
              <label style={{ display: 'block', marginBottom: 6 }}>繰り返し</label>
              <div style={{ display: 'flex', gap: 6 }}>
                {[['other', 'その他'], ['daily', '日次'], ['weekly', '週次'], ['monthly', '月次']].map(([v, l]) => (
                  <button
                    key={v}
                    type="button"
                    className={`workload-recurrence-btn${draftRecurrenceType === v ? ' is-active' : ''}`}
                    onClick={() => { setDraftRecurrenceType(v); setDraftRecurrenceConfig({}); }}
                  >
                    {l}
                  </button>
                ))}
              </div>

              {draftRecurrenceType === 'weekly' && (
                <div style={{ display: 'flex', gap: 4, marginTop: 10 }}>
                  {WEEKDAY_DOWS.map((dow) => (
                    <button
                      key={dow}
                      type="button"
                      className={`workload-dow-btn${(draftRecurrenceConfig.days || []).includes(dow) ? ' is-active' : ''}`}
                      onClick={() => toggleWeekday(dow)}
                    >
                      {DOW_LABELS[dow]}
                    </button>
                  ))}
                </div>
              )}

              {draftRecurrenceType === 'monthly' && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3, marginTop: 10 }}>
                  {Array.from({ length: 31 }, (_, i) => i + 1).map((dom) => (
                    <button
                      key={dom}
                      type="button"
                      className={`workload-dom-btn${(draftRecurrenceConfig.days || []).includes(dom) ? ' is-active' : ''}`}
                      onClick={() => toggleMonthDay(dom)}
                    >
                      {dom}
                    </button>
                  ))}
                </div>
              )}

              {draftRecurrenceType === 'daily' && (
                <p style={{ marginTop: 8, fontSize: 12, color: 'var(--gray-500)' }}>毎日この色で塗りつぶされます。</p>
              )}
              {draftRecurrenceType === 'other' && (
                <div style={{ display: 'flex', gap: 8, marginTop: 10, alignItems: 'center' }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                    <label style={{ fontSize: 11, color: 'var(--gray-500)' }}>開始日</label>
                    <input type="date" value={draftStartDate} onChange={(e) => setDraftStartDate(e.target.value)} style={{ fontSize: 13 }} />
                  </div>
                  <span style={{ marginTop: 16, color: 'var(--gray-400)' }}>〜</span>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                    <label style={{ fontSize: 11, color: 'var(--gray-500)' }}>終了日</label>
                    <input type="date" value={draftEndDate} onChange={(e) => setDraftEndDate(e.target.value)} style={{ fontSize: 13 }} />
                  </div>
                </div>
              )}
            </div>

            <div className="crm-modal-actions" style={{ marginTop: 16 }}>
              <button className="btn-secondary" onClick={closeEditor}>キャンセル</button>
              <button className="btn-primary" onClick={handleSubmitEditor} disabled={!draftTitle.trim()}>保存</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
