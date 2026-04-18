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

// 指定月の営業日一覧を返す
function getMonthBusinessDays(year, month) {
  const total = new Date(year, month, 0).getDate();
  const holidays = getJapaneseHolidays(year);
  const bds = [];
  for (let d = 1; d <= total; d++) {
    const date = new Date(year, month - 1, d);
    if (isWorkday(date)) bds.push(date);
  }
  return bds;
}

// dayNum 1-26 = 月初からN番目の営業日, 27-31 = 月末から逆算 (31=最終BD, 30=前日BD, ...)
function businessDayNumToDate(year, month, dayNum) {
  const bds = getMonthBusinessDays(year, month);
  if (dayNum >= 27) {
    const fromEnd = 32 - dayNum; // 31→1, 30→2, 29→3, 28→4, 27→5
    return bds[bds.length - fromEnd] || null;
  }
  return bds[dayNum - 1] || null;
}

function matchesRecurrence(type, config, date) {
  if (type === 'daily') return isWorkday(date);
  if (type === 'weekly') return (config?.days || []).includes(date.getDay());
  if (type === 'monthly') {
    const days = config?.days || [];
    if (config?.businessDays) {
      const y = date.getFullYear(), m = date.getMonth() + 1;
      return days.some(dn => {
        const bd = businessDayNumToDate(y, m, dn);
        return bd && bd.getDate() === date.getDate() && bd.getMonth() === date.getMonth();
      });
    }
    return days.includes(date.getDate());
  }
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

function getWeekDates(base = new Date()) {
  const d = new Date(base.getFullYear(), base.getMonth(), base.getDate());
  const dow = d.getDay();
  const monday = new Date(d);
  monday.setDate(d.getDate() - (dow === 0 ? 6 : dow - 1));
  return Array.from({ length: 7 }, (_, i) => {
    const r = new Date(monday);
    r.setDate(monday.getDate() + i);
    return r;
  });
}

function getRangeLabel(viewMode, monthKey, dates) {
  if (viewMode === 'month') return formatMonthLabel(monthKey);
  if (!dates.length) return '';
  const f = dates[0], l = dates[dates.length - 1];
  const fmt = (d) => `${d.getMonth() + 1}/${d.getDate()}`;
  if (viewMode === 'week') return `今週 ${fmt(f)}〜${fmt(l)}`;
  return `${fmt(f)}〜${fmt(l)}`;
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
};
const statusDef = (s) => STATUS_DEF[s] || { bar: '#93c5fd', text: '#3b82f6', bg: '#eff6ff', label: s || '不明' };

const DOW_JP = ['日', '月', '火', '水', '木', '金', '土'];

const VIEW_CONFIG = {
  month:    { dayW: 28, leftW: 300 },
  rolling:  { dayW: 28, leftW: 300 },
  '2weeks': { dayW: 36, leftW: 400 },
  week:     { dayW: 64, leftW: 500 },
};

function stripSlack(text) {
  return (text || '')
    .replace(/<[^>]+>/g, '')          // <@UXXX|name> etc.
    // leading @mentions: "@姓 名/EnglishFirst EnglishLast" のような複合名前パターンを除去
    .replace(/^(\s*@\S+(\s+\S*\/\S+)?(\s+[A-Za-z]\S*)?)+\s*/u, '')
    .replace(/\s+/g, ' ')
    .trim();
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
  const [filterStatus, setFilterStatus] = useState('in_progress');
  const [showSlackTasks, setShowSlackTasks] = useState(true);
  const [selectedTask, setSelectedTask] = useState(null);
  // Bar drag state: { id, delta, mode: 'resize'|'move', kind: 'task'|'item' }
  const [barDrag, setBarDrag] = useState(null);
  // Row reorder / cross-member drag state
  const [rowDragId, setRowDragId] = useState(null);
  const [rowDragOverId, setRowDragOverId] = useState(null);
  const [rowDragOverMemberId, setRowDragOverMemberId] = useState(null);
  // 自分の画面でのSlackタスクバー色（localStorage に個人設定として保存）
  const [myTaskColor, setMyTaskColor] = useState(() =>
    localStorage.getItem('gantt_my_task_color') || '#3b82f6'
  );

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

  const displayDates = useMemo(() => {
    if (viewMode === 'month')   return getMonthDates(monthKey);
    if (viewMode === 'week')    return getWeekDates();
    if (viewMode === '2weeks')  return getRollingDates(new Date(), 14);
    return getRollingDates(new Date(), 31); // 'rolling'
  }, [monthKey, viewMode]);
  const requiredMonthKeys = useMemo(() => getRequiredMonthKeys(displayDates), [displayDates]);
  const rangeLabel = useMemo(() => getRangeLabel(viewMode, monthKey, displayDates), [displayDates, monthKey, viewMode]);
  const todayKey = dateKey(new Date());
  const todayIdx = useMemo(() => displayDates.findIndex(d => dateKey(d) === todayKey), [displayDates, todayKey]);
  const ganttDayW = VIEW_CONFIG[viewMode]?.dayW ?? 28;
  const leftPanelW = VIEW_CONFIG[viewMode]?.leftW ?? 300;
  const totalWidth = displayDates.length * ganttDayW;
  const holidaySet = useMemo(() => {
    const merged = new Set();
    for (const y of new Set(displayDates.map(d => d.getFullYear()))) {
      for (const k of getJapaneseHolidays(y)) merged.add(k);
    }
    return merged;
  }, [displayDates]);

  // Slack tasks are loaded inside loadBoard (per team members)

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
      // 自分が直接メンバーの子チームを優先。なければ最初の親→最初の子
      const directLeaf = next.find(t => t.is_direct_member && t.parent_id)
        || next.find(t => t.is_direct_member);
      if (directLeaf) {
        const parentId = directLeaf.parent_id || directLeaf.id;
        setSelectedParentId(parentId);
        setSelectedTeamId(directLeaf.id);
      } else {
        const parents = next.filter((t) => !t.parent_id);
        const first = parents[0] || next[0];
        const children = next.filter((t) => t.parent_id === first.id);
        setSelectedParentId(first.id);
        setSelectedTeamId(children[0]?.id || first.id);
      }
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
      const loadedMembers = memberRes.members || [];
      setMembers(loadedMembers);
      setItems(uniqueItems(dataRes.flatMap((r) => r.items || [])));
      setCellsByItem(mergeCellMaps(dataRes.map((r) => buildCellsByItem(r.cells || []))));
      // チームメンバーのSlackタスクを取得（ロール制限を回避）
      if (loadedMembers.length > 0) {
        const assignees = loadedMembers.map(m => m.user_id).join(',');
        const taskRes = await api.tasks({ limit: 2000, assignees }).catch(() => ({ tasks: [] }));
        setAllTasks(taskRes.tasks || []);
      }
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
      if (filterStatus && t.status !== filterStatus) continue;
      if (t.task_type === 'broadcast') {
        // broadcast は target_user_ids で各メンバー行に振り分け
        for (const uid of (t.target_user_ids || [])) {
          if (teamMemberIds.size > 0 && !teamMemberIds.has(uid)) continue;
          if (!g[uid]) g[uid] = [];
          g[uid].push(t);
        }
      } else {
        if (teamMemberIds.size > 0 && t.assignee_id && !teamMemberIds.has(t.assignee_id)) continue;
        const k = t.assignee_id || '__unassigned__';
        if (!g[k]) g[k] = [];
        g[k].push(t);
      }
    }
    return g;
  }, [allTasks, teamMemberIds, filterStatus]);

  const itemsByOwner = useMemo(() => {
    const grouped = {};
    for (const item of items) {
      if (!grouped[item.owner_user_id]) grouped[item.owner_user_id] = [];
      grouped[item.owner_user_id].push(item);
    }
    return grouped;
  }, [items]);

  // Gantt bar helpers
  function getTaskBar(task, dueDateOverride) {
    const dueDateStr = !dueDateOverride ? task.due_date : null; // "YYYY-MM-DD" or null
    const overdue = !!dueDateStr && dueDateStr < todayKey;
    const s = task.created_at ? new Date(task.created_at) : null;
    const rawE = dueDateOverride || (task.due_date ? new Date(task.due_date) : null);
    if (!s && !rawE) return null;
    let effS = s || rawE, effE = rawE || s;
    // created_at が due_date より後になるケース（期限切れタスクが後で登録された場合等）
    if (effS > effE) effS = effE;
    const mStart = displayDates[0], mEnd = displayDates[displayDates.length - 1];
    if (effS > mEnd) return null;
    // 期限が表示期間より前 — 左端に固定して表示
    if (effE < mStart) {
      return { left: 0, width: Math.max(ganttDayW - 2, 4), clippedLeft: true, clippedRight: false, overdue };
    }
    const cS = effS < mStart ? mStart : effS;
    const cE = effE > mEnd ? mEnd : effE;
    const si = Math.max(0, Math.round((cS - mStart) / 86400000));
    const ei = Math.min(displayDates.length - 1, Math.round((cE - mStart) / 86400000));
    return {
      left: si * ganttDayW,
      width: Math.max((ei - si + 1) * ganttDayW - 2, 4),
      clippedLeft: effS < mStart,
      clippedRight: effE > mEnd && !dueDateOverride,
      overdue,
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
    return groupConsecutiveIndices(indices, ganttDayW);
  }

  // ── Bar drag (resize / move) ──────────────────────────────────────────────
  function startBarDrag(e, kind, id, mode) {
    e.preventDefault(); e.stopPropagation();
    const startX = e.clientX;
    let lastDelta = 0;
    setBarDrag({ id, kind, mode, delta: 0 });
    const onMouseMove = (me) => {
      const delta = Math.round((me.clientX - startX) / ganttDayW);
      if (delta !== lastDelta) { lastDelta = delta; setBarDrag({ id, kind, mode, delta }); }
    };
    const onMouseUp = async () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      setBarDrag(null);
      if (lastDelta === 0) return;
      if (kind === 'task') {
        await applyTaskBarDrag(id, mode, lastDelta);
      } else {
        await applyItemBarDrag(id, mode, lastDelta);
      }
    };
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }

  async function applyTaskBarDrag(taskId, mode, delta) {
    const task = allTasks.find(t => t.id === taskId);
    if (!task || !task.due_date) return;
    const newDue = new Date(task.due_date);
    newDue.setDate(newDue.getDate() + delta);
    const newDueStr = dateKey(newDue);
    // Optimistic local update — no re-fetch to avoid flicker
    setAllTasks(prev => prev.map(t => t.id === taskId ? { ...t, due_date: newDueStr } : t));
    await api.taskUpdate(taskId, { due_date: newDueStr }).catch(console.error);
  }

  async function applyItemBarDrag(itemId, mode, delta) {
    const cells = cellsByItem[itemId] || {};
    const activeDays = Object.keys(cells).filter(k => cells[k] > 0).sort();
    if (!activeDays.length) return;
    let newCellMap;
    if (mode === 'move') {
      newCellMap = {};
      for (const k of activeDays) {
        const d = new Date(k); d.setDate(d.getDate() + delta);
        newCellMap[dateKey(d)] = cells[k];
      }
    } else { // resize: extend/shrink end
      newCellMap = { ...cells };
      const lastDay = new Date(activeDays[activeDays.length - 1]);
      if (delta > 0) {
        for (let i = 1; i <= delta; i++) {
          const d = new Date(lastDay); d.setDate(lastDay.getDate() + i);
          newCellMap[dateKey(d)] = 2;
        }
      } else {
        for (let i = 0; i < -delta; i++) {
          const d = new Date(lastDay); d.setDate(lastDay.getDate() - i);
          delete newCellMap[dateKey(d)];
        }
      }
    }
    // Optimistic local update — no re-fetch to avoid flicker
    setCellsByItem(prev => ({ ...prev, [itemId]: newCellMap }));
    // Save per month; clear months that disappeared
    const monthCells = {};
    for (const [k, v] of Object.entries(newCellMap)) {
      const mk = k.slice(0, 7);
      if (!monthCells[mk]) monthCells[mk] = [];
      monthCells[mk].push({ dayNum: parseInt(k.slice(8, 10)), intensity: v });
    }
    const oldMonths = [...new Set(activeDays.map(k => k.slice(0, 7)))];
    const cleared = oldMonths.filter(m => !monthCells[m]);
    await Promise.all([
      ...Object.entries(monthCells).map(([mk, c]) => api.setWorkloadCells({ itemId, monthKey: mk, cells: c })),
      ...cleared.map(mk => api.setWorkloadCells({ itemId, monthKey: mk, cells: [] })),
    ]);
  }

  // ── Row reorder ──────────────────────────────────────────────────────────
  const persistOwnerItems = async (ownerUserId, ownerItems) => {
    await Promise.all(
      ownerItems.map((item, i) => api.updateWorkloadItem(item.id, {
        ...itemPayload(item), ownerUserId, sortOrder: i + 1,
      }))
    );
  };

  const handleRowDrop = async (targetOwnerUserId, targetItemId) => {
    if (!rowDragId || rowDragId === targetItemId) return;
    const ownerItems = [...(itemsByOwner[targetOwnerUserId] || [])];
    const fromIdx = ownerItems.findIndex(it => it.id === rowDragId);
    const toIdx   = ownerItems.findIndex(it => it.id === targetItemId);
    if (fromIdx === -1 || toIdx === -1) return;
    const reordered = [...ownerItems];
    const [moved] = reordered.splice(fromIdx, 1);
    reordered.splice(toIdx, 0, moved);
    setRowDragId(null); setRowDragOverId(null);
    // Optimistic local update — no re-fetch to avoid flicker
    const reorderedWithSort = reordered.map((item, i) => ({ ...item, sort_order: i + 1 }));
    setItems(prev => {
      const nonOwner = prev.filter(it => it.owner_user_id !== targetOwnerUserId);
      return [...nonOwner, ...reorderedWithSort];
    });
    await persistOwnerItems(targetOwnerUserId, reordered);
  };

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
    const isOther = !item.recurrence_type || item.recurrence_type === 'other';
    if (isOther) {
      const cells = cellsByItem[item.id] || {};
      const activeDays = Object.keys(cells).filter(k => cells[k] > 0).sort();
      setDraftStartDate(activeDays[0] || '');
      setDraftEndDate(activeDays[activeDays.length - 1] || '');
    } else {
      setDraftStartDate(''); setDraftEndDate('');
    }
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

  const handleDuplicateItem = async (item) => {
    const ownerItems = [...(itemsByOwner[item.owner_user_id] || [])];
    const origIdx = ownerItems.findIndex(it => it.id === item.id);
    const res = await api.createWorkloadItem({
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
    const newId = res.item?.id;
    // セル複製（その他タイプ）
    if (newId && (!item.recurrence_type || item.recurrence_type === 'other')) {
      const cells = cellsByItem[item.id] || {};
      const monthCells = {};
      for (const [k, v] of Object.entries(cells)) {
        if (!v) continue;
        const mk = k.slice(0, 7);
        if (!monthCells[mk]) monthCells[mk] = [];
        monthCells[mk].push({ dayNum: parseInt(k.slice(8, 10)), intensity: v });
      }
      await Promise.all(
        Object.entries(monthCells).map(([mk, c]) => api.setWorkloadCells({ itemId: newId, monthKey: mk, cells: c }))
      );
    }
    // 元アイテムの直後に挿入して並び順を再保存
    if (newId && origIdx !== -1) {
      ownerItems.splice(origIdx + 1, 0, res.item);
      await persistOwnerItems(item.owner_user_id, ownerItems);
    }
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
    setDraftRecurrenceConfig(c => ({
      ...c,
      days: days.includes(dom) ? days.filter((d) => d !== dom) : [...days, dom].sort((a, b) => a - b),
    }));
  };

  const handleMyTaskColorChange = (color) => {
    setMyTaskColor(color);
    localStorage.setItem('gantt_my_task_color', color);
  };

  const handleCrossMemberDrop = async (targetMemberId) => {
    const dragId = rowDragId;
    if (!dragId) return;
    const draggedItem = items.find(it => it.id === dragId);
    if (!draggedItem || draggedItem.owner_user_id === targetMemberId) {
      setRowDragId(null); setRowDragOverMemberId(null); return;
    }
    setRowDragId(null); setRowDragOverMemberId(null);
    const newSort = (itemsByOwner[targetMemberId]?.length || 0) + 1;
    setItems(prev => prev.map(it => it.id === dragId
      ? { ...it, owner_user_id: targetMemberId, sort_order: newSort }
      : it
    ));
    await api.updateWorkloadItem(dragId, {
      ...itemPayload(draggedItem),
      ownerUserId: targetMemberId,
      sortOrder: newSort,
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
          <button type="button" className={viewMode === 'week'    ? 'is-active' : ''} onClick={() => setViewMode('week')}>今週</button>
          <button type="button" className={viewMode === '2weeks'  ? 'is-active' : ''} onClick={() => setViewMode('2weeks')}>14日</button>
          <button type="button" className={viewMode === 'rolling' ? 'is-active' : ''} onClick={() => setViewMode('rolling')}>31日</button>
          <button type="button" className={viewMode === 'month'   ? 'is-active' : ''} onClick={() => setViewMode('month')}>月</button>
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
        <select className="filter-select" value={filterStatus} onChange={(e) => setFilterStatus(e.target.value)} disabled={!showSlackTasks}>
          <option value="in_progress">進行中のみ</option>
          <option value="">すべて表示</option>
          <option value="done">完了のみ</option>
        </select>
        <button
          type="button"
          className={showSlackTasks ? 'is-active' : ''}
          onClick={() => setShowSlackTasks(v => !v)}
          style={{ fontSize: 12, padding: '4px 10px', borderRadius: 6, border: '1px solid var(--gray-300)', background: showSlackTasks ? 'var(--primary)' : '#fff', color: showSlackTasks ? '#fff' : 'var(--gray-500)', cursor: 'pointer', whiteSpace: 'nowrap' }}
        >
          Slackタスク {showSlackTasks ? '表示中' : '非表示'}
        </button>
        {showSlackTasks && (
          <label title="Slackタスクの表示色（自分の画面のみ）" style={{ position: 'relative', width: 18, height: 18, borderRadius: '50%', background: myTaskColor, flexShrink: 0, cursor: 'pointer', border: '2px solid rgba(0,0,0,0.15)', display: 'inline-block' }}>
            <input type="color" value={myTaskColor} onChange={(e) => handleMyTaskColorChange(e.target.value)} style={{ position: 'absolute', inset: 0, opacity: 0, width: '100%', height: '100%', cursor: 'pointer' }} />
          </label>
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
            <div style={{ flexShrink: 0, width: leftPanelW, borderRight: '2px solid var(--gray-200)' }}>
              <div style={{ height: HEADER_H, borderBottom: '2px solid var(--gray-200)', display: 'flex', alignItems: 'center', padding: '0 16px', background: '#f8fafc', fontWeight: 600, fontSize: 12, color: 'var(--gray-500)' }}>
                担当者 / Slackタスク・業務
              </div>
              {members.map((member) => {
                const workItems = itemsByOwner[member.user_id] || [];
                const slackTasks = tasksByAssignee[member.user_id] || [];
                const name = (member.real_name || member.display_name || member.user_id).split('/')[0].trim();
                return (
                  <div key={member.user_id}>
                    <div
                      onDragOver={(e) => { if (rowDragId) { e.preventDefault(); setRowDragOverMemberId(member.user_id); } }}
                      onDragLeave={() => setRowDragOverMemberId(null)}
                      onDrop={(e) => { e.preventDefault(); handleCrossMemberDrop(member.user_id); }}
                      style={{ height: ROW_H, padding: '0 16px', display: 'flex', alignItems: 'center', gap: 8, background: rowDragOverMemberId === member.user_id ? '#dbeafe' : '#f1f5f9', borderBottom: '1px solid var(--gray-200)', borderTop: rowDragOverMemberId === member.user_id ? '2px solid var(--primary)' : '2px solid transparent', fontWeight: 700, fontSize: 13, color: 'var(--gray-700)', transition: 'background 0.1s' }}
                    >
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
                      const isDragOver = rowDragOverId === item.id && rowDragId !== item.id;
                      return (
                        <div key={item.id}
                          title={item.title + (item.notes ? ' — ' + item.notes : '')}
                          draggable
                          onDragStart={() => setRowDragId(item.id)}
                          onDragEnd={() => { setRowDragId(null); setRowDragOverId(null); setRowDragOverMemberId(null); }}
                          onDragOver={(e) => { e.preventDefault(); setRowDragOverId(item.id); }}
                          onDragLeave={() => setRowDragOverId(null)}
                          onDrop={(e) => { e.preventDefault(); handleRowDrop(member.user_id, item.id); }}
                          onClick={() => openEditModal(item)}
                          style={{
                            height: ROW_H, padding: '0 16px', display: 'flex', alignItems: 'center', gap: 6,
                            borderTop: isDragOver ? '2px solid var(--primary)' : '1px solid transparent',
                            borderBottom: '1px solid var(--gray-100)',
                            fontSize: 12, cursor: 'grab',
                            opacity: rowDragId === item.id ? 0.4 : 1,
                            background: rowDragId === item.id ? '#f0f0f0' : '',
                          }}
                        >
                          <span style={{ color: 'var(--gray-300)', fontSize: 13, flexShrink: 0, cursor: 'grab' }}>⠿</span>
                          <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: '50%', background: item.color || DEFAULT_ITEM_COLOR, flexShrink: 0 }} />
                          <span style={{ fontSize: 10, color: 'var(--gray-400)', flexShrink: 0 }}>{isRecurrence ? '↺' : '▬'}</span>
                          {catObj && <span style={{ fontSize: 9, fontWeight: 600, flexShrink: 0, color: catObj.color, background: catObj.color + '22', padding: '1px 5px', borderRadius: 6 }}>{catObj.name}</span>}
                          {item.rpo_client_name && <span style={{ fontSize: 9, fontWeight: 600, flexShrink: 0, color: '#6d28d9', background: '#ede9fe', padding: '1px 5px', borderRadius: 6 }} title="RPO案件">RPO: {item.rpo_client_name}</span>}
                          <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1, color: 'var(--gray-800)' }}>{item.title}</span>
                          <button onClick={e => { e.stopPropagation(); handleDuplicateItem(item); }} title="複製" style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--gray-300)', fontSize: 12, lineHeight: 1, padding: 0, flexShrink: 0 }}>⧉</button>
                          <button onClick={e => { e.stopPropagation(); handleDeleteItem(item.id); }} title="削除" style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--gray-300)', fontSize: 14, lineHeight: 1, padding: 0, flexShrink: 0 }}>×</button>
                        </div>
                      );
                    })}
                    {/* Slack task rows */}
                    {showSlackTasks && slackTasks.map((task) => {
                      const sc = statusDef(task.status);
                      const title = stripSlack(task.title || '（タイトルなし）');
                      const maxLen = Math.floor(leftPanelW / 7);
                      return (
                        <div key={task.id}
                          onClick={() => setSelectedTask(task)}
                          onMouseEnter={e => e.currentTarget.style.background = '#f0f7ff'}
                          onMouseLeave={e => e.currentTarget.style.background = ''}
                          style={{ height: ROW_H, padding: '0 16px', display: 'flex', alignItems: 'center', gap: 6, borderBottom: '1px solid var(--gray-100)', fontSize: 12, cursor: 'pointer' }}
                        >
                          {task.status !== 'in_progress' && (
                            <span style={{ flexShrink: 0, fontSize: 9, fontWeight: 600, color: sc.text, background: sc.bg, padding: '2px 5px', borderRadius: 7, whiteSpace: 'nowrap' }}>{sc.label}</span>
                          )}
                          <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1, color: task.status === 'done' || task.status === 'cancelled' ? 'var(--gray-400)' : 'var(--gray-800)', textDecoration: task.status === 'cancelled' ? 'line-through' : 'none' }}>
                            {title.length > maxLen ? title.slice(0, maxLen) + '…' : title}
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
                    const isHoliday = !isWeekend && holidaySet.has(_dateKey(d));
                    const headerBg = isToday ? '#dbeafe' : isHoliday ? 'rgba(239,68,68,0.08)' : 'transparent';
                    const numColor = isHoliday ? '#ef4444' : isWeekend ? 'var(--gray-400)' : 'var(--gray-700)';
                    const dowColor = d.getDay() === 0 || isHoliday ? '#ef4444' : d.getDay() === 6 ? '#3b82f6' : 'var(--gray-400)';
                    return (
                      <div key={i} style={{ width: ganttDayW, flexShrink: 0, borderRight: '1px solid var(--gray-100)', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', background: headerBg, fontSize: 11 }}>
                        <span style={{ fontWeight: isToday ? 700 : 400, color: numColor }}>{d.getDate()}</span>
                        <span style={{ fontSize: 10, color: dowColor }}>{DOW_JP[d.getDay()]}</span>
                      </div>
                    );
                  })}
                </div>
                {/* Per-member Gantt rows */}
                {members.map((member) => {
                  const workItems = itemsByOwner[member.user_id] || [];
                  const slackTasks = tasksByAssignee[member.user_id] || [];
                  const weekendBg = (i) => {
                    const d = displayDates[i];
                    if (!d) return null;
                    const isWeekend = d.getDay() === 0 || d.getDay() === 6;
                    const isHoliday = !isWeekend && holidaySet.has(_dateKey(d));
                    if (!isWeekend && !isHoliday) return null;
                    return <div key={i} style={{ position: 'absolute', top: 0, bottom: 0, left: i * ganttDayW, width: ganttDayW, background: isHoliday ? 'rgba(239,68,68,0.07)' : 'rgba(0,0,0,0.025)', pointerEvents: 'none' }} />;
                  };
                  const todayLine = todayIdx >= 0
                    ? <div style={{ position: 'absolute', top: 0, bottom: 0, left: todayIdx * ganttDayW + ganttDayW / 2, width: 1, background: '#3b82f6', opacity: 0.25 }} />
                    : null;
                  return (
                    <div key={member.user_id}>
                      <div
                        onDragOver={(e) => { if (rowDragId) { e.preventDefault(); setRowDragOverMemberId(member.user_id); } }}
                        onDragLeave={() => setRowDragOverMemberId(null)}
                        onDrop={(e) => { e.preventDefault(); handleCrossMemberDrop(member.user_id); }}
                        style={{ height: ROW_H, borderBottom: '1px solid var(--gray-200)', background: rowDragOverMemberId === member.user_id ? '#dbeafe' : '#f1f5f9', position: 'relative' }}
                      >
                        {todayIdx >= 0 && <div style={{ position: 'absolute', top: 0, bottom: 0, left: todayIdx * ganttDayW + ganttDayW / 2, width: 2, background: '#3b82f6', opacity: 0.3 }} />}
                      </div>
                      {workItems.map((item) => {
                        const isOther = !item.recurrence_type || item.recurrence_type === 'other';
                        const isDragging = barDrag?.kind === 'item' && barDrag.id === item.id;
                        const catObj = item.category ? categories.find((c) => c.name === item.category) : null;
                        const barColor = catObj?.color || item.color || DEFAULT_ITEM_COLOR;
                        let bars = getWorkloadBars(item);
                        if (isDragging) {
                          bars = bars.map((bar, i) => {
                            if (barDrag.mode === 'move') return { ...bar, left: bar.left + barDrag.delta * ganttDayW };
                            if (barDrag.mode === 'resize' && i === bars.length - 1) return { ...bar, width: Math.max(bar.width + barDrag.delta * ganttDayW, ganttDayW) };
                            return bar;
                          });
                        }
                        return (
                          <div key={item.id} style={{ height: ROW_H, borderBottom: '1px solid var(--gray-100)', position: 'relative', background: '#fff' }}>
                            {displayDates.map((_, i) => weekendBg(i))}
                            {todayLine}
                            {bars.map((bar, bi) => (
                              <div key={bi} style={{
                                position: 'absolute', top: '50%', transform: 'translateY(-50%)',
                                left: bar.left, width: bar.width, height: 18, borderRadius: 4,
                                background: barColor, opacity: 0.85,
                                display: 'flex', alignItems: 'center', overflow: 'visible',
                                cursor: isOther ? 'grab' : 'default', userSelect: 'none',
                              }}
                                onMouseDown={isOther ? (e) => { if (e.target === e.currentTarget) startBarDrag(e, 'item', item.id, 'move'); } : undefined}
                              >
                                {/* Move handle: inner area */}
                                {isOther && (
                                  <div title={item.title} style={{ flex: 1, height: '100%', cursor: 'grab' }}
                                    onMouseDown={(e) => startBarDrag(e, 'item', item.id, 'move')} />
                                )}
                                {/* Resize handle: right edge */}
                                {isOther && (
                                  <div
                                    onMouseDown={(e) => startBarDrag(e, 'item', item.id, 'resize')}
                                    style={{ position: 'absolute', right: -3, top: 0, bottom: 0, width: 8, cursor: 'ew-resize', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                                  >
                                    <div style={{ width: 3, height: 10, borderRadius: 2, background: 'rgba(255,255,255,0.7)' }} />
                                  </div>
                                )}
                              </div>
                            ))}
                          </div>
                        );
                      })}
                      {showSlackTasks && slackTasks.map((task) => {
                        const isDragging = barDrag?.kind === 'task' && barDrag.id === task.id;
                        const dueDateOverride = isDragging && task.due_date
                          ? (() => { const d = new Date(task.due_date); d.setDate(d.getDate() + barDrag.delta); return d; })()
                          : undefined;
                        const bar = getTaskBar(task, dueDateOverride);
                        const opacity = task.status === 'done' ? 0.65 : 1;
                        const barBg = bar?.overdue ? '#ef4444' : myTaskColor;
                        return (
                          <div key={task.id} style={{ height: ROW_H, borderBottom: '1px solid var(--gray-100)', position: 'relative', background: '#fff' }}>
                            {displayDates.map((_, i) => weekendBg(i))}
                            {todayLine}
                            {bar && (
                              <div
                                title={stripSlack(task.title || '') + (bar.overdue ? '（期限切れ）' : '')}
                                onClick={() => !isDragging && setSelectedTask(task)}
                                style={{ position: 'absolute', top: '50%', transform: 'translateY(-50%)', left: bar.left, width: bar.width, height: 18, borderRadius: 4, background: barBg, opacity, display: 'flex', alignItems: 'center', overflow: 'visible', cursor: 'grab', userSelect: 'none' }}
                              >
                                {bar.clippedLeft && <div style={{ width: 0, height: 0, borderTop: '9px solid transparent', borderBottom: '9px solid transparent', borderRight: '6px solid rgba(0,0,0,0.3)', flexShrink: 0, pointerEvents: 'none' }} />}
                                {/* Middle drag: move whole bar (shifts due_date) */}
                                <div style={{ flex: 1, height: '100%' }}
                                  onMouseDown={(e) => startBarDrag(e, 'task', task.id, 'move')} />
                                {/* Right edge drag: resize (extend/shorten due_date) */}
                                {task.due_date && (
                                  <div
                                    onMouseDown={(e) => startBarDrag(e, 'task', task.id, 'resize')}
                                    style={{ position: 'absolute', right: -3, top: 0, bottom: 0, width: 8, cursor: 'ew-resize', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                                  >
                                    <div style={{ width: 3, height: 10, borderRadius: 2, background: 'rgba(255,255,255,0.7)', pointerEvents: 'none' }} />
                                  </div>
                                )}
                                {bar.clippedRight && <div style={{ marginLeft: 'auto', width: 0, height: 0, borderTop: '9px solid transparent', borderBottom: '9px solid transparent', borderLeft: '6px solid rgba(0,0,0,0.3)', flexShrink: 0, pointerEvents: 'none' }} />}
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


      {/* Task detail modal */}
      {selectedTask && (() => {
        const t = selectedTask;
        const sc = statusDef(t.status);
        const title = stripSlack(t.title || '（タイトルなし）');
        const rawTitle = t.title || '';
        return (
          <div className="modal-overlay" onClick={() => setSelectedTask(null)}>
            <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ maxWidth: 480 }}>
              <h3 style={{ marginBottom: 12, fontSize: 16, lineHeight: 1.4 }}>{title}</h3>
              {rawTitle !== title && (
                <p style={{ fontSize: 11, color: 'var(--gray-400)', marginBottom: 12, wordBreak: 'break-all' }}>{rawTitle}</p>
              )}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, fontSize: 13 }}>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <span style={{ color: 'var(--gray-500)', width: 64, flexShrink: 0 }}>ステータス</span>
                  <span style={{ fontWeight: 600, color: sc.text, background: sc.bg, padding: '2px 8px', borderRadius: 8 }}>{sc.label}</span>
                </div>
                {t.due_date && (
                  <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                    <span style={{ color: 'var(--gray-500)', width: 64, flexShrink: 0 }}>期限</span>
                    <span>{new Date(t.due_date).toLocaleDateString('ja-JP')}</span>
                  </div>
                )}
                {t.created_at && (
                  <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                    <span style={{ color: 'var(--gray-500)', width: 64, flexShrink: 0 }}>起票日</span>
                    <span>{new Date(t.created_at).toLocaleDateString('ja-JP')}</span>
                  </div>
                )}
              </div>
              <div style={{ display: 'flex', gap: 8, marginTop: 16, justifyContent: 'flex-end', flexWrap: 'wrap' }}>
                {Object.entries(STATUS_DEF).map(([k, v]) => (
                  <button key={k}
                    className={t.status === k ? 'btn-primary' : 'btn-secondary'}
                    style={{ fontSize: 12 }}
                    onClick={async () => {
                      const updated = { ...t, status: k };
                      setSelectedTask(updated);
                      setAllTasks(prev => prev.map(task => task.id === t.id ? updated : task));
                      await api.taskSetStatus(t.id, k).catch(console.error);
                    }}
                  >{v.label}</button>
                ))}
              </div>
              <div className="crm-modal-actions" style={{ marginTop: 12 }}>
                <button className="btn-secondary" onClick={() => setSelectedTask(null)}>閉じる</button>
              </div>
            </div>
          </div>
        );
      })()}

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
                <label htmlFor="wl-color" style={{ color: draftCategory ? 'var(--gray-400)' : 'inherit' }}>
                  色{draftCategory && <span style={{ fontSize: 10, marginLeft: 4 }}>(カテゴリ色を使用)</span>}
                </label>
                <input
                  id="wl-color"
                  type="color"
                  value={draftCategory ? (categories.find(c => c.name === draftCategory)?.color || draftColor) : draftColor}
                  onChange={(e) => setDraftColor(e.target.value)}
                  disabled={!!draftCategory}
                  style={{ width: 48, height: 36, border: '1px solid var(--gray-200)', borderRadius: 6, padding: 2, cursor: draftCategory ? 'not-allowed' : 'pointer', opacity: draftCategory ? 0.5 : 1 }}
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
                <>
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
                  {(draftRecurrenceConfig.days || []).length > 0 && (
                    <label style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 8, fontSize: 12, cursor: 'pointer' }}>
                      <input
                        type="checkbox"
                        checked={!!draftRecurrenceConfig.businessDays}
                        onChange={(e) => setDraftRecurrenceConfig(c => ({ ...c, businessDays: e.target.checked }))}
                      />
                      <span>営業日数換算する（土日祝を省いてカウント。27〜31は月末からの逆算）</span>
                    </label>
                  )}
                </>
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
