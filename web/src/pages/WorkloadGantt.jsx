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

export default function WorkloadGantt() {
  const initialMonth = new Date().toISOString().slice(0, 7);
  const [teams, setTeams] = useState([]);
  const [selectedTeamId, setSelectedTeamId] = useState('');
  const [monthKey, setMonthKey] = useState(initialMonth);
  const [viewMode, setViewMode] = useState('month');
  const [members, setMembers] = useState([]);
  const [items, setItems] = useState([]);
  const [cellsByItem, setCellsByItem] = useState({});
  const [loading, setLoading] = useState(false);
  const [eraseMode, setEraseMode] = useState(false);
  const [clickSelect, setClickSelect] = useState(null); // { itemId, startKey } | null
  const [hoverCell, setHoverCell] = useState(null);    // { itemId, dateKey } | null
  const [draggingItemId, setDraggingItemId] = useState('');
  const [draggingOwnerUserId, setDraggingOwnerUserId] = useState('');
  const [categories, setCategories] = useState([]);
  const [catMgrOpen, setCatMgrOpen] = useState(false);
  const [catDraftName, setCatDraftName] = useState('');
  const [catDraftColor, setCatDraftColor] = useState('#6366f1');
  const [editingCatId, setEditingCatId] = useState('');

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

  const cellsRef = useRef({});
  useEffect(() => { cellsRef.current = cellsByItem; }, [cellsByItem]);

  const displayDates = useMemo(
    () => (viewMode === 'month' ? getMonthDates(monthKey) : getRollingDates(new Date(), 31)),
    [monthKey, viewMode],
  );
  const requiredMonthKeys = useMemo(() => getRequiredMonthKeys(displayDates), [displayDates]);
  const rangeLabel = useMemo(() => getRangeLabel(viewMode, monthKey, displayDates), [displayDates, monthKey, viewMode]);
  const todayKey = dateKey(new Date());

  const pendingRangeSet = useMemo(() => {
    if (!clickSelect || !hoverCell || clickSelect.itemId !== hoverCell.itemId) return new Set();
    return new Set(
      getDateRange(displayDates, clickSelect.startKey, hoverCell.dateKey)
        .map((k) => `${clickSelect.itemId}:${k}`),
    );
  }, [clickSelect, hoverCell, displayDates]);

  const loadTeams = useCallback(async () => {
    const res = await api.workloadTeams();
    const next = res.teams || [];
    setTeams(next);
    if (!selectedTeamId && next[0]?.id) setSelectedTeamId(next[0].id);
    if (!next.length) { setMembers([]); setItems([]); setCellsByItem({}); setLoading(false); }
  }, [selectedTeamId]);

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

  // Escape cancels pending select
  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') { setClickSelect(null); setHoverCell(null); } };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const itemsByOwner = useMemo(() => {
    const grouped = {};
    for (const item of items) {
      if (!grouped[item.owner_user_id]) grouped[item.owner_user_id] = [];
      grouped[item.owner_user_id].push(item);
    }
    return grouped;
  }, [items]);

  const saveCells = useCallback(async (itemId) => {
    await Promise.all(
      requiredMonthKeys.map((m) => api.setWorkloadCells({
        itemId,
        monthKey: m,
        cells: serializeCellsForMonth(cellsRef.current[itemId] || {}, m),
      }).catch(console.error)),
    );
  }, [requiredMonthKeys]);

  const handleCellClick = useCallback(async (item, fullDateKey) => {
    if (item.recurrence_type && item.recurrence_type !== 'other') return;

    if (!clickSelect) {
      setClickSelect({ itemId: item.id, startKey: fullDateKey });
      return;
    }

    if (clickSelect.itemId !== item.id) {
      setClickSelect({ itemId: item.id, startKey: fullDateKey });
      setHoverCell(null);
      return;
    }

    // Complete range
    const range = getDateRange(displayDates, clickSelect.startKey, fullDateKey);
    const intensity = eraseMode ? 0 : 2;
    setCellsByItem((cur) => {
      const next = { ...(cur[item.id] || {}) };
      for (const k of range) {
        if (intensity === 0) delete next[k];
        else next[k] = intensity;
      }
      const updated = { ...cur, [item.id]: next };
      cellsRef.current = updated;
      return updated;
    });
    setClickSelect(null);
    setHoverCell(null);
    setTimeout(() => saveCells(item.id), 0);
  }, [clickSelect, eraseMode, displayDates, saveCells]);

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
    setDraftTitle('');
    setDraftCategory('');
    setDraftNotes('');
    setDraftColor(DEFAULT_ITEM_COLOR);
    setDraftRecurrenceType('other');
    setDraftRecurrenceConfig({});
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
    setEditorOpen(true);
  };

  const closeEditor = () => {
    setEditorOpen(false);
    setDraftTitle(''); setDraftCategory(''); setDraftNotes('');
    setDraftColor(DEFAULT_ITEM_COLOR);
    setDraftRecurrenceType('other'); setDraftRecurrenceConfig({});
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
    if (editorMode === 'create') {
      await api.createWorkloadItem(payload);
    } else {
      const cur = items.find((it) => it.id === editingItemId);
      if (!cur) return;
      await api.updateWorkloadItem(editingItemId, { ...payload, sortOrder: cur.sort_order });
    }
    closeEditor();
    await loadBoard(selectedTeamId);
  };

  const handleDeleteItem = async (itemId) => {
    if (!window.confirm('この業務を削除しますか？')) return;
    await api.deleteWorkloadItem(itemId);
    await loadBoard(selectedTeamId);
  };

  const handleMoveItem = async (itemId, ownerUserId) => {
    const item = items.find((it) => it.id === itemId);
    if (!item || item.owner_user_id === ownerUserId) return;
    await api.updateWorkloadItem(itemId, { ...itemPayload(item), ownerUserId });
    await loadBoard(selectedTeamId);
  };

  const persistOwnerItems = async (ownerUserId, ownerItems) => {
    await Promise.all(
      ownerItems.map((item, i) => api.updateWorkloadItem(item.id, {
        ...itemPayload(item),
        ownerUserId,
        sortOrder: i + 1,
      })),
    );
  };

  const handleDropOnItem = async (targetOwnerUserId, targetItemId) => {
    if (!draggingItemId) return;
    const dragged = items.find((it) => it.id === draggingItemId);
    if (!dragged || dragged.id === targetItemId) return;

    const srcOwner = draggingOwnerUserId || dragged.owner_user_id;
    const srcItems = [...(itemsByOwner[srcOwner] || [])];
    const tgtItems = srcOwner === targetOwnerUserId ? srcItems : [...(itemsByOwner[targetOwnerUserId] || [])];
    const si = srcItems.findIndex((it) => it.id === dragged.id);
    const ti = tgtItems.findIndex((it) => it.id === targetItemId);
    if (si === -1 || ti === -1) return;

    if (srcOwner === targetOwnerUserId) {
      const reordered = [...srcItems];
      const [moved] = reordered.splice(si, 1);
      reordered.splice(si < ti ? ti - 1 : ti, 0, moved);
      await persistOwnerItems(targetOwnerUserId, reordered);
    } else {
      const nextSrc = [...srcItems]; const [moved] = nextSrc.splice(si, 1);
      const nextTgt = [...tgtItems]; nextTgt.splice(ti, 0, moved);
      await Promise.all([persistOwnerItems(srcOwner, nextSrc), persistOwnerItems(targetOwnerUserId, nextTgt)]);
    }
    setDraggingItemId(''); setDraggingOwnerUserId('');
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

  return (
    <div className="workload-page">
      <div className="page-header">
        <div>
          <h1>業務ガント</h1>
          <p className="page-subtitle">チームごとの月次業務と繁忙時期を可視化します。</p>
        </div>
      </div>

      <div className="workload-toolbar">
        <select className="filter-select" value={selectedTeamId} onChange={(e) => setSelectedTeamId(e.target.value)}>
          {teams.map((team) => <option key={team.id} value={team.id}>{team.name}</option>)}
        </select>

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

        <button
          type="button"
          className={`workload-erase-toggle${eraseMode ? ' is-active' : ''}`}
          onClick={() => setEraseMode((v) => !v)}
        >
          ✕ 消去{eraseMode ? 'モード中' : ''}
        </button>

        {clickSelect && (
          <span className="workload-pending-hint">開始日選択済み — 終了日をクリック（Escでキャンセル）</span>
        )}

        <button className="btn-primary" onClick={handleCopyPrevious} disabled={viewMode !== 'month'}>前月をコピー</button>
        <button className="btn-secondary" onClick={() => setCatMgrOpen(true)}>カテゴリ管理</button>
      </div>

      {!teams.length ? (
        <p className="empty-text">利用可能なチームがまだありません</p>
      ) : loading ? (
        <p className="empty-text">読み込み中...</p>
      ) : (
        <div className="workload-board">
          {members.map((member) => (
            <section
              key={member.user_id}
              className="workload-member-section"
              onDragOver={(e) => e.preventDefault()}
              onDrop={async () => {
                if (!draggingItemId) return;
                await handleMoveItem(draggingItemId, member.user_id);
                setDraggingItemId(''); setDraggingOwnerUserId('');
              }}
            >
              <div className="workload-member-header">
                <div>
                  <h2>{member.display_name || member.real_name || member.user_id}</h2>
                  <p>{member.real_name && member.real_name !== member.display_name ? member.real_name : member.user_id}</p>
                </div>
                <button className="btn-primary" onClick={() => openCreateModal(member.user_id)}>＋ 業務追加</button>
              </div>

              <div className="workload-grid" style={{ '--workload-days': displayDates.length }}>
                {/* Header row */}
                <div className="workload-grid-header workload-grid-row">
                  <div className="workload-item-label header">業務</div>
                  {displayDates.map((d) => {
                    const key = dateKey(d);
                    const isWeekend = d.getDay() === 0 || d.getDay() === 6;
                    return (
                      <div
                        key={`${member.user_id}-${key}`}
                        className={`workload-day-cell header${key === todayKey ? ' today' : ''}${isWeekend ? ' weekend' : ''}`}
                      >
                        <span className="workload-day-number">{d.getDate()}</span>
                      </div>
                    );
                  })}
                </div>

                {/* Item rows */}
                {(itemsByOwner[member.user_id] || []).map((item) => {
                  const itemColor = item.color || DEFAULT_ITEM_COLOR;
                  const isRecurrence = item.recurrence_type && item.recurrence_type !== 'other';
                  const catObj = item.category ? categories.find((c) => c.name === item.category) : null;
                  const chipColor = catObj?.color || itemColor;
                  return (
                    <div key={item.id} className="workload-grid-row">
                      {/* Label: draggable for row reorder */}
                      <div
                        className="workload-item-label"
                        draggable
                        onDragStart={() => { setDraggingItemId(item.id); setDraggingOwnerUserId(member.user_id); }}
                        onDragEnd={() => { setDraggingItemId(''); setDraggingOwnerUserId(''); }}
                        onDragOver={(e) => e.preventDefault()}
                        onDrop={async (e) => { e.preventDefault(); e.stopPropagation(); await handleDropOnItem(member.user_id, item.id); }}
                      >
                        <div className="workload-drag-handle">⠿</div>
                        <div className="workload-color-dot" style={{ background: itemColor }} />
                        <div className="workload-item-texts">
                          <div className="workload-item-title-line">
                            <span className="workload-item-title">{item.title}</span>
                            {item.category && (
                              <span
                                className="workload-category-chip"
                                style={{ borderColor: chipColor, background: hexToRgba(chipColor, 0.12), color: chipColor }}
                              >
                                {item.category}
                              </span>
                            )}
                          </div>
                          {item.notes && <span className="workload-item-notes">{item.notes}</span>}
                        </div>
                        <div className="workload-item-actions">
                          <button className="btn-sm" onClick={() => openEditModal(item)}>編集</button>
                          <button className="btn-sm btn-danger" onClick={() => handleDeleteItem(item.id)}>削除</button>
                        </div>
                      </div>

                      {/* Cells */}
                      {displayDates.map((d) => {
                        const fk = dateKey(d);
                        const isToday = fk === todayKey;
                        const isWeekend = d.getDay() === 0 || d.getDay() === 6;
                        const isPendingStart = clickSelect?.itemId === item.id && clickSelect.startKey === fk;
                        const isPendingRange = pendingRangeSet.has(`${item.id}:${fk}`);

                        let intensity = 0;
                        if (isRecurrence) {
                          intensity = matchesRecurrence(item.recurrence_type, item.recurrence_config, d) ? 2 : 0;
                        } else {
                          intensity = cellsByItem[item.id]?.[fk] || 0;
                        }

                        let bgColor = intensity > 0
                          ? (intensity === 1 ? hexToRgba(itemColor, 0.4) : itemColor)
                          : undefined;

                        // Preview overlay for pending range
                        if (!isRecurrence && isPendingRange && intensity === 0) {
                          bgColor = eraseMode ? hexToRgba('#ef4444', 0.2) : hexToRgba(itemColor, 0.35);
                        }

                        return (
                          <div
                            key={`${item.id}-${fk}`}
                            className={[
                              'workload-day-cell',
                              isToday ? 'today' : '',
                              isWeekend ? 'weekend' : '',
                              isPendingStart ? 'pending-start' : '',
                              !isRecurrence ? 'clickable' : '',
                            ].filter(Boolean).join(' ')}
                            style={bgColor ? { background: bgColor } : undefined}
                            onClick={() => handleCellClick(item, fk)}
                            onMouseEnter={() => { if (clickSelect) setHoverCell({ itemId: item.id, dateKey: fk }); }}
                            onMouseLeave={() => { if (hoverCell) setHoverCell(null); }}
                          />
                        );
                      })}
                    </div>
                  );
                })}

                {!(itemsByOwner[member.user_id] || []).length && (
                  <div className="empty-text">まだ業務がありません</div>
                )}
              </div>
            </section>
          ))}
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
                <p style={{ marginTop: 8, fontSize: 12, color: 'var(--gray-500)' }}>ガント上でクリックして期間を手動で塗ってください。</p>
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
