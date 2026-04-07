import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api/client';

const CATEGORY_PALETTE = [
  { bg: '#fff1f2', fg: '#be123c', border: '#fda4af' },
  { bg: '#fff7ed', fg: '#c2410c', border: '#fdba74' },
  { bg: '#fffbeb', fg: '#a16207', border: '#fcd34d' },
  { bg: '#f0fdf4', fg: '#15803d', border: '#86efac' },
  { bg: '#ecfeff', fg: '#0f766e', border: '#67e8f9' },
  { bg: '#eff6ff', fg: '#1d4ed8', border: '#93c5fd' },
  { bg: '#f5f3ff', fg: '#6d28d9', border: '#c4b5fd' },
  { bg: '#fdf2f8', fg: '#be185d', border: '#f9a8d4' },
];

function formatMonthLabel(monthKey) {
  const [year, month] = monthKey.split('-').map(Number);
  return `${year}年${month}月`;
}

function shiftMonth(monthKey, delta) {
  const [year, month] = monthKey.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1 + delta, 1));
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}`;
}

function dateKey(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function monthKeyFromDate(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}

function getMonthDates(monthKey) {
  const [year, month] = monthKey.split('-').map(Number);
  const daysInMonth = new Date(year, month, 0).getDate();
  return Array.from({ length: daysInMonth }, (_, index) => new Date(year, month - 1, index + 1));
}

function getRollingDates(baseDate = new Date(), length = 31) {
  const start = new Date(baseDate.getFullYear(), baseDate.getMonth(), baseDate.getDate());
  return Array.from({ length }, (_, index) => {
    const next = new Date(start);
    next.setDate(start.getDate() + index);
    return next;
  });
}

function getRangeLabel(viewMode, monthKey, dates) {
  if (viewMode === 'month') return formatMonthLabel(monthKey);
  if (!dates.length) return '直近31日';
  const first = dates[0];
  const last = dates[dates.length - 1];
  return `${first.getMonth() + 1}/${first.getDate()} - ${last.getMonth() + 1}/${last.getDate()}`;
}

function getRequiredMonthKeys(dates) {
  return Array.from(new Set(dates.map((date) => monthKeyFromDate(date))));
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
  for (const current of maps) {
    for (const [itemId, cellMap] of Object.entries(current || {})) {
      if (!merged[itemId]) merged[itemId] = {};
      Object.assign(merged[itemId], cellMap);
    }
  }
  return merged;
}

function serializeCellsForMonth(cellMap = {}, monthKey) {
  return Object.entries(cellMap)
    .filter(([fullDateKey, intensity]) => fullDateKey.startsWith(`${monthKey}-`) && [1, 2].includes(Number(intensity)))
    .map(([fullDateKey, intensity]) => ({
      dayNum: Number(fullDateKey.slice(-2)),
      intensity: Number(intensity),
    }))
    .sort((a, b) => a.dayNum - b.dayNum);
}

function uniqueItems(items = []) {
  const seen = new Map();
  for (const item of items) {
    if (!seen.has(item.id)) seen.set(item.id, item);
  }
  return Array.from(seen.values());
}

function categoryStyle(category = '') {
  if (!category) return null;
  let hash = 0;
  for (let i = 0; i < category.length; i += 1) {
    hash = (hash * 31 + category.charCodeAt(i)) % CATEGORY_PALETTE.length;
  }
  return CATEGORY_PALETTE[Math.abs(hash) % CATEGORY_PALETTE.length];
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
  const [paintMode, setPaintMode] = useState('2');
  const [draggingItemId, setDraggingItemId] = useState('');
  const [draggingOwnerUserId, setDraggingOwnerUserId] = useState('');
  const [editorOpen, setEditorOpen] = useState(false);
  const [editorMode, setEditorMode] = useState('create');
  const [editorOwnerUserId, setEditorOwnerUserId] = useState('');
  const [editingItemId, setEditingItemId] = useState('');
  const [draftTitle, setDraftTitle] = useState('');
  const [draftCategory, setDraftCategory] = useState('');
  const [draftNotes, setDraftNotes] = useState('');
  const dragStateRef = useRef({ active: false, dirty: new Set() });
  const cellsRef = useRef({});

  useEffect(() => {
    cellsRef.current = cellsByItem;
  }, [cellsByItem]);

  const displayDates = useMemo(
    () => (viewMode === 'month' ? getMonthDates(monthKey) : getRollingDates(new Date(), 31)),
    [monthKey, viewMode],
  );
  const requiredMonthKeys = useMemo(() => getRequiredMonthKeys(displayDates), [displayDates]);
  const rangeLabel = useMemo(() => getRangeLabel(viewMode, monthKey, displayDates), [displayDates, monthKey, viewMode]);
  const todayKey = dateKey(new Date());

  const loadTeams = useCallback(async () => {
    const res = await api.workloadTeams();
    const nextTeams = res.teams || [];
    setTeams(nextTeams);
    if (!selectedTeamId && nextTeams[0]?.id) {
      setSelectedTeamId(nextTeams[0].id);
    }
    if (!nextTeams.length) {
      setMembers([]);
      setItems([]);
      setCellsByItem({});
      setLoading(false);
    }
  }, [selectedTeamId]);

  const loadBoard = useCallback(async (dashTeamId) => {
    if (!dashTeamId) return;
    setLoading(true);
    try {
      const [memberRes, ...dataResponses] = await Promise.all([
        api.workloadUsers(dashTeamId),
        ...requiredMonthKeys.map((requiredMonth) => api.workloadData(dashTeamId, requiredMonth)),
      ]);
      setMembers(memberRes.members || []);
      setItems(uniqueItems(dataResponses.flatMap((response) => response.items || [])));
      setCellsByItem(mergeCellMaps(dataResponses.map((response) => buildCellsByItem(response.cells || []))));
    } catch (error) {
      console.error(error);
    } finally {
      setLoading(false);
    }
  }, [requiredMonthKeys]);

  useEffect(() => {
    loadTeams().catch(console.error);
  }, [loadTeams]);

  useEffect(() => {
    if (!selectedTeamId) return;
    loadBoard(selectedTeamId).catch(console.error);
  }, [selectedTeamId, loadBoard]);

  useEffect(() => {
    const onMouseUp = async () => {
      if (!dragStateRef.current.active) return;
      dragStateRef.current.active = false;
      const dirtyIds = Array.from(dragStateRef.current.dirty);
      dragStateRef.current.dirty = new Set();
      if (!dirtyIds.length) return;
      await Promise.all(
        dirtyIds.flatMap((itemId) => requiredMonthKeys.map((requiredMonth) => api.setWorkloadCells({
          itemId,
          monthKey: requiredMonth,
          cells: serializeCellsForMonth(cellsRef.current[itemId] || {}, requiredMonth),
        }).catch(console.error))),
      );
    };
    window.addEventListener('mouseup', onMouseUp);
    return () => window.removeEventListener('mouseup', onMouseUp);
  }, [requiredMonthKeys]);

  const itemsByOwner = useMemo(() => {
    const grouped = {};
    for (const item of items) {
      if (!grouped[item.owner_user_id]) grouped[item.owner_user_id] = [];
      grouped[item.owner_user_id].push(item);
    }
    return grouped;
  }, [items]);

  const applyPaint = (itemId, fullDateKey) => {
    const intensity = Number(paintMode);
    setCellsByItem((current) => {
      const nextItemCells = { ...(current[itemId] || {}) };
      if (intensity === 0) delete nextItemCells[fullDateKey];
      else nextItemCells[fullDateKey] = intensity;
      const next = { ...current, [itemId]: nextItemCells };
      cellsRef.current = next;
      return next;
    });
    dragStateRef.current.dirty.add(itemId);
  };

  const handleCellMouseDown = (itemId, fullDateKey) => {
    dragStateRef.current.active = true;
    applyPaint(itemId, fullDateKey);
  };

  const handleCellMouseEnter = (itemId, fullDateKey) => {
    if (!dragStateRef.current.active) return;
    applyPaint(itemId, fullDateKey);
  };

  const openCreateModal = (ownerUserId) => {
    setEditorMode('create');
    setEditorOwnerUserId(ownerUserId);
    setEditingItemId('');
    setDraftTitle('');
    setDraftCategory('');
    setDraftNotes('');
    setEditorOpen(true);
  };

  const openEditModal = (item) => {
    setEditorMode('edit');
    setEditorOwnerUserId(item.owner_user_id);
    setEditingItemId(item.id);
    setDraftTitle(item.title || '');
    setDraftCategory(item.category || '');
    setDraftNotes(item.notes || '');
    setEditorOpen(true);
  };

  const closeEditor = () => {
    setEditorOpen(false);
    setEditorMode('create');
    setEditorOwnerUserId('');
    setEditingItemId('');
    setDraftTitle('');
    setDraftCategory('');
    setDraftNotes('');
  };

  const handleSubmitEditor = async () => {
    const title = draftTitle.trim();
    if (!title || !selectedTeamId || !editorOwnerUserId) return;

    if (editorMode === 'create') {
      await api.createWorkloadItem({
        dashTeamId: selectedTeamId,
        ownerUserId: editorOwnerUserId,
        title,
        category: draftCategory.trim() || null,
        notes: draftNotes.trim() || null,
      });
    } else {
      const currentItem = items.find((item) => item.id === editingItemId);
      if (!currentItem) return;
      await api.updateWorkloadItem(editingItemId, {
        dashTeamId: selectedTeamId,
        ownerUserId: editorOwnerUserId,
        title,
        category: draftCategory.trim() || null,
        notes: draftNotes.trim() || null,
        sortOrder: currentItem.sort_order,
      });
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
    const item = items.find((row) => row.id === itemId);
    if (!item || item.owner_user_id === ownerUserId) return;
    await api.updateWorkloadItem(itemId, {
      dashTeamId: selectedTeamId,
      ownerUserId,
      title: item.title,
      category: item.category,
      notes: item.notes,
      sortOrder: item.sort_order,
    });
    await loadBoard(selectedTeamId);
  };

  const persistOwnerItems = async (ownerUserId, ownerItems) => {
    await Promise.all(
      ownerItems.map((item, orderIndex) => api.updateWorkloadItem(item.id, {
        dashTeamId: selectedTeamId,
        ownerUserId,
        title: item.title,
        category: item.category,
        notes: item.notes,
        sortOrder: orderIndex + 1,
      })),
    );
  };

  const handleReorder = async (ownerUserId, itemId, direction) => {
    const ownerItems = [...(itemsByOwner[ownerUserId] || [])];
    const index = ownerItems.findIndex((item) => item.id === itemId);
    if (index === -1) return;
    const swapIndex = direction === 'up' ? index - 1 : index + 1;
    if (swapIndex < 0 || swapIndex >= ownerItems.length) return;
    const reordered = [...ownerItems];
    [reordered[index], reordered[swapIndex]] = [reordered[swapIndex], reordered[index]];
    await persistOwnerItems(ownerUserId, reordered);
    await loadBoard(selectedTeamId);
  };

  const handleDropOnItem = async (targetOwnerUserId, targetItemId) => {
    if (!draggingItemId) return;
    const draggedItem = items.find((item) => item.id === draggingItemId);
    if (!draggedItem || draggedItem.id === targetItemId) return;

    const sourceOwnerUserId = draggingOwnerUserId || draggedItem.owner_user_id;
    const sourceItems = [...(itemsByOwner[sourceOwnerUserId] || [])];
    const targetItems = sourceOwnerUserId === targetOwnerUserId
      ? sourceItems
      : [...(itemsByOwner[targetOwnerUserId] || [])];

    const draggedIndex = sourceItems.findIndex((item) => item.id === draggedItem.id);
    const targetIndex = targetItems.findIndex((item) => item.id === targetItemId);
    if (draggedIndex === -1 || targetIndex === -1) return;

    if (sourceOwnerUserId === targetOwnerUserId) {
      const reordered = [...sourceItems];
      const [moved] = reordered.splice(draggedIndex, 1);
      const adjustedTargetIndex = draggedIndex < targetIndex ? targetIndex - 1 : targetIndex;
      reordered.splice(adjustedTargetIndex, 0, moved);
      await persistOwnerItems(targetOwnerUserId, reordered);
    } else {
      const nextSourceItems = [...sourceItems];
      const [moved] = nextSourceItems.splice(draggedIndex, 1);
      const nextTargetItems = [...targetItems];
      nextTargetItems.splice(targetIndex, 0, moved);
      await Promise.all([
        persistOwnerItems(sourceOwnerUserId, nextSourceItems),
        persistOwnerItems(targetOwnerUserId, nextTargetItems),
      ]);
    }

    setDraggingItemId('');
    setDraggingOwnerUserId('');
    await loadBoard(selectedTeamId);
  };

  const handleCopyPrevious = async () => {
    if (!selectedTeamId || viewMode !== 'month') return;
    await api.copyPreviousWorkloadMonth(selectedTeamId, monthKey);
    await loadBoard(selectedTeamId);
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
        <select
          className="filter-select"
          value={selectedTeamId}
          onChange={(event) => setSelectedTeamId(event.target.value)}
        >
          {teams.map((team) => (
            <option key={team.id} value={team.id}>{team.name}</option>
          ))}
        </select>

        <div className="workload-view-toggle">
          <button
            type="button"
            className={viewMode === 'month' ? 'is-active' : ''}
            onClick={() => setViewMode('month')}
          >
            月表示
          </button>
          <button
            type="button"
            className={viewMode === 'rolling' ? 'is-active' : ''}
            onClick={() => setViewMode('rolling')}
          >
            直近31日
          </button>
        </div>

        {viewMode === 'month' ? (
          <div className="month-switcher">
            <button className="filter-clear-btn" onClick={() => setMonthKey(shiftMonth(monthKey, -1))}>前月</button>
            <strong>{rangeLabel}</strong>
            <button className="filter-clear-btn" onClick={() => setMonthKey(shiftMonth(monthKey, 1))}>次月</button>
          </div>
        ) : (
          <div className="month-switcher">
            <strong>{rangeLabel}</strong>
          </div>
        )}

        <div className="paint-mode-group">
          <label><input type="radio" name="paint-mode" value="0" checked={paintMode === '0'} onChange={(event) => setPaintMode(event.target.value)} />消す</label>
          <label><input type="radio" name="paint-mode" value="1" checked={paintMode === '1'} onChange={(event) => setPaintMode(event.target.value)} />薄い</label>
          <label><input type="radio" name="paint-mode" value="2" checked={paintMode === '2'} onChange={(event) => setPaintMode(event.target.value)} />濃い</label>
        </div>

        <button className="btn-primary" onClick={handleCopyPrevious} disabled={viewMode !== 'month'}>前月をコピー</button>
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
              onDragOver={(event) => event.preventDefault()}
              onDrop={async () => {
                if (!draggingItemId) return;
                await handleMoveItem(draggingItemId, member.user_id);
                setDraggingItemId('');
                setDraggingOwnerUserId('');
              }}
            >
              <div className="workload-member-header">
                <div>
                  <h2>{member.display_name || member.real_name || member.user_id}</h2>
                  <p>{member.real_name && member.real_name !== member.display_name ? member.real_name : member.user_id}</p>
                </div>
                <button className="btn-primary" onClick={() => openCreateModal(member.user_id)}>＋ 業務追加</button>
              </div>

              <div className="workload-grid" style={{ ['--workload-days']: displayDates.length }}>
                <div className="workload-grid-header workload-grid-row">
                  <div className="workload-item-label header">業務</div>
                  {displayDates.map((currentDate) => {
                    const key = dateKey(currentDate);
                    const isToday = key === todayKey;
                    return (
                      <div key={`${member.user_id}-${key}`} className={`workload-day-cell header${isToday ? ' today' : ''}`}>
                        <span className="workload-day-number">{currentDate.getDate()}</span>
                        <span className="workload-day-meta">{currentDate.getMonth() + 1}/{currentDate.getDate()}</span>
                      </div>
                    );
                  })}
                </div>

                {(itemsByOwner[member.user_id] || []).map((item, index, ownerItems) => {
                  const chip = categoryStyle(item.category || '');
                  return (
                    <div
                      key={item.id}
                      className="workload-grid-row"
                      draggable
                      onDragStart={() => {
                        setDraggingItemId(item.id);
                        setDraggingOwnerUserId(member.user_id);
                      }}
                      onDragEnd={() => {
                        setDraggingItemId('');
                        setDraggingOwnerUserId('');
                      }}
                      onDragOver={(event) => event.preventDefault()}
                      onDrop={async (event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        await handleDropOnItem(member.user_id, item.id);
                      }}
                    >
                      <div className="workload-item-label">
                        <div className="workload-item-texts">
                          <div className="workload-item-title-line">
                            <strong>{item.title}</strong>
                            {item.category && (
                              <span className="workload-category-chip" style={{ background: chip.bg, color: chip.fg, borderColor: chip.border }}>
                                {item.category}
                              </span>
                            )}
                          </div>
                          <span>{item.notes || '補足未設定'}</span>
                        </div>
                        <div className="workload-item-actions">
                          <button className="btn-sm" onClick={() => openEditModal(item)}>編集</button>
                          <button className="btn-sm" onClick={() => handleReorder(member.user_id, item.id, 'up')} disabled={index === 0}>↑</button>
                          <button className="btn-sm" onClick={() => handleReorder(member.user_id, item.id, 'down')} disabled={index === ownerItems.length - 1}>↓</button>
                          <button className="btn-sm btn-danger" onClick={() => handleDeleteItem(item.id)}>削除</button>
                        </div>
                      </div>
                      {displayDates.map((currentDate) => {
                        const fullDateKey = dateKey(currentDate);
                        const intensity = cellsByItem[item.id]?.[fullDateKey] || 0;
                        return (
                          <div
                            key={`${item.id}-${fullDateKey}`}
                            className={`workload-day-cell intensity-${intensity}${fullDateKey === todayKey ? ' today' : ''}`}
                            onMouseDown={() => handleCellMouseDown(item.id, fullDateKey)}
                            onMouseEnter={() => handleCellMouseEnter(item.id, fullDateKey)}
                          />
                        );
                      })}
                    </div>
                  );
                })}

                {(itemsByOwner[member.user_id] || []).length === 0 && (
                  <div className="empty-text">まだ業務がありません</div>
                )}
              </div>
            </section>
          ))}
        </div>
      )}

      {editorOpen && (
        <div className="modal-overlay" onClick={closeEditor}>
          <div className="modal-content" onClick={(event) => event.stopPropagation()} style={{ maxWidth: 560 }}>
            <h3>{editorMode === 'create' ? '業務を追加' : '業務を編集'}</h3>
            <div className="admin-form-row" style={{ flexDirection: 'column', alignItems: 'stretch' }}>
              <label htmlFor="workload-title">タイトル</label>
              <input
                id="workload-title"
                type="text"
                value={draftTitle}
                onChange={(event) => setDraftTitle(event.target.value)}
                placeholder="業務タイトルを入力"
              />
            </div>
            <div className="admin-form-row" style={{ flexDirection: 'column', alignItems: 'stretch', marginTop: 12 }}>
              <label htmlFor="workload-category">カテゴリ</label>
              <input
                id="workload-category"
                type="text"
                value={draftCategory}
                onChange={(event) => setDraftCategory(event.target.value)}
                placeholder="カテゴリを入力"
              />
            </div>
            <div className="admin-form-row" style={{ flexDirection: 'column', alignItems: 'stretch', marginTop: 12 }}>
              <label htmlFor="workload-notes">補足</label>
              <textarea
                id="workload-notes"
                value={draftNotes}
                onChange={(event) => setDraftNotes(event.target.value)}
                placeholder="補足事項を入力"
                rows={4}
                style={{ width: '100%', resize: 'vertical' }}
              />
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
