import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api/client';

function monthToLabel(monthKey) {
  const [year, month] = monthKey.split('-').map(Number);
  return `${year}年${month}月`;
}

function shiftMonth(monthKey, delta) {
  const [year, month] = monthKey.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1 + delta, 1));
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}`;
}

function daysInMonth(monthKey) {
  const [year, month] = monthKey.split('-').map(Number);
  return new Date(year, month, 0).getDate();
}

function buildCellsByItem(cells) {
  const map = {};
  for (const cell of cells || []) {
    if (!map[cell.item_id]) map[cell.item_id] = {};
    map[cell.item_id][Number(cell.day_num)] = Number(cell.intensity);
  }
  return map;
}

function serializeCells(cellMap = {}) {
  return Object.entries(cellMap)
    .map(([dayNum, intensity]) => ({ dayNum: Number(dayNum), intensity: Number(intensity) }))
    .filter((cell) => [1, 2].includes(cell.intensity))
    .sort((a, b) => a.dayNum - b.dayNum);
}

export default function WorkloadGantt() {
  const initialMonth = new Date().toISOString().slice(0, 7);
  const [teams, setTeams] = useState([]);
  const [selectedTeamId, setSelectedTeamId] = useState('');
  const [monthKey, setMonthKey] = useState(initialMonth);
  const [members, setMembers] = useState([]);
  const [items, setItems] = useState([]);
  const [cellsByItem, setCellsByItem] = useState({});
  const [loading, setLoading] = useState(true);
  const [paintMode, setPaintMode] = useState('2');
  const [draggingItemId, setDraggingItemId] = useState('');
  const dragStateRef = useRef({ active: false, dirty: new Set() });
  const cellsRef = useRef({});

  useEffect(() => {
    cellsRef.current = cellsByItem;
  }, [cellsByItem]);

  const loadTeams = useCallback(async () => {
    const res = await api.workloadTeams();
    const nextTeams = res.teams || [];
    setTeams(nextTeams);
    if (!selectedTeamId && nextTeams[0]?.id) {
      setSelectedTeamId(nextTeams[0].id);
    }
  }, [selectedTeamId]);

  const loadBoard = useCallback(async (dashTeamId, nextMonthKey = monthKey) => {
    if (!dashTeamId) return;
    setLoading(true);
    try {
      const [memberRes, dataRes] = await Promise.all([
        api.workloadUsers(dashTeamId),
        api.workloadData(dashTeamId, nextMonthKey),
      ]);
      setMembers(memberRes.members || []);
      setItems(dataRes.items || []);
      setCellsByItem(buildCellsByItem(dataRes.cells || []));
    } catch (error) {
      console.error(error);
    } finally {
      setLoading(false);
    }
  }, [monthKey]);

  useEffect(() => {
    loadTeams().catch(console.error);
  }, [loadTeams]);

  useEffect(() => {
    if (!selectedTeamId) return;
    loadBoard(selectedTeamId, monthKey).catch(console.error);
  }, [selectedTeamId, monthKey, loadBoard]);

  useEffect(() => {
    const onMouseUp = async () => {
      if (!dragStateRef.current.active) return;
      dragStateRef.current.active = false;
      const dirtyIds = Array.from(dragStateRef.current.dirty);
      dragStateRef.current.dirty = new Set();
      if (!dirtyIds.length) return;
      await Promise.all(
        dirtyIds.map((itemId) => api.setWorkloadCells({
          itemId,
          monthKey,
          cells: serializeCells(cellsRef.current[itemId] || {}),
        }).catch(console.error)),
      );
    };
    window.addEventListener('mouseup', onMouseUp);
    return () => window.removeEventListener('mouseup', onMouseUp);
  }, [monthKey]);

  const itemsByOwner = useMemo(() => {
    const grouped = {};
    for (const item of items) {
      if (!grouped[item.owner_user_id]) grouped[item.owner_user_id] = [];
      grouped[item.owner_user_id].push(item);
    }
    return grouped;
  }, [items]);

  const dayCount = daysInMonth(monthKey);
  const days = Array.from({ length: dayCount }, (_, index) => index + 1);

  const applyPaint = (itemId, dayNum) => {
    const intensity = Number(paintMode);
    setCellsByItem((current) => {
      const nextItemCells = { ...(current[itemId] || {}) };
      if (intensity === 0) delete nextItemCells[dayNum];
      else nextItemCells[dayNum] = intensity;
      const next = { ...current, [itemId]: nextItemCells };
      cellsRef.current = next;
      return next;
    });
    dragStateRef.current.dirty.add(itemId);
  };

  const handleCellMouseDown = (itemId, dayNum) => {
    dragStateRef.current.active = true;
    applyPaint(itemId, dayNum);
  };

  const handleCellMouseEnter = (itemId, dayNum) => {
    if (!dragStateRef.current.active) return;
    applyPaint(itemId, dayNum);
  };

  const handleAddItem = async (ownerUserId) => {
    const title = window.prompt('業務名を入力してください');
    if (!title?.trim()) return;
    const category = window.prompt('カテゴリ名を入力してください（任意）') || '';
    await api.createWorkloadItem({
      dashTeamId: selectedTeamId,
      ownerUserId,
      title: title.trim(),
      category: category.trim(),
    });
    await loadBoard(selectedTeamId, monthKey);
  };

  const handleEditItem = async (item) => {
    const title = window.prompt('業務名を編集してください', item.title || '');
    if (title == null) return;
    const category = window.prompt('カテゴリ名を編集してください（任意）', item.category || '');
    if (category == null) return;
    await api.updateWorkloadItem(item.id, {
      dashTeamId: selectedTeamId,
      ownerUserId: item.owner_user_id,
      title: title.trim(),
      category: category.trim(),
      notes: item.notes,
      sortOrder: item.sort_order,
    });
    await loadBoard(selectedTeamId, monthKey);
  };

  const handleDeleteItem = async (itemId) => {
    if (!window.confirm('この業務を削除しますか？')) return;
    await api.deleteWorkloadItem(itemId);
    await loadBoard(selectedTeamId, monthKey);
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
    await loadBoard(selectedTeamId, monthKey);
  };

  const handleReorder = async (ownerUserId, itemId, direction) => {
    const ownerItems = [...(itemsByOwner[ownerUserId] || [])];
    const index = ownerItems.findIndex((item) => item.id === itemId);
    if (index === -1) return;
    const swapIndex = direction === 'up' ? index - 1 : index + 1;
    if (swapIndex < 0 || swapIndex >= ownerItems.length) return;
    const reordered = [...ownerItems];
    [reordered[index], reordered[swapIndex]] = [reordered[swapIndex], reordered[index]];
    await Promise.all(
      reordered.map((item, orderIndex) => api.updateWorkloadItem(item.id, {
        dashTeamId: selectedTeamId,
        ownerUserId: item.owner_user_id,
        title: item.title,
        category: item.category,
        notes: item.notes,
        sortOrder: orderIndex + 1,
      })),
    );
    await loadBoard(selectedTeamId, monthKey);
  };

  const handleCopyPrevious = async () => {
    if (!selectedTeamId) return;
    await api.copyPreviousWorkloadMonth(selectedTeamId, monthKey);
    await loadBoard(selectedTeamId, monthKey);
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

        <div className="month-switcher">
          <button className="filter-clear-btn" onClick={() => setMonthKey(shiftMonth(monthKey, -1))}>前月</button>
          <strong>{monthToLabel(monthKey)}</strong>
          <button className="filter-clear-btn" onClick={() => setMonthKey(shiftMonth(monthKey, 1))}>次月</button>
        </div>

        <div className="paint-mode-group">
          <label><input type="radio" name="paint-mode" value="0" checked={paintMode === '0'} onChange={(event) => setPaintMode(event.target.value)} />消す</label>
          <label><input type="radio" name="paint-mode" value="1" checked={paintMode === '1'} onChange={(event) => setPaintMode(event.target.value)} />薄い</label>
          <label><input type="radio" name="paint-mode" value="2" checked={paintMode === '2'} onChange={(event) => setPaintMode(event.target.value)} />濃い</label>
        </div>

        <button className="btn-primary" onClick={handleCopyPrevious}>前月をコピー</button>
      </div>

      {loading ? (
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
              }}
            >
              <div className="workload-member-header">
                <div>
                  <h2>{member.display_name || member.real_name || member.user_id}</h2>
                  <p>{member.real_name && member.real_name !== member.display_name ? member.real_name : member.user_id}</p>
                </div>
                <button className="btn-primary" onClick={() => handleAddItem(member.user_id)}>＋ 業務追加</button>
              </div>

              <div className="workload-grid">
                <div className="workload-grid-header workload-grid-row">
                  <div className="workload-item-label header">業務</div>
                  {days.map((day) => (
                    <div key={`${member.user_id}-${day}`} className="workload-day-cell header">{day}</div>
                  ))}
                </div>

                {(itemsByOwner[member.user_id] || []).map((item, index, ownerItems) => (
                  <div
                    key={item.id}
                    className="workload-grid-row"
                    draggable
                    onDragStart={() => setDraggingItemId(item.id)}
                    onDragEnd={() => setDraggingItemId('')}
                  >
                    <div className="workload-item-label">
                      <div className="workload-item-texts">
                        <strong>{item.title}</strong>
                        <span>{item.category || 'カテゴリ未設定'}</span>
                      </div>
                      <div className="workload-item-actions">
                        <button className="btn-sm" onClick={() => handleEditItem(item)}>編集</button>
                        <button className="btn-sm" onClick={() => handleReorder(member.user_id, item.id, 'up')} disabled={index === 0}>↑</button>
                        <button className="btn-sm" onClick={() => handleReorder(member.user_id, item.id, 'down')} disabled={index === ownerItems.length - 1}>↓</button>
                        <button className="btn-sm btn-danger" onClick={() => handleDeleteItem(item.id)}>削除</button>
                      </div>
                    </div>
                    {days.map((day) => {
                      const intensity = cellsByItem[item.id]?.[day] || 0;
                      return (
                        <div
                          key={`${item.id}-${day}`}
                          className={`workload-day-cell intensity-${intensity}`}
                          onMouseDown={() => handleCellMouseDown(item.id, day)}
                          onMouseEnter={() => handleCellMouseEnter(item.id, day)}
                        />
                      );
                    })}
                  </div>
                ))}

                {(itemsByOwner[member.user_id] || []).length === 0 && (
                  <div className="empty-text">まだ業務がありません</div>
                )}
              </div>
            </section>
          ))}
        </div>
      )}
    </div>
  );
}
