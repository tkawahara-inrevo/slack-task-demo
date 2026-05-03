function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Slack集計')
    .addItem('ランキング更新', 'updateRankingView')
    .addItem('今月をセット', 'setThisMonth')
    .addItem('先月をセット', 'setLastMonth')
    .addToUi();
}

function setThisMonth() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('ranking_view');
  const now = new Date();
  const from = new Date(now.getFullYear(), now.getMonth(), 1);
  const to = new Date(now.getFullYear(), now.getMonth() + 1, 0);

  sheet.getRange('D2').setValue(from);
  sheet.getRange('D3').setValue(to);
  sheet.getRange('D4').setValue('all');
  sheet.getRange('D5').setValue(20);
}

function setLastMonth() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('ranking_view');
  const now = new Date();
  const from = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const to = new Date(now.getFullYear(), now.getMonth(), 0);

  sheet.getRange('D2').setValue(from);
  sheet.getRange('D3').setValue(to);
  sheet.getRange('D4').setValue('all');
  sheet.getRange('D5').setValue(20);
}

function normalizeCategoryInput(value) {
  const v = String(value || '').trim();

  if (v === 'all') return 'all';
  if (v === '雑談') return 'chat';
  if (v === '出勤日報') return 'report_in_reply';
  if (v === '退勤日報') return 'report_out_reply';
  if (v === '日報合算') return 'report_reply';

  return v;
}

function updateRankingView() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawSheet = ss.getSheetByName('raw_daily');
  const viewSheet = ss.getSheetByName('ranking_view');

  const from = normalizeDate(viewSheet.getRange('D2').getValue());
  const to = normalizeDate(viewSheet.getRange('D3').getValue());
  const rawCategory = viewSheet.getRange('D4').getValue();
const category = normalizeCategoryInput(rawCategory);
  const limit = Number(viewSheet.getRange('D5').getValue() || 20);

  if (!from || !to) {
    throw new Error('D2/D3 に開始日・終了日を入れてね');
  }

  const values = rawSheet.getDataRange().getValues();
  if (values.length <= 1) {
    clearResultArea(viewSheet);
    return;
  }

  const header = values[0];
  const rows = values.slice(1);

  const idx = {
    run_id: header.indexOf('run_id'),
    date: header.indexOf('date'),
    category: header.indexOf('category'),
    user_id: header.indexOf('user_id'),
    user_name: header.indexOf('user_name'),
    count: header.indexOf('count')
  };

  const latestRunIdByDate = {};

  for (const row of rows) {
    const dateValue = normalizeDate(row[idx.date]);
    if (!dateValue) continue;
    if (dateValue < from || dateValue > to) continue;

    const dateKey = formatDateKey(dateValue);
    const runId = String(row[idx.run_id] || '').trim();
    if (!runId) continue;

    if (!latestRunIdByDate[dateKey] || runId > latestRunIdByDate[dateKey]) {
      latestRunIdByDate[dateKey] = runId;
    }
  }

  const map = {};

  for (const row of rows) {
    const dateValue = normalizeDate(row[idx.date]);
    if (!dateValue) continue;
    if (dateValue < from || dateValue > to) continue;

    const dateKey = formatDateKey(dateValue);
    const runId = String(row[idx.run_id] || '').trim();
    if (!runId) continue;
    if (latestRunIdByDate[dateKey] !== runId) continue;

    const cat = String(row[idx.category] || '').trim();
    if (!isCategoryTarget(category, cat)) continue;

    const userId = String(row[idx.user_id] || '').trim();
    const userName = String(row[idx.user_name] || '').trim();
    const count = Number(row[idx.count] || 0);

    if (!userId) continue;

    if (!map[userId]) {
      map[userId] = {
        userId,
        userName,
        chatCount: 0,
        reportInReplyCount: 0,
        reportOutReplyCount: 0,
        total: 0
      };
    }

    if (cat === 'chat') {
      map[userId].chatCount += count;
    } else if (cat === 'report_in_reply') {
      map[userId].reportInReplyCount += count;
    } else if (cat === 'report_out_reply') {
      map[userId].reportOutReplyCount += count;
    }

    map[userId].total =
      map[userId].chatCount +
      map[userId].reportInReplyCount +
      map[userId].reportOutReplyCount;

    if (userName) {
      map[userId].userName = userName;
    }
  }

  let ranking = Object.values(map).sort((a, b) => b.total - a.total);

  if (category === 'chat') {
    ranking = ranking.sort((a, b) => b.chatCount - a.chatCount);
  } else if (category === 'report_in_reply') {
    ranking = ranking.sort((a, b) => b.reportInReplyCount - a.reportInReplyCount);
  } else if (category === 'report_out_reply') {
    ranking = ranking.sort((a, b) => b.reportOutReplyCount - a.reportOutReplyCount);
  } else if (category === 'report_reply') {
    ranking = ranking.sort(
      (a, b) =>
        (b.reportInReplyCount + b.reportOutReplyCount) -
        (a.reportInReplyCount + a.reportOutReplyCount)
    );
  }

  if (limit > 0) {
    ranking = ranking.slice(0, limit);
  }

  clearResultArea(viewSheet);

  const output = ranking.map((r, i) => [
    i + 1,
    getMedal(i + 1),
    r.userName,
    r.chatCount,
    r.reportInReplyCount,
    r.reportOutReplyCount,
    r.total,
    r.userId
  ]);

  if (output.length > 0) {
    viewSheet.getRange(8, 1, output.length, output[0].length).setValues(output);
  }

  viewSheet.getRange('F2').setValue('最終更新');
  viewSheet.getRange('G2').setValue(new Date());
  viewSheet.getRange('F3').setValue('対象run_id数');
  viewSheet.getRange('G3').setValue(Object.keys(latestRunIdByDate).length);
}

function isCategoryTarget(selectedCategory, rowCategory) {
  if (selectedCategory === 'all') return true;
  if (selectedCategory === 'report_reply') {
    return rowCategory === 'report_in_reply' || rowCategory === 'report_out_reply';
  }
  return selectedCategory === rowCategory;
}

function clearResultArea(sheet) {
  const maxRows = sheet.getMaxRows();
  if (maxRows >= 8) {
    sheet.getRange(8, 1, maxRows - 7, 8).clearContent();
  }
}

function getMedal(rank) {
  if (rank === 1) return '🥇';
  if (rank === 2) return '🥈';
  if (rank === 3) return '🥉';
  return '';
}

function normalizeDate(value) {
  if (!value) return null;

  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value)) {
    return new Date(value.getFullYear(), value.getMonth(), value.getDate());
  }

  const d = new Date(value);
  if (isNaN(d)) return null;

  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function formatDateKey(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}