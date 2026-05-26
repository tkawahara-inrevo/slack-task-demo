// ================================================================
// inrevo 採用実技テスト - Google Apps Script
// ================================================================
// 【デプロイ手順】
// 1. テンプレートスプレッドシートのスクリプトエディタに全文貼り付け
// 2. プロジェクトの設定 → スクリプト プロパティ → WEBHOOK_SECRET を設定する
// 3. 「デプロイ」→「新しいデプロイ」→ 種類: ウェブアプリ
//    ・実行者: 自分
//    ・アクセス: 全員（匿名を含む）
// 4. 発行された URL を TaskHub 設定の「GAS Web App URL」に貼り付け
// 5. installPoller() を1回実行して定期チェックを開始する
// ================================================================
// 【採点方式】
//   問題1〜10: 各1点（計10点満点）
//   問題13: タイピングスコア（参考指標のみ・採点対象外）
//   問題14: 人事確認（自動採点なし・ノータッチ）
// ================================================================

function getSecret() {
  return PropertiesService.getScriptProperties().getProperty('WEBHOOK_SECRET') || '';
}

// ================================================================
// TaskHub からの呼び出し（シートコピー＋メタデータ保存）
// ================================================================
function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const { candidateId, name, email, templateId, webhookUrl, secret,
            fromEmail, emailSubject, emailBody } = data;

    if (secret !== getSecret()) return jsonRes({ error: 'unauthorized' });

    if (data.action === 'importFromSheet') return importFromSheet(data.spreadsheetId);
    if (data.action === 'gradeNow')        return gradeNow(data.spreadsheetId, data.candidateId, data.webhookUrl);

    // テンプレートをコピー
    const newFile = DriveApp.getFileById(templateId).makeCopy(`${name}様_実技テスト`);
    newFile.addEditor(email);
    const ss = SpreadsheetApp.open(newFile);

    // メタデータ保存
    const meta = ss.getSheetByName('__meta') || ss.insertSheet('__meta');
    meta.getRange('A1').setValue(candidateId);
    meta.getRange('A2').setValue(webhookUrl);
    meta.getRange('A3').setValue(newFile.getId());
    meta.hideSheet();

    setupCompleteCheckbox(ss);
    setupValidation(ss);
    setupConditionalFormatting(ss);

    // ★ editトリガーの代わりに「監視リスト」に追加
    addToWatchList(newFile.getId(), candidateId, webhookUrl);

    sendTestEmail(name, email, newFile.getUrl(), { fromEmail, emailSubject, emailBody });

    return jsonRes({ ok: true, spreadsheetUrl: newFile.getUrl(), spreadsheetId: newFile.getId() });

  } catch (err) {
    console.error(err);
    return jsonRes({ error: err.toString() });
  }
}

// ================================================================
// 監視リスト管理
// ================================================================
function addToWatchList(sheetId, candidateId, webhookUrl) {
  const props = PropertiesService.getScriptProperties();
  const list = JSON.parse(props.getProperty('WATCH_LIST') || '[]');
  list.push({ sheetId, candidateId, webhookUrl, addedAt: Date.now() });
  props.setProperty('WATCH_LIST', JSON.stringify(list));
}

function getWatchList() {
  return JSON.parse(PropertiesService.getScriptProperties().getProperty('WATCH_LIST') || '[]');
}

function setWatchList(list) {
  PropertiesService.getScriptProperties().setProperty('WATCH_LIST', JSON.stringify(list));
}

// ================================================================
// 定期ポーリング（時間ベーストリガーで自動実行）
// ================================================================
function pollCompletions() {
  const list = getWatchList();
  if (list.length === 0) return;

  const remaining = [];
  const EXPIRE_DAYS = 30; // 30日以上古いものは削除

  for (const item of list) {
    const { sheetId, candidateId, webhookUrl, addedAt } = item;

    // 30日以上経過したものは削除
    if (addedAt && Date.now() - addedAt > EXPIRE_DAYS * 86400 * 1000) {
      console.log(`[poller] expired, removing: ${sheetId}`);
      continue;
    }

    try {
      const ss = SpreadsheetApp.openById(sheetId);
      const sheet = ss.getSheetByName('test');
      if (!sheet) { remaining.push(item); continue; }

      // B31・B30・C19をまとめて取得して検証
      const vals = sheet.getRange('B19:C31').getValues();
      const c19      = vals[0][1];   // C19: タイピングスコア
      const b30      = vals[11][0];  // B30: スクショ確認
      const isComplete = vals[12][0] === true; // B31: 提出チェック

      if (!isComplete) { remaining.push(item); continue; }

      // B30チェック（チェックボックスが存在する新形式のみ。古いシートはスキップ）
      const b30HasCheckbox = (b30 === true || b30 === false);
      if (b30HasCheckbox && !b30) {
        sheet.getRange('B31').setValue(false);
        SpreadsheetApp.flush();
        remaining.push(item);
        continue;
      }

      // C19未入力・無効 → B31を外すだけ
      const c19Num = Number(c19);
      const validScore = c19 !== '' && c19 !== null &&
                         !isNaN(c19Num) && Number.isInteger(c19Num) && c19Num >= 0 && c19Num <= 1000;
      if (!validScore) {
        sheet.getRange('B31').setValue(false);
        SpreadsheetApp.flush();
        remaining.push(item);
        continue;
      }

      // すでに完了表示に書き換え済みかチェック（重複送信防止）
      const c31 = sheet.getRange('C31').getDisplayValue() || '';
      if (c31.includes('提出が完了')) {
        // 既に処理済み → リストから外す
        console.log(`[poller] already processed: ${sheetId}`);
        continue;
      }

      // 採点
      console.log(`[poller] processing: ${candidateId}`);
      sheet.getRange('C31').setValue('⏳ 採点・提出中...').setFontColor('#d97706').setFontWeight('bold');
      SpreadsheetApp.flush();

      const result = calculateScore(ss);

      UrlFetchApp.fetch(webhookUrl, {
        method: 'POST',
        contentType: 'application/json',
        payload: JSON.stringify({
          candidateId, secret: getSecret(),
          score: result.total, scoreDetail: result.detail,
          typingLevel: result.detail.q13_level,
        }),
        muteHttpExceptions: true,
      });

      sheet.getRange('C31:F31').merge()
           .setValue('✅ 提出が完了しました！採点結果は採用担当者よりご連絡します。ありがとうございました。')
           .setFontColor('#15803d').setFontWeight('bold').setBackground('#dcfce7');
      sheet.getRange('B32').setValue('このスプレッドシートは閉じていただいて構いません。')
           .setFontColor('#6b7280').setFontStyle('italic');
      sheet.getRange('B31').protect().setDescription('提出済み').setWarningOnly(true);

      console.log(`[poller] done: ${candidateId}`);
      // 完了 → リストから外す（remaining に追加しない）

    } catch (err) {
      console.error(`[poller] error for ${sheetId}:`, err.message);
      remaining.push(item); // エラー時は次回再試行
    }
  }

  setWatchList(remaining);
}

// ================================================================
// 即時採点（TaskHubから「再採点」ボタン経由で呼ばれる）
// ポーリング待ちにせず、その場でcalculateScoreしてwebhookに送信。
// 監視リストにも入れて以降の再採点漏れにも備える。
// ================================================================
function gradeNow(spreadsheetId, candidateId, webhookUrl) {
  try {
    if (!spreadsheetId || !candidateId || !webhookUrl) {
      return jsonRes({ error: 'spreadsheetId/candidateId/webhookUrl required' });
    }
    const ss = SpreadsheetApp.openById(spreadsheetId);
    const sheet = ss.getSheetByName('test');
    if (!sheet) return jsonRes({ error: 'sheet "test" not found' });

    // 監視リストにも入れておく（次回以降の保険）
    const list = getWatchList();
    if (!list.some(i => i.sheetId === spreadsheetId)) {
      list.push({ sheetId: spreadsheetId, candidateId, webhookUrl, addedAt: Date.now() });
      setWatchList(list);
    }

    const result = calculateScore(ss);
    UrlFetchApp.fetch(webhookUrl, {
      method: 'POST',
      contentType: 'application/json',
      payload: JSON.stringify({
        candidateId, secret: getSecret(),
        score: result.total, scoreDetail: result.detail,
        typingLevel: result.detail.q13_level,
      }),
      muteHttpExceptions: true,
    });

    // C31の表示を完了に
    sheet.getRange('C31:F31').merge()
         .setValue('✅ 提出が完了しました！採点結果は採用担当者よりご連絡します。ありがとうございました。')
         .setFontColor('#15803d').setFontWeight('bold').setBackground('#dcfce7');

    return jsonRes({ ok: true, score: result.total });
  } catch (err) {
    console.error('gradeNow error:', err);
    return jsonRes({ error: err.toString() });
  }
}

// ================================================================
// 定期トリガーのセットアップ（1回だけ手動実行）
// ================================================================
function installPoller() {
  // 既存の pollCompletions トリガーを削除してから再作成
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'pollCompletions') ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger('pollCompletions').timeBased().everyMinutes(1).create();
  Browser.msgBox('✅ 定期チェック（1分ごと）を設定しました');
}

function uninstallPoller() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'pollCompletions') ScriptApp.deleteTrigger(t);
  });
  Browser.msgBox('定期チェックを停止しました');
}

// ================================================================
// スプレッドシートから候補者を取り込む
// ================================================================
function importFromSheet(spreadsheetId) {
  try {
    const ss = SpreadsheetApp.openById(spreadsheetId);
    const sheet = ss.getSheets()[0];
    const lastRow = sheet.getLastRow();
    const rows = [];
    for (let i = 2; i <= lastRow; i++) {
      const name  = String(sheet.getRange(i, 1).getValue() || '').trim();
      const email = String(sheet.getRange(i, 2).getValue() || '').trim();
      const flag  = String(sheet.getRange(i, 3).getValue() || '').trim();
      if (!name || !email || flag) continue;
      rows.push({ name, email, row: i });
    }
    for (const r of rows) sheet.getRange(r.row, 3).setValue('取り込み済み');
    return jsonRes({ ok: true, rows: rows.map(r => ({ name: r.name, email: r.email })) });
  } catch (err) {
    return jsonRes({ ok: false, error: err.toString() });
  }
}

// ================================================================
// 完了チェックボックスをシートに設置（B30: スクショ確認 / B31: 提出）
// ================================================================
function setupCompleteCheckbox(ss) {
  const sheet = ss.getSheetByName('test');
  if (!sheet) return;

  // B30: スクリーンショット確認チェックボックス
  sheet.setRowHeight(30, 44);
  const ssCb = sheet.getRange('B30');
  ssCb.insertCheckboxes(); ssCb.setValue(false);
  ssCb.setBackground('#fffbeb')
      .setBorder(true, true, true, false, false, false, '#d97706', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  const ssLabel = sheet.getRange('C30:F30');
  ssLabel.merge()
         .setValue('📸 問13のタイピングテストのスクリーンショットを貼り付けましたか？　→ 貼り付け済みならチェックを入れてください')
         .setFontSize(11).setFontWeight('bold').setFontColor('#92400e')
         .setVerticalAlignment('middle').setBackground('#fffbeb')
         .setBorder(true, false, true, true, false, false, '#d97706', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);

  // B31: 提出チェックボックス
  sheet.setRowHeight(31, 50); sheet.setRowHeight(32, 30);
  const cbCell = sheet.getRange('B31');
  cbCell.insertCheckboxes(); cbCell.setValue(false);
  cbCell.setBackground('#f0fdf4')
        .setBorder(true, true, true, false, false, false, '#16a34a', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  const labelRange = sheet.getRange('C31:F31');
  labelRange.merge()
            .setFormula('=IF(AND(B31=TRUE,AND(ISNUMBER(C19),C19>=0,C19<=1000,B30=TRUE)),"⏳ 提出を受け付けました！採点が完了すると、このメッセージが更新されます。スプレッドシートを閉じても問題ありません。",IF(AND(B31=TRUE,NOT(AND(ISNUMBER(C19),C19>=0,C19<=1000,B30=TRUE))),IF(NOT(AND(ISNUMBER(C19),C19>=0,C19<=1000)),"⚠ 問13のタイピングスコア（C19）が未入力です。0〜1000の数字を入力してください","⚠ B30のスクリーンショット確認にチェックを入れてください"),"テスト完了したらチェックを入れてください → 自動採点・提出されます"))')
            .setFontSize(11).setFontWeight('bold').setFontColor('#15803d')
            .setVerticalAlignment('middle').setBackground('#f0fdf4')
            .setBorder(true, false, true, true, false, false, '#16a34a', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  sheet.getRange('B32').setValue('※ B30・B31の両方にチェックを入れると提出されます。提出後は変更できません。')
       .setFontColor('#6b7280').setFontSize(9).setFontStyle('italic');
}

// ================================================================
// 入力バリデーション設定
// ================================================================
function setupValidation(ss) {
  const test = ss.getSheetByName('test');
  if (!test) return;
  const typingRule = SpreadsheetApp.newDataValidation()
    .requireNumberBetween(0, 1000).setAllowInvalid(false)
    .setHelpText('e-typingのスコア（数値）を入力してください。0〜1000の整数のみ有効です。').build();
  test.getRange('C19').setDataValidation(typingRule);
}

// ================================================================
// 条件付き書式（ブラウザ側で即時反映・GASトリガー不要）
// C19未入力 or B30未チェック → B31行が赤くなる
// 両方OK → B31行が緑になる
// ================================================================
function setupConditionalFormatting(ss) {
  const sheet = ss.getSheetByName('test');
  if (!sheet) return;
  const conditionOk = 'AND(ISNUMBER(C19),C19>=0,C19<=1000,B30=TRUE)';
  const rules = sheet.getConditionalFormatRules();

  // B31行: B31チェック済み かつ 条件NG → 赤（エラー）
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenFormulaSatisfied(`=AND(B31=TRUE,NOT(${conditionOk}))`)
      .setBackground('#fecaca').setFontColor('#991b1b')
      .setRanges([sheet.getRange('B31:F31')])
      .build()
  );
  // B31行: 条件OK → 緑（提出可能 or 処理中）
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenFormulaSatisfied(`=${conditionOk}`)
      .setBackground('#dcfce7').setFontColor('#15803d')
      .setRanges([sheet.getRange('B31:F31')])
      .build()
  );
  sheet.setConditionalFormatRules(rules);
}

// ================================================================
// B30チェック時にB31を強制リセット（テンプレートコピーに自動引き継ぎ）
// B30→B31の順番を強制し、意図しない提出を防ぐ
// ================================================================
function onEdit(e) {
  try {
    const sheet = e.range.getSheet();
    if (sheet.getName() !== 'test') return;
    if (e.range.getA1Notation() !== 'B30') return;
    if (e.value !== 'TRUE') return;

    const b31 = sheet.getRange('B31');
    if (b31.getValue() !== true) return; // B31未チェックなら何もしない

    // B31チェック済み → C19を確認
    const c19 = sheet.getRange('C19').getValue();
    const num = Number(c19);
    const validScore = c19 !== '' && c19 !== null &&
                       !isNaN(num) && Number.isInteger(num) && num >= 0 && num <= 1000;

    if (!validScore) {
      // C19がまだ無効 → B31を外す
      b31.setValue(false);
      SpreadsheetApp.flush();
    }
    // C19もOK → B31はそのまま（ポーラーが提出処理する）
  } catch (err) {
    console.error('onEdit error:', err);
  }
}

// ================================================================
// メール送信
// ================================================================
function sendTestEmail(name, email, spreadsheetUrl, opts) {
  opts = opts || {};
  const subject = opts.emailSubject || '【inrevo】実技試験のご案内';
  const body = opts.emailBody
    ? opts.emailBody.replace(/\{name\}/g, name).replace(/\{url\}/g, spreadsheetUrl)
    : `${name} 様\n\nこの度は inrevo の採用選考にお申し込みいただき、誠にありがとうございます。\n\n実技試験の URL をお送りします。\n\n${spreadsheetUrl}\n\nご不明な点はお気軽にご連絡ください。\n\ninrevo 採用担当`;

  if (opts.fromEmail) {
    try { GmailApp.sendEmail(email, subject, body, { from: opts.fromEmail, name: 'inrevo 採用担当' }); return; }
    catch (_) {}
  }
  MailApp.sendEmail(email, subject, body);
}

// ================================================================
// 採点ロジック（10点満点）
// ================================================================
function calculateScore(ss) {
  const test   = ss.getSheetByName('test');
  const sheet6 = ss.getSheetByName('問題6');
  let total = 0;
  const detail = {};

  detail.q1 = matchText(test.getRange('C3').getValue(), '株式会社INREVO') ? 1 : 0;
  detail.q2 = (fwToHalf(test.getRange('C4').getDisplayValue()).trim() === '0922821670') ? 1 : 0;
  detail.q3 = (Number(test.getRange('C6').getValue()) === 141) ? 1 : 0;

  if (sheet6) {
    detail.q4 = (Number(sheet6.getRange('G1').getValue()) === 757) ? 1 : 0;
    detail.q5 = (sheet6.getFrozenRows() >= 1) ? 1 : 0;
    detail.q6 = scoreFilterHR(sheet6);
  } else { detail.q4 = detail.q5 = detail.q6 = 0; }

  [
    { key:'q7',  cell:'C14', ok:['ctrl+c','control+c'] },
    { key:'q8',  cell:'C15', ok:['ctrl+v','control+v'] },
    { key:'q9',  cell:'C16', ok:['ctrl+shift+v','ctrl+alt+v','control+shift+v','control+alt+v'] },
    { key:'q10', cell:'C17', ok:['ctrl+z','control+z'] },
  ].forEach(({ key, cell, ok }) => {
    const val = normalizeShortcut(test.getRange(cell).getValue());
    detail[key] = ok.some(a => val === a || val.includes(a)) ? 1 : 0;
  });

  total = detail.q1+detail.q2+detail.q3+detail.q4+detail.q5+detail.q6+detail.q7+detail.q8+detail.q9+detail.q10;

  const typingRaw = Number(fwToHalf(test.getRange('C19').getDisplayValue())) || 0;
  detail.q13_raw = typingRaw;
  detail.q13_level = getTypingLevel(typingRaw);
  detail.typing_level = detail.q13_level;
  detail.note = '自動採点10点満点。タイピングは参考指標のみ。問題14は人事確認。';
  return { total, detail };
}

function scoreFilterHR(sheet6) {
  const filter = sheet6.getFilter();
  if (!filter) return 0;
  try {
    const criteria = filter.getColumnFilterCriteria(3);
    if (!criteria) return 0;
    const hidden = criteria.getHiddenValues() || [];
    const mustHide = ['MK','BC','CP','CEO'];
    return (mustHide.every(v => hidden.includes(v)) && !hidden.includes('HR')) ? 1 : 0;
  } catch (_) { return 0; }
}

// ================================================================
// ユーティリティ
// ================================================================
function getTypingLevel(s) {
  return ([
    [400,'Professor'],[375,'Comet'],[350,'Ninja'],[325,'Thunder'],
    [300,'Fast'],[277,'Good!'],[260,'S'],[243,'A+'],[226,'A'],[209,'A-'],
    [192,'B+'],[175,'B'],[158,'B-'],[141,'C+'],[124,'C'],[107,'C-'],
    [90,'D+'],[73,'D'],[56,'D-'],[39,'E+'],[22,'E'],[0,'E-'],
  ].find(([min]) => s >= min) || [0,'E-'])[1];
}
// 全角英数記号 → 半角に正規化（Ａ→A, ０→0, ＋→+, 　→半角スペース）
// ※ 範囲を全角リテラルで書くとエディタ／コピペ／保存エンコーディングで化けることがあるので
//    必ず \uFFxx の明示的unicode escapeで指定する。書き換えないこと。
function fwToHalf(s) {
  return String(s)
    .replace(/[\uFF01-\uFF5E]/g, ch => String.fromCharCode(ch.charCodeAt(0) - 0xFEE0))
    .replace(/\u3000/g, ' ');
}
function matchText(val, expected) {
  return fwToHalf(val).trim().replace(/\s+/g,'') === fwToHalf(expected).replace(/\s+/g,'');
}
function normalizeShortcut(val) {
  return fwToHalf(val).trim().toLowerCase().replace(/\s+/g,'');
}
function jsonRes(obj) { return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON); }

// ================================================================
// デバッグ用
// ================================================================
function testGmailAlias() {
  try {
    GmailApp.sendEmail(Session.getActiveUser().getEmail(), 'テスト送信', 'GmailApp alias テスト', { from: 'jinji@inrevo.jp', name: 'inrevo人事' });
    Browser.msgBox('✅ 成功！jinji@inrevo.jp から送信できました');
  } catch (e) { Browser.msgBox('❌ エラー: ' + e.message); }
}

function testSendEmailFull() {
  const testEmail = Session.getActiveUser().getEmail();
  sendTestEmail('テスト太郎', testEmail, 'https://example.com/test', { fromEmail:'jinji@inrevo.jp', emailSubject:'【テスト】', emailBody:'{name} 様\n\nテストです。\n\n{url}' });
  Browser.msgBox('送信完了。受信トレイを確認してください');
}

function showWatchList() {
  const list = getWatchList();
  Browser.msgBox(`監視中: ${list.length}件\n${list.map(i => i.candidateId).join('\n')}`);
}

// 採点の動作確認用: 指定スプレッドシートのスコアをログ出力（webhookには送らない）
function debugScore(spreadsheetId) {
  // 例: debugScore('1-GoOzUl-fNm4svrFsNvOF8UWCIJRRowMXUXgW2S9CxQ')
  const ss = SpreadsheetApp.openById(spreadsheetId);
  const test = ss.getSheetByName('test');
  // 実際のセル値を出力してロジック側の参照位置とずれてないか確認
  const cells = ['C3','C4','C6','C14','C15','C16','C17','C19'];
  cells.forEach(addr => {
    const v = test.getRange(addr).getValue();
    const d = test.getRange(addr).getDisplayValue();
    console.log(`${addr}: value=${JSON.stringify(v)} display=${JSON.stringify(d)}`);
  });
  // testシート全体の中身も浅くダンプ（B列〜C列の上位30行）
  console.log('--- B2:C20 dump ---');
  test.getRange('B2:C20').getValues().forEach((row, i) => {
    console.log(`row${i+2}: B=${JSON.stringify(row[0])} C=${JSON.stringify(row[1])}`);
  });

  const result = calculateScore(ss);
  console.log('total:', result.total);
  console.log('detail:', JSON.stringify(result.detail));
  console.log('fwToHalf("ｃｔｒｌ+ｃ"):', fwToHalf('ｃｔｒｌ+ｃ'));
  console.log('fwToHalf("１４８"):', fwToHalf('１４８'));
}

function authorizeAllScopes() {
  ScriptApp.getProjectTriggers();
  DriveApp.getRootFolder();
  SpreadsheetApp.getActiveSpreadsheet();
  GmailApp.getInboxThreads(0, 1);
  Browser.msgBox('✅ 権限承認が完了しました（GmailApp含む）。');
}
