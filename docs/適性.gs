// ================================================================
// 適性診断 スプレッドシート - Google Apps Script
// ================================================================
// 【セットアップ手順（1回だけ）】
// 1. このスクリプトをスプレッドシートのApps Scriptに貼り付け・保存
// 2. スクリプトプロパティに以下を設定:
//    TASKHUB_WEBHOOK_URL    ... https://inrevo-task.com/api/dashboard/recruitment/webhook/personality
//    TASKHUB_WEBHOOK_SECRET ... TaskHub設定画面のWebhookシークレットと同じ値
//    EMAIL_FROM             ... 送信元メールアドレス（Gmailエイリアス）
//    RESPONSE_SHEET_NAME    ... フォーム回答シートの名前（省略時: "フォームの回答 1"）
//    NAME_COLUMN            ... 氏名の列番号（省略時: 2 = B列）
// 3. installScanner() を1回手動実行
// ================================================================

const SEIKAKU_FORM_URL = 'https://forms.gle/NmQNRrxn9QNwycCL6';

function getProps() {
  return PropertiesService.getScriptProperties();
}

// ================================================================
// 【メイン】 5分ごとに新しい回答をスキャンしてTaskHubに通知
// ================================================================
function scanNewResponses() {
  const props = getProps();
  const webhookUrl = props.getProperty('TASKHUB_WEBHOOK_URL');
  if (!webhookUrl) { console.log('[scanner] TASKHUB_WEBHOOK_URL 未設定'); return; }

  const secret   = props.getProperty('TASKHUB_WEBHOOK_SECRET') || '';
  const sheetName = props.getProperty('RESPONSE_SHEET_NAME') || 'フォームの回答 1';
  const nameCol  = Number(props.getProperty('NAME_COLUMN') || 2); // B列=2がデフォルト

  const ss    = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) { console.log(`[scanner] シート "${sheetName}" が見つかりません`); return; }

  // A列（タイムスタンプ）に値がある最終行を取得（空行・数式行を無視）
  const tsValues = sheet.getRange('A:A').getValues().flat();
  let lastRow = 1;
  for (let i = tsValues.length - 1; i >= 1; i--) {
    if (tsValues[i]) { lastRow = i + 1; break; } // 1-indexed
  }
  if (lastRow < 2) return; // データなし

  // 前回処理済みの行番号を取得（初回は1行目=ヘッダーのみ）
  const lastProcessedKey = 'LAST_PROCESSED_ROW';
  const lastProcessed = Number(props.getProperty(lastProcessedKey) || 1);

  if (lastRow <= lastProcessed) return; // 新しい回答なし

  console.log(`[scanner] 新しい回答を検出: 行${lastProcessed + 1}〜${lastRow}`);

  // 新しい行を処理
  let processed = lastProcessed;
  for (let row = lastProcessed + 1; row <= lastRow; row++) {
    try {
      const rowData = sheet.getRange(row, 1, 1, sheet.getLastColumn()).getValues()[0];
      const timestamp = rowData[0]; // A列：タイムスタンプ
      const name      = (rowData[nameCol - 1] || '').toString().trim(); // 氏名

      if (!name) { processed = row; continue; }

      console.log(`[scanner] 送信: 行${row} 氏名=${name}`);

      UrlFetchApp.fetch(webhookUrl, {
        method: 'POST',
        contentType: 'application/json',
        payload: JSON.stringify({
          secret,
          action: 'personalityCompleted',
          email: '',
          name,
          submittedAt: timestamp ? new Date(timestamp).toISOString() : new Date().toISOString(),
        }),
        muteHttpExceptions: true,
      });

      processed = row;
      Utilities.sleep(500); // API負荷軽減
    } catch (err) {
      console.error(`[scanner] 行${row} エラー:`, err.message);
    }
  }

  // 処理済み行番号を保存
  props.setProperty(lastProcessedKey, String(processed));
  console.log(`[scanner] 完了: 処理済み行=${processed}`);
}

// ================================================================
// スキャナーのトリガー設置（1回だけ手動実行）
// ================================================================
function installScanner() {
  // 既存トリガー削除
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'scanNewResponses') ScriptApp.deleteTrigger(t);
  });
  // 5分ごとに実行
  ScriptApp.newTrigger('scanNewResponses').timeBased().everyMinutes(5).create();

  // 回答シートの実際のデータ行数を数える（ヘッダー=1行目を除く）
  const sheetName = getProps().getProperty('RESPONSE_SHEET_NAME') || 'フォームの回答 1';
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    Browser.msgBox(`❌ シート "${sheetName}" が見つかりません。\nRESPONSE_SHEET_NAME を確認してください。`);
    return;
  }
  // A列（タイムスタンプ）に値がある最終行を取得（ヘッダー除く）
  const tsCol = sheet.getRange('A:A').getValues().flat();
  let lastDataRow = 1;
  for (let i = tsCol.length - 1; i >= 1; i--) {
    if (tsCol[i]) { lastDataRow = i + 1; break; } // 1-indexed
  }
  getProps().setProperty('LAST_PROCESSED_ROW', String(lastDataRow));
  Browser.msgBox(`✅ スキャナーを設置しました\nシート: ${sheetName}\n現在の最終行: ${lastDataRow}行（これ以降の回答から検知します）`);
}

function uninstallScanner() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'scanNewResponses') ScriptApp.deleteTrigger(t);
  });
  Browser.msgBox('スキャナーを削除しました');
}

// ================================================================
// 今すぐ手動でスキャン（テスト用）
// ================================================================
function scanNow() {
  scanNewResponses();
  Browser.msgBox('スキャン完了。Apps Scriptのログを確認してください。');
}

// ================================================================
// TaskHub からの呼び出し（ウェブアプリとして公開）
// ================================================================
function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const secret = getProps().getProperty('TASKHUB_WEBHOOK_SECRET') || '';
    if (data.secret !== secret) return jsonRes({ error: 'unauthorized' });

    if (data.action === 'sendPersonalityEmail') {
      const { name, email, fromEmail, emailSubject, emailBody } = data;
      sendPersonalityEmail(name, email, { fromEmail, emailSubject, emailBody });
      return jsonRes({ ok: true });
    }

    if (data.action === 'generatePdf') {
      const { candidateId, name } = data;
      const url = generatePdfForCandidate(candidateId, name);
      return jsonRes({ ok: true, pdfUrl: url });
    }

    return jsonRes({ error: 'unknown_action' });
  } catch (err) {
    console.error(err);
    return jsonRes({ error: err.toString() });
  }
}

// ================================================================
// メール送信
// ================================================================
function sendPersonalityEmail(name, email, opts) {
  opts = opts || {};
  const subject = opts.emailSubject || '【inrevo】適性診断のご案内';
  const body = opts.emailBody
    ? opts.emailBody.replace(/\{name\}/g, name).replace(/\{url\}/g, SEIKAKU_FORM_URL)
    : `${name} 様\n\nお世話になっております。\ninrevo 採用担当です。\n\n選考のご案内として、適性診断のご回答をお願いします。\n以下のURLよりご回答ください（所要時間：約10分）。\n\n${SEIKAKU_FORM_URL}\n\nご不明な点はお気軽にご連絡ください。\n\ninrevo 採用担当`;

  const fromEmail = opts.fromEmail || getProps().getProperty('EMAIL_FROM');
  if (fromEmail) {
    try { GmailApp.sendEmail(email, subject, body, { from: fromEmail, name: 'inrevo 採用担当' }); return; }
    catch (_) {}
  }
  MailApp.sendEmail(email, subject, body);
}

// ================================================================
// PDF生成（「結果」シートをDriveに保存してURLを返す）
// ================================================================
function generatePdfForCandidate(candidateId, name) {
  const ss      = SpreadsheetApp.getActiveSpreadsheet();
  const result  = ss.getSheetByName('結果');
  if (!result) throw new Error('「結果」シートが見つかりません');

  // 回答シートから対象者の行を探してコピー
  const sheetName = getProps().getProperty('RESPONSE_SHEET_NAME') || 'フォームの回答 1';
  const nameCol   = Number(getProps().getProperty('NAME_COLUMN') || 2);
  const respSheet = ss.getSheetByName(sheetName);

  if (respSheet && name) {
    const dataRange = respSheet.getDataRange().getValues();
    const normName  = (n) => (n || '').replace(/　/g, ' ').replace(/\s+/g, '').trim();
    const targetName = normName(name);

    // 最新の回答から逆順で検索（同名が複数いる場合は最新を使用）
    let targetRow = -1;
    for (let i = dataRange.length - 1; i >= 1; i--) {
      const rowName = normName(String(dataRange[i][nameCol - 1] || ''));
      if (rowName === targetName || rowName.includes(targetName) || targetName.includes(rowName)) {
        targetRow = i;
        break;
      }
    }

    if (targetRow >= 0) {
      // 対象者の回答行を「結果」シートの2行目にコピー
      const srcRange = respSheet.getRange(targetRow + 1, 1, 1, dataRange[targetRow].length);
      const dstRange = result.getRange(2, 1, 1, dataRange[targetRow].length);
      srcRange.copyTo(dstRange, SpreadsheetApp.CopyPasteType.PASTE_VALUES, false);
      SpreadsheetApp.flush(); // 数式の再計算を待つ
      Utilities.sleep(1000);
    }
  }

  // PDFエクスポート
  const ssFile  = DriveApp.getFileById(ss.getId());
  const parents = ssFile.getParents();
  const folder  = parents.hasNext() ? parents.next() : DriveApp.getRootFolder();
  const url     = 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?';
  const opts    = { exportFormat:'pdf', format:'pdf', size:'A4', portrait:false,
                    fitw:true, sheetnames:false, printtitle:false, pagenumbers:true,
                    gridlines:false, fzr:false, gid:result.getSheetId() };
  const query   = Object.keys(opts).map(k => k + '=' + opts[k]).join('&');
  const token   = ScriptApp.getOAuthToken();
  const resp    = UrlFetchApp.fetch(url + query, { headers: { Authorization: 'Bearer ' + token } });

  const date = Utilities.formatDate(new Date(), ss.getSpreadsheetTimeZone(), 'yyyy-MM-dd');
  const file = folder.createFile(resp.getBlob().setName((name || candidateId) + '_適性診断_' + date + '.pdf'));
  file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
  return file.getUrl();
}

// ================================================================
// ユーティリティ
// ================================================================
function jsonRes(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}

// 手動PDF保存（シート上のPDFボタン用）
function saveRangeAsPDF() {
  const ss    = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('結果');
  const ssFile  = DriveApp.getFileById(ss.getId());
  const parents = ssFile.getParents();
  const folder  = parents.hasNext() ? parents.next() : DriveApp.getRootFolder();
  const url    = 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?';
  const exportOptions = { exportFormat:'pdf', format:'pdf', size:'A4', portrait:false,
                          fitw:true, sheetnames:false, printtitle:false, pagenumbers:true,
                          gridlines:false, fzr:false, gid:sheet.getSheetId() };
  const query  = Object.keys(exportOptions).map(k => k + '=' + exportOptions[k]).join('&');
  const token  = ScriptApp.getOAuthToken();
  const resp   = UrlFetchApp.fetch(url + query, { headers: { Authorization: 'Bearer ' + token } });
  const name   = sheet.getRange('B2').getValue();
  const date   = Utilities.formatDate(new Date(), ss.getSpreadsheetTimeZone(), 'yyyy-MM-dd');
  folder.createFile(resp.getBlob().setName(name + '_' + date + '.pdf'));
  SpreadsheetApp.getUi().alert('PDF保存が完了しました！');
}
