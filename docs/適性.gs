// ================================================================
// 適性診断 スプレッドシート - Google Apps Script
// ================================================================
// 【セットアップ手順】
// 1. このスクリプトをスプレッドシートのApps Scriptに貼り付け・保存
// 2. スクリプトプロパティに以下を設定:
//    TASKHUB_WEBHOOK_URL  ... TaskHubのWebhook URL
//    TASKHUB_WEBHOOK_SECRET ... 共有シークレット
//    EMAIL_FROM           ... 送信元メールアドレス（Gmailエイリアス）
// 3. installFormTrigger() を1回手動実行 → フォーム送信トリガーを設置
// ================================================================

const FORM_URL = 'https://forms.gle/NmQNRrxn9QNwycCL6';

function getProps() {
  return PropertiesService.getScriptProperties();
}

// ================================================================
// TaskHub からの呼び出し（ウェブアプリとして公開）
// ================================================================
function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const secret = getProps().getProperty('TASKHUB_WEBHOOK_SECRET') || '';
    if (data.secret !== secret) return jsonRes({ error: 'unauthorized' });

    // メール送信アクション
    if (data.action === 'sendPersonalityEmail') {
      const { name, email, fromEmail, emailSubject, emailBody } = data;
      sendPersonalityEmail(name, email, { fromEmail, emailSubject, emailBody });
      return jsonRes({ ok: true });
    }

    // PDF生成アクション
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
    ? opts.emailBody.replace(/\{name\}/g, name).replace(/\{url\}/g, FORM_URL)
    : `${name} 様\n\nお世話になっております。\ninrevo 採用担当です。\n\n選考のご案内として、適性診断のご回答をお願いします。\n以下のURLよりご回答ください（所要時間：約15分）。\n\n${FORM_URL}\n\nご不明な点はお気軽にご連絡ください。\n\ninrevo 採用担当`;

  const fromEmail = opts.fromEmail || getProps().getProperty('EMAIL_FROM');
  if (fromEmail) {
    try {
      GmailApp.sendEmail(email, subject, body, { from: fromEmail, name: 'inrevo 採用担当' });
      return;
    } catch (_) {}
  }
  MailApp.sendEmail(email, subject, body);
}

// ================================================================
// PDF生成（「結果」シートをDriveに保存してURLを返す）
// ================================================================
function generatePdfForCandidate(candidateId, name) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('結果');
  if (!sheet) throw new Error('「結果」シートが見つかりません');

  const folder = DriveApp.getFolderById('1ODfj0faotwHgvRhujvJF6uiBjcCy-192');
  const url = 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?';
  const exportOptions = {
    exportFormat: 'pdf', format: 'pdf', size: 'A4',
    portrait: false, fitw: true, sheetnames: false,
    printtitle: false, pagenumbers: true, gridlines: false,
    fzr: false, gid: sheet.getSheetId()
  };
  const query = Object.keys(exportOptions).map(k => k + '=' + exportOptions[k]).join('&');
  const token = ScriptApp.getOAuthToken();
  const response = UrlFetchApp.fetch(url + query, { headers: { Authorization: 'Bearer ' + token } });

  const date = Utilities.formatDate(new Date(), ss.getSpreadsheetTimeZone(), 'yyyy-MM-dd');
  const fileName = (name || candidateId || 'candidate') + '_適性診断_' + date + '.pdf';
  const file = folder.createFile(response.getBlob().setName(fileName));
  file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
  return file.getUrl();
}

// ================================================================
// フォーム送信トリガー（onFormSubmit）
// ================================================================
function onFormSubmitHandler(e) {
  try {
    const webhookUrl = getProps().getProperty('TASKHUB_WEBHOOK_URL');
    if (!webhookUrl) return;

    const secret = getProps().getProperty('TASKHUB_WEBHOOK_SECRET') || '';
    const responses = e.namedValues || {};

    // フォームのメールアドレス・氏名フィールドを取得（フォームの項目名に合わせて変更）
    const email = (responses['メールアドレス'] || responses['Email'] || responses['email'] || [''])[0].trim();
    const name  = (responses['氏名'] || responses['お名前'] || responses['名前'] || [''])[0].trim();

    UrlFetchApp.fetch(webhookUrl, {
      method: 'POST',
      contentType: 'application/json',
      payload: JSON.stringify({
        secret,
        action: 'personalityCompleted',
        email,
        name,
        responses: Object.fromEntries(
          Object.entries(responses).map(([k, v]) => [k, v[0]])
        ),
        submittedAt: new Date().toISOString(),
      }),
      muteHttpExceptions: true,
    });
  } catch (err) {
    console.error('onFormSubmitHandler error:', err);
  }
}

function installFormTrigger() {
  // 既存トリガー削除
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'onFormSubmitHandler') ScriptApp.deleteTrigger(t);
  });
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ScriptApp.newTrigger('onFormSubmitHandler').forSpreadsheet(ss).onFormSubmit().create();
  Browser.msgBox('✅ フォーム送信トリガーを設置しました');
}

function uninstallFormTrigger() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'onFormSubmitHandler') ScriptApp.deleteTrigger(t);
  });
  Browser.msgBox('フォーム送信トリガーを削除しました');
}

// ================================================================
// ユーティリティ
// ================================================================
function jsonRes(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}

// ================================================================
// 元のPDF保存関数（手動実行用・そのまま残す）
// ================================================================
function saveRangeAsPDF() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('結果');
  const folder = DriveApp.getFolderById('1ODfj0faotwHgvRhujvJF6uiBjcCy-192');
  const range = sheet.getRange('A3:R40');
  const tempSheet = ss.insertSheet('PDF_temp_' + new Date().getTime());
  range.copyTo(tempSheet.getRange(1, 1), SpreadsheetApp.CopyPasteType.PASTE_NORMAL, false);
  for (let col = 1; col <= 18; col++) {
    tempSheet.setColumnWidth(col, sheet.getColumnWidth(col));
  }
  const url = 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?';
  const exportOptions = {
    exportFormat: 'pdf', format: 'pdf', size: 'A4', portrait: false,
    fitw: true, sheetnames: false, printtitle: false, pagenumbers: true,
    gridlines: false, fzr: false, gid: sheet.getSheetId()
  };
  const query = Object.keys(exportOptions).map(k => k + '=' + exportOptions[k]).join('&');
  const token = ScriptApp.getOAuthToken();
  const response = UrlFetchApp.fetch(url + query, { headers: { Authorization: 'Bearer ' + token } });
  const name = sheet.getRange('B2').getValue();
  const date = Utilities.formatDate(new Date(), ss.getSpreadsheetTimeZone(), 'yyyy-MM-dd');
  folder.createFile(response.getBlob().setName(name + '_' + date + '.pdf'));
  ss.deleteSheet(tempSheet);
  SpreadsheetApp.getUi().alert('PDF保存が完了しました！');
}
