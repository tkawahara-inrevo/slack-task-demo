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
// ================================================================
// 【採点方式】
//   問題1〜10: 各1点（計10点満点）
//   問題13: タイピングスコア（参考指標のみ・採点対象外）
//   問題14: 人事確認（自動採点なし・ノータッチ）
// ================================================================

// WEBHOOK_SECRET は Script Properties に保存（コードには書かない）
// 設定方法: GASエディタ左メニュー「プロジェクトの設定」→「スクリプト プロパティ」
//           プロパティ名: WEBHOOK_SECRET  値: TaskHub設定と同じ値
function getSecret() {
  return PropertiesService.getScriptProperties().getProperty('WEBHOOK_SECRET') || '';
}

// ================================================================
// TaskHub からの呼び出し（シートコピー＋チェックボックス設置）
// ================================================================
function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const {
      candidateId, name, email, templateId, webhookUrl, secret,
      fromEmail, emailSubject, emailBody,
    } = data;

    if (secret !== getSecret()) {
      return jsonRes({ error: 'unauthorized' });
    }

    // スプレッドシートからの候補者取り込み
    if (data.action === 'importFromSheet') {
      return importFromSheet(data.spreadsheetId);
    }

    const newFile = DriveApp.getFileById(templateId).makeCopy(`${name}様_実技テスト`);
    newFile.addEditor(email);

    const ss = SpreadsheetApp.open(newFile);

    const meta = ss.getSheetByName('__meta') || ss.insertSheet('__meta');
    meta.getRange('A1').setValue(candidateId);
    meta.getRange('A2').setValue(webhookUrl);
    meta.getRange('A3').setValue(newFile.getId());
    meta.hideSheet();

    setupCompleteCheckbox(ss);
    setupValidation(ss);

    ScriptApp.newTrigger('onCompleteCheckbox')
      .forSpreadsheet(ss)
      .onEdit()
      .create();

    sendTestEmail(name, email, newFile.getUrl(), { fromEmail, emailSubject, emailBody });

    return jsonRes({
      ok: true,
      spreadsheetUrl: newFile.getUrl(),
      spreadsheetId: newFile.getId(),
    });

  } catch (err) {
    console.error(err);
    return jsonRes({ error: err.toString() });
  }
}

// ================================================================
// スプレッドシートから候補者を取り込む
// A列=氏名, B列=メアド, C列=取り込み済みフラグ（空行のみ対象）
// ================================================================
function importFromSheet(spreadsheetId) {
  try {
    const ss    = SpreadsheetApp.openById(spreadsheetId);
    const sheet = ss.getSheets()[0]; // 先頭シートを使用
    const lastRow = sheet.getLastRow();
    const rows = [];

    for (let i = 2; i <= lastRow; i++) { // 1行目はヘッダーとしてスキップ
      const name  = String(sheet.getRange(i, 1).getValue() || '').trim();
      const email = String(sheet.getRange(i, 2).getValue() || '').trim();
      const flag  = String(sheet.getRange(i, 3).getValue() || '').trim();

      if (!name || !email) continue;   // 空行スキップ
      if (flag)            continue;   // 取り込み済みスキップ

      rows.push({ name, email, row: i });
    }

    // 取り込み対象行のC列に「取り込み済み」を書き込む
    for (const r of rows) {
      sheet.getRange(r.row, 3).setValue('取り込み済み');
    }

    return jsonRes({ ok: true, rows: rows.map(r => ({ name: r.name, email: r.email })) });
  } catch (err) {
    return jsonRes({ ok: false, error: err.toString() });
  }
}

// ================================================================
// 完了チェックボックスをシートに設置
// ================================================================
function setupCompleteCheckbox(ss) {
  const sheet = ss.getSheetByName('test');
  if (!sheet) return;

  sheet.setRowHeight(30, 20);
  sheet.setRowHeight(31, 50);
  sheet.setRowHeight(32, 30);

  const cbCell = sheet.getRange('B31');
  cbCell.insertCheckboxes();
  cbCell.setValue(false);
  cbCell.setBackground('#f0fdf4')
        .setBorder(true, true, true, false, false, false,
                   '#16a34a', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);

  const labelRange = sheet.getRange('C31:F31');
  labelRange.merge();
  labelRange.setValue('テスト完了したらチェックを入れてください → 自動採点・提出されます')
            .setFontSize(12).setFontWeight('bold').setFontColor('#15803d')
            .setVerticalAlignment('middle').setBackground('#f0fdf4')
            .setBorder(true, false, true, true, false, false,
                       '#16a34a', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);

  sheet.getRange('B32')
       .setValue('※ すべての回答を終えてからチェックを入れてください。提出後は変更できません。')
       .setFontColor('#6b7280').setFontSize(9).setFontStyle('italic');
}

// ================================================================
// 入力バリデーション設定
// ================================================================
function setupValidation(ss) {
  const test = ss.getSheetByName('test');
  if (!test) return;

  // C19: e-typingスコア（整数 0〜999 のみ）
  const typingRule = SpreadsheetApp.newDataValidation()
    .requireNumberBetween(0, 999)
    .setAllowInvalid(false)
    .setHelpText('e-typingのスコア（数値）を入力してください。0〜999の整数のみ有効です。')
    .build();
  test.getRange('C19').setDataValidation(typingRule);
}

// ================================================================
// チェックボックス ON 時のトリガー
// ================================================================
function onCompleteCheckbox(e) {
  try {
    if (!e || !e.range) return;
    if (e.range.getSheet().getName() !== 'test') return;
    if (e.range.getA1Notation() !== 'B31') return;
    if (e.value !== 'TRUE') return;

    const ss    = e.source;
    const sheet = ss.getSheetByName('test');
    const meta  = ss.getSheetByName('__meta');
    if (!meta) return;

    const candidateId = meta.getRange('A1').getValue();
    const webhookUrl  = meta.getRange('A2').getValue();
    if (!candidateId || !webhookUrl) {
      sheet.getRange('C31').setValue('❌ 設定エラー。採用担当者にご連絡ください。').setFontColor('#dc2626');
      return;
    }

    sheet.getRange('C31').setValue('⏳ 採点・提出中...')
         .setFontColor('#d97706').setFontWeight('bold');
    SpreadsheetApp.flush();

    const result = calculateScore(ss);

    UrlFetchApp.fetch(webhookUrl, {
      method: 'POST',
      contentType: 'application/json',
      payload: JSON.stringify({
        candidateId:  candidateId,
        secret:       getSecret(),
        score:        result.total,
        scoreDetail:  result.detail,
        typingLevel:  result.detail.q13_level,
      }),
      muteHttpExceptions: true,
    });

    sheet.getRange('C31:F31').merge()
         .setValue('✅ 提出が完了しました！採点結果は採用担当者よりご連絡します。ありがとうございました。')
         .setFontColor('#15803d').setFontWeight('bold').setBackground('#dcfce7');
    sheet.getRange('B32')
         .setValue('このスプレッドシートは閉じていただいて構いません。')
         .setFontColor('#6b7280').setFontStyle('italic');

    sheet.getRange('B31').protect().setDescription('提出済み').setWarningOnly(true);

    ScriptApp.getProjectTriggers().forEach(t => {
      if (t.getHandlerFunction() === 'onCompleteCheckbox' &&
          t.getTriggerSourceId() === ss.getId()) {
        ScriptApp.deleteTrigger(t);
      }
    });

  } catch (err) {
    console.error(err);
    try {
      e.source.getSheetByName('test')
        .getRange('C31')
        .setValue('❌ エラー: ' + err.message + ' 採用担当者にご連絡ください。')
        .setFontColor('#dc2626');
    } catch (_) {}
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
    : `${name} 様

この度は inrevo の採用選考にお申し込みいただき、誠にありがとうございます。

実技試験の URL をお送りします。
下記 URL よりスプレッドシートを開き、全問題に回答してください。

━━━━━━━━━━━━━━━━━━
▼ 実技テスト URL
${spreadsheetUrl}
━━━━━━━━━━━━━━━━━━

【受験方法】
1. 上記 URL を開く
2. 全問題（問題1〜10）に回答する
   ※ 問題13のタイピングテストは https://www.e-typing.ne.jp/ で受験後、
      スコア数値を入力しスクリーンショットを貼り付けてください
3. 最後にスプレッドシート内の完了チェックボックスにチェックを入れてください
   → 自動採点・提出されます

ご不明な点はお気軽にご連絡ください。
よろしくお願いいたします。

inrevo 採用担当`;

  // from_email が設定されている場合は GmailApp で試みる（エイリアス登録済みのみ有効）
  if (opts.fromEmail) {
    try {
      GmailApp.sendEmail(email, subject, body, { from: opts.fromEmail });
      return;
    } catch (_) {
      // エイリアス未登録の場合は MailApp にフォールバック
    }
  }
  MailApp.sendEmail(email, subject, body);
}

// ================================================================
// 採点ロジック（10点満点 / 問題1〜10 各1点）
// ================================================================
function calculateScore(ss) {
  const test   = ss.getSheetByName('test');
  const sheet6 = ss.getSheetByName('問題6');

  let total = 0;
  const detail = {};

  // ── 問題1: 株式会社INREVO をC3に入力（テキスト一致）
  detail.q1 = matchText(test.getRange('C3').getValue(), '株式会社INREVO') ? 1 : 0;

  // ── 問題2: 0922821670 をC4に入力（表示値一致）
  detail.q2 = (test.getRange('C4').getDisplayValue().trim() === '0922821670') ? 1 : 0;

  // ── 問題3: 5+12×34÷3 を関数で入力（答え: 141）→ C6
  detail.q3 = (Number(test.getRange('C6').getValue()) === 141) ? 1 : 0;

  // ── 問題4〜6: 問題6シート操作（例外）
  if (sheet6) {
    // 問題4: 月収合計をG1に記載
    // ※ G1の期待値は問題6シートの実データによる → 要確認
    const G1_EXPECTED = 757; // 問題6シート 社員番号1-20の月収合計
    detail.q4 = (Number(sheet6.getRange('G1').getValue()) === G1_EXPECTED) ? 1 : 0;

    // 問題5: 1行目を固定
    detail.q5 = (sheet6.getFrozenRows() >= 1) ? 1 : 0;

    // 問題6: A-D列にフィルター、HR部署のみ表示
    detail.q6 = scoreFilterHR(sheet6);
  } else {
    detail.q4 = detail.q5 = detail.q6 = 0;
  }

  // ── 問題7〜10: ショートカットキー（C列）
  [
    { key: 'q7',  cell: 'C14', ok: ['ctrl+c', 'control+c'] },
    { key: 'q8',  cell: 'C15', ok: ['ctrl+v', 'control+v'] },
    { key: 'q9',  cell: 'C16', ok: ['ctrl+shift+v', 'ctrl+alt+v', 'control+shift+v', 'control+alt+v'] },
    { key: 'q10', cell: 'C17', ok: ['ctrl+z', 'control+z'] },
  ].forEach(({ key, cell, ok }) => {
    const val = normalizeShortcut(test.getRange(cell).getValue());
    detail[key] = ok.some(a => val === a || val.includes(a)) ? 1 : 0;
  });

  total = detail.q1 + detail.q2 + detail.q3
        + detail.q4 + detail.q5 + detail.q6
        + detail.q7 + detail.q8 + detail.q9 + detail.q10;

  // ── 問題13: タイピング（参考指標のみ・採点対象外）→ C19
  const typingRaw     = Number(test.getRange('C19').getValue()) || 0;
  detail.q13_raw      = typingRaw;
  detail.q13_level    = getTypingLevel(typingRaw);
  detail.typing_level = detail.q13_level;

  // ── 問題14: 人事確認（ノータッチ）

  detail.note = '自動採点10点満点。タイピングは参考指標のみ。問題14は人事確認。';
  return { total, detail };
}

// ================================================================
// 問題6 フィルター採点（HR部署のみ表示）
// ================================================================
function scoreFilterHR(sheet6) {
  const filter = sheet6.getFilter();
  if (!filter) return 0;
  try {
    // 部署列（C列 = 列3）のフィルター条件を確認
    const criteria = filter.getColumnFilterCriteria(3);
    if (!criteria) return 0;
    const hidden   = criteria.getHiddenValues() || [];
    // HR 以外が非表示になっていれば正解
    const mustHide = ['MK', 'BC', 'CP', 'CEO'];
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

function matchText(val, expected) {
  return String(val).trim().replace(/\s+/g, '') === expected.replace(/\s+/g, '');
}

function normalizeShortcut(val) {
  return String(val).trim().toLowerCase().replace(/[\s　]/g, '').replace(/＋/g, '+');
}

function jsonRes(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

// ================================================================
// 初回セットアップ用（スクリプトエディタから1回だけ手動実行）
// ================================================================
function authorizeAllScopes() {
  const triggers = ScriptApp.getProjectTriggers();
  console.log('現在のトリガー数:', triggers.length);
  DriveApp.getRootFolder();
  SpreadsheetApp.getActiveSpreadsheet();
  Browser.msgBox('✅ 権限承認が完了しました。');
}
