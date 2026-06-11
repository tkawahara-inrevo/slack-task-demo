// #is-採用_日程調整通知 チャンネルの通知投稿から「担当者」を抽出し、
// 担当者がマップに登録されていればスレッドにメンション返信を投稿する。
//
// 期待する投稿フォーマット（WFが出力）:
//   ...
//   ■担当者
//   - 板金 慎太郎
//   ...
//
// 環境変数:
//   RECRUIT_NOTIFY_CHANNEL          対象チャンネルID（デフォルト: C086G2B60SK）
//   RECRUIT_NOTIFY_HANDLER_MAP      "名前:UserID,名前:UserID,..." 形式
//                                    例: "板金:U08KXJ599QW,外山:U123ABC"

const DEFAULT_CHANNEL = 'C086G2B60SK';

function parseHandlerMap(raw) {
  const m = new Map();
  if (!raw) return m;
  for (const part of String(raw).split(',')) {
    const [name, uid] = part.split(':').map(s => (s || '').trim());
    if (name && uid && /^U[A-Z0-9]+$/i.test(uid)) m.set(name, uid);
  }
  return m;
}

function extractHandlerNames(text) {
  if (!text) return [];
  // ■担当者 以降のセクション（次の ■ または末尾まで）を取り出す
  const sec = text.match(/■担当者\s*\n([\s\S]*?)(?=\n■|$)/);
  if (!sec) return [];
  const lines = sec[1].split('\n').map(s => s.trim()).filter(Boolean);
  const names = [];
  for (const line of lines) {
    // 行頭の "- " や "・" を除去
    const cleaned = line.replace(/^[-・•]\s*/, '').trim();
    if (cleaned) names.push(cleaned);
  }
  return names;
}

function registerRecruitNotify({ app }) {
  const CHANNEL = process.env.RECRUIT_NOTIFY_CHANNEL || DEFAULT_CHANNEL;
  const HANDLER_MAP = parseHandlerMap(process.env.RECRUIT_NOTIFY_HANDLER_MAP || '板金:U08KXJ599QW');

  if (!HANDLER_MAP.size) {
    console.warn('[recruit-notify] no handler map configured, listener inactive');
    return;
  }

  app.message(async ({ message, client }) => {
    try {
      if (message.channel !== CHANNEL) return;
      if (message.thread_ts && message.thread_ts !== message.ts) return; // スレッド返信は無視
      if (message.subtype === 'message_changed' || message.subtype === 'message_deleted') return;

      const text = message.text || '';
      if (!text.includes('■担当者')) return;

      const names = extractHandlerNames(text);
      if (!names.length) return;

      // マップ上のキー（姓など）が担当者文字列に含まれていればヒット
      const mentions = [];
      const seen = new Set();
      for (const name of names) {
        for (const [key, uid] of HANDLER_MAP) {
          if (name.includes(key) && !seen.has(uid)) {
            mentions.push({ key, uid, name });
            seen.add(uid);
          }
        }
      }
      if (!mentions.length) {
        console.log(`[recruit-notify] no handler matched: ${names.join(' / ')}`);
        return;
      }

      const mentionText = mentions.map(m => `<@${m.uid}>`).join(' ');
      await client.chat.postMessage({
        channel: message.channel,
        thread_ts: message.ts,
        text: `${mentionText} 商談入りました！前後の稼働時間ブロック等の対応をお願いします🙏`,
        unfurl_links: false,
        unfurl_media: false,
      });
      console.log(`[recruit-notify] mentioned ${mentions.map(m => m.key).join(',')} on ${message.ts}`);
    } catch (e) {
      console.warn('[recruit-notify] handler error:', e.message);
    }
  });

  console.log(`[recruit-notify] active on ${CHANNEL}, ${HANDLER_MAP.size} handler(s) mapped`);
}

module.exports = { registerRecruitNotify, parseHandlerMap, extractHandlerNames };
