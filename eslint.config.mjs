// eslint.config.mjs
import globals from "globals";

export default [
  {
    files: ["**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "script", // CommonJS (require) 前提ならこれが安心
      globals: {
        ...globals.node, // require / process / __dirname などを認識させる
      },
    },
    rules: {
      // ✅ 目的：使ってない関数/変数を見つけたい
      "no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],

      // 🔕 今回の目的じゃない/ノイズになりがちなやつはOFF
      "no-undef": "off", // require/process系の “not defined” を止める（globals.node入れてれば基本不要だけど保険）
      "no-empty": "off", // Empty block statement を止める
    },
  },
];