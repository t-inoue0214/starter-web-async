package com.example.async.common;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ネストしない（フラットな）JSONオブジェクトだけを扱う、学習用の最小JSONヘルパー。
 * 値は文字列・数値・真偽値のみ対応する。配列やネストしたオブジェクトは扱えない。
 * （配布済みクラス。この講座の主題は Pub/Sub なので、JSON の自作には深入りしません）
 */
public final class SimpleJson {

    private SimpleJson() {
    }

    /**
     * Map を JSON 文字列に変換する。値が Number / Boolean ならそのまま、それ以外は文字列として出力する。
     *
     * @param fields キーと値の組（順序を保ちたいときは LinkedHashMap を渡す）
     * @return JSON 文字列
     */
    public static String stringify(Map<String, Object> fields) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> e : fields.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(escape(e.getKey())).append("\":");
            Object v = e.getValue();
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
            } else {
                sb.append('"').append(escape(v.toString())).append('"');
            }
        }
        return sb.append('}').toString();
    }

    /**
     * フラットな JSON オブジェクト文字列を解析し、すべての値を文字列として返す。
     *
     * @param json JSON 文字列（例: {"orderId":"A-1","amount":1200}）
     * @return キーと値（文字列）の Map。null 値はキーごと含めない
     * @throws IllegalArgumentException JSON として解釈できない場合
     */
    public static Map<String, String> parse(String json) {
        Map<String, String> result = new LinkedHashMap<>();
        String s = json.trim();
        if (!s.startsWith("{") || !s.endsWith("}")) {
            throw new IllegalArgumentException("JSONオブジェクトではありません: " + json);
        }
        int i = 1;
        int end = s.length() - 1;
        while (true) {
            i = skipSpaces(s, i);
            if (i >= end) {
                break;
            }
            // キーを読む
            if (s.charAt(i) != '"') {
                throw new IllegalArgumentException("キーが文字列ではありません: " + json);
            }
            int[] next = new int[1];
            String key = readString(s, i, next);
            i = skipSpaces(s, next[0]);
            if (s.charAt(i) != ':') {
                throw new IllegalArgumentException("':' がありません: " + json);
            }
            i = skipSpaces(s, i + 1);
            // 値を読む
            String value;
            if (s.charAt(i) == '"') {
                value = readString(s, i, next);
                i = next[0];
            } else {
                int start = i;
                while (i < end && s.charAt(i) != ',' && !Character.isWhitespace(s.charAt(i))) {
                    i++;
                }
                value = s.substring(start, i);
                if (value.isEmpty() || value.startsWith("{") || value.startsWith("[")) {
                    throw new IllegalArgumentException("未対応の値です: " + json);
                }
                if (value.equals("null")) {
                    value = null;
                }
            }
            if (value != null) {
                result.put(key, value);
            }
            i = skipSpaces(s, i);
            if (i < end && s.charAt(i) == ',') {
                i++;
            } else if (i < end) {
                throw new IllegalArgumentException("',' がありません: " + json);
            }
        }
        return result;
    }

    private static int skipSpaces(String s, int i) {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return i;
    }

    private static String readString(String s, int start, int[] next) {
        StringBuilder sb = new StringBuilder();
        int i = start + 1;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '"') {
                next[0] = i + 1;
                return sb.toString();
            }
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(i + 1);
                switch (n) {
                    case 'n' -> sb.append('\n');
                    case 't' -> sb.append('\t');
                    case 'r' -> sb.append('\r');
                    case 'u' -> {
                        sb.append((char) Integer.parseInt(s.substring(i + 2, i + 6), 16));
                        i += 4;
                    }
                    default -> sb.append(n);
                }
                i += 2;
            } else {
                sb.append(c);
                i++;
            }
        }
        throw new IllegalArgumentException("文字列が閉じていません: " + s);
    }

    private static String escape(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
