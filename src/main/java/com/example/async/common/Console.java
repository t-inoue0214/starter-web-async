package com.example.async.common;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * 時刻とスレッド名つきでコンソールに出力する、学習用の小さなロガー。
 * Pub/Sub は「いつ・どのスレッドで」受信したかが重要なので、すべての出力に付けている。
 * （配布済みクラス。学習者が書き換える必要はありません）
 */
public final class Console {
    // 時:分:秒.ミリ秒 の形式
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Console() {
    }

    /**
     * 1行出力する。
     *
     * @param message 出力するメッセージ
     */
    public static void log(String message) {
        // 複数スレッドから呼ばれても行が混ざらないよう、1回の println で出力する
        System.out.println("[" + LocalTime.now().format(FORMAT) + "][" + Thread.currentThread().getName() + "] " + message);
    }

    /**
     * 指定ミリ秒だけ待つ。重い処理の代わりに使う。
     *
     * @param millis 待つミリ秒
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // 割り込みフラグを立て直してから抜ける（握りつぶさない）
            Thread.currentThread().interrupt();
            throw new IllegalStateException("待機中に割り込まれました", e);
        }
    }
}
