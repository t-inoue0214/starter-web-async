package com.example.async.inprocess;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 同じプロセスの中だけで動く、最小のイベントバス。
 * トピック名ごとに購読者（ハンドラー）を登録しておき、発行されたら全員に配る。
 */
public class MiniEventBus {
    // トピック名 → 購読者の一覧
    private final Map<String, List<Consumer<String>>> subscribers = new HashMap<>();

    /**
     * トピックを購読する。
     *
     * @param topic   トピック名
     * @param handler イベントが届いたときに呼ばれる処理
     */
    public synchronized void subscribe(String topic, Consumer<String> handler) {
        // トピックの一覧がなければ作ってから、購読者を追加する
        subscribers.computeIfAbsent(topic, t -> new ArrayList<>()).add(handler);
    }

    /**
     * トピックにイベントを発行する。購読者がいなければ、何も起きない。
     *
     * @param topic   トピック名
     * @param payload イベントの本文
     */
    public void publish(String topic, String payload) {
        // 購読者の一覧をコピーして取り出す（配っている最中に subscribe されても壊れないように）
        List<Consumer<String>> targets;
        synchronized (this) {
            targets = new ArrayList<>(subscribers.getOrDefault(topic, List.of()));
        }
        // 全員に、呼び出し元のスレッドの中で順番に配る
        for (Consumer<String> handler : targets) {
            handler.accept(payload);
        }
    }
}
