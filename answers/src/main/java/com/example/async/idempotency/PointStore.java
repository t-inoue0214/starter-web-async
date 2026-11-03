package com.example.async.idempotency;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ポイント残高と「処理済みメッセージID」をメモリ上に持つクラス。
 * 学習用に DB の代わりをさせている。プログラムを止めると記録は消える。
 */
public class PointStore {
    // 顧客ID → ポイント残高
    private final Map<String, Long> balances = new HashMap<>();
    // 処理済みのメッセージID
    private final Set<String> processedMessageIds = new HashSet<>();

    /**
     * 重複を気にせずポイントを加算する（素朴な実装）。
     * handleDelivery は複数のスレッドから呼ばれうるので、synchronized で1つずつ実行させる。
     *
     * @param customerId 顧客ID
     * @param points     加算するポイント
     */
    public synchronized void addPoints(String customerId, long points) {
        // 今の残高（なければ 0）に加算して保存する
        balances.put(customerId, balance(customerId) + points);
    }

    /**
     * まだ処理していないメッセージIDのときだけ、ポイントを加算する。
     *
     * @param messageId  メッセージID
     * @param customerId 顧客ID
     * @param points     加算するポイント
     * @return 加算したら true、処理済みだったら false
     */
    public synchronized boolean addPointsOnce(String messageId, String customerId, long points) {
        // Set.add は、すでに入っていると false を返す。それが「処理済み」の判定になる
        if (!processedMessageIds.add(messageId)) {
            return false;
        }
        addPoints(customerId, points);
        return true;
    }

    /**
     * @param customerId 顧客ID
     * @return 残高（記録がなければ 0）
     */
    public synchronized long balance(String customerId) {
        return balances.getOrDefault(customerId, 0L);
    }
}
