package com.example.async.message;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.example.async.common.SimpleJson;

/**
 * 「注文が確定した」という事実を表すイベント。イミュータブル。
 * 後の章（冪等性・Outbox）でもこのクラスを使う。
 */
public class OrderPlacedEvent {
    /** イベントの種類。受信側はこれを見て処理を振り分ける */
    public static final String TYPE = "order.placed";
    /** このイベントのスキーマ（項目構成）のバージョン */
    public static final int SCHEMA_VERSION = 1;

    // イベント1件ごとに一意なID。重複検知に使う
    private final String eventId;
    // 注文ID
    private final String orderId;
    // 顧客ID
    private final String customerId;
    // 合計金額（円）
    private final long amount;
    // 出来事が起きた時刻（送信時刻ではない）
    private final Instant occurredAt;

    /**
     * @param eventId    イベントID
     * @param orderId    注文ID
     * @param customerId 顧客ID
     * @param amount     合計金額（円）
     * @param occurredAt 出来事が起きた時刻
     */
    public OrderPlacedEvent(String eventId, String orderId, String customerId, long amount, Instant occurredAt) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.occurredAt = occurredAt;
    }

    /**
     * 新しいイベントIDと現在時刻でイベントを作る。
     *
     * @param orderId    注文ID
     * @param customerId 顧客ID
     * @param amount     合計金額（円）
     * @return 新しいイベント
     */
    public static OrderPlacedEvent create(String orderId, String customerId, long amount) {
        return new OrderPlacedEvent(UUID.randomUUID().toString(), orderId, customerId, amount, Instant.now());
    }

    /** @return イベントID */
    public String eventId() {
        return eventId;
    }

    /** @return 注文ID */
    public String orderId() {
        return orderId;
    }

    /** @return 顧客ID */
    public String customerId() {
        return customerId;
    }

    /** @return 合計金額（円） */
    public long amount() {
        return amount;
    }

    /** @return 出来事が起きた時刻 */
    public Instant occurredAt() {
        return occurredAt;
    }

    /**
     * JSON 文字列に変換する。
     *
     * @return JSON 文字列
     */
    public String toJson() {
        // 項目の順序を保つため LinkedHashMap を使う
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("eventId", eventId);
        fields.put("orderId", orderId);
        fields.put("customerId", customerId);
        fields.put("amount", amount);
        // 時刻は ISO-8601 形式の文字列（例: 2026-11-03T09:00:00Z）にする
        fields.put("occurredAt", occurredAt.toString());
        return SimpleJson.stringify(fields);
    }

    /**
     * JSON 文字列からイベントを復元する。
     *
     * @param json JSON 文字列
     * @return 復元したイベント
     * @throws IllegalArgumentException 必須項目が欠けている、または形式が不正な場合
     */
    public static OrderPlacedEvent fromJson(String json) {
        Map<String, String> m = SimpleJson.parse(json);
        return new OrderPlacedEvent(
                required(m, "eventId"),
                required(m, "orderId"),
                required(m, "customerId"),
                Long.parseLong(required(m, "amount")),
                Instant.parse(required(m, "occurredAt")));
    }

    // 必須項目を取り出す。なければ例外にする
    private static String required(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null) {
            throw new IllegalArgumentException("必須項目 " + key + " がありません");
        }
        return v;
    }
}
