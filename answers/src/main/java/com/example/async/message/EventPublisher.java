package com.example.async.message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

/**
 * OrderPlacedEvent を、メタデータ（プロパティとヘッダー）付きで発行する部品。
 * 後の章でもこのクラスを使って発行する。
 */
public class EventPublisher {
    // 発行に使うチャネル
    private final Channel channel;
    // 発行先のエクスチェンジ
    private final String exchange;

    /**
     * @param channel  発行に使うチャネル
     * @param exchange 発行先のエクスチェンジ名
     */
    public EventPublisher(Channel channel, String exchange) {
        this.channel = channel;
        this.exchange = exchange;
    }

    /**
     * イベントをルーティングキー order.placed.jp で発行する。
     *
     * @param event 発行するイベント
     * @throws IOException 送信に失敗した場合
     */
    public void publish(OrderPlacedEvent event) throws IOException {
        // メッセージのプロパティ（AMQP が定める標準のメタデータ）を組み立てる
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                // 受信側が重複を見分けるための一意なID
                .messageId(event.eventId())
                // 何のイベントか
                .type(OrderPlacedEvent.TYPE)
                // 本文の形式と文字コード
                .contentType("application/json")
                .contentEncoding("UTF-8")
                // 発行した時刻（AMQP の timestamp は秒単位）
                .timestamp(new Date())
                // どのアプリが発行したか
                .appId("order-service")
                // 2 = persistent（ディスクに保存してほしい）
                .deliveryMode(2)
                // 標準プロパティにない情報は headers に入れる
                .headers(Map.of("schema-version", OrderPlacedEvent.SCHEMA_VERSION))
                .build();
        // 本文は JSON を UTF-8 のバイト列にしたもの
        channel.basicPublish(exchange, "order.placed.jp", props, event.toJson().getBytes(StandardCharsets.UTF_8));
    }
}
