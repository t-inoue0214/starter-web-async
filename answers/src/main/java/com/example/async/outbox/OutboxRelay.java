package com.example.async.outbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.example.async.message.EventPublisher;
import com.example.async.message.OrderPlacedEvent;
import com.example.async.topic.OrderEventTopicPublisher;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

/**
 * outbox テーブルの未送信イベントを読み出し、RabbitMQ へ送るリレー（中継役）。
 * 送れなければ何度でもやり直す。送れたら published_at を記録する。
 */
public class OutboxRelay {
    // 1回の読み出しで扱う最大件数
    private static final int BATCH_SIZE = 10;

    /**
     * Ctrl+C で止めるまで、1秒ごとに未送信イベントを送る。
     *
     * @param args 使わない
     * @throws Exception 起動時の接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        try (Connection db = ShopDatabase.open();
                com.rabbitmq.client.Connection mq = RabbitConnection.open("outbox-relay")) {
            Channel channel = mq.createChannel();
            channel.exchangeDeclare(OrderEventTopicPublisher.EXCHANGE, BuiltinExchangeType.TOPIC, true);
            channel.confirmSelect();
            EventPublisher publisher = new EventPublisher(channel, OrderEventTopicPublisher.EXCHANGE);
            Console.log("リレーを開始しました。Ctrl+C で終了します");
            while (true) {
                try {
                    relayOnce(db, channel, publisher);
                } catch (Exception e) {
                    // RabbitMQ が止まっていても落ちずに、次の周回でやり直す
                    Console.log("送信失敗。1秒後にやり直します（" + e.getClass().getSimpleName() + "）");
                }
                Thread.sleep(1000);
            }
        }
    }

    // 未送信のイベントを古い順に読み、1件ずつ送って送信済みにする
    private static void relayOnce(Connection db, Channel channel, EventPublisher publisher) throws Exception {
        List<OutboxRow> rows = selectUnpublished(db);
        for (OutboxRow row : rows) {
            // 保存しておいた JSON からイベントを復元して送る（messageId には event_id が入る）
            publisher.publish(OrderPlacedEvent.fromJson(row.payload));
            // ブローカーが受け取ったことを確認する
            channel.waitForConfirmsOrDie(5000);
            // 確認できてから送信済みにする。ここで落ちると次の周回でもう一度送る（＝重複はありうる）
            markPublished(db, row.id);
            Console.log("送信済み outbox.id=" + row.id + " eventId=" + row.eventId);
        }
    }

    // 未送信の行を古い順に BATCH_SIZE 件まで読む
    private static List<OutboxRow> selectUnpublished(Connection db) throws SQLException {
        List<OutboxRow> rows = new ArrayList<>();
        String sql = "SELECT id, event_id, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT ?";
        try (PreparedStatement ps = db.prepareStatement(sql)) {
            ps.setInt(1, BATCH_SIZE);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new OutboxRow(rs.getLong("id"), rs.getString("event_id"), rs.getString("payload")));
                }
            }
        }
        return rows;
    }

    // 送信済みの時刻を記録する
    private static void markPublished(Connection db, long id) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE outbox SET published_at = CURRENT_TIMESTAMP WHERE id = ?")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    // outbox の1行ぶんのデータ
    private static final class OutboxRow {
        private final long id;
        private final String eventId;
        private final String payload;

        private OutboxRow(long id, String eventId, String payload) {
            this.id = id;
            this.eventId = eventId;
            this.payload = payload;
        }
    }
}
