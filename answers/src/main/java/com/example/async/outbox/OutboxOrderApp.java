package com.example.async.outbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.example.async.common.Console;
import com.example.async.message.OrderPlacedEvent;

/**
 * Outbox パターンの注文アプリ。RabbitMQ には一切接続しない。
 * 注文と「送るべきイベント」を、同じ DB の同じトランザクションで保存する。
 */
public class OutboxOrderApp {

    /**
     * 2秒ごとに注文を1件確定させる。
     *
     * @param args [0] 注文件数（省略時は15件）
     * @throws Exception DB 操作に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 15;
        try (Connection db = ShopDatabase.open()) {
            String prefix = "X" + (System.currentTimeMillis() / 1000 % 100000) + "-";
            for (int i = 1; i <= count; i++) {
                String orderId = prefix + i;
                placeOrder(db, OrderPlacedEvent.create(orderId, "C-1", 1000));
                Console.log("注文 " + orderId + " : 注文と outbox を1トランザクションで保存 OK");
                Thread.sleep(2000);
            }
        }
    }

    // 注文の INSERT と outbox の INSERT を1つのトランザクションで行う
    private static void placeOrder(Connection db, OrderPlacedEvent event) throws SQLException {
        // トランザクションを開始する
        db.setAutoCommit(false);
        try {
            try (PreparedStatement ps = db.prepareStatement(
                    "INSERT INTO orders (order_id, customer_id, amount) VALUES (?, ?, ?)")) {
                ps.setString(1, event.orderId());
                ps.setString(2, event.customerId());
                ps.setLong(3, event.amount());
                ps.executeUpdate();
            }
            // 送るべきイベントを JSON のまま保存する。送信はリレーに任せる
            try (PreparedStatement ps = db.prepareStatement("INSERT INTO outbox (event_id, payload) VALUES (?, ?)")) {
                ps.setString(1, event.eventId());
                ps.setString(2, event.toJson());
                ps.executeUpdate();
            }
            // 両方まとめて確定する。注文だけ・イベントだけ、という中途半端な状態にはならない
            db.commit();
        } catch (SQLException e) {
            db.rollback();
            throw e;
        }
    }
}
