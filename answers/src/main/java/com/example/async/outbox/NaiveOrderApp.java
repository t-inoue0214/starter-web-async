package com.example.async.outbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.example.async.message.EventPublisher;
import com.example.async.message.OrderPlacedEvent;
import com.example.async.topic.OrderEventTopicPublisher;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

/**
 * 「DB に注文を保存してから、RabbitMQ にイベントを送る」を素朴に書いた注文アプリ。
 * 2つの別々のシステムへの書き込み（二重書き込み）が、片方だけ成功する問題を体験する。
 */
public class NaiveOrderApp {

    /**
     * 2秒ごとに注文を1件確定させる。途中で RabbitMQ を止めてみる。
     *
     * @param args [0] 注文件数（省略時は15件）
     * @throws Exception 起動時の接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 15;
        // イベントを送れなかった件数
        int lost = 0;
        com.rabbitmq.client.Connection mq = RabbitConnection.open("naive-order-app");
        try (Connection db = ShopDatabase.open()) {
            Channel channel = mq.createChannel();
            channel.exchangeDeclare(OrderEventTopicPublisher.EXCHANGE, BuiltinExchangeType.TOPIC, true);
            channel.confirmSelect();
            EventPublisher publisher = new EventPublisher(channel, OrderEventTopicPublisher.EXCHANGE);
            // 起動時刻を注文IDに入れて、再実行しても ID がぶつからないようにする
            String prefix = "N" + (System.currentTimeMillis() / 1000 % 100000) + "-";
            for (int i = 1; i <= count; i++) {
                String orderId = prefix + i;
                // (1) DB に注文を保存する（自動コミットなので、ここで確定する）
                insertOrder(db, orderId, "C-1", 1000);
                try {
                    // (2) イベントを送り、ブローカーの確認を待つ
                    publisher.publish(OrderPlacedEvent.create(orderId, "C-1", 1000));
                    channel.waitForConfirmsOrDie(5000);
                    Console.log("注文 " + orderId + " : DB保存 OK / イベント送信 OK");
                } catch (Exception e) {
                    // (1) は確定済みなので、注文はあるのにイベントは誰にも届かない
                    lost++;
                    Console.log("注文 " + orderId + " : DB保存 OK / イベント送信 NG（" + e.getClass().getSimpleName() + "）");
                }
                Thread.sleep(2000);
            }
        } finally {
            // close() は切断中だと例外を投げるので、エラーを無視して閉じる abort() を使う
            mq.abort();
        }
        Console.log("終了: 注文 " + count + " 件のうち、イベントが失われたのは " + lost + " 件");
    }

    // 注文を1件 INSERT する
    private static void insertOrder(Connection db, String orderId, String customerId, long amount) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("INSERT INTO orders (order_id, customer_id, amount) VALUES (?, ?, ?)")) {
            ps.setString(1, orderId);
            ps.setString(2, customerId);
            ps.setLong(3, amount);
            ps.executeUpdate();
        }
    }
}
