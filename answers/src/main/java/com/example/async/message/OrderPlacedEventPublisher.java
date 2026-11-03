package com.example.async.message;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.example.async.topic.OrderEventTopicPublisher;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * OrderPlacedEvent を order-events エクスチェンジへ発行するアプリ。
 */
public class OrderPlacedEventPublisher {

    /**
     * 引数で指定した件数（省略時は2件）のイベントを発行する。
     *
     * @param args 発行する件数
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 2;
        try (Connection connection = RabbitConnection.open("order-placed-event-publisher");
                Channel channel = connection.createChannel()) {
            // 第18章で作った topic エクスチェンジを使う
            channel.exchangeDeclare(OrderEventTopicPublisher.EXCHANGE, BuiltinExchangeType.TOPIC, true);
            EventPublisher publisher = new EventPublisher(channel, OrderEventTopicPublisher.EXCHANGE);
            for (int i = 1; i <= count; i++) {
                // 注文ID・顧客ID・金額からイベントを作る
                OrderPlacedEvent event = OrderPlacedEvent.create("A-" + i, "C-1", 1000L * i);
                publisher.publish(event);
                Console.log("発行: " + event.toJson());
            }
        }
    }
}
