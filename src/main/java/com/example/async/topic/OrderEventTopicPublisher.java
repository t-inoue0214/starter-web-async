package com.example.async.topic;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * 注文に関するさまざまなイベントを、種類ごとのルーティングキーを付けて order-events エクスチェンジへ発行する。
 */
public class OrderEventTopicPublisher {
    /** 注文イベント用の topic エクスチェンジ名（後の章でも使う） */
    public static final String EXCHANGE = "order-events";

    /**
     * 注文の確定・キャンセル・発送のイベントを、地域（jp / us）を変えながら発行する。
     *
     * @param args 使わない
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        // 発行するイベントのルーティングキー。「<集約>.<出来事>.<地域>」という命名規則にしている
        String[] routingKeys = {
            "order.placed.jp", "order.placed.us", "order.cancelled.jp", "order.shipped.jp", "order.shipped.us"
        };
        try (Connection connection = RabbitConnection.open("order-event-topic-publisher");
                Channel channel = connection.createChannel()) {
            // topic 型のエクスチェンジを宣言する
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, true);
            for (int i = 0; i < routingKeys.length; i++) {
                String event = "{\"orderId\":\"A-" + (i + 1) + "\",\"type\":\"" + routingKeys[i] + "\"}";
                // ルーティングキーを付けて発行する。どのキューに届くかはバインディング次第
                channel.basicPublish(EXCHANGE, routingKeys[i], MessageProperties.PERSISTENT_TEXT_PLAIN,
                        event.getBytes(StandardCharsets.UTF_8));
                Console.log("発行: routingKey=" + routingKeys[i]);
            }
        }
    }
}
