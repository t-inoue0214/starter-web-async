package com.example.async.topic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * バインディングキー（受け取りたいイベントのパターン）を指定して購読するサブスクライバー。
 */
public class TopicSubscriber extends DefaultConsumer {
    // ログに出すサービス名
    private final String serviceName;

    /**
     * @param channel     使うチャネル
     * @param serviceName サービス名
     */
    public TopicSubscriber(Channel channel, String serviceName) {
        super(channel);
        this.serviceName = serviceName;
    }

    /**
     * 受け取ったイベントのルーティングキーを表示して ack する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        // envelope から「どのルーティングキーで発行されたか」がわかる
        Console.log("[" + serviceName + "] 受信 routingKey=" + envelope.getRoutingKey()
                + " body=" + new String(body, StandardCharsets.UTF_8));
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * サービス名とバインディングキーを指定して購読を始める。
     *
     * @param args [0] サービス名, [1..] バインディングキー（例: order.placed.* や order.#）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("使い方: ./run.sh topic.TopicSubscriber <サービス名> <バインディングキー>...");
            return;
        }
        String service = args[0];
        // 2つめ以降の引数をすべてバインディングキーとして扱う
        String[] bindingKeys = Arrays.copyOfRange(args, 1, args.length);
        Connection connection = RabbitConnection.open("topic-subscriber-" + service);
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(OrderEventTopicPublisher.EXCHANGE, BuiltinExchangeType.TOPIC, true);
        String queue = OrderEventTopicPublisher.EXCHANGE + "." + service;
        channel.queueDeclare(queue, true, false, false, null);
        // 1つのキューに複数のバインディングを張れる。どれか1つにマッチすれば届く
        for (String key : bindingKeys) {
            channel.queueBind(queue, OrderEventTopicPublisher.EXCHANGE, key);
        }
        channel.basicConsume(queue, false, new TopicSubscriber(channel, service));
        Console.log("[" + service + "] " + queue + " を購読中 bindingKeys=" + Arrays.toString(bindingKeys));
    }
}
