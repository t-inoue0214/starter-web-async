package com.example.async.fanout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 注文確定イベントを購読するサブスクライバー。
 * 起動引数のサービス名ごとに専用のキューを作り、エクスチェンジにバインドする。
 */
public class OrderPlacedSubscriber extends DefaultConsumer {
    // ログに出すサービス名
    private final String serviceName;

    /**
     * @param channel     使うチャネル
     * @param serviceName mail / inventory / point などのサービス名
     */
    public OrderPlacedSubscriber(Channel channel, String serviceName) {
        super(channel);
        this.serviceName = serviceName;
    }

    /**
     * イベントを受け取ったら、そのサービスの仕事をした「つもり」でログを出して ack する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        String event = new String(body, StandardCharsets.UTF_8);
        Console.log("[" + serviceName + "] 受信: " + event);
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * 指定したサービス名で購読を始める。
     *
     * @param args [0] サービス名（mail / inventory / point など。省略時は mail）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        String service = args.length > 0 ? args[0] : "mail";
        Connection connection = RabbitConnection.open("subscriber-" + service);
        Channel channel = connection.createChannel();
        // パブリッシャーと同じ設定でエクスチェンジを宣言する（どちらが先に起動してもよいように）
        channel.exchangeDeclare(OrderPlacedPublisher.EXCHANGE, BuiltinExchangeType.FANOUT, true);
        // サービスごとに専用のキューを作る（例: order-placed.mail）
        String queue = OrderPlacedPublisher.EXCHANGE + "." + service;
        channel.queueDeclare(queue, true, false, false, null);
        // キューをエクスチェンジにバインドする。これが「購読の登録」にあたる
        channel.queueBind(queue, OrderPlacedPublisher.EXCHANGE, "");
        channel.basicConsume(queue, false, new OrderPlacedSubscriber(channel, service));
        Console.log("[" + service + "] " + queue + " を購読中です。Ctrl+C で終了します");
    }
}
