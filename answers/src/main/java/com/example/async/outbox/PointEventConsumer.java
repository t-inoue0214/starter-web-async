package com.example.async.outbox;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.example.async.idempotency.PointStore;
import com.example.async.message.OrderPlacedEvent;
import com.example.async.topic.OrderEventTopicPublisher;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 注文確定イベントを購読して、金額の1%をポイントとして付与する、冪等な受け手。
 * 第17章の冪等な作りを、Pub/Sub のイベント（OrderPlacedEvent）の受け取りに使ったもの。
 */
public class PointEventConsumer extends DefaultConsumer {
    // ポイントを保存する場所
    private final PointStore store;

    /**
     * @param channel 使うチャネル
     * @param store   ポイントの保存先
     */
    public PointEventConsumer(Channel channel, PointStore store) {
        super(channel);
        this.store = store;
    }

    /**
     * イベントを解釈し、処理済みのメッセージIDでなければポイントを付与して、ack する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        OrderPlacedEvent event = OrderPlacedEvent.fromJson(new String(body, StandardCharsets.UTF_8));
        long points = event.amount() / 100;
        boolean applied = store.addPointsOnce(properties.getMessageId(), event.customerId(), points);
        Console.log((applied ? "付与 " : "重複のためスキップ ") + event.orderId() + " +" + points
                + "pt → 残高 " + store.balance(event.customerId()) + "pt");
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * order-events の注文確定イベントを購読する。
     *
     * @param args 使わない
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        Connection connection = RabbitConnection.open("point-event-consumer");
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(OrderEventTopicPublisher.EXCHANGE, BuiltinExchangeType.TOPIC, true);
        String queue = OrderEventTopicPublisher.EXCHANGE + ".point";
        channel.queueDeclare(queue, true, false, false, null);
        // 地域を問わず、注文確定イベントだけを受け取る
        channel.queueBind(queue, OrderEventTopicPublisher.EXCHANGE, "order.placed.*");
        channel.basicQos(10);
        channel.basicConsume(queue, false, new PointEventConsumer(channel, new PointStore()));
        Console.log("注文確定イベントを購読中です。Ctrl+C で終了します");
    }
}
