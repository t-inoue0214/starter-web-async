package com.example.async.message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.example.async.topic.OrderEventTopicPublisher;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 受信したメッセージのプロパティ・ヘッダー・本文を表示し、イベントとして解釈するサブスクライバー。
 */
public class EventInspector extends DefaultConsumer {

    /**
     * @param channel 使うチャネル
     */
    public EventInspector(Channel channel) {
        super(channel);
    }

    /**
     * メタデータを表示し、type を見てから本文を解釈する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        // 標準プロパティを表示する
        Console.log("messageId=" + properties.getMessageId() + " type=" + properties.getType()
                + " contentType=" + properties.getContentType() + " appId=" + properties.getAppId()
                + " timestamp=" + properties.getTimestamp());
        // 独自ヘッダーを表示する
        Console.log("headers=" + properties.getHeaders());
        String json = new String(body, StandardCharsets.UTF_8);
        // type を見て、知っている種類のイベントだけ解釈する
        if (OrderPlacedEvent.TYPE.equals(properties.getType())) {
            OrderPlacedEvent event = OrderPlacedEvent.fromJson(json);
            Console.log("注文確定イベント orderId=" + event.orderId() + " amount=" + event.amount()
                    + " occurredAt=" + event.occurredAt());
        } else {
            // 知らない種類は無視する（エラーにしない）
            Console.log("未知の type のためスキップ: " + json);
        }
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * order-events の全イベントを購読する。
     *
     * @param args 使わない
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        Connection connection = RabbitConnection.open("event-inspector");
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(OrderEventTopicPublisher.EXCHANGE, BuiltinExchangeType.TOPIC, true);
        String queue = OrderEventTopicPublisher.EXCHANGE + ".inspector";
        channel.queueDeclare(queue, true, false, false, null);
        // すべてのイベントを受け取る
        channel.queueBind(queue, OrderEventTopicPublisher.EXCHANGE, "#");
        channel.basicConsume(queue, false, new EventInspector(channel));
        Console.log(queue + " を購読中です。Ctrl+C で終了します");
    }
}
