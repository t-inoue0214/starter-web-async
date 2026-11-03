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
 * 起動している間だけ注文確定イベントを眺める、一時的なサブスクライバー（管理者向けのライブ画面の想定）。
 * ブローカーが名前を付ける使い捨てのキューを使う。
 */
public class LiveMonitor extends DefaultConsumer {

    /**
     * @param channel 使うチャネル
     */
    public LiveMonitor(Channel channel) {
        super(channel);
    }

    /**
     * 受け取ったイベントを表示する（autoAck なので ack は不要）。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        Console.log("[monitor] " + new String(body, StandardCharsets.UTF_8));
    }

    /**
     * 使い捨てキューを作って購読を始める。
     *
     * @param args 使わない
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        Connection connection = RabbitConnection.open("live-monitor");
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(OrderPlacedPublisher.EXCHANGE, BuiltinExchangeType.FANOUT, true);
        // 引数なしの queueDeclare は「名前はブローカー任せ・この接続専用・接続が切れたら削除」のキューを作る
        String queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, OrderPlacedPublisher.EXCHANGE, "");
        // 見るだけなので autoAck=true でよい
        channel.basicConsume(queue, true, new LiveMonitor(channel));
        Console.log("[monitor] 一時キュー " + queue + " で購読中です。Ctrl+C で終了します");
    }
}
