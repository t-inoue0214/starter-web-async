package com.example.async.p2p;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * hello キューからメッセージを受け取るコンシューマー（受信側）。
 * DefaultConsumer を継承し、メッセージが届いたときに呼ばれる handleDelivery を上書きする。
 */
public class HelloConsumer extends DefaultConsumer {

    /**
     * @param channel このコンシューマーが使うチャネル
     */
    public HelloConsumer(Channel channel) {
        super(channel);
    }

    /**
     * ブローカーからメッセージが「押し出されて」きたときに、ライブラリのスレッドから呼ばれる。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        // バイト列を送信側と同じ文字コードで文字列に戻す
        String text = new String(body, StandardCharsets.UTF_8);
        // deliveryTag はこのチャネル内での配達番号（1, 2, 3, ...）
        Console.log("受信: " + text + " (deliveryTag=" + envelope.getDeliveryTag() + ")");
    }

    /**
     * hello キューの購読を開始し、Ctrl+C で止めるまで待ち続ける。
     *
     * @param args 使わない
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        // 受信側は待ち続けるので try-with-resources は使わず、開きっぱなしにする
        Connection connection = RabbitConnection.open("hello-consumer");
        Channel channel = connection.createChannel();
        // 送信側より先に起動してもよいように、受信側でもキューを宣言しておく
        channel.queueDeclare(HelloProducer.QUEUE, true, false, false, null);
        // 購読を開始する。第2引数 autoAck=true は「届けた時点で処理済みとみなす」という指定（第11章で掘り下げる）
        channel.basicConsume(HelloProducer.QUEUE, true, new HelloConsumer(channel));
        Console.log("hello キューを購読中です。Ctrl+C で終了します");
    }
}
