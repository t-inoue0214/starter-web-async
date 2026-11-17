package com.example.async.dlq;

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
 * メール送信依頼を処理するコンシューマー。
 * naive モードは失敗したらキューへ戻し、dlq モードは失敗したメッセージを DLQ へ逃がす。
 */
public class MailConsumer extends DefaultConsumer {
    // 失敗したメッセージを DLQ へ逃がすかどうか
    private final boolean useDlq;

    /**
     * @param channel 使うチャネル
     * @param useDlq  true なら失敗したメッセージを DLQ へ送る
     */
    public MailConsumer(Channel channel, boolean useDlq) {
        super(channel);
        this.useDlq = useDlq;
    }

    /**
     * メールを送り、成功なら ack、失敗なら戻すか DLQ へ送る。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        String address = new String(body, StandardCharsets.UTF_8);
        try {
            sendMail(address);
            Console.log("送信成功: " + address);
            getChannel().basicAck(envelope.getDeliveryTag(), false);
        } catch (IllegalArgumentException e) {
            Console.log("送信失敗: " + address + " / " + e.getMessage());
            if (useDlq) {
                // requeue=false で拒否すると、キューに戻らず DLX（そして DLQ）へ送られる
                getChannel().basicReject(envelope.getDeliveryTag(), false);
            } else {
                // 少し待ってから、requeue=true でキューへ戻す。すぐまた自分に届く
                Console.sleep(500);
                getChannel().basicNack(envelope.getDeliveryTag(), false, true);
            }
        }
    }

    // メール送信の代わり。宛先に @ がなければ、何度やっても失敗する
    private void sendMail(String address) {
        Console.sleep(200);
        if (!address.contains("@")) {
            throw new IllegalArgumentException("宛先の形式が不正です");
        }
    }

    /**
     * 指定したモードで購読を始める。
     *
     * @param args [0] naive / dlq（省略時は naive）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        boolean useDlq = args.length > 0 && args[0].equals("dlq");
        Connection connection = RabbitConnection.open("mail-consumer");
        Channel channel = connection.createChannel();
        MailQueues.declare(channel);
        channel.basicQos(1);
        channel.basicConsume(MailQueues.QUEUE, false, new MailConsumer(channel, useDlq));
        Console.log((useDlq ? "dlq" : "naive") + " モードで購読中です。Ctrl+C で終了します");
    }
}
