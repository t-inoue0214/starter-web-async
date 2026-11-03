package com.example.async.idempotency;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * 同じポイント付与ジョブを2回積んで、重複を人工的に起こすパブリッシャー。
 * 「確認が返ってこなかったので念のため再送した」という状況の再現。
 */
public class DuplicatePublisher {

    /**
     * A-1（1万円）を2回、A-2（5千円）を1回積む。
     *
     * @param args 使わない
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        try (Connection connection = RabbitConnection.open("duplicate-publisher");
                Channel channel = connection.createChannel()) {
            channel.queueDeclare(PointGrantConsumer.QUEUE, true, false, false, null);
            channel.confirmSelect();
            // 顧客 C-1 の注文 A-1（1万円）。ジョブIDは job-A-1
            publish(channel, "job-A-1", "A-1,C-1,10000");
            // 確認待ちがタイムアウトした、という想定で、同じジョブID（同じ messageId）のジョブをもう一度送る
            publish(channel, "job-A-1", "A-1,C-1,10000");
            // 別の注文 A-2（5千円）
            publish(channel, "job-A-2", "A-2,C-1,5000");
            channel.waitForConfirmsOrDie(5000);
            Console.log("A-1 を2回、A-2 を1回積みました。正しいポイントは 100 + 50 = 150 です");
        }
    }

    // messageId を付けて、永続メッセージとして1件積む
    private static void publish(Channel channel, String jobId, String body) throws Exception {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .messageId(jobId).deliveryMode(2).build();
        channel.basicPublish("", PointGrantConsumer.QUEUE, props, body.getBytes(StandardCharsets.UTF_8));
    }
}
