package com.example.async.ack;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * 「ポイントを付与せよ」というメッセージを point-tasks キューへ5件送る。
 */
public class PointTaskProducer {
    /** 実験用のキュー名 */
    static final String QUEUE = "point-tasks";

    /**
     * 5件送る。
     *
     * @param args 使わない
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        try (Connection connection = RabbitConnection.open("point-task-producer");
                Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE, true, false, false, null);
            for (int i = 1; i <= 5; i++) {
                String task = "order-" + i;
                channel.basicPublish("", QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        task.getBytes(StandardCharsets.UTF_8));
                Console.log("送信: " + task);
            }
        }
    }
}
