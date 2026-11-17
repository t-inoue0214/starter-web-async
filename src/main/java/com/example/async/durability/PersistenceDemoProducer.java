package com.example.async.durability;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * 同じ durable キューに、非永続メッセージと永続メッセージを3件ずつ送る。
 * ブローカー再起動後にどちらが残るかを確かめるための送信側。
 */
public class PersistenceDemoProducer {
    /** 実験用のキュー名 */
    static final String QUEUE = "durability-demo";

    /**
     * 非永続3件・永続3件を送る。
     *
     * @param args 使わない
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        try (Connection connection = RabbitConnection.open("persistence-demo-producer");
                Channel channel = connection.createChannel()) {
            // キュー自体は durable（再起動後も定義が残る）にする
            channel.queueDeclare(QUEUE, true, false, false, null);
            for (int i = 1; i <= 3; i++) {
                // TEXT_PLAIN は deliveryMode=1（非永続：メモリにだけ置けばよい）
                channel.basicPublish("", QUEUE, MessageProperties.TEXT_PLAIN,
                        ("transient-" + i).getBytes(StandardCharsets.UTF_8));
                // PERSISTENT_TEXT_PLAIN は deliveryMode=2（永続：ディスクにも書く）
                channel.basicPublish("", QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        ("persistent-" + i).getBytes(StandardCharsets.UTF_8));
            }
            Console.log("非永続3件・永続3件を送りました");
        }
    }
}
