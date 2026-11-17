package com.example.async.durability;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

/**
 * キューに残っているメッセージをすべて取り出して表示する（basicGet によるプル型の受信）。
 */
public class QueueDrainer {

    /**
     * 指定したキュー（省略時は durability-demo）を空になるまで読む。
     *
     * @param args [0] キュー名
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        String queue = args.length > 0 ? args[0] : PersistenceDemoProducer.QUEUE;
        try (Connection connection = RabbitConnection.open("queue-drainer");
                Channel channel = connection.createChannel()) {
            int count = 0;
            while (true) {
                // basicGet は「1件ください」と取りに行く。autoAck=true なので取り出したら消える
                GetResponse response = channel.basicGet(queue, true);
                // null ならキューが空
                if (response == null) {
                    break;
                }
                count++;
                Console.log("残っていた: " + new String(response.getBody(), StandardCharsets.UTF_8));
            }
            Console.log(queue + " から " + count + " 件取り出しました");
        }
    }
}
