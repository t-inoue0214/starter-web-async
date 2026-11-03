package com.example.async.workqueue;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * 「領収書PDFを作る」というタスクを receipt-tasks キューに投入するプロデューサー。
 * 奇数番のタスクは重く（3秒）、偶数番は軽い（0.3秒）という想定。
 */
public class ReceiptTaskProducer {
    // タスクを入れるキュー名
    static final String QUEUE = "receipt-tasks";

    /**
     * 引数で指定した件数（省略時は8件）のタスクを投入する。
     *
     * @param args 投入する件数
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 8;
        try (Connection connection = RabbitConnection.open("receipt-task-producer");
                Channel channel = connection.createChannel()) {
            // 再起動しても消えない(durable)キューを宣言する
            channel.queueDeclare(QUEUE, true, false, false, null);
            for (int i = 1; i <= count; i++) {
                // 奇数番は重いタスク、偶数番は軽いタスク
                String weight = (i % 2 == 1) ? "heavy" : "light";
                String task = "receipt-" + i + ":" + weight;
                // PERSISTENT_TEXT_PLAIN は「ディスクに保存してほしい・本文はテキスト」という属性（第11章で扱う）
                channel.basicPublish("", QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        task.getBytes(StandardCharsets.UTF_8));
                Console.log("投入: " + task);
            }
        }
    }
}
