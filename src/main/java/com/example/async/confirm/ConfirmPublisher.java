package com.example.async.confirm;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * パブリッシャー確認（publisher confirms）を使い、ブローカーが受け取ったことを確かめながら送る。
 * 1件ごとに確認する方式と、まとめて確認する方式の速さも比べる。
 */
public class ConfirmPublisher {
    // 実験用のキュー名
    private static final String QUEUE = "confirm-demo";
    // 確認を待つ最大時間
    private static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(5);

    /**
     * 指定件数（省略時は1000件）を2通りの方式で送り、所要時間を表示する。
     *
     * @param args [0] 送る件数
     * @throws Exception 接続や送信、確認に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 1000;
        try (Connection connection = RabbitConnection.open("confirm-publisher");
                Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE, true, false, false, null);
            // このチャネルを「確認モード」にする。以後の発行にはブローカーから ack / nack が返ってくる
            channel.confirmSelect();

            // 方式1: 1件送るごとに確認を待つ（確実だが遅い）
            long start = System.currentTimeMillis();
            for (int i = 1; i <= count; i++) {
                channel.basicPublish("", QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        ("one-by-one-" + i).getBytes(StandardCharsets.UTF_8));
                // ブローカーが ack を返すまで待つ。nack やタイムアウトなら例外になる
                channel.waitForConfirmsOrDie(CONFIRM_TIMEOUT.toMillis());
            }
            Console.log("1件ずつ確認: " + count + "件 " + (System.currentTimeMillis() - start) + "ms");

            // 方式2: 100件送るごとにまとめて確認を待つ（速いが、失敗時にどれが失敗したか分かりにくい）
            start = System.currentTimeMillis();
            for (int i = 1; i <= count; i++) {
                channel.basicPublish("", QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        ("batch-" + i).getBytes(StandardCharsets.UTF_8));
                if (i % 100 == 0) {
                    channel.waitForConfirmsOrDie(CONFIRM_TIMEOUT.toMillis());
                }
            }
            // 端数分の確認を待つ
            channel.waitForConfirmsOrDie(CONFIRM_TIMEOUT.toMillis());
            Console.log("100件ごとに確認: " + count + "件 " + (System.currentTimeMillis() - start) + "ms");
            // 実験用キューを空にしておく
            channel.queuePurge(QUEUE);
        }
    }
}
