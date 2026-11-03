package com.example.async.dlq;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

/**
 * DLQ にたまったメッセージを「消さずに」のぞき見る運用ツール。
 */
public class DlqInspector {

    /**
     * DLQ のメッセージを本文とヘッダーつきで表示する。
     *
     * @param args 使わない
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        try (Connection connection = RabbitConnection.open("dlq-inspector");
                Channel channel = connection.createChannel()) {
            MailQueues.declare(channel);
            int count = 0;
            while (true) {
                // autoAck=false で取り出す。ack しないまま接続を閉じれば DLQ に戻る
                GetResponse response = channel.basicGet(MailQueues.DLQ, false);
                if (response == null) {
                    break;
                }
                count++;
                Console.log("DLQ: " + new String(response.getBody(), StandardCharsets.UTF_8));
                // x-death ヘッダーには「いつ・どのキューで・なぜ」デッドレターになったかが記録されている
                Console.log("  headers=" + response.getProps().getHeaders());
            }
            Console.log("DLQ には " + count + " 件あります（ack していないので、閉じると元に戻ります）");
        }
    }
}
