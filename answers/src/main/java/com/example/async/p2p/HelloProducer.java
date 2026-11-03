package com.example.async.p2p;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * hello キューにメッセージを送るプロデューサー（送信側）。
 */
public class HelloProducer {
    // 送り先のキュー名
    static final String QUEUE = "hello";

    /**
     * 引数で指定した件数（省略時は3件）のメッセージを hello キューへ送る。
     *
     * @param args 送る件数
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        // 送る件数を決める
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        try (Connection connection = RabbitConnection.open("hello-producer");
                Channel channel = connection.createChannel()) {
            // キューを宣言する。すでに同じ設定で存在すれば何もしない（冪等な操作）
            // 引数: キュー名, durable(再起動後も残す), exclusive(この接続専用), autoDelete(使われなくなったら消す), その他の設定
            channel.queueDeclare(QUEUE, true, false, false, null);
            for (int i = 1; i <= count; i++) {
                // 本文はバイト列で送る。文字列なら文字コードを明示して変換する
                String text = "こんにちは #" + i;
                // 既定エクスチェンジ("")に、ルーティングキー=キュー名で送ると、そのキューに直接届く
                channel.basicPublish("", QUEUE, null, text.getBytes(StandardCharsets.UTF_8));
                Console.log("送信: " + text);
            }
        }
    }
}
