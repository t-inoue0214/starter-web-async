package com.example.async.hello;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * RabbitMQ に AMQP で接続できるか確認するクラス。
 * 接続とチャネルを開いたまま30秒待つので、その間に管理画面で様子を見られる。
 */
public class ConnectionCheck {

    /**
     * 接続・チャネルを開き、ブローカーの情報を表示して30秒後に閉じる。
     *
     * @param args 使わない
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        // try-with-resources で、最後に必ず接続とチャネルを閉じる
        try (Connection connection = RabbitConnection.open("connection-check");
                Channel channel = connection.createChannel()) {
            // ブローカーが接続時に教えてくれた製品名とバージョンを表示する
            Console.log("接続OK product=" + connection.getServerProperties().get("product")
                    + " version=" + connection.getServerProperties().get("version"));
            // チャネル番号を表示する（1本の接続の中に複数のチャネルを作れる）
            Console.log("チャネル番号=" + channel.getChannelNumber());
            // 管理画面の Connections / Channels タブを確認する時間をとる
            Console.log("30秒待ちます。管理画面の Connections タブを見てみましょう");
            Thread.sleep(30_000);
        }
        Console.log("接続を閉じました");
    }
}
