package com.example.async.confirm;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * どのキューにもバインドされていないルーティングキーで発行すると、メッセージがどうなるかを確かめる。
 */
public class UnroutableDemo {
    // 実験用のエクスチェンジ（キューは1つもバインドしない）
    private static final String EXCHANGE = "unroutable-demo";

    /**
     * mandatory なし・ありの2通りで発行する。
     *
     * @param args 使わない
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        try (Connection connection = RabbitConnection.open("unroutable-demo");
                Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
            // mandatory=true で発行したメッセージが行き場をなくしたとき、ブローカーから返送される。それを受け取るリスナー
            channel.addReturnListener(returned -> Console.log("返送されてきた: replyCode=" + returned.getReplyCode()
                    + " replyText=" + returned.getReplyText()
                    + " body=" + new String(returned.getBody(), StandardCharsets.UTF_8)));

            // 1) mandatory=false（既定）。行き場がなければ黙って捨てられる
            channel.basicPublish(EXCHANGE, "no-such-key", false, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    "mandatory=false のメッセージ".getBytes(StandardCharsets.UTF_8));
            Console.log("mandatory=false で発行（例外は出ない）");

            // 2) mandatory=true。行き場がなければ basic.return で送信者に返ってくる
            channel.basicPublish(EXCHANGE, "no-such-key", true, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    "mandatory=true のメッセージ".getBytes(StandardCharsets.UTF_8));
            Console.log("mandatory=true で発行");

            // 返送は非同期に届くので、少し待ってから閉じる
            Thread.sleep(1000);
        }
    }
}
