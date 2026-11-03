package com.example.async.fanout;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * 「注文が確定した」というイベントを order-placed エクスチェンジへ発行するパブリッシャー。
 * 誰が受け取るかは知らない。エクスチェンジに投げるだけ。
 */
public class OrderPlacedPublisher {
    // イベントを発行する先のエクスチェンジ名
    static final String EXCHANGE = "order-placed";

    /**
     * 引数で指定した件数（省略時は3件）の注文確定イベントを発行する。
     *
     * @param args 発行する件数
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        try (Connection connection = RabbitConnection.open("order-placed-publisher");
                Channel channel = connection.createChannel()) {
            // fanout 型のエクスチェンジを宣言する。fanout は「バインドされた全キューにコピーを配る」
            // 第3引数 durable=true でブローカー再起動後も残す
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT, true);
            for (int i = 1; i <= count; i++) {
                String event = "{\"orderId\":\"A-" + i + "\",\"amount\":" + (1000 * i) + "}";
                // fanout ではルーティングキーは使われないので空文字でよい
                channel.basicPublish(EXCHANGE, "", MessageProperties.PERSISTENT_TEXT_PLAIN,
                        event.getBytes(StandardCharsets.UTF_8));
                Console.log("発行: " + event);
            }
        }
    }
}
