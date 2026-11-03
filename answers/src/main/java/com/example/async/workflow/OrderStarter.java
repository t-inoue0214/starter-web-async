package com.example.async.workflow;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * 注文確定のイベント order.placed を発行して、ワークフローを開始する。
 */
public class OrderStarter {

    /**
     * 指定した件数（省略時は3件）の注文を開始する。
     *
     * @param args [0] 注文の件数
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        try (Connection connection = RabbitConnection.open("order-starter");
                Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(StepConsumer.EXCHANGE, BuiltinExchangeType.TOPIC, false);
            for (int i = 1; i <= count; i++) {
                String orderId = "A-" + i;
                channel.basicPublish(StepConsumer.EXCHANGE, "order.placed", null, orderId.getBytes(StandardCharsets.UTF_8));
                Console.log("注文確定 " + orderId + " → order.placed を発行");
                Console.sleep(500);
            }
        }
    }
}
