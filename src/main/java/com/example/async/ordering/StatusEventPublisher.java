package com.example.async.ordering;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * 3件の注文について、ステータス変更イベント（作成→支払済→発送済→配達済）を正しい順番で発行する。
 */
public class StatusEventPublisher {
    // 注文がたどるステータス。配列の添字+1 がバージョン番号になる
    private static final String[] STATUSES = {"CREATED", "PAID", "SHIPPED", "DELIVERED"};

    /**
     * イベントを発行する。
     *
     * @param args 使わない
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        String[] orderIds = {"A-1", "A-2", "A-3"};
        try (Connection connection = RabbitConnection.open("status-event-publisher");
                Channel channel = connection.createChannel()) {
            StatusQueues.declare(channel);
            // 注文ごとに、バージョン1（CREATED）から4（DELIVERED）まで続けて発行する
            for (String orderId : orderIds) {
                for (int v = 1; v <= STATUSES.length; v++) {
                    // 本文は「注文ID,バージョン,ステータス」
                    String body = orderId + "," + v + "," + STATUSES[v - 1];
                    channel.basicPublish("", StatusQueues.QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN,
                            body.getBytes(StandardCharsets.UTF_8));
                    Console.log("発行: " + body);
                }
            }
        }
    }
}
