package com.example.async.dlq;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * メール送信依頼を3件送る。うち1件は宛先が不正で、何度やっても失敗する（毒メッセージ）。
 */
public class MailRequestPublisher {

    /**
     * 3件のメール送信依頼を送る。
     *
     * @param args 使わない
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        String[] addresses = {"user1@example.com", "invalid-address", "user2@example.com"};
        try (Connection connection = RabbitConnection.open("mail-request-publisher");
                Channel channel = connection.createChannel()) {
            MailQueues.declare(channel);
            for (String address : addresses) {
                channel.basicPublish("", MailQueues.QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        address.getBytes(StandardCharsets.UTF_8));
                Console.log("送信依頼: " + address);
            }
        }
    }
}
