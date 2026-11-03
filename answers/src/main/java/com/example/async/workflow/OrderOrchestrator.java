package com.example.async.workflow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 注文ワークフローの進行を一手に管理するオーケストレーター。
 * 各注文の「今どこまで進んだか」を覚えておき、結果に応じて次の命令を出す。失敗したら補償（返金）も命じる。
 */
public class OrderOrchestrator extends DefaultConsumer {

    /** 注文の進行状況 */
    enum State {
        /** 開始した（決済を命じた） */
        STARTED,
        /** 決済が済んだ（在庫を命じた） */
        PAID,
        /** 在庫を引き当てた（発送を命じた） */
        STOCKED,
        /** 在庫切れで返金中 */
        REFUNDING,
        /** すべて完了 */
        COMPLETED,
        /** 取り消しが完了 */
        CANCELLED
    }

    // 注文ID → 進行状況。どの注文がどこまで進んだかは、ここを見ればわかる
    private final Map<String, State> states = new ConcurrentHashMap<>();

    /**
     * @param channel 使うチャネル
     */
    public OrderOrchestrator(Channel channel) {
        super(channel);
    }

    /**
     * 届いた知らせ（ルーティングキー）と現在の進行状況から、次の命令を決める。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        String orderId = new String(body, StandardCharsets.UTF_8);
        switch (envelope.getRoutingKey()) {
            case "order.placed" -> advance(orderId, State.STARTED, "cmd.payment");
            case "payment.completed" -> advance(orderId, State.PAID, "cmd.inventory");
            case "inventory.reserved" -> advance(orderId, State.STOCKED, "cmd.shipping");
            // 補償: 在庫切れなら、すでに済んだ決済を取り消す
            case "inventory.failed" -> advance(orderId, State.REFUNDING, "cmd.refund");
            case "shipping.arranged" -> advance(orderId, State.COMPLETED, null);
            case "payment.refunded" -> advance(orderId, State.CANCELLED, null);
            default -> Console.log("未知の知らせ: " + envelope.getRoutingKey());
        }
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    // 進行状況を更新し、次の命令があれば発行する
    private void advance(String orderId, State next, String command) throws IOException {
        State before = states.put(orderId, next);
        Console.log("[orchestrator] " + orderId + ": " + (before == null ? "(新規)" : before) + " → " + next);
        if (command != null) {
            getChannel().basicPublish(StepConsumer.EXCHANGE, command, null, orderId.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * オーケストレーターを起動する。
     *
     * @param args 使わない
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        Connection connection = RabbitConnection.open("order-orchestrator");
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(StepConsumer.EXCHANGE, BuiltinExchangeType.TOPIC, false);
        channel.queueDeclare("wf.orchestrator", false, true, true, null);
        // 開始の合図と、各手順の結果をすべて聞く
        for (String key : new String[] {"order.placed", "payment.completed", "inventory.reserved",
            "inventory.failed", "shipping.arranged", "payment.refunded"}) {
            channel.queueBind("wf.orchestrator", StepConsumer.EXCHANGE, key);
        }
        channel.basicConsume("wf.orchestrator", false, new OrderOrchestrator(channel));
        Console.log("オーケストレーター起動。Ctrl+C で終了します");
    }
}
