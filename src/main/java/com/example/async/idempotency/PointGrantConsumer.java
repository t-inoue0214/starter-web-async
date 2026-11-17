package com.example.async.idempotency;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * point-jobs キューからポイント付与のジョブを受け取り、金額の1%をポイントとして付与するワーカー。
 * naive モードは重複をそのまま処理し、idempotent モードは処理済みのメッセージIDで重複を弾く。
 */
public class PointGrantConsumer extends DefaultConsumer {
    /** ポイント付与のジョブを入れるキュー名 */
    public static final String QUEUE = "point-jobs";
    // ポイントを保存する場所
    private final PointStore store;
    // 冪等モードかどうか
    private final boolean idempotent;

    /**
     * @param channel    使うチャネル
     * @param store      ポイントの保存先
     * @param idempotent true なら重複を弾く
     */
    public PointGrantConsumer(Channel channel, PointStore store, boolean idempotent) {
        super(channel);
        this.store = store;
        this.idempotent = idempotent;
    }

    /**
     * ジョブを解釈してポイントを付与し、保存が終わってから ack する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        // 本文は「注文ID,顧客ID,金額」
        String[] parts = new String(body, StandardCharsets.UTF_8).split(",");
        String orderId = parts[0];
        String customerId = parts[1];
        // 金額の1%をポイントにする
        long points = Long.parseLong(parts[2]) / 100;
        if (idempotent) {
            // メッセージIDで「すでに処理したか」を判定しながら加算する
            boolean applied = store.addPointsOnce(properties.getMessageId(), customerId, points);
            Console.log((applied ? "付与 " : "重複のためスキップ ") + orderId + " +" + points
                    + "pt → 残高 " + store.balance(customerId) + "pt");
        } else {
            // 何も考えずに加算する
            store.addPoints(customerId, points);
            Console.log("付与 " + orderId + " +" + points + "pt → 残高 " + store.balance(customerId) + "pt");
        }
        // 保存が終わってから ack する（逆順だと、ack 後に落ちたときポイントが消える）
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * 指定したモードで購読を始める。
     *
     * @param args [0] naive / idempotent（省略時は naive）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        boolean idempotent = args.length > 0 && args[0].equals("idempotent");
        PointStore store = new PointStore();
        Connection connection = RabbitConnection.open("point-grant-consumer");
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE, true, false, false, null);
        channel.basicQos(10);
        channel.basicConsume(QUEUE, false, new PointGrantConsumer(channel, store, idempotent));
        Console.log((idempotent ? "idempotent" : "naive") + " モードで購読中です。Ctrl+C で終了します");
    }
}
