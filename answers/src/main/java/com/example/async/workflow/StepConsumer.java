package com.example.async.workflow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import com.example.async.common.Console;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * ワークフローの「1つの手順」を担当するコンシューマー。
 * 指定したルーティングキーのメッセージ（注文ID）を受け取り、手順を実行して、結果のルーティングキーで次のメッセージを発行する。
 */
public class StepConsumer extends DefaultConsumer {
    /** ワークフロー用の topic エクスチェンジ名 */
    public static final String EXCHANGE = "workflow-events";
    // ログに出す手順名
    private final String stepName;
    // 手順の中身。注文IDを受け取り、次に発行するルーティングキーを返す（発行しないなら null）
    private final Function<String, String> action;

    /**
     * @param channel  使うチャネル
     * @param stepName 手順名
     * @param action   手順の中身
     */
    public StepConsumer(Channel channel, String stepName, Function<String, String> action) {
        super(channel);
        this.stepName = stepName;
        this.action = action;
    }

    /**
     * 注文IDを受け取って手順を実行し、結果を次のメッセージとして発行する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        String orderId = new String(body, StandardCharsets.UTF_8);
        // 手順を実行して、次に発行するルーティングキーを得る
        String next = action.apply(orderId);
        if (next != null) {
            getChannel().basicPublish(EXCHANGE, next, null, orderId.getBytes(StandardCharsets.UTF_8));
            Console.log("  [" + stepName + "] " + orderId + " → " + next + " を発行");
        }
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * 手順を購読する。キューは接続専用（exclusive）・切断時に自動削除で作る。
     * 実験を繰り返しても、前回の残りメッセージが混ざらないようにするため。
     *
     * @param channel    使うチャネル
     * @param stepName   手順名（キュー名にも使う）
     * @param bindingKey 受け取るルーティングキー
     * @param action     手順の中身
     * @throws IOException 宣言や購読に失敗した場合
     */
    public static void start(Channel channel, String stepName, String bindingKey,
            Function<String, String> action) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, false);
        String queue = "wf." + stepName;
        channel.queueDeclare(queue, false, true, true, null);
        channel.queueBind(queue, EXCHANGE, bindingKey);
        channel.basicConsume(queue, false, new StepConsumer(channel, stepName, action));
    }
}
