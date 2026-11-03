package com.example.async.ack;

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
 * 1件に3秒かかるポイント付与処理を行うコンシューマー。
 * 自動 ack と手動 ack を切り替えて、途中で強制終了したときに何が起きるかを確かめる。
 */
public class SlowPointConsumer extends DefaultConsumer {
    // 手動 ack かどうか
    private final boolean manualAck;

    /**
     * @param channel   使うチャネル
     * @param manualAck true なら処理が終わってから自分で ack する
     */
    public SlowPointConsumer(Channel channel, boolean manualAck) {
        super(channel);
        this.manualAck = manualAck;
    }

    /**
     * 3秒かけて処理し、手動 ack のときだけ ack を返す。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        String task = new String(body, StandardCharsets.UTF_8);
        // isRedeliver() が true なら「以前にも誰かに届けたが ack されなかった」メッセージ
        Console.log("処理開始: " + task + " redelivered=" + envelope.isRedeliver());
        Console.sleep(3000);
        Console.log("処理完了: " + task);
        // 処理が終わってから ack する
        if (manualAck) {
            getChannel().basicAck(envelope.getDeliveryTag(), false);
        }
    }

    /**
     * 指定したモードで購読を始める。
     *
     * @param args [0] auto / manual（省略時は auto）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        boolean manualAck = args.length > 0 && args[0].equals("manual");
        Connection connection = RabbitConnection.open("slow-point-consumer");
        Channel channel = connection.createChannel();
        channel.queueDeclare(PointTaskProducer.QUEUE, true, false, false, null);
        // 手動 ack のときは、手元に受け取る未 ack メッセージを1件までに絞る
        if (manualAck) {
            channel.basicQos(1);
        }
        // 第2引数 autoAck: true なら届けた時点で処理済みとみなされる
        channel.basicConsume(PointTaskProducer.QUEUE, !manualAck, new SlowPointConsumer(channel, manualAck));
        Console.log((manualAck ? "manual" : "auto") + " モードで購読中です。Ctrl+C で強制終了してみましょう");
    }
}
