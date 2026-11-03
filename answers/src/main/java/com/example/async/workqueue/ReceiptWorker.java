package com.example.async.workqueue;

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
 * receipt-tasks キューからタスクを取り出して処理するワーカー。
 * 複数起動すると、1つのキューを奪い合って（競合して）処理を分担する。
 */
public class ReceiptWorker extends DefaultConsumer {
    // ログに出すワーカー名
    private final String workerName;

    /**
     * @param channel    このワーカーが使うチャネル
     * @param workerName ログに出す名前
     */
    public ReceiptWorker(Channel channel, String workerName) {
        super(channel);
        this.workerName = workerName;
    }

    /**
     * タスクを1件処理し、終わったら ack（処理完了の通知）を返す。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        String task = new String(body, StandardCharsets.UTF_8);
        Console.log(workerName + " 開始: " + task);
        // heavy なら3秒、light なら0.3秒かかる処理の代わり
        Console.sleep(task.endsWith("heavy") ? 3000 : 300);
        Console.log(workerName + " 完了: " + task);
        // 処理が終わってから ack する。第2引数 multiple=false は「この1件だけ」という意味
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * ワーカーを起動する。
     *
     * @param args [0] ワーカー名, [1] prefetch 数（0 なら無制限）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        String name = args.length > 0 ? args[0] : "worker";
        int prefetch = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        Connection connection = RabbitConnection.open("receipt-" + name);
        Channel channel = connection.createChannel();
        channel.queueDeclare(ReceiptTaskProducer.QUEUE, true, false, false, null);
        // prefetch: ack を返していないメッセージを、最大何件まで手元に受け取るか（0 は無制限）
        channel.basicQos(prefetch);
        // autoAck=false にして、処理完了後に自分で ack を返す
        channel.basicConsume(ReceiptTaskProducer.QUEUE, false, new ReceiptWorker(channel, name));
        Console.log(name + " 起動（prefetch=" + prefetch + "）。Ctrl+C で終了します");
    }
}
