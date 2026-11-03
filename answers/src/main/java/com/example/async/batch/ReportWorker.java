package com.example.async.batch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * report-jobs キューから売上レポートのジョブを取り出して処理するワーカー。
 * 同じジョブID（messageId）を2回受け取ったら、2回目は処理せずに捨てる。
 */
public class ReportWorker extends DefaultConsumer {
    // 1件の処理にかかる時間（ミリ秒）
    private final long workMillis;
    // 処理済みのジョブID（学習用にメモリ上で持つ。本番は DB に保存する）
    private final Set<String> processed = new HashSet<>();

    /**
     * @param channel    使うチャネル
     * @param workMillis 1件の処理にかかる時間（ミリ秒）
     */
    public ReportWorker(Channel channel, long workMillis) {
        super(channel);
        this.workMillis = workMillis;
    }

    /**
     * レポート作成のジョブを1件処理する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        String jobId = properties.getMessageId();
        String text = new String(body, StandardCharsets.UTF_8);
        if (!processed.add(jobId)) {
            // Set.add が false を返すのは、すでに処理済みのジョブID
            Console.log("重複のためスキップ " + jobId);
        } else {
            Console.log("レポート作成 開始 " + jobId + "（" + text + "）");
            Console.sleep(workMillis);
            Console.log("レポート作成 完了 " + jobId);
        }
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * ワーカーを起動する。
     *
     * @param args [0] 1件の処理時間（秒、省略時は 1）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        long workMillis = (args.length > 0 ? Long.parseLong(args[0]) : 1) * 1000;
        Connection connection = RabbitConnection.open("report-worker");
        Channel channel = connection.createChannel();
        channel.queueDeclare(ReportScheduler.QUEUE, true, false, false, null);
        channel.basicQos(1);
        channel.basicConsume(ReportScheduler.QUEUE, false, new ReportWorker(channel, workMillis));
        Console.log("ワーカー起動（1件 " + workMillis / 1000 + " 秒）。Ctrl+C で終了します");
    }
}
