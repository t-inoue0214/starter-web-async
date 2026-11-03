package com.example.async.batch;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * 一定間隔で「売上レポートを作る」ジョブを report-jobs キューに積む、定期バッチのスケジューラー。
 * 本物の定期バッチは「毎晩2時」のような時刻で動くが、実験しやすいように3秒間隔にしている。
 */
public class ReportScheduler {
    /** ジョブを積むキュー名 */
    public static final String QUEUE = "report-jobs";
    // ジョブを積む間隔（ミリ秒）
    private static final long INTERVAL_MILLIS = 3000;

    /**
     * スケジューラーを起動する。Ctrl+C で止めるまで、一定間隔でジョブを積み続ける。
     *
     * @param args [0] スケジューラー名（省略時は scheduler）, [1] naive / safe（省略時は naive）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        String name = args.length > 0 ? args[0] : "scheduler";
        boolean safe = args.length > 1 && args[1].equals("safe");
        Connection connection = RabbitConnection.open("report-scheduler-" + name);
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE, true, false, false, null);

        // 一定間隔で処理を呼び出す仕組み（JDK 標準）
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                tick(channel, name, safe);
            } catch (Exception e) {
                // 1回の失敗でスケジューラー全体を止めない。次の周期でまたやり直す
                Console.log(name + " ジョブを積めませんでした: " + e);
            }
        }, 0, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        Console.log(name + " 起動（" + (safe ? "safe" : "naive") + " モード）。" + INTERVAL_MILLIS + "ms ごとにジョブを積みます");
    }

    // 1回ぶんの処理。ジョブを1件積む
    private static void tick(Channel channel, String name, boolean safe) throws Exception {
        // 今キューにたまっている件数。ワーカーが追い付いているかの目安になる
        int backlog = channel.queueDeclarePassive(QUEUE).getMessageCount();
        // 「第何周期か」を表す番号。同じ周期なら、どのスケジューラーが積んでも同じ値になる
        long slot = System.currentTimeMillis() / INTERVAL_MILLIS;
        String jobId;
        if (safe) {
            // 前回のジョブがまだ残っているなら、積まずにスキップする（積み上がりを防ぐ）
            if (backlog > 0) {
                Console.log(name + " スキップ: 未処理のジョブが " + backlog + " 件あります");
                return;
            }
            // 周期の番号からジョブIDを決める。同じ周期のジョブは、必ず同じIDになる
            jobId = "report-" + slot;
        } else {
            // 毎回、新しい一意なIDを振る
            jobId = "report-" + UUID.randomUUID();
        }
        // ジョブIDを messageId に入れて、永続メッセージとして積む
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .messageId(jobId).deliveryMode(2).build();
        channel.basicPublish("", QUEUE, props, ("売上レポート " + slot).getBytes(StandardCharsets.UTF_8));
        Console.log(name + " ジョブを積みました " + jobId + "（キューの滞留 " + backlog + " 件）");
    }
}
