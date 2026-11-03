package com.example.async.ordering;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 注文ステータスを更新するコンシューマー。1つのプロセスの中で2つのコンシューマーを並行に動かす。
 * plain モードは届いた順に上書きし、versioned モードは古いバージョンのイベントを捨てる。
 */
public class StatusConsumer extends DefaultConsumer {
    // 処理時間をばらつかせるための乱数
    private static final Random RANDOM = new Random();
    // 注文ID → 現在のバージョン。2つのコンシューマーで共有する
    private static final Map<String, Integer> VERSIONS = new ConcurrentHashMap<>();
    // 注文ID → 現在のステータス
    private static final Map<String, String> STATUSES = new ConcurrentHashMap<>();
    // ログに出すコンシューマー名
    private final String name;
    // 古いイベントを捨てるかどうか
    private final boolean checkVersion;

    /**
     * @param channel      使うチャネル
     * @param name         ログに出す名前
     * @param checkVersion true なら、今より古いバージョンのイベントを捨てる
     */
    public StatusConsumer(Channel channel, String name, boolean checkVersion) {
        super(channel);
        this.name = name;
        this.checkVersion = checkVersion;
    }

    /**
     * 処理時間をばらつかせてからステータスを更新する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        // 「注文ID,バージョン,ステータス」を分解する
        String[] parts = new String(body, StandardCharsets.UTF_8).split(",");
        String orderId = parts[0];
        int version = Integer.parseInt(parts[1]);
        String status = parts[2];
        // 外部API呼び出しなどで処理時間がばらつく、という想定
        Console.sleep(100 + RANDOM.nextInt(700));
        // compute は同じキーに対して排他的に実行されるので、読んで比べて書くまでが割り込まれない
        VERSIONS.compute(orderId, (id, current) -> {
            int now = current == null ? 0 : current;
            if (checkVersion && version <= now) {
                Console.log(name + " 破棄: " + orderId + " " + status + "(v" + version + ") は現在の v" + now + " より古い");
                return now;
            }
            if (version < now) {
                Console.log(name + " ⚠ 逆転: " + orderId + " v" + now + " の後に " + status + "(v" + version + ") を適用した");
            } else {
                Console.log(name + " 更新: " + orderId + " → " + status + "(v" + version + ")");
            }
            STATUSES.put(orderId, status);
            return version;
        });
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * 2つのコンシューマーを起動し、15秒後に各注文の最終ステータスを表示して終了する。
     *
     * @param args [0] plain / versioned（省略時は plain）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        boolean checkVersion = args.length > 0 && args[0].equals("versioned");
        Connection connection = RabbitConnection.open("status-consumer");
        for (int i = 0; i < 2; i++) {
            // コンシューマーごとにチャネルを分ける（チャネル単位で並行に配達される）
            Channel channel = connection.createChannel();
            StatusQueues.declare(channel);
            channel.basicQos(1);
            channel.basicConsume(StatusQueues.QUEUE, false, new StatusConsumer(channel, "consumer-" + i, checkVersion));
        }
        Console.log((checkVersion ? "versioned" : "plain") + " モードで購読中です。15秒後に最終状態を表示して終了します");
        Thread.sleep(15_000);
        // 注文ID順に並べて表示する
        Console.log("最終状態: " + new TreeMap<>(STATUSES));
        connection.close();
    }
}
