package com.example.async.workflow;

import java.util.concurrent.atomic.AtomicInteger;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * 決済→在庫→発送の連鎖を、「各手順が前の手順の結果イベントを聞いて動く」形（コレオグラフィ）で動かす。
 * 全体の流れを管理する者はいない。1つのプロセスの中に、手順ごとのコンシューマーを起動する。
 */
public class ChoreographyWorkers {

    /**
     * 手順ごとのコンシューマーを起動する。
     *
     * @param args [0] 在庫数（省略時は 2）, [1] compensate を渡すと、在庫切れ時に返金する手順も起動する
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        AtomicInteger stock = new AtomicInteger(args.length > 0 ? Integer.parseInt(args[0]) : 2);
        boolean compensate = args.length > 1 && args[1].equals("compensate");
        Connection connection = RabbitConnection.open("choreography-workers");

        // ① 決済: 注文確定を聞いて、決済を済ませる
        StepConsumer.start(connection.createChannel(), "payment", "order.placed", orderId -> {
            Console.sleep(300);
            Console.log("決済 完了 " + orderId);
            return "payment.completed";
        });
        // ② 在庫: 決済完了を聞いて、在庫を引き当てる。在庫がなければ失敗を知らせる
        StepConsumer.start(connection.createChannel(), "inventory", "payment.completed", orderId -> {
            Console.sleep(300);
            if (stock.decrementAndGet() < 0) {
                Console.log("在庫切れ " + orderId);
                return "inventory.failed";
            }
            Console.log("在庫引当 完了 " + orderId + "（残り " + stock.get() + "）");
            return "inventory.reserved";
        });
        // ③ 発送: 在庫引当を聞いて、発送を手配する
        StepConsumer.start(connection.createChannel(), "shipping", "inventory.reserved", orderId -> {
            Console.sleep(300);
            Console.log("発送手配 完了 " + orderId);
            return "shipping.arranged";
        });
        if (compensate) {
            // ④ 返金: 在庫切れの知らせを聞いて、決済を取り消す（補償）
            StepConsumer.start(connection.createChannel(), "refund", "inventory.failed", orderId -> {
                Console.sleep(300);
                Console.log("返金 完了 " + orderId);
                return "payment.refunded";
            });
        }
        Console.log("コレオグラフィ起動（在庫 " + stock.get() + (compensate ? "、返金あり" : "、返金なし") + "）。Ctrl+C で終了します");
    }
}
