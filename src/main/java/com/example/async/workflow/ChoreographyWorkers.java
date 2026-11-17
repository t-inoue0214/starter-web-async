package com.example.async.workflow;

import java.util.concurrent.atomic.AtomicInteger;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * 決済→在庫→発送の連鎖を、「各手順が前の手順の結果イベントを聞いて動く」形で動かす。
 * 全体の流れを管理する者はいない。1つのプロセスの中に、手順ごとの受け手（コンシューマー）を起動する。
 */
public class ChoreographyWorkers {

    /**
     * 手順ごとの受け手（コンシューマー）を起動する。
     *
     * @param args [0] 在庫数（省略時は 2）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        AtomicInteger stock = new AtomicInteger(args.length > 0 ? Integer.parseInt(args[0]) : 2);
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
        // 在庫切れ（inventory.failed）を聞く手順は、あえて用意していない
        Console.log("連鎖の手順を起動しました（在庫 " + stock.get() + "）。Ctrl+C で終了します");
    }
}
