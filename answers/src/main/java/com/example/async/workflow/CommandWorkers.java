package com.example.async.workflow;

import java.util.concurrent.atomic.AtomicInteger;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Connection;

/**
 * オーケストレーターからの「命令」を受けて実行する手順たち。
 * 決済・在庫・発送・返金のそれぞれが、命令（cmd.○○）を受け、結果（○○.completed など）を返すだけで、
 * 次に何をするかは知らない。
 */
public class CommandWorkers {

    /**
     * 手順ごとのコンシューマーを起動する。
     *
     * @param args [0] 在庫数（省略時は 2）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        AtomicInteger stock = new AtomicInteger(args.length > 0 ? Integer.parseInt(args[0]) : 2);
        Connection connection = RabbitConnection.open("command-workers");

        StepConsumer.start(connection.createChannel(), "cmd-payment", "cmd.payment", orderId -> {
            Console.sleep(300);
            Console.log("決済 完了 " + orderId);
            return "payment.completed";
        });
        StepConsumer.start(connection.createChannel(), "cmd-inventory", "cmd.inventory", orderId -> {
            Console.sleep(300);
            if (stock.decrementAndGet() < 0) {
                Console.log("在庫切れ " + orderId);
                return "inventory.failed";
            }
            Console.log("在庫引当 完了 " + orderId + "（残り " + stock.get() + "）");
            return "inventory.reserved";
        });
        StepConsumer.start(connection.createChannel(), "cmd-shipping", "cmd.shipping", orderId -> {
            Console.sleep(300);
            Console.log("発送手配 完了 " + orderId);
            return "shipping.arranged";
        });
        StepConsumer.start(connection.createChannel(), "cmd-refund", "cmd.refund", orderId -> {
            Console.sleep(300);
            Console.log("返金 完了 " + orderId);
            return "payment.refunded";
        });
        Console.log("コマンドワーカー起動（在庫 " + stock.get() + "）。Ctrl+C で終了します");
    }
}
