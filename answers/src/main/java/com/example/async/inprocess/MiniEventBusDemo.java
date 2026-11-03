package com.example.async.inprocess;

import com.example.async.common.Console;

/**
 * MiniEventBus を使って、注文確定イベントをメール・ポイントの2つの購読者へ配るデモ。
 */
public class MiniEventBusDemo {

    /**
     * 購読者を2つ登録して、イベントを2件発行する。
     *
     * @param args 使わない
     */
    public static void main(String[] args) {
        MiniEventBus bus = new MiniEventBus();
        // メールとポイントが、それぞれ注文確定を購読する
        bus.subscribe("order.placed", payload -> {
            Console.sleep(800);
            Console.log("  [mail] " + payload);
        });
        bus.subscribe("order.placed", payload -> Console.log("  [point] " + payload));

        for (int i = 1; i <= 2; i++) {
            long start = System.currentTimeMillis();
            // 注文サービスは、誰が購読しているかを知らずに発行する
            bus.publish("order.placed", "A-" + i);
            Console.log("注文 A-" + i + " 確定 OK（" + (System.currentTimeMillis() - start) + "ms）");
        }
    }
}
