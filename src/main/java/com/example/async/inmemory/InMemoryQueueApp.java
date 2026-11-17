package com.example.async.inmemory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.example.async.common.Console;

/**
 * 後続処理（メール送信）をメモリ上のキューに入れ、別スレッドに任せるアプリ。
 * 注文の応答は速くなるが、プロセスが落ちるとキューの中身が消えることを確かめる。
 */
public class InMemoryQueueApp {
    // メモリ上のキュー。スレッド間で安全に受け渡せる BlockingQueue を使う
    private static final BlockingQueue<String> QUEUE = new LinkedBlockingQueue<>();
    // ワーカーに「もう仕事はない」と伝える合図
    private static final String STOP = "STOP";

    /**
     * 5件の注文を受け付ける。引数に crash を渡すと、途中でプロセスを強制終了する。
     * 渡さなければ、ワーカーが全件を処理し終えるまで待ってから終了する。
     *
     * @param args [0] crash なら強制終了を再現する
     */
    public static void main(String[] args) {
        // 後続処理を担当するワーカースレッドを起動する
        Thread worker = new Thread(() -> {
            while (true) {
                try {
                    // take は、キューが空なら入ってくるまで待つ
                    String orderId = QUEUE.take();
                    // 終了の合図が来たら、ワーカーを終える
                    if (orderId.equals(STOP)) {
                        return;
                    }
                    Console.sleep(800);
                    Console.log("  メール送信 完了 " + orderId);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "mail-worker");
        worker.start();

        for (int i = 1; i <= 5; i++) {
            long start = System.currentTimeMillis();
            // キューに入れるだけで、すぐ戻ってくる
            QUEUE.offer("A-" + i);
            Console.log("注文 A-" + i + " 確定 OK（" + (System.currentTimeMillis() - start) + "ms）");
        }

        if (args.length > 0 && args[0].equals("crash")) {
            Console.sleep(1500);
            Console.log("まだキューに残っている件数: " + QUEUE.size() + "。ここでプロセスが強制終了したとします");
            // halt は後始末をせずに即座に終了する。デプロイでの強制終了や OOM Killer の再現
            Runtime.getRuntime().halt(1);
        }

        // 通常は、終了の合図をキューの最後に入れて、ワーカーが残りを片付けるのを待つ
        QUEUE.offer(STOP);
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
