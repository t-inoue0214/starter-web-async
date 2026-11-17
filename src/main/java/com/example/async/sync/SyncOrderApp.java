package com.example.async.sync;

import java.util.Random;

import com.example.async.common.Console;

/**
 * 注文確定のあとの処理（メール・在庫・ポイント）を、同期呼び出しで順番に実行するアプリ。
 * 非同期の仕組みを使わない構成で「何が困るか」を体験するための出発点。
 */
public class SyncOrderApp {
    // 乱数のシードを固定して、毎回同じ結果になるようにする
    private static final Random RANDOM = new Random(3);

    /**
     * 5件の注文を順番に確定させ、1件ごとの所要時間と成否を表示する。
     *
     * @param args 使わない
     */
    public static void main(String[] args) {
        // 5件の注文を処理する
        for (int i = 1; i <= 5; i++) {
            // 注文IDを作る
            String orderId = "A-" + i;
            // 処理開始時刻を記録する
            long start = System.currentTimeMillis();
            try {
                // 注文確定処理（中で3つのサービスを順番に呼ぶ）
                placeOrder(orderId);
                // かかった時間を表示する
                Console.log("注文 " + orderId + " 確定 OK（" + (System.currentTimeMillis() - start) + "ms）");
            } catch (RuntimeException e) {
                // どこか1つでも失敗すると、注文確定そのものが失敗になる
                Console.log("注文 " + orderId + " 確定 NG（" + (System.currentTimeMillis() - start) + "ms）: " + e.getMessage());
            }
        }
    }

    // 注文確定。あとに続く3つのサービスを同期的に（終わるまで待ちながら）順に呼ぶ
    private static void placeOrder(String orderId) {
        // 注文をDBに保存した、という想定
        Console.log("注文 " + orderId + " をDBに保存");
        // 確認メールを送る（外部のメール配信サービス。遅い）
        sendMail(orderId);
        // 在庫を引き当てる
        reserveInventory(orderId);
        // ポイントを付与する（ときどき障害が起きる）
        grantPoint(orderId);
    }

    // メール配信サービスの呼び出しを模したもの。800ms かかる
    private static void sendMail(String orderId) {
        Console.sleep(800);
        Console.log("  メール送信 完了 " + orderId);
    }

    // 在庫サービスの呼び出しを模したもの。300ms かかる
    private static void reserveInventory(String orderId) {
        Console.sleep(300);
        Console.log("  在庫引当 完了 " + orderId);
    }

    // ポイントサービスの呼び出しを模したもの。30% の確率で障害になる
    private static void grantPoint(String orderId) {
        Console.sleep(200);
        if (RANDOM.nextInt(100) < 30) {
            throw new IllegalStateException("ポイントサービスが応答しません");
        }
        Console.log("  ポイント付与 完了 " + orderId);
    }
}
