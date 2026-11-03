package com.example.async.http;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import com.example.async.common.Console;

/**
 * HTTP API で hello-http キューを一定間隔で「取りに行く」（ポーリングする）コンシューマー。
 * メッセージがなくても毎回リクエストが飛ぶこと、届いてから受け取るまで遅延があることを体験する。
 */
public class HttpPollingConsumer {
    // 管理 HTTP API のベースURL
    private static final String BASE = "http://localhost:15672/api";
    // Basic 認証ヘッダーの値
    private static final String AUTH = "Basic "
            + Base64.getEncoder().encodeToString("app:app".getBytes(StandardCharsets.UTF_8));
    // ポーリング間隔（ミリ秒）
    private static final long INTERVAL_MILLIS = 3000;

    /**
     * Ctrl+C で止めるまで、3秒ごとにキューからメッセージを1件取り出す。
     *
     * @param args 使わない
     * @throws Exception 通信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        // RabbitMQ の管理 HTTP API は HTTP/2 へのアップグレードに対応しないため、HTTP/1.1 を明示する
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        // ackmode=ack_requeue_false は「取り出したらキューから消す」という指定
        String body = "{\"count\":1,\"ackmode\":\"ack_requeue_false\",\"encoding\":\"auto\"}";
        // 何回目のリクエストかを数える
        int polls = 0;
        while (true) {
            polls++;
            HttpRequest request = HttpRequest.newBuilder(URI.create(BASE + "/queues/%2F/hello-http/get"))
                    .header("Authorization", AUTH)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            // 空配列 [] ならメッセージがなかった（空振り）
            if (response.body().equals("[]")) {
                Console.log("poll #" + polls + " 空振り");
            } else {
                Console.log("poll #" + polls + " 受信: " + response.body());
            }
            // 次のポーリングまで待つ
            Thread.sleep(INTERVAL_MILLIS);
        }
    }
}
