package com.example.async.http;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import com.example.async.common.Console;

/**
 * RabbitMQ 管理プラグインの HTTP API を使って、キューにメッセージを1件送るクラス。
 * AMQP ライブラリを使わず、JDK 標準の HttpClient だけで動く。
 */
public class HttpPublisher {
    // 管理 HTTP API のベースURL（%2F は仮想ホスト "/" をURLエンコードしたもの）
    private static final String BASE = "http://localhost:15672/api";
    // Basic 認証ヘッダーの値（app:app を Base64 にしたもの）
    private static final String AUTH = "Basic "
            + Base64.getEncoder().encodeToString("app:app".getBytes(StandardCharsets.UTF_8));

    /**
     * 引数の文字列を hello-http キューへ送る。
     *
     * @param args 送る本文（省略時は固定の文字列）
     * @throws Exception 通信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        // 送る本文を決める
        String text = args.length > 0 ? args[0] : "HTTPから送った注文 A-1";
        // HttpClient はスレッドセーフなので1つ作って使い回す
        // RabbitMQ の管理 HTTP API は HTTP/2 へのアップグレードに対応しないため、HTTP/1.1 を明示する
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        // 既定エクスチェンジ（amq.default）に routing_key=キュー名 で送ると、そのキューに直接届く
        String body = "{\"properties\":{},\"routing_key\":\"hello-http\","
                + "\"payload\":\"" + text + "\",\"payload_encoding\":\"string\"}";
        HttpRequest request = HttpRequest.newBuilder(URI.create(BASE + "/exchanges/%2F/amq.default/publish"))
                // 認証ヘッダーを付ける
                .header("Authorization", AUTH)
                // 送る本文は JSON
                .header("Content-Type", "application/json")
                // POST で送る
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        // 同期的に送信してレスポンスを受け取る
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        // {"routed":true} なら、どこかのキューに届いたという意味
        Console.log("status=" + response.statusCode() + " body=" + response.body());
    }
}
