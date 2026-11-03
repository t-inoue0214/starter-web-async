# 第7章: HTTP APIでメッセージを送受信する

## この章のゴール

RabbitMQ の管理 HTTP API を使い、curl と Java の `HttpClient` でメッセージを送受信します。HTTP で「取りに行く（ポーリングする）」受信方法の限界を体験し、次の章から専用のクライアントライブラリを使う理由を理解します。

## Why: なぜこれを学ぶのか

「ブローカーにメッセージを送って受け取るだけなら、HTTP でよいのでは」と考えるのは自然です。HTTP なら curl でも Bash でも書けて、ライブラリも要りません。実際、Amazon SQS や Google Cloud Pub/Sub は HTTP（や gRPC）の API で操作します。

一方で、HTTP だけでは「メッセージが届いた瞬間に受け取る」ことが苦手です。この章では、あえて HTTP だけで送受信を作り、どこで困るのかを確かめます。

## 前提

第6章で、既定エクスチェンジにルーティングキー＝キュー名で送ると、そのキューに届くことを学びました。RabbitMQ が起動していて、管理画面に `app` / `app` でログインできる状態です。

## ステップ1: curl でキューを作る

管理 HTTP API は、管理画面と同じ 15672 番ポートの `/api/` 以下で提供されています。まずは `hello-http` というキューを作ります。

```bash
# PUT /api/queues/{仮想ホスト}/{キュー名} でキューを作る（%2F は仮想ホスト "/" を URL エンコードしたもの）
curl -i -u app:app -X PUT \
  -H 'content-type: application/json' \
  -d '{"durable":true}' \
  http://localhost:15672/api/queues/%2F/hello-http
```

`HTTP/1.1 201 Created` が返れば成功です。管理画面の Queues and Streams タブを開くと、`hello-http` キューが増えています。

**Why**: URL の `%2F` は仮想ホスト `/` を表します。`/` をそのまま URL に入れるとパスの区切りと区別できないため、URL エンコードしています。`"durable":true` は、ブローカーを再起動してもキューの定義を残すという指定です（第11章で詳しく扱います）。

## ステップ2: curl でメッセージを送る

```bash
# 既定エクスチェンジ（API 上の名前は amq.default）に、routing_key=キュー名 で送る
curl -s -u app:app -X POST \
  -H 'content-type: application/json' \
  -d '{"properties":{},"routing_key":"hello-http","payload":"注文 A-1 が確定しました","payload_encoding":"string"}' \
  http://localhost:15672/api/exchanges/%2F/amq.default/publish
```

`{"routed":true}` が返ります。これは「どこかのキューに届いた」という意味です。管理画面で `hello-http` の `Ready` が 1 になっていることを確認します。

**🔍 確認・実験してみよう**: `routing_key` を存在しないキュー名 `no-such-queue` に変えて送ると、レスポンスはどうなるでしょうか。エラーになるのか、それとも別の結果になるのか、予想してから確かめてみましょう。この結果は第11章で大事な意味を持ちます。

## ステップ3: curl でメッセージを受け取る

```bash
# POST /api/queues/{仮想ホスト}/{キュー名}/get で、キューからメッセージを取り出す
curl -s -u app:app -X POST \
  -H 'content-type: application/json' \
  -d '{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}' \
  http://localhost:15672/api/queues/%2F/hello-http/get
```

次のような JSON の配列が返ります。

```json
[{"payload_bytes":32,"redelivered":false,"exchange":"","routing_key":"hello-http","message_count":0,"properties":[],"payload":"注文 A-1 が確定しました","payload_encoding":"string"}]
```

もう一度同じコマンドを実行すると、今度は空の配列 `[]` が返ります。

**Why**: `ackmode` は「取り出したメッセージをどう扱うか」の指定です。

| ackmode | 意味 |
| :--- | :--- |
| `ack_requeue_false` | 取り出したらキューから消す |
| `ack_requeue_true` | 中身を見るだけで、キューに戻す |
| `reject_requeue_false` | 処理を拒否して、キューに戻さない |
| `reject_requeue_true` | 処理を拒否して、キューに戻す |

1回目で `ack_requeue_false` を指定したのでメッセージは消え、2回目は空になりました。

## ステップ4: Java の HttpClient でメッセージを送る

同じことを Java で書きます。`src/main/java/com/example/async/http/HttpPublisher.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/http/HttpPublisher.java -->
```java
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
```

実行します。

```bash
./run.sh http.HttpPublisher
```

```text
[09:15:18.496][main] status=200 body={"routed":true}
```

**Why**: `starter-java-first-steps` 第11章で使った `HttpClient` で、curl と同じリクエストを組み立てています。Basic 認証は「`ユーザー名:パスワード` を Base64 にして `Authorization` ヘッダーに入れる」という決まりなので、`Base64.getEncoder()` で作っています。

**⚠️ よくある失敗・誤解**: `HttpClient.newHttpClient()` をそのまま使うと、`java.io.IOException: EOF reached while reading` で失敗します。Java の `HttpClient` は既定で HTTP/2 を使おうとし、平文の `http://` では HTTP/2 へのアップグレードを要求します。RabbitMQ の管理 API はこれに応じず接続を切るため、`.version(HttpClient.Version.HTTP_1_1)` で HTTP/1.1 を明示しています。

## ステップ5: Java でポーリングして受け取る

次に、受信側を書きます。HTTP には「サーバーから突然データを送り付ける」仕組みが基本的にないので、一定間隔でキューに「何か届いていますか」と聞きに行きます。これを **ポーリング** と呼びます。

`src/main/java/com/example/async/http/HttpPollingConsumer.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/http/HttpPollingConsumer.java -->
```java
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
```

ターミナルを2つ開き、1つめで受信側を起動します。

```bash
./run.sh http.HttpPollingConsumer
```

2つめのターミナルで、何回か送信します。

```bash
./run.sh http.HttpPublisher "注文 A-2"
```

受信側には、次のように表示されます。

```text
[09:15:22.216][main] poll #1 受信: [{"payload_bytes":29,...,"payload":"注文 A-2",...}]
[09:15:25.230][main] poll #2 空振り
[09:15:28.243][main] poll #3 空振り
[09:15:31.251][main] poll #4 空振り
```

確認できたら、受信側を Ctrl+C で止めます。

**Why**: ポーリングには、次の2つの問題があります。

1. **遅延**: 送ってから受け取るまで、最大でポーリング間隔（3秒）待たされる
2. **無駄**: メッセージがなくても、3秒ごとにリクエストが飛び続ける（空振り）

間隔を短くすれば遅延は減りますが、空振りのリクエストが増えてブローカーの負荷になります。間隔を長くすれば負荷は減りますが、遅延が増えます。ポーリングでは、この2つを同時に解決できません。

**🔍 確認・実験してみよう**: `HttpPollingConsumer` を2つのターミナルで同時に起動し、`HttpPublisher` で5回送ってみましょう。1件のメッセージが両方に届くでしょうか、それともどちらか一方だけでしょうか。第15章の P2P と Pub/Sub のどちらの動きになるか、予想してから確かめてみましょう。

## ステップ6: HTTP API の向き・不向きを知る

RabbitMQ の公式ドキュメントでも、管理 HTTP API でのメッセージ送受信は **開発やテスト、障害調査のためのもの** とされています。本番のアプリが大量のメッセージをやり取りする用途には向きません。

| 観点 | 管理 HTTP API | AMQP クライアント（次の章から） |
| :--- | :--- | :--- |
| 受信の方式 | プル（ポーリング） | プッシュ（届いた瞬間に通知される） |
| 接続 | リクエストごと | 1本の接続を張りっぱなし |
| ack | 取り出した時点で決める | 処理が終わってから返せる |
| 用途 | 動作確認、運用ツール、監視 | アプリ本体の送受信 |

**💼 現場Tip**: クラウドのサービスは HTTP ベースでも、ポーリングの弱点を補う工夫をしています。

- Amazon SQS の **ロングポーリング**: 受信リクエストを最大20秒間サーバー側で待たせ、その間にメッセージが届いたらすぐ返す。空振りと遅延の両方を減らせる
- Google Cloud Pub/Sub の **プッシュサブスクリプション**: メッセージが届くと、ブローカーのほうから指定した URL に HTTP POST してくる（Webhook）
- 各社の SDK は、内部で gRPC のストリーミングなどを使い、プッシュに近い受信を実現している

**💼 現場Tip**: 管理 HTTP API は、運用の自動化では大いに役立ちます。たとえば `GET /api/queues` でキューごとの滞留件数を取得し、しきい値を超えたらアラートを出す監視スクリプトは、よく作られます。

## 動作確認

次の3つを確認できたら完了です。

1. curl でキューを作り、送信し、受信できた
2. `./run.sh http.HttpPublisher` が `{"routed":true}` を表示した
3. `./run.sh http.HttpPollingConsumer` で、受信できた回と空振りの回の両方を確認できた

## もっと良くしてみよう

### 理解確認

1. `ackmode` に `ack_requeue_true` を指定すると、何回 get しても同じメッセージが返ってくるのはなぜでしょうか。
2. ポーリング間隔を短くすると、何がよくなり、何が悪くなるでしょうか。
3. Java の `HttpClient` で HTTP/1.1 を明示しないと失敗するのはなぜでしょうか。

### 発展改善クイズ

1. `HttpPollingConsumer` は `ack_requeue_false` で取り出しています。取り出した直後、処理の途中でプログラムが落ちると、そのメッセージはどうなるでしょうか。
2. 空振りのときだけポーリング間隔を少しずつ延ばし、受信できたら短く戻す、という工夫が考えられます。この工夫でも解決できない問題は何でしょうか。

---

| [← 第6章: AMQPとはどのようなプロトコルか](./06-amqp.md) | [全章目次](./README.md) | [第8章: Javaクライアントを導入して接続する →](./08-java-client-setup.md) |
| - | :-: | --: |
