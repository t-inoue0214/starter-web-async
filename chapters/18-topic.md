# 第18章: topicで受け取るイベントを選ぶ

## この章のゴール

topic エクスチェンジとルーティングキーのワイルドカード（`*` と `#`）を使い、サービスごとに必要なイベントだけを受け取れるようにします。ルーティングキーの命名規則を設計する考え方を学びます。

## Why: なぜこれを学ぶのか

第17章の fanout では、Binding されたキューにすべてのイベントが届きました。注文には「確定」のほかに「キャンセル」「発送」などの出来事があり、地域（日本・米国）によって担当チームも違います。受け取りたいイベントはサービスごとに違います。たとえばメールサービスは確定だけ、返金サービスはキャンセルだけを受け取りたいはずです。日本の運用チームは日本の注文すべてを、分析基盤は全部を受け取りたいでしょう。

受け取ってから読み捨てることもできますが、不要なメッセージの転送と処理は無駄です。ブローカーの段階で振り分けるのが topic エクスチェンジです。

## 前提

第17章で、fanout エクスチェンジでサービスごとのキューへイベントを配りました。

## ステップ1: ルーティングキーの命名規則を決める

topic エクスチェンジでは、ルーティングキーを `.` で区切った単語の並びとして扱います。この章では、次の命名規則にします。

```text
<集約>.<出来事>.<地域>
例: order.placed.jp / order.cancelled.jp / order.shipped.us
```

**Why**: 「集約」は、注文や商品のように、イベントの主語になるもののまとまりです。命名規則を先に決めておくと、受け手は「注文のことは全部」「キャンセルだけ」「日本のものだけ」といった条件を、ワイルドカードで表現できます。命名規則は、送り手と受け手の間の **契約** です。後から変えると全受け手に影響するので、最初に慎重に決めます。

## ステップ2: いろいろなイベントを発行するパブリッシャーを書く

`src/main/java/com/example/async/topic/OrderEventTopicPublisher.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/topic/OrderEventTopicPublisher.java -->
```java
package com.example.async.topic;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * 注文に関するさまざまなイベントを、種類ごとのルーティングキーを付けて order-events エクスチェンジへ発行する。
 */
public class OrderEventTopicPublisher {
    /** 注文イベント用の topic エクスチェンジ名（後の章でも使う） */
    public static final String EXCHANGE = "order-events";

    /**
     * 注文の確定・キャンセル・発送のイベントを、地域（jp / us）を変えながら発行する。
     *
     * @param args 使わない
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        // 発行するイベントのルーティングキー。「<集約>.<出来事>.<地域>」という命名規則にしている
        String[] routingKeys = {
            "order.placed.jp", "order.placed.us", "order.cancelled.jp", "order.shipped.jp", "order.shipped.us"
        };
        try (Connection connection = RabbitConnection.open("order-event-topic-publisher");
                Channel channel = connection.createChannel()) {
            // topic 型のエクスチェンジを宣言する
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, true);
            for (int i = 0; i < routingKeys.length; i++) {
                String event = "{\"orderId\":\"A-" + (i + 1) + "\",\"type\":\"" + routingKeys[i] + "\"}";
                // ルーティングキーを付けて発行する。どのキューに届くかはバインディング次第
                channel.basicPublish(EXCHANGE, routingKeys[i], MessageProperties.PERSISTENT_TEXT_PLAIN,
                        event.getBytes(StandardCharsets.UTF_8));
                Console.log("発行: routingKey=" + routingKeys[i]);
            }
        }
    }
}
```

**Why**: エクスチェンジ名の定数 `EXCHANGE` を `public` にしているのは、第19章以降のクラスからも、この `order-events` エクスチェンジを使うためです。

## ステップ3: バインディングキーを指定して購読するサブスクライバーを書く

`src/main/java/com/example/async/topic/TopicSubscriber.java` を新規作成します。2つめ以降の起動引数を、バインディングキー（受け取りたいパターン）として扱います。

<!-- code: answers/src/main/java/com/example/async/topic/TopicSubscriber.java -->
```java
package com.example.async.topic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * バインディングキー（受け取りたいイベントのパターン）を指定して購読するサブスクライバー。
 */
public class TopicSubscriber extends DefaultConsumer {
    // ログに出すサービス名
    private final String serviceName;

    /**
     * @param channel     使うチャネル
     * @param serviceName サービス名
     */
    public TopicSubscriber(Channel channel, String serviceName) {
        super(channel);
        this.serviceName = serviceName;
    }

    /**
     * 受け取ったイベントのルーティングキーを表示して ack する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        // envelope から「どのルーティングキーで発行されたか」がわかる
        Console.log("[" + serviceName + "] 受信 routingKey=" + envelope.getRoutingKey()
                + " body=" + new String(body, StandardCharsets.UTF_8));
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * サービス名とバインディングキーを指定して購読を始める。
     *
     * @param args [0] サービス名, [1..] バインディングキー（例: order.placed.* や order.#）
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("使い方: ./run.sh topic.TopicSubscriber <サービス名> <バインディングキー>...");
            return;
        }
        String service = args[0];
        // 2つめ以降の引数をすべてバインディングキーとして扱う
        String[] bindingKeys = Arrays.copyOfRange(args, 1, args.length);
        Connection connection = RabbitConnection.open("topic-subscriber-" + service);
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(OrderEventTopicPublisher.EXCHANGE, BuiltinExchangeType.TOPIC, true);
        String queue = OrderEventTopicPublisher.EXCHANGE + "." + service;
        channel.queueDeclare(queue, true, false, false, null);
        // 1つのキューに複数のバインディングを張れる。どれか1つにマッチすれば届く
        for (String key : bindingKeys) {
            channel.queueBind(queue, OrderEventTopicPublisher.EXCHANGE, key);
        }
        channel.basicConsume(queue, false, new TopicSubscriber(channel, service));
        Console.log("[" + service + "] " + queue + " を購読中 bindingKeys=" + Arrays.toString(bindingKeys));
    }
}
```

**Why**: バインディングキーに使えるワイルドカードは次の2つです。

| 記号 | 意味 | 例 | 一致するルーティングキー |
| :--- | :--- | :--- | :--- |
| `*` | ちょうど1つの単語 | `order.placed.*` | `order.placed.jp`、`order.placed.us` |
| `#` | 0個以上の単語 | `order.#` | `order`、`order.placed`、`order.placed.jp` |

`envelope.getRoutingKey()` を見ると、どのルーティングキーで発行されたメッセージかがわかります。

## ステップ4: 4つのサービスで購読して発行する

4つのターミナルで、それぞれ次のサブスクライバーを起動します。バインディングキーはシングルクォートで囲みます。

```bash
# メール: 確定だけ（地域は問わない）
./run.sh topic.TopicSubscriber mail 'order.placed.*'
```

```bash
# 日本の運用チーム: 日本の出来事すべて
./run.sh topic.TopicSubscriber jp-ops '*.*.jp'
```

```bash
# 分析基盤: すべて
./run.sh topic.TopicSubscriber analytics '#'
```

```bash
# 返金: キャンセルだけ
./run.sh topic.TopicSubscriber refund 'order.cancelled.*'
```

5つめのターミナルで発行します。

```bash
./run.sh topic.OrderEventTopicPublisher
```

各サービスに届くイベントは次のとおりです。

| サービス | バインディングキー | 届く件数 | 届くルーティングキー |
| :--- | :--- | :--- | :--- |
| mail | `order.placed.*` | 2 | `order.placed.jp`、`order.placed.us` |
| jp-ops | `*.*.jp` | 3 | `order.placed.jp`、`order.cancelled.jp`、`order.shipped.jp` |
| analytics | `#` | 5 | すべて |
| refund | `order.cancelled.*` | 1 | `order.cancelled.jp` |

**⚠️ よくある失敗・誤解**: バインディングキーをシングルクォートで囲まないと、Bash が `*` や `#` を特別な記号として解釈してしまいます。`*` はカレントディレクトリのファイル名に展開され、`#` 以降はコメントとして無視されます。

**🔍 確認・実験してみよう**: バインディングキー `order.*` で購読すると、`order.placed.jp` は届くでしょうか。`*` と `#` の違いを思い出し、予想してから確かめてみましょう。

## ステップ5: 1つのキューに複数の Binding を張る

返金サービスが「キャンセル」に加えて「発送」も受け取りたくなったとします。`refund` を Ctrl+C で止め、バインディングキーを2つ指定して起動し直します。

```bash
./run.sh topic.TopicSubscriber refund 'order.cancelled.*' 'order.shipped.*'
```

発行すると、`refund` には3件が届きます。

**Why**: 1つのキューに複数の Binding を張ると、**どれか1つに一致すれば** そのキューに届きます。1件のメッセージが同じキューの複数の Binding に一致しても、キューに入るのは1件だけです。

**⚠️ よくある失敗・誤解**: 今度は `refund` を止めて、`order.cancelled.*` だけで起動し直してみましょう。発行すると、まだ「発送」も届きます。`queueBind` は Binding を **追加** するだけで、以前の Binding を消さないためです。durable なキューの Binding は、プログラムを止めても残り続けます。不要になった Binding は、`channel.queueUnbind(...)` を呼ぶか、管理画面のキュー詳細の Bindings 欄にある「Unbind」ボタンで消します。

## ステップ6: direct と headers エクスチェンジを知る

topic のほかに、第6章で紹介した2種類のエクスチェンジがあります。

| 種類 | 振り分けの規則 | 向いている場面 |
| :--- | :--- | :--- |
| direct | ルーティングキーの完全一致 | 振り分け先が決まった数しかない場合（第20章で使う） |
| topic | `.` 区切りのパターン一致 | 階層的な分類で、受け手ごとに粒度を変えたい場合 |
| headers | ヘッダーの値の一致（全部一致 `x-match=all` か、どれか一致 `x-match=any`） | 振り分け条件が1つの階層に並べにくい場合 |

**💼 現場Tip**: 「受け手ごとに必要なイベントだけを受け取る」機能は、製品ごとに次のような名前で提供されています。

| 製品 | 機能 |
| :--- | :--- |
| RabbitMQ | topic / headers エクスチェンジ |
| Amazon SNS | サブスクリプションのフィルターポリシー（メッセージの属性や本文で絞り込む） |
| Amazon EventBridge | ルール（イベントのパターンで配り先を決める） |
| Google Cloud Pub/Sub | サブスクリプションのフィルター（属性で絞り込む） |
| Apache Kafka | 絞り込み機能は基本的になく、トピックを分けるか、受け手側で読み捨てる |

## 動作確認

4つのサブスクライバーを起動して `OrderEventTopicPublisher` を実行し、ステップ4の表のとおりの件数が各サービスに届いたら完了です。

## もっと良くしてみよう

### 理解確認

1. `order.*` と `order.#` の違いは何でしょうか。
2. 1つのキューに張った2つの Binding の両方に一致したメッセージは、そのキューに何件入るでしょうか。
3. サブスクライバーのバインディングキーを変えて起動し直しても、以前のイベントが届き続けるのはなぜでしょうか。

### 発展改善クイズ

1. 将来「店舗受け取り」のような新しい注文の種類が増え、`order.placed.jp.store` のように単語を1つ増やしたくなったとします。今のバインディングキー `order.placed.*` や `*.*.jp` で購読しているサービスには、どのような影響が出るでしょうか。
2. 今のサブスクライバーは、受け取った本文をそのまま表示しているだけです。本文がどのような形式なのか、どの版の項目構成なのかを、受け手はどうやって知ればよいでしょうか。

---

| [← 第17章: fanoutでPub/Subを実現する](./17-fanout.md) | [全章目次](./README.md) | [第19章: メッセージを設計する →](./19-message-design.md) |
| - | :-: | --: |
