# 第19章: メッセージを設計する

## この章のゴール

「注文確定」イベントを表す `OrderPlacedEvent` クラスを作り、`messageId`・`type`・`contentType`・`timestamp`・独自ヘッダーなどのメタデータを付けて発行します。受け手がメタデータを見てイベントを解釈し、知らない種類のイベントを安全に読み飛ばす作りにします。

## Why: なぜこれを学ぶのか

これまでの章では、本文の JSON を文字列連結で手作りしていました。受け手は「この JSON にはこういう項目があるはず」と思い込んで読むしかありません。送り手と受け手が別のチームになり、受け手の数が増えていくと、この思い込みは事故のもとになります。

メッセージは、送り手と受け手の間の **契約（API）** です。HTTP の API に仕様書があるように、メッセージにも「何のイベントか」「どの形式か」「どの版か」「いつ起きたか」を明示する必要があります。この章で作る `OrderPlacedEvent` と `EventPublisher` は、第12章と第23章でも使います。

## 前提

第18章で、`order-events` という topic エクスチェンジを作りました。`common/SimpleJson.java` は配布済みです。

## ステップ1: メッセージの「封筒」と「中身」を分けて考える

メッセージの情報は、AMQP のプロパティ（封筒）と本文（中身）に分けて持たせます。

| 置き場所 | 入れるもの | この章での例 |
| :--- | :--- | :--- |
| プロパティ（標準） | 配送や処理の振り分けに必要な情報 | `messageId`、`type`、`contentType`、`timestamp`、`appId` |
| プロパティ（headers） | 標準にない、配送や処理に関わる情報 | `schema-version` |
| 本文 | 業務の情報そのもの | 注文ID、顧客ID、金額、出来事が起きた時刻 |

**Why**: プロパティに置いた情報は、本文を解析しなくても読めます。受け手は、知らない `type` のイベントを本文を開かずに読み飛ばせます。headers エクスチェンジやトレースのツールなど、ブローカーや周辺の仕組みもプロパティを見て動きます。

## ステップ2: イベントクラスを書く

`src/main/java/com/example/async/message/OrderPlacedEvent.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/message/OrderPlacedEvent.java -->
```java
package com.example.async.message;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.example.async.common.SimpleJson;

/**
 * 「注文が確定した」という事実を表すイベント。イミュータブル。
 * 後の章（冪等性・Outbox）でもこのクラスを使う。
 */
public class OrderPlacedEvent {
    /** イベントの種類。受信側はこれを見て処理を振り分ける */
    public static final String TYPE = "order.placed";
    /** このイベントのスキーマ（項目構成）のバージョン */
    public static final int SCHEMA_VERSION = 1;

    // イベント1件ごとに一意なID。重複検知に使う
    private final String eventId;
    // 注文ID
    private final String orderId;
    // 顧客ID
    private final String customerId;
    // 合計金額（円）
    private final long amount;
    // 出来事が起きた時刻（送信時刻ではない）
    private final Instant occurredAt;

    /**
     * @param eventId    イベントID
     * @param orderId    注文ID
     * @param customerId 顧客ID
     * @param amount     合計金額（円）
     * @param occurredAt 出来事が起きた時刻
     */
    public OrderPlacedEvent(String eventId, String orderId, String customerId, long amount, Instant occurredAt) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.occurredAt = occurredAt;
    }

    /**
     * 新しいイベントIDと現在時刻でイベントを作る。
     *
     * @param orderId    注文ID
     * @param customerId 顧客ID
     * @param amount     合計金額（円）
     * @return 新しいイベント
     */
    public static OrderPlacedEvent create(String orderId, String customerId, long amount) {
        return new OrderPlacedEvent(UUID.randomUUID().toString(), orderId, customerId, amount, Instant.now());
    }

    /** @return イベントID */
    public String eventId() {
        return eventId;
    }

    /** @return 注文ID */
    public String orderId() {
        return orderId;
    }

    /** @return 顧客ID */
    public String customerId() {
        return customerId;
    }

    /** @return 合計金額（円） */
    public long amount() {
        return amount;
    }

    /** @return 出来事が起きた時刻 */
    public Instant occurredAt() {
        return occurredAt;
    }

    /**
     * JSON 文字列に変換する。
     *
     * @return JSON 文字列
     */
    public String toJson() {
        // 項目の順序を保つため LinkedHashMap を使う
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("eventId", eventId);
        fields.put("orderId", orderId);
        fields.put("customerId", customerId);
        fields.put("amount", amount);
        // 時刻は ISO-8601 形式の文字列（例: 2026-11-03T09:00:00Z）にする
        fields.put("occurredAt", occurredAt.toString());
        return SimpleJson.stringify(fields);
    }

    /**
     * JSON 文字列からイベントを復元する。
     *
     * @param json JSON 文字列
     * @return 復元したイベント
     * @throws IllegalArgumentException 必須項目が欠けている、または形式が不正な場合
     */
    public static OrderPlacedEvent fromJson(String json) {
        Map<String, String> m = SimpleJson.parse(json);
        return new OrderPlacedEvent(
                required(m, "eventId"),
                required(m, "orderId"),
                required(m, "customerId"),
                Long.parseLong(required(m, "amount")),
                Instant.parse(required(m, "occurredAt")));
    }

    // 必須項目を取り出す。なければ例外にする
    private static String required(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null) {
            throw new IllegalArgumentException("必須項目 " + key + " がありません");
        }
        return v;
    }
}
```

**Why**: 設計上の判断がいくつか入っています。

- **eventId**: イベント1件ごとに `UUID` で一意な ID を振る。第12章で、同じイベントが2回届いたことを見分けるために使う
- **occurredAt**: 送信した時刻ではなく、**出来事が起きた時刻** を持たせる。送信が遅れたり再送されたりしても、いつの出来事かがわかる
- **amount を long にする**: 金額を `double` で持つと、`0.1 + 0.2` が `0.30000000000000004` になるような誤差が出る。円なら整数で持つ
- **イミュータブル**: 一度発行したイベントは「過去の事実」なので、後から書き換えられないようにする
- **fromJson が知らない項目を無視する**: 送り手が項目を追加しても、受け手は壊れない（ステップ6で扱う）

## ステップ3: メタデータを付けて発行する部品を書く

`src/main/java/com/example/async/message/EventPublisher.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/message/EventPublisher.java -->
```java
package com.example.async.message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

/**
 * OrderPlacedEvent を、メタデータ（プロパティとヘッダー）付きで発行する部品。
 * 後の章でもこのクラスを使って発行する。
 */
public class EventPublisher {
    // 発行に使うチャネル
    private final Channel channel;
    // 発行先のエクスチェンジ
    private final String exchange;

    /**
     * @param channel  発行に使うチャネル
     * @param exchange 発行先のエクスチェンジ名
     */
    public EventPublisher(Channel channel, String exchange) {
        this.channel = channel;
        this.exchange = exchange;
    }

    /**
     * イベントをルーティングキー order.placed.jp で発行する。
     *
     * @param event 発行するイベント
     * @throws IOException 送信に失敗した場合
     */
    public void publish(OrderPlacedEvent event) throws IOException {
        // メッセージのプロパティ（AMQP が定める標準のメタデータ）を組み立てる
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                // 受信側が重複を見分けるための一意なID
                .messageId(event.eventId())
                // 何のイベントか
                .type(OrderPlacedEvent.TYPE)
                // 本文の形式と文字コード
                .contentType("application/json")
                .contentEncoding("UTF-8")
                // 発行した時刻（AMQP の timestamp は秒単位）
                .timestamp(new Date())
                // どのアプリが発行したか
                .appId("order-service")
                // 2 = persistent（ディスクに保存してほしい）
                .deliveryMode(2)
                // 標準プロパティにない情報は headers に入れる
                .headers(Map.of("schema-version", OrderPlacedEvent.SCHEMA_VERSION))
                .build();
        // 本文は JSON を UTF-8 のバイト列にしたもの
        channel.basicPublish(exchange, "order.placed.jp", props, event.toJson().getBytes(StandardCharsets.UTF_8));
    }
}
```

**Why**: `AMQP.BasicProperties.Builder` で、第6章で紹介したプロパティを組み立てています。`messageId` にイベントの `eventId` を入れているのがポイントです。受け手は本文を解析しなくても、プロパティだけで「このメッセージは何者か」を判断できます。

**⚠️ よくある失敗・誤解**: AMQP の `timestamp` プロパティは **秒単位** です。`new Date()` を渡してもミリ秒は切り捨てられます。ミリ秒以下の精度が必要な時刻は、本文の `occurredAt` のように別に持たせます。

## ステップ4: 発行するアプリを書く

`src/main/java/com/example/async/message/OrderPlacedEventPublisher.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/message/OrderPlacedEventPublisher.java -->
```java
package com.example.async.message;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.example.async.topic.OrderEventTopicPublisher;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * OrderPlacedEvent を order-events エクスチェンジへ発行するアプリ。
 */
public class OrderPlacedEventPublisher {

    /**
     * 引数で指定した件数（省略時は2件）のイベントを発行する。
     *
     * @param args 発行する件数
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 2;
        try (Connection connection = RabbitConnection.open("order-placed-event-publisher");
                Channel channel = connection.createChannel()) {
            // 第18章で作った topic エクスチェンジを使う
            channel.exchangeDeclare(OrderEventTopicPublisher.EXCHANGE, BuiltinExchangeType.TOPIC, true);
            EventPublisher publisher = new EventPublisher(channel, OrderEventTopicPublisher.EXCHANGE);
            for (int i = 1; i <= count; i++) {
                // 注文ID・顧客ID・金額からイベントを作る
                OrderPlacedEvent event = OrderPlacedEvent.create("A-" + i, "C-1", 1000L * i);
                publisher.publish(event);
                Console.log("発行: " + event.toJson());
            }
        }
    }
}
```

## ステップ5: メタデータを読んで解釈するサブスクライバーを書く

`src/main/java/com/example/async/message/EventInspector.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/message/EventInspector.java -->
```java
package com.example.async.message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.example.async.topic.OrderEventTopicPublisher;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 受信したメッセージのプロパティ・ヘッダー・本文を表示し、イベントとして解釈するサブスクライバー。
 */
public class EventInspector extends DefaultConsumer {

    /**
     * @param channel 使うチャネル
     */
    public EventInspector(Channel channel) {
        super(channel);
    }

    /**
     * メタデータを表示し、type を見てから本文を解釈する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        // 標準プロパティを表示する
        Console.log("messageId=" + properties.getMessageId() + " type=" + properties.getType()
                + " contentType=" + properties.getContentType() + " appId=" + properties.getAppId()
                + " timestamp=" + properties.getTimestamp());
        // 独自ヘッダーを表示する
        Console.log("headers=" + properties.getHeaders());
        String json = new String(body, StandardCharsets.UTF_8);
        // type を見て、知っている種類のイベントだけ解釈する
        if (OrderPlacedEvent.TYPE.equals(properties.getType())) {
            OrderPlacedEvent event = OrderPlacedEvent.fromJson(json);
            Console.log("注文確定イベント orderId=" + event.orderId() + " amount=" + event.amount()
                    + " occurredAt=" + event.occurredAt());
        } else {
            // 知らない種類は無視する（エラーにしない）
            Console.log("未知の type のためスキップ: " + json);
        }
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * order-events の全イベントを購読する。
     *
     * @param args 使わない
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        Connection connection = RabbitConnection.open("event-inspector");
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(OrderEventTopicPublisher.EXCHANGE, BuiltinExchangeType.TOPIC, true);
        String queue = OrderEventTopicPublisher.EXCHANGE + ".inspector";
        channel.queueDeclare(queue, true, false, false, null);
        // すべてのイベントを受け取る
        channel.queueBind(queue, OrderEventTopicPublisher.EXCHANGE, "#");
        channel.basicConsume(queue, false, new EventInspector(channel));
        Console.log(queue + " を購読中です。Ctrl+C で終了します");
    }
}
```

起動して、別のターミナルから発行します。

```bash
./run.sh message.EventInspector
```

```bash
./run.sh message.OrderPlacedEventPublisher 1
```

```text
[09:18:51.923][pool-1-thread-4] messageId=116b65b6-5168-4197-a6a9-2104d5616b49 type=order.placed contentType=application/json appId=order-service timestamp=Tue Nov 03 09:18:51 UTC 2026
[09:18:51.924][pool-1-thread-4] headers={schema-version=1}
[09:18:51.931][pool-1-thread-4] 注文確定イベント orderId=A-1 amount=1000 occurredAt=2026-11-03T09:18:51.902885929Z
```

管理画面でも確認できます。`EventInspector` を止めた状態で発行し、Queues and Streams タブで `order-events.inspector` キューを開きます。「Get messages」欄の「Get Message(s)」ボタンを押すと、プロパティと本文を見られます。

**Why**: `EventInspector` は、本文を解析する前に `type` を確かめています。第18章の `OrderEventTopicPublisher` のように `type` を付けずに発行されたメッセージは、解析せずに読み飛ばします。

**🔍 確認・実験してみよう**: `EventInspector` を起動したまま `./run.sh topic.OrderEventTopicPublisher` を実行すると、どう表示されるでしょうか。`type` がないメッセージを、もし `OrderPlacedEvent.fromJson` で解析しようとしたら何が起きるかも考えてみましょう。

## ステップ6: スキーマの変更に備える

サービスを運用していると、イベントの項目は必ず変わります。どのような変更なら受け手を壊さないかを整理しておきます。

| 変更 | 受け手への影響 | 対応 |
| :--- | :--- | :--- |
| 項目を **追加** する | 知らない項目を無視する受け手なら壊れない | そのまま追加してよい |
| 項目を **削除・改名** する | その項目を読む受け手が壊れる | `schema-version` を上げる。旧版と新版をしばらく並行して発行する |
| 項目の **意味や型** を変える | 受け手が誤った処理をする（壊れたことに気づきにくい） | 新しい `type` のイベントとして別に定義する |

**Why**: 「受け手は知らない項目を無視し、必要な項目だけを読む」という作り方を **寛容な読み手（Tolerant Reader）** と呼びます。`OrderPlacedEvent.fromJson` は、`SimpleJson.parse` が返した Map から必要なキーだけを取り出しているので、項目が増えても壊れません。

**💼 現場Tip**: イベントの仕様を組織で管理するための仕組みがあります。

- **CloudEvents**: イベントのメタデータ（`id`・`source`・`type`・`time` など）の標準仕様。製品をまたいで同じ形式を使える
- **AsyncAPI**: OpenAPI のメッセージング版。どのチャネルにどんなメッセージが流れるかを文書化できる
- **スキーマレジストリ**: Kafka などで、イベントのスキーマを登録し、互換性のない変更を発行前に検出する

## ステップ7: 本文に何を入れるかを決める

イベントの本文にどこまで情報を入れるかには、2つの考え方があります。

| 方式 | 本文の中身 | 利点 | 欠点 |
| :--- | :--- | :--- | :--- |
| 通知型（薄いイベント） | 注文IDだけ | 本文が小さい。最新の情報は API で取りに行く | 受け手が注文サービスの API を呼ぶ必要があり、結局つながりが残る |
| 状態転送型（厚いイベント） | 注文の内容（顧客・金額・明細など） | 受け手は本文だけで処理できる | 本文が大きくなる。個人情報が多くの受け手に広がる |

この講座の `OrderPlacedEvent` は、ポイント付与に必要な情報を持つ状態転送型です。

**💼 現場Tip**: メッセージはブローカーに保存され、管理画面や DLQ（第13章）から中身を見られます。クレジットカード番号やパスワードなどの機密情報は、本文に入れないのが原則です。大きなファイル（画像や CSV）も本文には入れず、ストレージへ置いてその場所だけを送ります。これを **Claim Check パターン** と呼びます。

**💼 現場Tip**: 処理が複数のサービスにまたがると、「この注文のポイント付与はどこで止まったのか」を追うのが難しくなります。HTTP のリクエストで受け取ったトレース ID（W3C Trace Context の `traceparent` など）を headers に入れて引き継ぐと、分散トレーシングのツールで一連の処理をつなげて見られます。

## 動作確認

`EventInspector` を起動して `OrderPlacedEventPublisher` を実行し、次の3つが表示されたら完了です。

1. `messageId`・`type=order.placed`・`contentType=application/json` などのプロパティ
2. `headers={schema-version=1}`
3. 本文から復元した `orderId`・`amount`・`occurredAt`

## もっと良くしてみよう

### 理解確認

1. `messageId` と `type` を、本文ではなくプロパティに入れるのはなぜでしょうか。
2. イベントに、送信時刻ではなく「出来事が起きた時刻」を持たせるのはなぜでしょうか。
3. イベントの項目を追加しても受け手が壊れないようにするには、受け手をどう作ればよいでしょうか。

### 発展改善クイズ

1. `OrderPlacedEvent` に「割引額」を追加することになりました。古い受け手と新しい受け手が混在する期間に、問題が起きないようにするには、どう進めればよいでしょうか。
2. 今の `EventPublisher` は、`basicPublish` が例外を投げなければ「送れた」とみなしています。ブローカーが本当にメッセージを受け取ったかどうかは、どうすれば確かめられるでしょうか。

---

| [← 第18章: topicで受け取るイベントを選ぶ](./18-topic.md) | [全章目次](./README.md) | [第20章: メッセージの順序を考える →](./20-ordering.md) |
| - | :-: | --: |
