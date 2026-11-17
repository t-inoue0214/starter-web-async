# 第12章: 重複メッセージと冪等なコンシューマー

## この章のゴール

同じポイント付与のジョブが2回届くと、ポイントが二重に付与されることを体験します。メッセージのIDを記録して重複を弾く、**冪等（べきとう）な受け手（コンシューマー）** の基本を学びます。

## Why: なぜこれを学ぶのか

第11章で、消失を防ごうとすると重複が起きうることを学びました。受け手（コンシューマー）が処理完了の通知（ack）を返す直前に落ちれば、届け直し（再配達）が起き、送り手（パブリッシャー）が確認のタイムアウトで再送すれば、同じメッセージが2通届きます。

つまり、重複は「起きたら困るバグ」ではなく **「必ず起きる前提」** です。重複を防ぐのはブローカーではなく、受け手（コンシューマー）の仕事になります。具体的には、「同じメッセージが何度届いても、結果が変わらない」ように受け手（コンシューマー）を作ります。これを冪等にする、と呼びます。メッセージングを使う業務システムで、いちばん大切な考え方の1つです。

## 前提

第10章で作ったジョブキューの形を使います。ジョブには、ジョブごとに一意な `messageId`（メッセージのID）を付けます。第11章で、受け手（コンシューマー）が落ちると再配達が起きることを体験しました。

## ステップ1: 重複が起きる3つの場面

| 場面 | 何が起きるか |
| :--- | :--- |
| 受け手（コンシューマー）が ack の直前に落ちた | ブローカーは処理済みと知らないので、再配達する |
| 送り手（パブリッシャー）が確認のタイムアウトで再送した | ブローカーが受け取っていたら、同じメッセージが2通になる |
| リトライや運用での再投入 | 失敗した分をやり直した結果、成功済みの分と重なる |

1つめの場面を図にすると、次のようになります。

```mermaid
sequenceDiagram
    participant B as RabbitMQ
    participant C as ポイント付与ワーカー
    participant DB as ポイント残高
    B->>C: A-1 +100pt
    C->>DB: 100pt 加算
    Note over C: ack を返す直前に落ちて、再起動する
    B->>C: A-1 +100pt（再配達）
    C->>DB: 100pt 加算（2回目）
    C-->>B: ack
    Note over DB: 合計 200pt 増えてしまう
```

## ステップ2: 冪等とは何かを知る

**冪等** とは、同じ操作を何回行っても、1回行ったときと結果が変わらない性質のことです。

| 操作 | 冪等か | 理由 |
| :--- | :--- | :--- |
| 注文の状態を「支払済」にする | 冪等 | 2回実行しても「支払済」のまま |
| 商品の価格を 1000 円に設定する | 冪等 | 2回実行しても 1000 円のまま |
| ポイント残高に 100 を **加算** する | 冪等ではない | 2回実行すると 200 増える |
| 確認メールを **送信** する | 冪等ではない | 2回実行すると2通届く |

冪等でない処理を冪等にするには、主に次の方法があります。

1. **操作を冪等な形に変える**: 「加算する」ではなく「注文 A-1 の付与ポイントを 100 に設定する」と記録する
2. **処理済みの ID を記録する**: 一度処理したメッセージの ID を覚え、同じ ID が来たら処理しない
3. **条件付きで更新する**: 「バージョンが 3 のときだけ 4 に更新する」のように、状態を確かめてから更新する（第19章）

この章では、2つめの方法を体験します。

## ステップ3: ポイントの保存先と、重複を起こす送り手（パブリッシャー）を読む

学習用に、DB の代わりにメモリ上へポイントと処理済みIDを保存するクラスを使います。[PointStore.java](../src/main/java/com/example/async/idempotency/PointStore.java) を開きます。中心となる部分を抜粋します。

<!-- code: src/main/java/com/example/async/idempotency/PointStore.java#addPointsOnce -->
```java
/**
 * まだ処理していないメッセージIDのときだけ、ポイントを加算する。
 *
 * @param messageId  メッセージID
 * @param customerId 顧客ID
 * @param points     加算するポイント
 * @return 加算したら true、処理済みだったら false
 */
public synchronized boolean addPointsOnce(String messageId, String customerId, long points) {
    // Set.add は、すでに入っていると false を返す。それが「処理済み」の判定になる
    if (!processedMessageIds.add(messageId)) {
        return false;
    }
    addPoints(customerId, points);
    return true;
}
```

**Why**: `addPointsOnce` が主役です。`Set.add` は、すでに入っている ID を追加しようとすると `false` を返します。これを「処理済み」の判定に使っています。`synchronized` を付けているのは、受信処理（`handleDelivery`）が複数のスレッドから呼ばれることがあるためです。

次に、同じジョブを2回積む送り手（パブリッシャー）です。[DuplicatePublisher.java](../src/main/java/com/example/async/idempotency/DuplicatePublisher.java) を開きます。中心となる部分を抜粋します。

<!-- code: src/main/java/com/example/async/idempotency/DuplicatePublisher.java#publish -->
```java
// messageId を付けて、永続メッセージとして1件積む
private static void publish(Channel channel, String jobId, String body) throws Exception {
    AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
            .messageId(jobId).deliveryMode(2).build();
    channel.basicPublish("", PointGrantConsumer.QUEUE, props, body.getBytes(StandardCharsets.UTF_8));
}
```

**Why**: `job-A-1` という **同じジョブID** で2回積んでいるので、2回とも同じ `messageId` が付きます。「確認がタイムアウトしたので再送した」状況の再現です。

## ステップ4: ポイントを付与する受け手（コンシューマー）を読む

[PointGrantConsumer.java](../src/main/java/com/example/async/idempotency/PointGrantConsumer.java) を開きます。起動引数で、重複を気にしない `naive` と、重複を弾く `idempotent` を切り替えます。中心となる部分を抜粋します。

<!-- code: src/main/java/com/example/async/idempotency/PointGrantConsumer.java#handleDelivery -->
```java
/**
 * ジョブを解釈してポイントを付与し、保存が終わってから ack する。
 */
@Override
public void handleDelivery(String consumerTag, Envelope envelope,
        AMQP.BasicProperties properties, byte[] body) throws IOException {
    // 本文は「注文ID,顧客ID,金額」
    String[] parts = new String(body, StandardCharsets.UTF_8).split(",");
    String orderId = parts[0];
    String customerId = parts[1];
    // 金額の1%をポイントにする
    long points = Long.parseLong(parts[2]) / 100;
    if (idempotent) {
        // メッセージIDで「すでに処理したか」を判定しながら加算する
        boolean applied = store.addPointsOnce(properties.getMessageId(), customerId, points);
        Console.log((applied ? "付与 " : "重複のためスキップ ") + orderId + " +" + points
                + "pt → 残高 " + store.balance(customerId) + "pt");
    } else {
        // 何も考えずに加算する
        store.addPoints(customerId, points);
        Console.log("付与 " + orderId + " +" + points + "pt → 残高 " + store.balance(customerId) + "pt");
    }
    // 保存が終わってから ack する（逆順だと、ack 後に落ちたときポイントが消える）
    getChannel().basicAck(envelope.getDeliveryTag(), false);
}
```

## ステップ5: 素朴な受け手（コンシューマー）で重複を体験する

```bash
./run.sh idempotency.PointGrantConsumer naive
```

別のターミナルで、重複を起こす送り手（パブリッシャー）を実行します。

```bash
./run.sh idempotency.DuplicatePublisher
```

```text
[09:22:21.645][pool-1-thread-4] 付与 A-1 +100pt → 残高 100pt
[09:22:21.647][pool-1-thread-5] 付与 A-1 +100pt → 残高 200pt
[09:22:21.648][pool-1-thread-5] 付与 A-2 +50pt → 残高 250pt
```

正しくは 150pt のはずが、250pt になりました。A-1 のポイントが二重に付与されています。

## ステップ6: 冪等な受け手（コンシューマー）で重複を弾く

`naive` の受け手（コンシューマー）を Ctrl+C で止め、`idempotent` で起動し直します。

```bash
./run.sh idempotency.PointGrantConsumer idempotent
```

もう一度、送り手（パブリッシャー）を実行します。

```bash
./run.sh idempotency.DuplicatePublisher
```

```text
[09:22:33.409][pool-1-thread-4] 付与 A-1 +100pt → 残高 100pt
[09:22:33.417][pool-1-thread-5] 重複のためスキップ A-1 +100pt → 残高 100pt
[09:22:33.420][pool-1-thread-5] 付与 A-2 +50pt → 残高 150pt
```

2回目の A-1 がスキップされ、残高は正しく 150pt になりました。

冪等な受け手（コンシューマー）の判定の流れは、次のとおりです。

```mermaid
flowchart TD
    R[メッセージを受信] --> D{"messageId は<br>処理済みか"}
    D -->|"まだ"| A[ポイントを加算し<br>ID を記録する]
    D -->|"処理済み"| S[何もしない]
    A --> ACK[ack を返す]
    S --> ACK
```

**Why**: 重複と判断したメッセージにも、**ack を返している** ことに注意してください。「処理済み」は「成功」と同じ扱いです。ack を返さないと、そのメッセージは何度も届き直します。

**💼 現場Tip**: ポイントの二重付与は、お客さまには気づかれにくくても、会社には直接の損失です。逆に二重の **減算**（決済の二重引き落としなど）は、すぐに問い合わせや信用問題になります。金額や在庫を扱う受け手（コンシューマー）は、必ず冪等に作ります。

## ステップ7: 本番で気をつけること

この章の `PointStore` は、学習用にメモリ上へ記録しています。実際の業務では、次の点に気をつけます。

**記録はデータベースに保存する**: メモリ上の記録は、受け手（コンシューマー）を再起動すると消えます。DB の一意制約（同じ値を2つ入れられない決まり。主キーもその1つ）を使うと、確実に重複を弾けます。骨格は次のとおりです。

```sql
-- 処理済みのメッセージID。主キーなので、同じIDは2回 INSERT できない
CREATE TABLE processed_messages (
  message_id   VARCHAR(100) PRIMARY KEY,
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**記録と業務データの更新は、同じトランザクションで行う**: トランザクションとは、複数のDB操作を「全部成功」か「全部なかったこと」のどちらかにまとめる仕組みです。1つのトランザクションの中で `processed_messages` に INSERT し、続けてポイントを加算して、まとめてコミットします。

| 落ちた場所 | DB の状態 | 再配達されたとき |
| :--- | :--- | :--- |
| コミット前 | 記録も加算もない | 最初から処理し直す（正しい） |
| コミット後・ack 前 | 記録も加算もある | 処理済みとして、スキップして ack する（正しい） |

別々のトランザクションにすると穴が空きます。加算を先に確定して記録の前に落ちれば二重加算になり、記録を先に確定して加算の前に落ちればポイントが付きません。

**そのほかの注意点**:

- 送り手（パブリッシャー）が `messageId` を付け忘れると、重複を見分けられません。受け手（コンシューマー）で `messageId` がないメッセージを拒否するのも手です
- 送り手（パブリッシャー）が **再送のたびに新しいID** を振ると、重複を見分けられません。IDは、ジョブを作った時点で1回だけ作ります
- 処理済みの記録は増え続けます。再配達や再送が起こりうる期間（数日〜数週間）が過ぎたものは、定期的に削除します
- メール送信や外部の決済 API は、DB のトランザクションに入れられません。多くの決済 API が用意している **冪等キー**（Idempotency-Key）に、`messageId` を渡します
- 「ちょうど1回だけ届ける」をうたう機能が、製品によってはあります。しかし多くは **ブローカーの中だけ** の話で、受け手（コンシューマー）の DB 更新や外部 API まで含めると、この章のような受け手（コンシューマー）の工夫が必要です

## 動作確認

次の2点を確認できたら完了です。

1. `naive` では残高が 250pt（二重付与）になった
2. `idempotent` では2回目の A-1 がスキップされ、残高が 150pt になった

## もっと良くしてみよう

### 理解確認

1. ポイントの「加算」が冪等でないのはなぜでしょうか。冪等な形に変えるとしたら、どう記録すればよいでしょうか。
2. 重複としてスキップしたメッセージにも、ack を返すのはなぜでしょうか。
3. 処理済みの記録とポイントの加算を、別々のトランザクションにすると何が起きるでしょうか。

### 発展改善クイズ

1. `PointStore` は、受信プログラムを再起動すると記録が消えます。再起動の直後に、再配達されたメッセージが届くと、どうなるでしょうか。
2. 本文の JSON が壊れていて、何度処理しても例外になるメッセージが届いたら、`PointGrantConsumer` はどうなるでしょうか。次の章で、その行き先を学びます。

---

| [← 第11章: メッセージが消えないために](./11-not-losing-messages.md) | [全章目次](./README.md) | [第13章: 処理できないメッセージとデッドレターキュー →](./13-dead-letter-queue.md) |
| - | :-: | --: |
