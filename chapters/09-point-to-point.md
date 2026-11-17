# 第9章: キューに送ってDefaultConsumerで受け取る

## この章のゴール

`hello` キューにメッセージを送る送り手（パブリッシャー）と、`DefaultConsumer` を継承してプッシュ型で受け取る受け手（コンシューマー）を作ります。HTTP のポーリングと違い、メッセージが届いた瞬間に受け取れることを体験します。

## Why: なぜこれを学ぶのか

第7章のポーリングでは、遅延と空振りのどちらかを我慢する必要がありました。AMQP の `basic.consume` を使うと、受け手（コンシューマー）は「このキューのメッセージを受け取ります」と一度伝えるだけで済みます。この「受け取りの登録」を、以後 **購読** と呼びます。あとはブローカーが、メッセージが届くたびに押し出してきます。

この章で作る「送る側」と「受け取る側」の形は、以後のすべての章の土台になります。

## 前提

第8章で `amqp-client` を追加し、`RabbitConnection.open(...)` で接続できるようになりました。

## ステップ1: 送り手（パブリッシャー）を読む

[HelloProducer.java](../src/main/java/com/example/async/p2p/HelloProducer.java) を開いて、コードを読んでみましょう。

**Why**: `queueDeclare` の引数は次の意味を持ちます。

| 引数 | 値 | 意味 |
| :--- | :--- | :--- |
| `queue` | `"hello"` | キュー名 |
| `durable` | `true` | ブローカーを再起動してもキューの定義を残す |
| `exclusive` | `false` | この接続専用にしない（他の接続からも使える） |
| `autoDelete` | `false` | 受け手（コンシューマー）がいなくなっても削除しない |
| `arguments` | `null` | その他の設定（第13章で使う） |

`basicPublish("", QUEUE, null, body)` は、第6章で学んだ **既定エクスチェンジ** に、ルーティングキー＝キュー名で送っています。本文は `byte[]` で渡します。ブローカーは本文の中身を解釈しないので、文字列なら送る側で文字コードを決めてバイト列にします。

## ステップ2: 受け手（コンシューマー）がいない状態で送る

受け手（コンシューマー）を起動する前に、送り手（パブリッシャー）だけを実行してみます。

```bash
./run.sh p2p.HelloProducer
```

```text
[09:16:04.336][main] 送信: こんにちは #1
[09:16:04.341][main] 送信: こんにちは #2
[09:16:04.342][main] 送信: こんにちは #3
```

管理画面の Queues and Streams タブで `hello` キューを見ると、`Ready` が 3 になっています。

**Why**: 受け手（コンシューマー）がまだ存在しないのに、送信は成功しました。メッセージはキューにたまり、受け手（コンシューマー）が来るのを待っています。このように、送る側と受け取る側が同時に動いていなくてもよいことを、**時間の切り離し** と呼びます。第15章でも取り上げます。

## ステップ3: DefaultConsumer を継承して受け手（コンシューマー）を読む

[HelloConsumer.java](../src/main/java/com/example/async/p2p/HelloConsumer.java) を開きます。中心となる部分を抜粋します。

<!-- code: src/main/java/com/example/async/p2p/HelloConsumer.java#handleDelivery -->
```java
/**
 * ブローカーからメッセージが「押し出されて」きたときに、ライブラリのスレッドから呼ばれる。
 */
@Override
public void handleDelivery(String consumerTag, Envelope envelope,
        AMQP.BasicProperties properties, byte[] body) throws IOException {
    // バイト列を送信側と同じ文字コードで文字列に戻す
    String text = new String(body, StandardCharsets.UTF_8);
    // deliveryTag はこのチャネル内での配達番号（1, 2, 3, ...）
    Console.log("受信: " + text + " (deliveryTag=" + envelope.getDeliveryTag() + ")");
}
```

**Why**: `DefaultConsumer` は、ブローカーからの通知を受け取るためのメソッドを、何もしない形で一通り実装したクラスです。必要なメソッドだけを上書き（オーバーライド）して使います。

| メソッド | 呼ばれるとき |
| :--- | :--- |
| `handleDelivery` | メッセージが届いたとき |
| `handleConsumeOk` | 購読（受け取りの登録）をブローカーが受け付けたとき |
| `handleCancel` | キューが削除されるなど、ブローカー側から購読を打ち切られたとき |
| `handleShutdownSignal` | チャネルや接続が閉じたとき |

`handleDelivery` の引数のうち、`Envelope` は届いたメッセージの宛名情報です。届いたエクスチェンジ・ルーティングキー・配達番号（deliveryTag）・再配達フラグを持っています。`AMQP.BasicProperties` は、送る側の付けたプロパティです。

`main` の最後で接続を閉じていないことにも注目してください。`basicConsume` はすぐに戻ってきますが、ライブラリが内部で動かしているスレッドが残るため、プログラムは終了せずに待ち続けます。

## ステップ4: 受け手（コンシューマー）を起動する

```bash
./run.sh p2p.HelloConsumer
```

```text
[09:16:07.560][pool-1-thread-4] 受信: こんにちは #1 (deliveryTag=1)
[09:16:07.560][main] hello キューを購読中です。Ctrl+C で終了します
[09:16:07.564][pool-1-thread-4] 受信: こんにちは #2 (deliveryTag=2)
[09:16:07.564][pool-1-thread-4] 受信: こんにちは #3 (deliveryTag=3)
```

キューにたまっていた3件が、起動と同時に届きました。管理画面の `hello` キューの `Ready` は 0 になっています。

受け手（コンシューマー）を起動したまま、別のターミナルで送り手（パブリッシャー）をもう一度実行します。

```bash
./run.sh p2p.HelloProducer
```

送信した瞬間に、受け手（コンシューマー）側に「受信」が表示されます。ログの時刻を見比べると、送信から受信までの差は数ミリ秒です。

**Why**: 第7章のポーリングとの違いを確かめましょう。受け手（コンシューマー）は何もリクエストを送っていません。ブローカーがメッセージを受け取った瞬間に `basic.deliver` で押し出し、ライブラリが `handleDelivery` を呼び出しています。

送信から受信までの流れを図にすると、次のようになります。受け手（コンシューマー）が購読を伝えるのは最初の1回だけです。

```mermaid
sequenceDiagram
    participant P as HelloProducer
    participant B as RabbitMQ
    participant C as HelloConsumer
    C->>B: basic.consume（購読を1回だけ伝える）
    B-->>C: basic.consume-ok
    P->>B: basic.publish こんにちは #1
    B->>C: basic.deliver こんにちは #1
    Note over C: handleDelivery が呼ばれる
    P->>B: basic.publish こんにちは #2
    B->>C: basic.deliver こんにちは #2
```

ログのスレッド名が `main` ではなく `pool-1-thread-4` になっていることにも注目してください。`handleDelivery` は、ライブラリが用意したスレッドプールのスレッドから呼ばれます。

**⚠️ よくある失敗・誤解**: 1つのチャネルに届いたメッセージの `handleDelivery` は、**1件ずつ順番に** 呼ばれます。`handleDelivery` の中で時間のかかる処理をすると、その間、同じチャネルの次のメッセージは待たされます。「受信が遅い」と感じたら、まず `handleDelivery` の中で何に時間がかかっているかを疑いましょう。

## ステップ5: 受け手（コンシューマー）を2つ起動する

受け手（コンシューマー）を Ctrl+C で止め、2つのターミナルでそれぞれ起動し直します。

```bash
./run.sh p2p.HelloConsumer
```

3つめのターミナルで、6件送ります。

```bash
./run.sh p2p.HelloProducer 6
```

1つめの受け手（コンシューマー）には `#1`・`#3`・`#5`、2つめには `#2`・`#4`・`#6` が届きます。

**Why**: 1つのキューに複数の受け手（コンシューマー）がいると、ブローカーはメッセージを **順番に1件ずつ配ります（ラウンドロビン）**。1件のメッセージは、どちらか一方にしか届きません。1件のメッセージを、受け取る1人だけに渡すこの形を **P2P（ポイント・ツー・ポイント）** と呼びます。受け手（コンシューマー）を増やすだけで、処理を分担させられます。

配られ方を図にすると、次のようになります。

```mermaid
flowchart LR
    P[HelloProducer] --> Q[(hello)]
    Q -->|"#1, #3, #5"| C1[コンシューマー 1]
    Q -->|"#2, #4, #6"| C2[コンシューマー 2]
```

**🔍 確認・実験してみよう**: 受け手（コンシューマー）を2つ起動したまま、管理画面で `hello` キューを開き、下のほうにある「Delete」ボタンでキューを削除してみましょう。受け手（コンシューマー）には何が起きるでしょうか。`handleCancel` を上書きしてログを出すようにすると、何がわかるか試してみましょう。

**💼 現場Tip**: `DefaultConsumer` を継承する方法のほかに、ラムダ式で書ける `DeliverCallback` を使う方法もあります。

```java
// ラムダ式で受信処理を渡す書き方。中身は handleDelivery と同じ
channel.basicConsume("hello", true,
        (consumerTag, delivery) -> Console.log(new String(delivery.getBody(), StandardCharsets.UTF_8)),
        consumerTag -> Console.log("購読が打ち切られました"));
```

短い処理ならラムダ式が手軽です。この講座では、受信処理を1つのクラスとして名前を付けて扱え、`handleCancel` などの他の通知も同じクラスで受けられる `DefaultConsumer` の継承を使います。

## 動作確認

次の3つを確認できたら完了です。

1. 受け手（コンシューマー）がいない状態で送ったメッセージが、管理画面の `Ready` に残った
2. 受け手（コンシューマー）を起動すると、たまっていたメッセージがすぐに届いた
3. 受け手（コンシューマー）を2つ起動すると、メッセージが交互に分配された

## もっと良くしてみよう

### 理解確認

1. 送り手（パブリッシャー）と受け手（コンシューマー）の両方で `queueDeclare` しているのはなぜでしょうか。
2. `handleDelivery` はどのスレッドで呼ばれるでしょうか。`main` メソッドが終わってもプログラムが終了しないのはなぜでしょうか。
3. 1つのキューに受け手（コンシューマー）が2つあるとき、1件のメッセージは何人に届くでしょうか。

### 発展改善クイズ

1. `basicConsume` の第2引数 `autoAck` を `true` にしています。`handleDelivery` の処理の途中でプログラムが落ちたら、そのメッセージはどうなるでしょうか。
2. 処理に3秒かかるメッセージと0.3秒で終わるメッセージが交互に届く場合、ラウンドロビンで2つの受け手（コンシューマー）に配ると何が起きそうでしょうか。

---

| [← 第8章: Javaクライアントを導入して接続する](./08-java-client-setup.md) | [全章目次](./README.md) | [第10章: ジョブキューで時間のかかる処理を任せる →](./10-job-queue.md) |
| - | :-: | --: |
