# 第21章: ジョブの連鎖　ワークフロー

## この章のゴール

注文確定から、決済・在庫・発送へと進む処理を、各手順が前の手順の結果を聞いて次のジョブを積む **連鎖** として作ります。途中で在庫切れが起きると「決済は済んだのに在庫がない」状態になり、しかも誰も全体の状況を把握していないことを体験します。

## Why: なぜこれを学ぶのか

第17章の fanout では、1つの出来事（注文確定）を、メール・在庫・ポイントの全員に **同時に、別々に** 知らせました。それぞれの処理は独立していて、順番も関係ありませんでした。

ところが現場には、順番に意味のある処理があります。決済が済むまで在庫は確保できず、在庫を確保するまで発送はできません。**1つの出来事から、ジョブが次々とつながっていく** この形が、ワークフロー（またはパイプライン）です。Pub/Sub との違いを、体験して確かめましょう。

## 前提

第10〜13章でジョブキューの基本を、第17〜18章で topic エクスチェンジを学びました。第3章の地図の④にあたる章です。

## ステップ1: ワークフローとは何か

```mermaid
flowchart LR
    S[注文確定] --> P[決済] --> I[在庫引当] --> D[発送手配]
```

| 観点 | Pub/Sub（第17章の fanout） | ワークフロー（この章） |
| :--- | :--- | :--- |
| 受け手どうしの関係 | 独立している。同時に動いてよい | **依存している**。前の手順が終わってから動く |
| 順序 | 関係ない | 手順の順序が業務の意味を持つ |
| 全体で見ると | 別々の処理の集まり | **1つの業務** を、複数の手順で実現している |
| 失敗したとき | その受け手の問題 | 前の手順の結果を元に戻す必要が出る |

**Why**: 土台（ジョブキュー）に **「ジョブが終わったら次のジョブを積む」** が加わった形です。第3章の地図で、④ ワークフローは「ジョブが連鎖する」と整理しました。

## ステップ2: 連鎖を作る

各手順は、同じ形をしています。「ある合図（ルーティングキー）を受け取る → 手順を実行する → 結果の合図を発行する」です。この共通の形を `StepConsumer` に書きます。`src/main/java/com/example/async/workflow/StepConsumer.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/workflow/StepConsumer.java -->
```java
package com.example.async.workflow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import com.example.async.common.Console;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * ワークフローの「1つの手順」を担当するコンシューマー。
 * 指定したルーティングキーのメッセージ（注文ID）を受け取り、手順を実行して、結果のルーティングキーで次のメッセージを発行する。
 */
public class StepConsumer extends DefaultConsumer {
    /** ワークフロー用の topic エクスチェンジ名 */
    public static final String EXCHANGE = "workflow-events";
    // ログに出す手順名
    private final String stepName;
    // 手順の中身。注文IDを受け取り、次に発行するルーティングキーを返す（発行しないなら null）
    private final Function<String, String> action;

    /**
     * @param channel  使うチャネル
     * @param stepName 手順名
     * @param action   手順の中身
     */
    public StepConsumer(Channel channel, String stepName, Function<String, String> action) {
        super(channel);
        this.stepName = stepName;
        this.action = action;
    }

    /**
     * 注文IDを受け取って手順を実行し、結果を次のメッセージとして発行する。
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        String orderId = new String(body, StandardCharsets.UTF_8);
        // 手順を実行して、次に発行するルーティングキーを得る
        String next = action.apply(orderId);
        if (next != null) {
            getChannel().basicPublish(EXCHANGE, next, null, orderId.getBytes(StandardCharsets.UTF_8));
            Console.log("  [" + stepName + "] " + orderId + " → " + next + " を発行");
        }
        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }

    /**
     * 手順を購読する。キューは接続専用（exclusive）・切断時に自動削除で作る。
     * 実験を繰り返しても、前回の残りメッセージが混ざらないようにするため。
     *
     * @param channel    使うチャネル
     * @param stepName   手順名（キュー名にも使う）
     * @param bindingKey 受け取るルーティングキー
     * @param action     手順の中身
     * @throws IOException 宣言や購読に失敗した場合
     */
    public static void start(Channel channel, String stepName, String bindingKey,
            Function<String, String> action) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, false);
        String queue = "wf." + stepName;
        channel.queueDeclare(queue, false, true, true, null);
        channel.queueBind(queue, EXCHANGE, bindingKey);
        channel.basicConsume(queue, false, new StepConsumer(channel, stepName, action));
    }
}
```

**Why**: `action` は、注文IDを受け取って、次に発行する合図（ルーティングキー）を返す関数です。手順が成功したか失敗したかは、**返す合図が違う** ことで表します。たとえば在庫の手順は、成功なら `inventory.reserved`、在庫切れなら `inventory.failed` を返します。

キューを「接続専用（exclusive）・切断時に自動削除」で作っているのは、実験を繰り返しても前回の残りのメッセージが混ざらないようにするためです。そのぶん、**ワーカーを先に起動してから** 注文を開始する必要があります。本番のワークフローで使うキューは、第11章のとおり永続化します。

次に、決済→在庫→発送の3つの手順を起動するプログラムを書きます。`src/main/java/com/example/async/workflow/ChoreographyWorkers.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/workflow/ChoreographyWorkers.java -->
```java
package com.example.async.workflow;

import java.util.concurrent.atomic.AtomicInteger;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * 決済→在庫→発送の連鎖を、「各手順が前の手順の結果イベントを聞いて動く」形（コレオグラフィ）で動かす。
 * 全体の流れを管理する者はいない。1つのプロセスの中に、手順ごとのコンシューマーを起動する。
 */
public class ChoreographyWorkers {

    /**
     * 手順ごとのコンシューマーを起動する。
     *
     * @param args [0] 在庫数（省略時は 2）, [1] compensate を渡すと、在庫切れ時に返金する手順も起動する
     * @throws Exception 接続に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        AtomicInteger stock = new AtomicInteger(args.length > 0 ? Integer.parseInt(args[0]) : 2);
        boolean compensate = args.length > 1 && args[1].equals("compensate");
        Connection connection = RabbitConnection.open("choreography-workers");

        // ① 決済: 注文確定を聞いて、決済を済ませる
        StepConsumer.start(connection.createChannel(), "payment", "order.placed", orderId -> {
            Console.sleep(300);
            Console.log("決済 完了 " + orderId);
            return "payment.completed";
        });
        // ② 在庫: 決済完了を聞いて、在庫を引き当てる。在庫がなければ失敗を知らせる
        StepConsumer.start(connection.createChannel(), "inventory", "payment.completed", orderId -> {
            Console.sleep(300);
            if (stock.decrementAndGet() < 0) {
                Console.log("在庫切れ " + orderId);
                return "inventory.failed";
            }
            Console.log("在庫引当 完了 " + orderId + "（残り " + stock.get() + "）");
            return "inventory.reserved";
        });
        // ③ 発送: 在庫引当を聞いて、発送を手配する
        StepConsumer.start(connection.createChannel(), "shipping", "inventory.reserved", orderId -> {
            Console.sleep(300);
            Console.log("発送手配 完了 " + orderId);
            return "shipping.arranged";
        });
        if (compensate) {
            // ④ 返金: 在庫切れの知らせを聞いて、決済を取り消す（補償）
            StepConsumer.start(connection.createChannel(), "refund", "inventory.failed", orderId -> {
                Console.sleep(300);
                Console.log("返金 完了 " + orderId);
                return "payment.refunded";
            });
        }
        Console.log("コレオグラフィ起動（在庫 " + stock.get() + (compensate ? "、返金あり" : "、返金なし") + "）。Ctrl+C で終了します");
    }
}
```

最後に、注文確定のイベントを発行して連鎖を開始するプログラムを書きます。`src/main/java/com/example/async/workflow/OrderStarter.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/workflow/OrderStarter.java -->
```java
package com.example.async.workflow;

import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;
import com.example.async.common.RabbitConnection;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * 注文確定のイベント order.placed を発行して、ワークフローを開始する。
 */
public class OrderStarter {

    /**
     * 指定した件数（省略時は3件）の注文を開始する。
     *
     * @param args [0] 注文の件数
     * @throws Exception 接続や送信に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        try (Connection connection = RabbitConnection.open("order-starter");
                Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(StepConsumer.EXCHANGE, BuiltinExchangeType.TOPIC, false);
            for (int i = 1; i <= count; i++) {
                String orderId = "A-" + i;
                channel.basicPublish(StepConsumer.EXCHANGE, "order.placed", null, orderId.getBytes(StandardCharsets.UTF_8));
                Console.log("注文確定 " + orderId + " → order.placed を発行");
                Console.sleep(500);
            }
        }
    }
}
```

| 手順 | 受け取る合図 | 成功したとき返す合図 | 失敗したとき返す合図 |
| :--- | :--- | :--- | :--- |
| 決済 | `order.placed` | `payment.completed` | （この章では失敗しない） |
| 在庫 | `payment.completed` | `inventory.reserved` | `inventory.failed` |
| 発送 | `inventory.reserved` | `shipping.arranged` | （この章では失敗しない） |

## ステップ3: 連鎖を動かす

在庫が2個しかない設定で、手順のワーカーを起動します。

```bash
# ターミナル1
./run.sh workflow.ChoreographyWorkers 2
```

別のターミナルで、3件の注文を開始します。

```bash
# ターミナル2
./run.sh workflow.OrderStarter 3
```

ターミナル1に、次のような出力が出ます（時刻とスレッド名は省略しています）。

```text
決済 完了 A-1
  [payment] A-1 → payment.completed を発行
在庫引当 完了 A-1（残り 1）
  [inventory] A-1 → inventory.reserved を発行
決済 完了 A-2
  [payment] A-2 → payment.completed を発行
発送手配 完了 A-1
  [shipping] A-1 → shipping.arranged を発行
在庫引当 完了 A-2（残り 0）
  [inventory] A-2 → inventory.reserved を発行
決済 完了 A-3
  [payment] A-3 → payment.completed を発行
発送手配 完了 A-2
  [shipping] A-2 → shipping.arranged を発行
在庫切れ A-3
  [inventory] A-3 → inventory.failed を発行
```

A-1 と A-2 は、決済→在庫→発送と順に進みました。複数の注文の手順が、交互に進んでいる様子にも注目してください。手順どうしは、キューを通じてつながっているだけで、互いを直接呼び出してはいません。

## ステップ4: 途中で失敗するとどうなるか

A-3 を見てください。決済は **完了** しました。しかし在庫は **切れていて**、`inventory.failed` が発行されました。この合図を聞いている手順は、**どこにもありません**。

結果として、次の状態になっています。

| 項目 | 状態 |
| :--- | :--- |
| 決済 | 完了（お客さまのカードに請求が立っている） |
| 在庫 | 引き当てられていない |
| 発送 | 手配されない |
| 返金 | **誰もしない** |

**Why**: この連鎖には、次の3つの問題があります。

1. **全体の状況を知る者がいない**: 「A-3 は今どこまで進んだか」を、誰も把握していません。各手順は、自分の前後しか見ていません
2. **失敗の後始末をする者がいない**: 在庫切れを知っても、決済を取り消す役の手順が存在しません
3. **流れがコードに散らばる**: 「決済→在庫→発送」という業務の流れは、3つの手順の「受け取る合図」の組み合わせとして、**暗黙に** 決まっているだけです。流れを変えたいとき、どのコードを直せばよいかがわかりにくくなります

**⚠️ よくある失敗・誤解**: 在庫切れの注文は、お客さまへ連絡されるはず、と考えるのは誤解です。このコードには、お客さまへ連絡する手順がありません。ログに `在庫切れ A-3` と1行出るだけです。静かに起きる障害ほど、気づくのが遅れます。

**💼 現場Tip**: 現場では、この種の問題が「決済は成功したのに注文が存在しない」という、お客さまからの問い合わせとして発覚します。お金が絡む連鎖では、**途中で失敗したときの後始末を、最初に設計する** のが鉄則です。

## ステップ5: 連鎖を作るときに気をつけること

- **各手順は冪等にする**: メッセージは重複して届くことがあるので（第12章）、同じ注文の決済が2回実行されないようにします。決済 API の冪等キーに、注文IDを使うのが定石です
- **手順が応答しないときのタイムアウトを決める**: 在庫の手順が落ちていると、成功の合図も失敗の合図も返ってきません。何分待っても進まない注文を見つけ、人が調べる仕組みを用意します
- **取り消しにくい処理は最後に置く**: 発送手配や、お客さまへのメール送信は、実行してしまうと取り消せません。取り消せる処理（決済、在庫の引当）を先に済ませ、取り消せない処理を最後に置くと、後始末が減ります
- **手順の数が増えるほど、全体が見えなくなる**: 5手順、10手順になったとき、「流れを一目で確認できる場所」が欲しくなります。次の章で、その作り方を学びます

## 動作確認

次の3点を確認できたら完了です。

1. A-1 と A-2 が、決済→在庫→発送の順に完了した
2. A-3 は決済が済んだ後、在庫切れで止まった
3. `inventory.failed` を聞く手順がなく、決済の取り消しが行われなかった

## もっと良くしてみよう

### 理解確認

1. Pub/Sub の fanout で配る処理と、ワークフローの連鎖は、何が違うのでしょうか。
2. A-3 の在庫切れが、誰にも対処されずに終わったのはなぜでしょうか。
3. 取り消しにくい処理を、連鎖の最後に置くのはなぜでしょうか。

### 発展改善クイズ

1. 在庫の手順が落ちていて、何も返ってこないとします。注文 A-4 が「止まっている」ことに、どうすれば気づけるでしょうか。
2. 手順を1つ追加して、お客さまに「在庫切れでした」とメールを送りたくなりました。`inventory.failed` を聞く手順を足せば実現できます。では、返金の手順と、メールの手順の、どちらが先に動くべきでしょうか。その順番は、どこに書かれるべきでしょうか。

---

| [← 第20章: メッセージの順序を考える](./20-ordering.md) | [全章目次](./README.md) | [第22章: 連鎖の失敗と補償 →](./22-workflow-compensation.md) |
| - | :-: | --: |
