# 第2章: 同期の代わりの選択肢を試す

## この章のゴール

第1章の問題（遅さ・障害の波及・変更のしにくさ）に対して、Pub/Sub 以外の解決策を自分で作って試します。メモリ上のキュー、ファイル、データベース、UDP を比べ、それぞれがどこで行き詰まるかを体験します。スタックとキューの違いも、ここで押さえます。

## Why: なぜこれを学ぶのか

第1章の解決策は、Pub/Sub でなければならないわけではありません。「後続処理を後回しにしたい」だけなら、ファイルに書き出して別のプログラムに拾わせる方法でも、UDP で知らせる方法でも、一見できそうです。実際、小さなシステムではそれで十分なことも多くあります。

大切なのは、「Pub/Sub が必要だ」と最初から決めつけないことです。自分で作ってみて、足りないところが見えたときに初めて、ブローカーの価値が実感できます。この章は、第15章以降の「なぜ」を支える土台です。

## 前提

第1章で、同期呼び出しの3つの問題を体験しました。RabbitMQ はまだ使いません。

## ステップ1: 解決したいことを書き出す

第1章の問題から、満たしたい要件を5つに整理します。以後、各案をこの5つで採点します。

| 番号 | 要件 | 第1章の問題との対応 |
| :--- | :--- | :--- |
| R1 | 注文の応答が、後続処理を待たずに速く返る | 遅さ |
| R2 | 後続処理が落ちていても、注文は成功する | 障害の波及 |
| R3 | 後続処理の前にプログラムが落ちても、依頼が消えない | （新しく気づく問題） |
| R4 | 失敗した依頼をやり直せる。ただし二重に実行しない | 第1章のコラム |
| R5 | 連携先が増えても、注文サービスの改修が小さく済む | 変更のしにくさ |

**Why**: R3 は、第1章では見えていなかった要件です。後続処理を「後回し」にした瞬間、「後回しにした依頼をどこに置いておくか」という新しい問題が生まれます。

## ステップ2: スタックとキューの違いを知る

後回しにした依頼は、どこかに順番に並べて置きます。このとき、データ構造としての **スタック** と **キュー** のどちらを使うかで、取り出す順番が変わります。

`src/main/java/com/example/async/inmemory/StackAndQueueDemo.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/inmemory/StackAndQueueDemo.java -->
```java
package com.example.async.inmemory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;

import com.example.async.common.Console;

/**
 * スタック（後入れ先出し）とキュー（先入れ先出し）の違いを確かめる小さなプログラム。
 */
public class StackAndQueueDemo {

    /**
     * 同じ順番で3件入れて、取り出す順番を比べる。
     *
     * @param args 使わない
     */
    public static void main(String[] args) {
        // スタック: 入れた順と逆に取り出される（LIFO: Last In, First Out）
        Deque<String> stack = new ArrayDeque<>();
        // push で積む
        stack.push("A-1");
        stack.push("A-2");
        stack.push("A-3");
        Console.log("スタック: " + stack.pop() + " → " + stack.pop() + " → " + stack.pop());

        // キュー: 入れた順に取り出される（FIFO: First In, First Out）
        Queue<String> queue = new ArrayDeque<>();
        // offer で末尾に並ぶ
        queue.offer("A-1");
        queue.offer("A-2");
        queue.offer("A-3");
        Console.log("キュー  : " + queue.poll() + " → " + queue.poll() + " → " + queue.poll());
    }
}
```

```bash
./run.sh inmemory.StackAndQueueDemo
```

```text
[12:38:49.886][main] スタック: A-3 → A-2 → A-1
[12:38:49.887][main] キュー  : A-1 → A-2 → A-3
```

| 名前 | 取り出す順番 | 略称 | たとえ |
| :--- | :--- | :--- | :--- |
| スタック | 最後に入れたものから | LIFO（後入れ先出し） | 積み重ねた皿。上から取る |
| キュー | 最初に入れたものから | FIFO（先入れ先出し） | レジの行列。先に並んだ人から |

**Why**: 後続処理の依頼は、受け付けた順に処理するのが自然です。スタックにすると、新しい依頼が次々と来る間、古い依頼はいつまでも取り出されません。そのため依頼の置き場にはキューを使います。メッセージングの世界で「キュー」と呼ぶのは、この先入れ先出しの置き場のことです。

**⚠️ よくある失敗・誤解**: 後の章で「キューは順序を保証する」と聞いても、**処理が終わる順番** まで保証されるわけではありません。置き場の中の順序と、複数のワーカーが処理する順序は別の話です。第20章で体験します。

## ステップ3: 案1　メモリ上のキューと別スレッド

最初に思いつく案です。依頼を `BlockingQueue`（スレッド間で安全に受け渡せるキュー）に入れ、別スレッドのワーカーに処理させます。

`src/main/java/com/example/async/inmemory/InMemoryQueueApp.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/inmemory/InMemoryQueueApp.java -->
```java
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
```

まず通常どおり実行します。

```bash
./run.sh inmemory.InMemoryQueueApp
```

```text
[12:44:11.883][main] 注文 A-1 確定 OK（6ms）
[12:44:11.889][main] 注文 A-2 確定 OK（0ms）
[12:44:11.889][main] 注文 A-3 確定 OK（0ms）
[12:44:11.889][main] 注文 A-4 確定 OK（0ms）
[12:44:11.889][main] 注文 A-5 確定 OK（0ms）
[12:44:12.683][mail-worker]   メール送信 完了 A-1
...
[12:44:14.285][mail-worker]   メール送信 完了 A-5
```

注文の確定は0〜6ミリ秒で返ります。第1章の1.3秒と比べて、R1（速さ）は解決しました。メールは裏のスレッドが順に送っています。

次に、途中でプロセスが強制終了した場合を再現します。

```bash
./run.sh inmemory.InMemoryQueueApp crash
```

```text
[12:44:14.408][main] 注文 A-5 確定 OK（0ms）
[12:44:15.174][mail-worker]   メール送信 完了 A-1
[12:44:15.914][main] まだキューに残っている件数: 3。ここでプロセスが強制終了したとします
```

メールが送られたのは A-1 だけです。A-2 から A-5 のうち、処理中だった1件と待っていた3件の依頼が、プロセスと一緒に消えました。しかもお客さまの画面には「注文完了」と表示されています。

**Why**: メモリ上のキューは、プロセスが終わると消えます。デプロイのたびの再起動、メモリ不足による強制終了、サーバーの障害など、プロセスが突然終わる原因はいくらでもあります。これが R3 を満たせない理由です。

| 要件 | 評価 | 理由 |
| :--- | :--- | :--- |
| R1 速さ | ○ | キューに入れるだけで戻れる |
| R2 障害の分離 | ○ | メールが遅くても落ちても、注文は成功する |
| R3 消えない | × | プロセスと一緒に消える |
| R4 やり直し | × | 失敗した依頼を覚えておく場所がない |
| R5 連携先の追加 | × | ワーカーも同じプロセスの中。追加には注文サービスの改修が必要 |

**💼 現場Tip**: `java.util.concurrent` の `BlockingQueue` と `ExecutorService` は、1つのプロセスの中で処理を分担するのにとても有用です。ただし「プロセスが落ちても消えてはいけない依頼」には向きません。道具の向き不向きを見分けましょう。

## ステップ4: 案2　ファイルに書き出して別のプログラムに拾わせる

消えないようにするには、ディスクに書けばよさそうです。依頼を1件1ファイルとして書き出し、別のプログラム（ワーカー）がディレクトリを見に行って拾います。

`src/main/java/com/example/async/spool/SpoolOrderApp.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/spool/SpoolOrderApp.java -->
```java
package com.example.async.spool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.example.async.common.Console;

/**
 * 注文を受け付けたら、後続処理の依頼を1件1ファイルとしてディレクトリに書き出すアプリ。
 */
public class SpoolOrderApp {
    /** 依頼ファイルを置くディレクトリ */
    static final Path SPOOL_DIR = Path.of("data", "spool");

    /**
     * 5件の注文を受け付けて、依頼ファイルを書き出す。
     *
     * @param args 使わない
     * @throws IOException ファイルの書き込みに失敗した場合
     */
    public static void main(String[] args) throws IOException {
        // ディレクトリがなければ作る
        Files.createDirectories(SPOOL_DIR);
        // 起動ごとに違う接頭辞を付けて、ファイル名が重ならないようにする
        String prefix = "A" + (System.currentTimeMillis() / 1000 % 100000) + "-";
        for (int i = 1; i <= 5; i++) {
            String orderId = prefix + i;
            // 注文IDを書いたファイルを作る。ファイル名の末尾 .txt が「依頼」の目印
            Files.writeString(SPOOL_DIR.resolve(orderId + ".txt"), orderId, StandardCharsets.UTF_8);
            Console.log("注文 " + orderId + " 確定 OK（依頼ファイルを書き出した）");
        }
    }
}
```

ワーカーも作ります。`src/main/java/com/example/async/spool/SpoolWorker.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/spool/SpoolWorker.java -->
```java
package com.example.async.spool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import com.example.async.common.Console;

/**
 * 依頼ファイルのディレクトリを1秒ごとに見に行き、見つけたファイルを処理して削除するワーカー。
 */
public class SpoolWorker {

    /**
     * Ctrl+C で止めるまで、ディレクトリを見に行き続ける。
     *
     * @param args [0] ワーカー名（省略時は worker）
     * @throws Exception ファイル操作に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        String name = args.length > 0 ? args[0] : "worker";
        Files.createDirectories(SpoolOrderApp.SPOOL_DIR);
        Console.log(name + " 起動。" + SpoolOrderApp.SPOOL_DIR + " を1秒ごとに見に行きます");
        while (true) {
            for (Path file : listRequests()) {
                // メール送信の代わり
                Console.sleep(800);
                Console.log(name + " 処理: " + file.getFileName());
                // 処理が終わったらファイルを消して「済み」にする
                Files.deleteIfExists(file);
            }
            Thread.sleep(1000);
        }
    }

    // 依頼ファイル（.txt）の一覧を取得する
    private static List<Path> listRequests() throws IOException {
        try (Stream<Path> files = Files.list(SpoolOrderApp.SPOOL_DIR)) {
            return files.filter(p -> p.toString().endsWith(".txt")).sorted().toList();
        }
    }
}
```

まず依頼を書き出します。

```bash
./run.sh spool.SpoolOrderApp
```

```text
[12:44:50.957][main] 注文 A9890-1 確定 OK（依頼ファイルを書き出した）
...
```

`data/spool` ディレクトリに5つのファイルができています。ワーカーを起動すると、順に処理して削除します。

```bash
./run.sh spool.SpoolWorker w1
```

```text
[12:44:52.001][main] w1 起動。data/spool を1秒ごとに見に行きます
[12:44:52.814][main] w1 処理: A9890-1.txt
...
```

ワーカーの起動前に注文が入っても、依頼はファイルとして残ります。プロセスが落ちても消えません。R3 は満たせました。

では、ワーカーを増やして処理を速くしようとすると、どうなるでしょうか。依頼を5件書き出し直し、ワーカーを2つのターミナルで同時に起動します。

```bash
./run.sh spool.SpoolOrderApp
```

```bash
# ターミナル1
./run.sh spool.SpoolWorker w1
```

```bash
# ターミナル2
./run.sh spool.SpoolWorker w2
```

```text
w1 処理: A9890-1.txt
w2 処理: A9890-1.txt
w1 処理: A9890-2.txt
w2 処理: A9890-2.txt
...
```

**同じ依頼を2つのワーカーが両方とも処理しました。** メールが2通届き、ポイントは二重に付与されます。

**Why**: 2つのワーカーは、ほぼ同時にディレクトリの一覧を取得し、同じファイルを見つけます。「このファイルは自分が担当する」と他のワーカーに伝える仕組みがありません。ファイルシステムには、「取り出した人だけに渡す」機能がないのです。

ファイル方式には、ほかにも次のような問題があります。

| 問題 | 内容 |
| :--- | :--- |
| 書きかけを読む | 書き込み中のファイルをワーカーが拾い、中身が途中で切れている |
| 削除前に落ちる | 処理は終わったのにファイルを消す前に落ち、再起動後にもう一度処理される |
| 失敗の扱い | 何度やっても失敗するファイルが、毎回の一覧に残り続ける |
| 複数のサーバー | 注文サービスとワーカーが別のサーバーにあると、ディスクを共有する仕組みが要る |
| 空振り | 依頼がなくても、1秒ごとにディレクトリを見に行く（第7章のポーリング） |

これらを1つずつ解決すると、結局は次のような仕組みを自作することになります。取り出した人にだけ渡す。ack が返るまで預かる。返らなければ別の人に渡す。失敗が続けば別の場所へ移す。それはまさにメッセージブローカーです。

**💼 現場Tip**: 共有ディレクトリにファイルを置いて連携する方式は、古くからの業務システム（バッチ連携、ファイル転送）で今も使われています。「ファイルを置き終えたら、最後に `.done` のような合図のファイルを置く」「処理中のファイルは別のディレクトリに移動してから処理する」といった工夫で、問題の多くを避けています。

## ステップ5: 案3　データベースのジョブテーブル

依頼の置き場に、すでに使っているデータベースを使う案です。`jobs` テーブルに依頼を1行ずつ書き、ワーカーが未処理の行を探して処理します。

```sql
-- 依頼を保存するテーブル
CREATE TABLE jobs (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id   VARCHAR(50) NOT NULL,
  status     VARCHAR(20) NOT NULL DEFAULT 'NEW',  -- NEW / RUNNING / DONE / FAILED
  attempts   INT NOT NULL DEFAULT 0,               -- 試行回数
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 注文の保存と同じトランザクションで、依頼を1行追加する
INSERT INTO jobs (order_id) VALUES ('A-1');

-- ワーカー: 未処理の行を1件、他のワーカーに取られないようにロックして取り出す
SELECT id, order_id FROM jobs WHERE status = 'NEW' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED;
```

| 要件 | 評価 | 理由 |
| :--- | :--- | :--- |
| R1 速さ | ○ | 行を1つ追加するだけ |
| R2 障害の分離 | ○ | 後続処理が落ちていても、行は残る |
| R3 消えない | ○ | データベースに保存される。注文と同じトランザクションにもできる |
| R4 やり直し | ○ | `status` と `attempts` で、失敗の回数や状態を管理できる |
| R5 連携先の追加 | △ | 連携先ごとに別のテーブルや列が要る。受け手の数が増えると管理が重くなる |

**Why**: 案2の「取り出した人にだけ渡す」問題を、`FOR UPDATE SKIP LOCKED`（他のワーカーがロックしている行を飛ばして読む SQL）が解決しています。データベースのトランザクションの機能を借りているわけです。

**💼 現場Tip**: 件数が少なく、受け手が1種類なら、**ジョブテーブルは十分に実用的な選択肢** です。インフラが増えず、トランザクションも使えます。この方式は第23章の Outbox パターンの土台になります。件数が増えたり、受け手が増えたり、リアルタイム性が必要になったりしたときに、ブローカーの出番になります。

## ステップ6: 案4　UDP で知らせる

「ファイルやデータベースを使わず、ネットワークで直接知らせればよい」という案もあります。UDP は、接続を張らずにデータ（データグラム）を送りっぱなしにできる、軽いプロトコルです。

`src/main/java/com/example/async/udp/UdpReceiver.java` と `UdpSender.java` を新規作成します。

<!-- code: answers/src/main/java/com/example/async/udp/UdpReceiver.java -->
```java
package com.example.async.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;

/**
 * UDP の 9999 番ポートで待ち受け、届いたデータを表示する受信側。
 */
public class UdpReceiver {
    /** 待ち受けるポート番号 */
    static final int PORT = 9999;

    /**
     * Ctrl+C で止めるまで受信し続ける。
     *
     * @param args 使わない
     * @throws Exception ソケットの操作に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        try (DatagramSocket socket = new DatagramSocket(PORT)) {
            Console.log("UDP " + PORT + " 番で待ち受け中です");
            byte[] buffer = new byte[1024];
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                // 届くまで待つ
                socket.receive(packet);
                Console.log("受信: " + new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8));
            }
        }
    }
}
```

<!-- code: answers/src/main/java/com/example/async/udp/UdpSender.java -->
```java
package com.example.async.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;

/**
 * 注文確定の知らせを UDP で5件送る送信側。
 */
public class UdpSender {

    /**
     * 0.5秒おきに5件送る。受信側がいなくてもエラーにならないことを確かめる。
     *
     * @param args 使わない
     * @throws Exception ソケットの操作に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        InetAddress address = InetAddress.getByName("localhost");
        try (DatagramSocket socket = new DatagramSocket()) {
            for (int i = 1; i <= 5; i++) {
                byte[] data = ("order-A-" + i).getBytes(StandardCharsets.UTF_8);
                // 宛先を指定して送るだけ。相手が受け取ったかどうかは、送信側にはわからない
                socket.send(new DatagramPacket(data, data.length, address, UdpReceiver.PORT));
                Console.log("送信: order-A-" + i + "（例外は出ていない）");
                Thread.sleep(500);
            }
        }
    }
}
```

受信側を起動しない状態で、先に送信側を実行します。

```bash
./run.sh udp.UdpSender
```

```text
[12:44:26.173][main] 送信: order-A-1（例外は出ていない）
[12:44:26.686][main] 送信: order-A-2（例外は出ていない）
...
```

受信側が存在しないのに、送信側は **何のエラーも出さずに** 5件を送り終えました。5件の通知は誰にも届かず、消えています。

次に、受信側を先に起動してから送信します。

```bash
# ターミナル1
./run.sh udp.UdpReceiver
```

```bash
# ターミナル2
./run.sh udp.UdpSender
```

```text
[12:44:30.803][main] 受信: order-A-1
[12:44:31.352][main] 受信: order-A-2
...
```

受信側が起動していれば届きます。

**Why**: UDP は、送る側が受信側の状態を確かめません。受信側が落ちていても、ネットワークの途中で捨てられても、送信側には何も伝わりません。同じマシンの中（localhost）ではめったに起きませんが、実際のネットワークでは、パケットの消失・順序の入れ替わり・重複が普通に起こります。1つのデータグラムに入れられるサイズにも限りがあります。

| 特徴 | UDP | TCP | メッセージブローカー |
| :--- | :--- | :--- | :--- |
| 接続 | なし | あり | あり（TCP の上） |
| 届いたかの確認 | なし | あり（通信路の中のみ） | あり（ack） |
| 受信側が落ちている間 | 消える | 接続エラーになる | ブローカーが預かる |
| 順序 | 保証なし | 1つの接続の中では保証 | キューの中では保証 |

「TCP で常時つないでデータを流し続ける（ストリーミング）」方法でも、受信側が落ちている間は送れません。落ちている間のデータを預かる場所が、どこにもないからです。

**💼 現場Tip**: UDP が選ばれるのは、**少し消えても困らず、速さを優先したいデータ** です。アプリの性能指標（StatsD）、ログの転送（syslog）、動画や音声の配信、ゲームの位置情報などが代表例です。注文確定のように、1件も落とせない通知には向きません。

## ステップ7: 比較して結論を出す

| 案 | R1 速さ | R2 障害の分離 | R3 消えない | R4 やり直し | R5 連携先の追加 | 向いている場面 |
| :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| 同期呼び出し（第1章） | × | × | - | - | × | 結果をすぐに使いたい処理 |
| 案1 メモリ上のキュー | ○ | ○ | × | × | × | 落ちても困らない、プロセス内の分担 |
| 案2 ファイル | ○ | ○ | ○ | △ | △ | 既存のバッチ連携、単純な受け渡し |
| 案3 DB のジョブテーブル | ○ | ○ | ○ | ○ | △ | 小規模で、受け手が1種類 |
| 案4 UDP | ○ | ○ | × | × | ○ | 消えてもよい計測値やログ |
| メッセージブローカー | ○ | ○ | ○ | ○ | ○ | 次の章から学ぶ |

**Why**: どの案も、要件の一部を満たします。しかしすべてを満たそうとすると、「依頼を預かる」「受け手に1件ずつ渡す」「処理完了を確認する」「失敗したらやり直す」「複数の受け手に配る」を、自分で作ることになります。メッセージブローカーは、これらを専用の製品として引き受けたものです。

ブローカーを入れると、運用するサーバーが1つ増えます。そのため「小さな規模ならジョブテーブルで十分」という判断も、正しい選択になりえます。次の章からは、ブローカーが解く問題を持つ Pub/Sub パターンを学びます。

## 動作確認

次の4点を確認できたら完了です。

1. `StackAndQueueDemo` で、スタックとキューの取り出し順が逆になった
2. `InMemoryQueueApp crash` で、プロセスの強制終了により依頼が消えた
3. 2つの `SpoolWorker` が、同じ依頼ファイルを両方とも処理した
4. 受信側のいない `UdpSender` が、エラーを出さずに送り終えた

## もっと良くしてみよう

### 理解確認

1. 後続処理の依頼の置き場に、スタックではなくキューを使うのはなぜでしょうか。
2. メモリ上のキューが、プロセスの強制終了で消えるのはなぜでしょうか。
3. UDP で送った通知が届かなくても、送信側がエラーにならないのはなぜでしょうか。

### 発展改善クイズ

1. ファイル方式で、2つのワーカーが同じファイルを処理しないようにするには、どんな工夫が考えられるでしょうか。ファイルの移動（rename）が1つの操作として行われることを手がかりにしてみましょう。
2. ジョブテーブルの方式で、ワーカーが処理の途中で落ちると、`RUNNING` のまま残る行が出ます。この行を、あとで安全にやり直すにはどうすればよいでしょうか。

---

| [← 第1章: 同期呼び出しの限界を体験する](./01-sync-call-limits.md) | [全章目次](./README.md) | [第3章: Webサービスの非同期処理の地図 →](./03-async-map.md) |
| - | :-: | --: |
