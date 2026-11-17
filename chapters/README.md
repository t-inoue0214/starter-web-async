# 章一覧（目次）

本編は全21章（第0章を除く）です。始める前に、これから学ぶものの全体像と環境を確認する第0章から読んでください。上から順番に進めることを前提に書いています。

この講座は、Webサービスの非同期処理の **土台（キューとワーカー）** を作り、そこに何が1つ増えるかで、定期バッチ・Pub/Sub・ワークフローを学ぶ構成です。地図は [第3章](./03-async-map.md) にあります。

用語の意味を忘れたときは、[用語集](./glossary.md) で確認できます。

この講座のコードは、すべて `src/main/java/com/example/async/` 以下に用意してあります。各章では、章の図表と抜粋で考え方をつかみ、リンク先のコードを開いて読み、`./run.sh` で実行して結果を確かめます。章の中の「確認・実験してみよう」では、コードを少し書き換えて動きの違いを確かめてください。

## Part 0. これから学ぶものを知る（準備）

| 章 | タイトル | 読むファイル | ゴール |
| :--- | :--- | :--- | :--- |
| 00 | [これから学ぶものを知る](./00-overview.md) | `broker.sh`, `run.sh` | RabbitMQ が起動し、管理画面を開けることを確認する |

## Part A. 非同期処理の地図と準備

| 章 | タイトル | 読むファイル | ゴール |
| :--- | :--- | :--- | :--- |
| 01 | [同期呼び出しの限界を体験する](./01-sync-call-limits.md) | `sync/SyncOrderApp.java` | 後続処理を同期で呼ぶと、遅さと障害が注文に波及することを体験する |
| 02 | [同期の代わりの選択肢を試す](./02-async-alternatives.md) | `inmemory/*.java`, `spool/*.java` | メモリ上のキュー・ファイル・DB・UDP の限界を体験し、スタックとキューの違いを説明できる |
| 03 | [Webサービスの非同期処理の地図](./03-async-map.md) | - | ジョブキュー・定期バッチ・Pub/Sub・ワークフロー・イベントログを「土台の変形」で整理できる |
| 04 | [設計の考え方　同期にするか、非同期にするか](./04-design-thinking.md) | - | 最初の問いと3つの確認、判断フローで、同期と非同期を使い分けられる |
| 05 | [メッセージング製品の全体像](./05-messaging-products.md) | - | RabbitMQ・Kafka・クラウドの製品を「キュー型」「ログ型」で分類でき、イベントログの考え方がわかる |
| 06 | [AMQPとはどのようなプロトコルか](./06-amqp.md) | - | Connection・Channel・Exchange・Queue・Binding の関係を説明できる |

## Part B. ジョブキュー（土台）

| 章 | タイトル | 読むファイル | ゴール |
| :--- | :--- | :--- | :--- |
| 07 | [HTTP APIでメッセージを送受信する](./07-http-api.md) | -（curl を使う） | curl で送受信し、ポーリングの限界を体験する |
| 08 | [Javaクライアントを導入して接続する](./08-java-client-setup.md) | `pom.xml`, `common/RabbitConnection.java`, `hello/ConnectionCheck.java` | `amqp-client` の役割を知り、AMQP で接続できることを確認する |
| 09 | [キューに送ってDefaultConsumerで受け取る](./09-point-to-point.md) | `p2p/HelloProducer.java`, `p2p/HelloConsumer.java` | `DefaultConsumer` を継承し、プッシュ型でリアルタイムに受信する |
| 10 | [ジョブキューで時間のかかる処理を任せる](./10-job-queue.md) | `workqueue/ReceiptTaskProducer.java`, `workqueue/ReceiptWorker.java` | ジョブを積んでワーカーで分担し、prefetch と ack の役割を理解する |
| 11 | [メッセージが消えないために](./11-not-losing-messages.md) | `durability/*.java`, `ack/*.java` | 再起動と強制終了でジョブが消える様子を体験し、永続化・手動 ack・送信の確認の役割を説明できる |
| 12 | [重複メッセージと冪等なコンシューマー](./12-idempotency.md) | `idempotency/*.java` | ポイントの二重付与を体験し、メッセージIDで重複を弾く基本を理解する |
| 13 | [処理できないメッセージとデッドレターキュー](./13-dead-letter-queue.md) | `dlq/*.java` | 毒メッセージが後続をせき止める様子を体験し、DLQ へ逃がす基本を理解する |

## Part C. 定期バッチ（時刻がジョブを積む）

| 章 | タイトル | 読むファイル | ゴール |
| :--- | :--- | :--- | :--- |
| 14 | [定期バッチ　時刻がジョブを積む](./14-batch.md) | `batch/*.java` | スケジューラーを2台にして二重投入を体験し、周期ごとのジョブIDで重複を弾く |

## Part D. Pub/Sub（出来事を複数の相手に知らせる）

| 章 | タイトル | 読むファイル | ゴール |
| :--- | :--- | :--- | :--- |
| 15 | [Pub/Subパターンとは](./15-what-is-pubsub.md) | - | 送り手・受け手・ブローカーの役割と P2P との違いを説明でき、Pub/Sub・キュー・イベントバスの言葉を整理できる |
| 16 | [fanoutでPub/Subを実現する](./16-fanout.md) | `fanout/OrderPlacedPublisher.java`, `fanout/OrderPlacedSubscriber.java`, `fanout/LiveMonitor.java` | 1つのイベントを複数のサービスへ配る |
| 17 | [topicで受け取るイベントを選ぶ](./17-topic.md) | `topic/OrderEventTopicPublisher.java`, `topic/TopicSubscriber.java` | ルーティングキーとワイルドカードで購読対象を絞り込む |
| 18 | [メッセージを設計する](./18-message-design.md) | `message/OrderPlacedEvent.java`, `message/EventPublisher.java` ほか | messageId・type・ヘッダー・スキーマのバージョンを持つイベントを作る |
| 19 | [メッセージの順序を考える](./19-ordering.md) | `ordering/*.java` | 処理順の逆転を体験し、バージョン番号で古いイベントを捨てる対処を知る |

## Part E. ワークフロー（ジョブの連鎖）

| 章 | タイトル | 読むファイル | ゴール |
| :--- | :--- | :--- | :--- |
| 20 | [ジョブの連鎖　ワークフロー](./20-workflow-chain.md) | `workflow/StepConsumer.java`, `workflow/ChoreographyWorkers.java`, `workflow/OrderStarter.java` | 決済→在庫→発送の連鎖を作り、途中失敗で状態が宙に浮くことを体験する |

## Part F. まとめ

| 章 | タイトル | 読むファイル | ゴール |
| :--- | :--- | :--- | :--- |
| 21 | [まとめと次のステップ](./21-wrap-up.md) | - | 学んだ考え方を各製品の機能に対応付け、設計チェックリストと7つの質問で設計メモを書ける。Saga・Outbox など次に学ぶテーマを知る |
