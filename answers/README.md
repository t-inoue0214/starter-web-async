# 完成コード（answers）

全章を終えた時点の `src/` と `pom.xml` です。すべて RabbitMQ に接続して動作確認済みです。

- 章を進める途中で自分のコードが動かないときに、見比べるために使ってください
- 写すのではなく、まず章の本文と自分のコードを見比べ、違いの理由を考えてから参照するのがおすすめです

## 完成コードをそのまま動かしたい場合

リポジトリ直下で、自分の `src/` と `pom.xml` を退避してから差し替えます。

```bash
mv src src.mine && mv pom.xml pom.xml.mine
cp -r answers/src . && cp answers/pom.xml .
./run.sh p2p.HelloConsumer
# 元に戻す
rm -rf src pom.xml && mv src.mine src && mv pom.xml.mine pom.xml
```

## 教材の執筆者向け

`chapters/*.md` の Java コードブロックは、このディレクトリのファイルから `python3 tools/sync-chapter-code.py`（`npm run sync:code`）で差し込んでいます。コードを直すときはこちらを直し、RabbitMQ を起動した状態で実際に動かして確認してから、スクリプトを実行してください。
