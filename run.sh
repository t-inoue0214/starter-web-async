#!/usr/bin/env bash
# 使い方: ./run.sh <パッケージ以下のクラス名> [引数...]
#   例: ./run.sh p2p.HelloConsumer
#       ./run.sh workqueue.TaskProducer 10
# com.example.async. は省略できます。
# コンパイルしてから、Maven の依存ライブラリをクラスパスに載せて java コマンドで起動します。
set -euo pipefail
cd "$(dirname "$0")"

if [ $# -lt 1 ]; then
  echo "使い方: ./run.sh <クラス名> [引数...]   例: ./run.sh p2p.HelloConsumer" >&2
  exit 1
fi

main="$1"
shift
case "$main" in
  com.example.async.*) ;;
  *) main="com.example.async.$main" ;;
esac

# コンパイルと依存ライブラリのクラスパス書き出し（-q で余計なログを抑える）
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt

exec java -cp "target/classes:$(cat target/classpath.txt)" "$main" "$@"
