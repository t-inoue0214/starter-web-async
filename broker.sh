#!/usr/bin/env bash
# 学習用の RabbitMQ ブローカーを操作するスクリプト。RabbitMQ は開発コンテナの中で直接動いています。
#   ./broker.sh start     起動する（Codespaces の起動時にも自動で実行されます）
#   ./broker.sh stop      停止する
#   ./broker.sh restart   再起動する（永続化の実験で使います）
#   ./broker.sh status    動いているか確認する
#   ./broker.sh logs      ログを表示し続ける（Ctrl+C で終了）
#   ./broker.sh reset     キューもメッセージも全部消して作り直す
set -euo pipefail
SUDO=""
if [ "$(id -u)" -ne 0 ]; then SUDO="sudo"; fi
LOG_DIR=/var/log/rabbitmq

is_running() {
  $SUDO rabbitmq-diagnostics -q ping > /dev/null 2>&1
}

start() {
  if is_running; then
    echo "RabbitMQ はすでに起動しています"
    return
  fi
  echo "RabbitMQ を起動しています（10秒ほどかかります）..."
  # -detached でバックグラウンドで起動する（開発コンテナには systemd がないため）
  $SUDO rabbitmq-server -detached > /dev/null 2>&1
  # -detached はノードの起動を待たずに戻ってくる。応答するまで最大60秒待つ
  for _ in $(seq 1 60); do
    if is_running; then break; fi
    sleep 1
  done
  if ! is_running; then
    echo "RabbitMQ が起動しませんでした。./broker.sh logs でログを確認してください" >&2
    exit 1
  fi
  # アプリ（管理画面や Binding など）の起動が終わるまで待つ
  $SUDO rabbitmqctl -q await_startup --timeout 60
  ensure_user
  echo "RabbitMQ を起動しました（AMQP: 5672番 / 管理画面: 15672番、ユーザー app / パスワード app）"
}

stop() {
  if ! is_running; then
    echo "RabbitMQ は停止しています"
    return
  fi
  # shutdown は、ブローカーが完全に止まるまで待ってから戻ってくる
  $SUDO rabbitmqctl -q shutdown
  echo "RabbitMQ を停止しました"
}

# 学習用ユーザー app を作る（すでにあれば何もしない）
ensure_user() {
  if ! $SUDO rabbitmqctl -q list_users | grep -q '^app\b'; then
    $SUDO rabbitmqctl -q add_user app app
    $SUDO rabbitmqctl -q set_user_tags app administrator
    $SUDO rabbitmqctl -q set_permissions -p / app '.*' '.*' '.*'
  fi
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status)
    if is_running; then echo "RabbitMQ は起動しています"; else echo "RabbitMQ は停止しています"; exit 1; fi ;;
  logs) $SUDO tail -n 50 -f $LOG_DIR/rabbit@localhost.log ;;
  reset)
    stop
    # データディレクトリを消す。キュー・エクスチェンジ・メッセージ・ユーザーがすべて消える
    $SUDO rm -rf /var/lib/rabbitmq/mnesia
    start ;;
  *)
    echo "使い方: ./broker.sh start|stop|restart|status|logs|reset" >&2
    exit 1 ;;
esac
