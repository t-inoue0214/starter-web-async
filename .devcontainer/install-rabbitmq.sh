#!/usr/bin/env bash
# 開発コンテナの作成時（postCreateCommand）に1回だけ実行され、RabbitMQ をインストールして設定します。
# Debian 13 (trixie) の標準パッケージを使うので、amd64（Codespaces）でも arm64（Apple シリコンの Mac）でも動きます。
set -euo pipefail
SUDO=""
if [ "$(id -u)" -ne 0 ]; then SUDO="sudo"; fi

# 開発コンテナの土台イメージに、署名の期限が切れたリポジトリ（yarn など）が残っていると update がエラーになる。
# RabbitMQ のインストールには無関係なので、update の失敗では止めない（install が失敗すれば、そこで止まる）
$SUDO apt-get update -qq || echo "注意: apt-get update に失敗したリポジトリがありますが、続行します"
# --no-install-recommends で、使わない関連パッケージを入れずに済ませる
$SUDO env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends rabbitmq-server

# ノード名を rabbit@localhost に固定する。
# RabbitMQ はノード名（既定は rabbit@<ホスト名>）でデータの保存場所を決めるため、
# ホスト名が変わる環境でも、以前のキューやメッセージが見えなくならないようにする
echo 'NODENAME=rabbit@localhost' | $SUDO tee /etc/rabbitmq/rabbitmq-env.conf > /dev/null

# 管理画面と管理 HTTP API（15672番ポート）を有効にする（ブローカーを起動していなくても設定できる）
$SUDO rabbitmq-plugins enable --offline rabbitmq_management > /dev/null
echo "RabbitMQ をインストールしました: $(dpkg-query -W -f='${Version}' rabbitmq-server)"
