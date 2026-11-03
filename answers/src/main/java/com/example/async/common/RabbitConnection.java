package com.example.async.common;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * RabbitMQ への接続を作る共通クラス。
 * 接続先は環境変数で差し替えられ、未設定なら開発コンテナ内の学習用ブローカーに接続する。
 */
public final class RabbitConnection {

    private RabbitConnection() {
    }

    /**
     * RabbitMQ に接続する。
     *
     * @param connectionName 管理画面の Connections 一覧に表示される名前
     * @return 確立した接続（使い終わったら close すること）
     * @throws IOException      ネットワークや認証に失敗した場合
     * @throws TimeoutException 接続がタイムアウトした場合
     */
    public static Connection open(String connectionName) throws IOException, TimeoutException {
        // 接続設定の入れ物を作る
        ConnectionFactory factory = new ConnectionFactory();
        // 接続先ホスト・ポート・ユーザー・パスワードを環境変数から読む（なければ学習用の既定値）
        factory.setHost(env("RABBITMQ_HOST", "localhost"));
        factory.setPort(Integer.parseInt(env("RABBITMQ_PORT", "5672")));
        factory.setUsername(env("RABBITMQ_USER", "app"));
        factory.setPassword(env("RABBITMQ_PASSWORD", "app"));
        // 仮想ホスト（ブローカー内の論理的な区画）。既定は "/"
        factory.setVirtualHost(env("RABBITMQ_VHOST", "/"));
        // 接続が切れたら自動で再接続する（amqp-client 4.0 以降は既定で有効だが、明示しておく）
        factory.setAutomaticRecoveryEnabled(true);
        // 名前を付けて接続する
        return factory.newConnection(connectionName);
    }

    // 環境変数を読み、未設定または空なら既定値を返す
    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }
}
