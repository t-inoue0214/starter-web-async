package com.example.async.outbox;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 注文サービスの DB（orders テーブルと outbox テーブル）を開く部品。
 */
public final class ShopDatabase {
    // 注文アプリとリレーが別プロセスから同時に開けるよう AUTO_SERVER=TRUE にする
    private static final String URL = "jdbc:h2:file:./data/shop;AUTO_SERVER=TRUE";

    private ShopDatabase() {
    }

    /**
     * DB を開き、テーブルがなければ作る。
     *
     * @return JDBC 接続（使い終わったら close すること）
     * @throws SQLException 接続や DDL に失敗した場合
     */
    public static Connection open() throws SQLException {
        Connection conn = DriverManager.getConnection(URL, "sa", "");
        try (Statement st = conn.createStatement()) {
            // 注文そのもの
            st.execute("CREATE TABLE IF NOT EXISTS orders ("
                    + "order_id VARCHAR(50) PRIMARY KEY, customer_id VARCHAR(50) NOT NULL, amount BIGINT NOT NULL)");
            // 送るべきイベント。published_at が NULL の行が「まだ送っていない」イベント
            st.execute("CREATE TABLE IF NOT EXISTS outbox ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
                    + "event_id VARCHAR(100) NOT NULL, "
                    + "payload VARCHAR(2000) NOT NULL, "
                    + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                    + "published_at TIMESTAMP NULL)");
        }
        return conn;
    }
}
