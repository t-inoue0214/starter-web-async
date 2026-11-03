package com.example.async.outbox;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.example.async.common.Console;

/**
 * 注文件数と、outbox の送信済み・未送信の件数を表示する確認用ツール。
 */
public class OutboxStatus {

    /**
     * 件数を表示する。
     *
     * @param args 使わない
     * @throws Exception DB 操作に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        try (Connection db = ShopDatabase.open(); Statement st = db.createStatement()) {
            Console.log("orders 全件       : " + count(st, "SELECT COUNT(*) FROM orders"));
            Console.log("  うち Outbox 経由 : " + count(st, "SELECT COUNT(*) FROM orders WHERE order_id LIKE 'X%'"));
            Console.log("outbox 送信済み   : " + count(st, "SELECT COUNT(*) FROM outbox WHERE published_at IS NOT NULL"));
            Console.log("outbox 未送信     : " + count(st, "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"));
        }
    }

    // COUNT(*) の結果を返す
    private static long count(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
