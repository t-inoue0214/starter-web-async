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
