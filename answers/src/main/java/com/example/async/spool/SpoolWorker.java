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
