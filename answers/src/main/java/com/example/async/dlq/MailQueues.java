package com.example.async.dlq;

import java.io.IOException;
import java.util.Map;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

/**
 * メール送信キューと、そのデッドレターキュー（DLQ）を宣言する部品。
 * 送信側・受信側・運用ツールのどれから呼んでも同じ設定になるよう、1か所にまとめている。
 */
public final class MailQueues {
    /** メール送信依頼のキュー */
    public static final String QUEUE = "mail-requests";
    /** 処理できなかったメッセージの送り先エクスチェンジ（Dead Letter Exchange） */
    public static final String DLX = "mail-requests.dlx";
    /** 処理できなかったメッセージがたまるキュー（Dead Letter Queue） */
    public static final String DLQ = "mail-requests.dlq";

    private MailQueues() {
    }

    /**
     * DLX・DLQ・本キューを宣言する。
     *
     * @param channel 使うチャネル
     * @throws IOException 宣言に失敗した場合（既存キューと設定が違うと 406 PRECONDITION_FAILED）
     */
    public static void declare(Channel channel) throws IOException {
        // 行き場をなくしたメッセージを受け取るエクスチェンジと、それをためるキュー
        channel.exchangeDeclare(DLX, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(DLQ, true, false, false, null);
        channel.queueBind(DLQ, DLX, "");
        // 本キュー。x-dead-letter-exchange を指定すると、reject（requeue=false）されたメッセージが DLX へ送られる
        Map<String, Object> arguments = Map.of("x-dead-letter-exchange", DLX);
        channel.queueDeclare(QUEUE, true, false, false, arguments);
    }
}
