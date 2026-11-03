package com.example.async.ordering;

import java.io.IOException;

import com.rabbitmq.client.Channel;

/**
 * 注文ステータス変更イベント用のキューを宣言する部品。
 */
public final class StatusQueues {
    /** 全注文のステータス変更イベントを集めるキュー */
    public static final String QUEUE = "order-status";

    private StatusQueues() {
    }

    /**
     * キューを宣言する。
     *
     * @param channel 使うチャネル
     * @throws IOException 宣言に失敗した場合
     */
    public static void declare(Channel channel) throws IOException {
        channel.queueDeclare(QUEUE, true, false, false, null);
    }
}
