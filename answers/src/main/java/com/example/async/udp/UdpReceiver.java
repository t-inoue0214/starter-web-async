package com.example.async.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;

/**
 * UDP の 9999 番ポートで待ち受け、届いたデータを表示する受信側。
 */
public class UdpReceiver {
    /** 待ち受けるポート番号 */
    static final int PORT = 9999;

    /**
     * Ctrl+C で止めるまで受信し続ける。
     *
     * @param args 使わない
     * @throws Exception ソケットの操作に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        try (DatagramSocket socket = new DatagramSocket(PORT)) {
            Console.log("UDP " + PORT + " 番で待ち受け中です");
            byte[] buffer = new byte[1024];
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                // 届くまで待つ
                socket.receive(packet);
                Console.log("受信: " + new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8));
            }
        }
    }
}
