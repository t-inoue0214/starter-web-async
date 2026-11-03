package com.example.async.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

import com.example.async.common.Console;

/**
 * 注文確定の知らせを UDP で5件送る送信側。
 */
public class UdpSender {

    /**
     * 0.5秒おきに5件送る。受信側がいなくてもエラーにならないことを確かめる。
     *
     * @param args 使わない
     * @throws Exception ソケットの操作に失敗した場合
     */
    public static void main(String[] args) throws Exception {
        InetAddress address = InetAddress.getByName("localhost");
        try (DatagramSocket socket = new DatagramSocket()) {
            for (int i = 1; i <= 5; i++) {
                byte[] data = ("order-A-" + i).getBytes(StandardCharsets.UTF_8);
                // 宛先を指定して送るだけ。相手が受け取ったかどうかは、送信側にはわからない
                socket.send(new DatagramPacket(data, data.length, address, UdpReceiver.PORT));
                Console.log("送信: order-A-" + i + "（例外は出ていない）");
                Thread.sleep(500);
            }
        }
    }
}
