package com.example.async.inmemory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;

import com.example.async.common.Console;

/**
 * スタック（後入れ先出し）とキュー（先入れ先出し）の違いを確かめる小さなプログラム。
 */
public class StackAndQueueDemo {

    /**
     * 同じ順番で3件入れて、取り出す順番を比べる。
     *
     * @param args 使わない
     */
    public static void main(String[] args) {
        // スタック: 入れた順と逆に取り出される（LIFO: Last In, First Out）
        Deque<String> stack = new ArrayDeque<>();
        // push で積む
        stack.push("A-1");
        stack.push("A-2");
        stack.push("A-3");
        Console.log("スタック: " + stack.pop() + " → " + stack.pop() + " → " + stack.pop());

        // キュー: 入れた順に取り出される（FIFO: First In, First Out）
        Queue<String> queue = new ArrayDeque<>();
        // offer で末尾に並ぶ
        queue.offer("A-1");
        queue.offer("A-2");
        queue.offer("A-3");
        Console.log("キュー  : " + queue.poll() + " → " + queue.poll() + " → " + queue.poll());
    }
}
