package com.queue;

import java.util.Date;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * LinkedBlockingDeque：一个由链表结构组成的双向阻塞队列
 */
public class  ClientThread implements Runnable {
    private LinkedBlockingDeque<String> requestList;

    public ClientThread(LinkedBlockingDeque<String> requestList) {
        super();
        this.requestList = requestList;
    }

    @Override
    public void run() {
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 5; j++) {
                StringBuilder request = new StringBuilder();
                request.append(i);
                request.append(":");
                request.append(j);
                try {
                    requestList.put(request.toString());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Clint: " + request + " " + new Date());
            }
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Client: End");
    }
}