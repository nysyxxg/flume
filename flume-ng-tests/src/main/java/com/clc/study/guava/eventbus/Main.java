package com.clc.study.guava.eventbus;

/**
 * 消息发布
 */
public class Main {
    public static void main(String[] args) {

        DataObserver1 observer1 = new DataObserver1();
        DataObserver2 observer2 = new DataObserver2();

        //注册观察者
        EventBusManager.register(observer1);
        EventBusManager.register(observer2);

        //添加事件
        EventBusManager.post("hello world!");

        EventBusManager.post(12345);
        EventBusManager.unregister(observer1);

        EventBusManager.post("hello world2!");
        EventBusManager.post(123456);

    }
}