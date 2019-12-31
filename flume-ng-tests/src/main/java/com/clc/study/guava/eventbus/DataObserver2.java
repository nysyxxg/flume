package com.clc.study.guava.eventbus;

import com.google.common.eventbus.Subscribe;

//消息处理类（观察者）：
public class DataObserver2 {

    @Subscribe
    public void handleEvent(Integer num) {
        System.out.println("Observer2: "+num);
    }
}