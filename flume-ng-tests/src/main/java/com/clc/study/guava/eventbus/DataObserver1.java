package com.clc.study.guava.eventbus;

import com.google.common.eventbus.Subscribe;

// 消息处理类（观察者）：
public class DataObserver1 {

    /**
     * 使用Guava之后, 如果要订阅消息, 就不用再继承指定的接口, 只需要在指定的方法上加上@Subscribe注解即可。代码如下：
     *
     * 只有通过@Subscribe注解的方法才会被注册进EventBus
     * 而且方法有且只能有1个参数，在接收到消息后，eventBus会用反射选择对应的处理函数。
     *
     * @param msg
     */
    @Subscribe
    public void handleEvent(String msg) {
        System.out.println("Observer1: " + msg);
    }

}
