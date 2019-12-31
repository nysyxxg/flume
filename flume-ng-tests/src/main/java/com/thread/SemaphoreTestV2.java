package com.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Semaphore也是一个线程同步的辅助类，可以维护当前访问自身的线程个数，并提供了同步机制。
 * 使用Semaphore可以控制同时访问资源的线程个数，
 * 例如，实现一个文件允许的并发访问数。
 * Semaphore的主要方法摘要：
 * 　　void acquire():从此信号量获取一个许可，在提供一个许可前一直将线程阻塞，否则线程被中断。
 * 　　void release():释放一个许可，将其返回给信号量。
 * 　　int availablePermits():返回此信号量中当前可用的许可数。
 * 　　boolean hasQueuedThreads():查询是否有线程正在等待获取。
 * 下面是一个例子：
 */
public class SemaphoreTestV2 {
    public static void main(String[] args) {

        ExecutorService service = Executors.newCachedThreadPool();
        int permits = 3;
        final Semaphore sp = new Semaphore(permits);//创建Semaphore信号量，初始化许可大小为3
        for (int i = 0; i < 10; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e2) {
                e2.printStackTrace();
            }
            Runnable runnable = new MyTaskThread(sp, permits);
            service.execute(runnable);
        }
    }

}

class MyTaskThread implements Runnable {
    private int permits;
    private Semaphore sp;

    public MyTaskThread(Semaphore sp, int permits) {
        this.sp = sp;
        this.permits = permits;
    }

    public void run() {
        try {
            sp.acquire();//请求获得许可，如果有可获得的许可则继续往下执行，许可数减1。否则进入阻塞状态
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        System.out.println("线程" + Thread.currentThread().getName() + "进入，当前已有" + (permits - sp.availablePermits()) + "个并发");
        try {
            Thread.sleep((long) (Math.random() * 10000));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("线程" + Thread.currentThread().getName() + "即将离开");
        sp.release();//释放许可，许可数加1
        //下面代码有时候执行不准确，因为其没有和上面的代码合成原子单元
        System.out.println("线程" + Thread.currentThread().getName() + "已离开，当前已有" + (permits - sp.availablePermits()) + "个并发");
    }
}