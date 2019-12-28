/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * MemoryChannel is the recommended channel to use when speeds which
 * writing to disk is impractical is required or durability of data is not
 * required.
 * </p>
 * <p>
 * Additionally, MemoryChannel should be used when a channel is required for
 * unit testing purposes.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Recyclable
public class MemoryChannel extends BasicChannelSemantics {
    private static Logger LOGGER = LoggerFactory.getLogger(MemoryChannel.class);
    private static final Integer defaultCapacity = 100;
    private static final Integer defaultTransCapacity = 100;
    private static final double byteCapacitySlotSize = 100;
    private static final Long defaultByteCapacity = (long) (Runtime.getRuntime().maxMemory() * .80);
    private static final Integer defaultByteCapacityBufferPercentage = 20;

    private static final Integer defaultKeepAlive = 3;

    /**
     * 会根据事务容量 transCapacity 创建两个阻塞双端队列putList和takeList，
     * 这两个队列主要就是用于事务处理的，当从Source往 Channel中放事件event 时，会先将event放入 putList 队列（相当于一个临时缓冲队列），
     * 然后将putList队列中的event 放入 MemoryChannel的queue中；当从 Channel 中将数据传送给 Sink 时，
     * 则会将event先放入 takeList 队列中，然后从takeList队列中将event送入Sink，不论是 put 还是 take 发生异常，
     * 都会调用 rollback 方法回滚事务，会先给 Channel 加锁防止回滚时有其他线程访问，若takeList 不为空，
     * 就将写入 takeList中的event再次放入 Channel 中，然后移除 putList 中的所有event（即就是丢弃写入putList临时队列的 event）。
     * 从上边代码发现这里只是具体方法的实现，实际的的调用是发生在 Source 端写事件和 Sink 读事件时，也就是事务发生时，
     * 如下代码逻辑，具体的实现可以参看前一篇博文《flume Source启动过程分析》
     * Channel ch = ...
     *  Transaction tx = ch.getTransaction();
     *  try {
     *    tx.begin();
     *    ...
     *    // ch.put(event) or ch.take()    Source写事件调用put方法，Sink读事件调用take方法
     *    ...
     *    tx.commit();
     *  } catch (ChannelException ex) {   // 发生异常则回滚事务
     *    tx.rollback();
     *    ...
     *  } finally {
     *    tx.close();
     *  }
     * ————————————————
     * 原文链接：https://blog.csdn.net/ty_laurel/article/details/53907926
     *
     */
    //核心重点类 ：保证了内存事务的线程安全
    private class MemoryTransaction extends BasicTransactionSemantics {
        // LinkedBlockingDeque是一个由链表结构组成的双向阻塞队列，即可以从队列的两端插入和移除元素。
        // 双向队列因为多了一个操作队列的入口，在多线程同时入队时，也就减少了一半的竞争。
        //阻塞双端队列，从channel中取event先放入takeList，输送到sink，commit成功，从channel queue中删除
        private LinkedBlockingDeque<Event> takeList;
        // 从source 会先放至putList，然后commit传送到channel queue队列
        private LinkedBlockingDeque<Event> putList;
        private final ChannelCounter channelCounter; //ChannelCounter类定义了监控指标数据的一些属性方法
        private int putByteCounter = 0;
        private int takeByteCounter = 0;
        //MemoryTransaction方法中初始化事务需要的两个阻塞双端队列
        public MemoryTransaction(int transCapacity, ChannelCounter counter) {
            putList = new LinkedBlockingDeque<Event>(transCapacity);
            takeList = new LinkedBlockingDeque<Event>(transCapacity);

            channelCounter = counter;
        }
        //重写父类BasicChannelSemantics中的几个事务处理方法，往putList队列中添加指定Event
        @Override // 将event放入到阻塞队列中
        protected void doPut(Event event) throws InterruptedException {
            channelCounter.incrementEventPutAttemptCount();//将正在尝试放入channel 的event计数器原子的加一
            int eventByteSize = (int) Math.ceil(estimateEventSize(event) / byteCapacitySlotSize);

            /**
             * * offer：如果立即可行且不违反容量限制，则将指定的元素插入putList阻塞双端队列中（队尾）
             * 并在成功时返回，如果当前没有空间可用，则返回false
             *  将source端的数据，插入到putList的队尾
             */
            if (!putList.offer(event)) {
                throw new ChannelException(//队列满，抛异常
                        "Put queue for MemoryTransaction of capacity " +
                                putList.size() + " full, consider committing more frequently, " +
                                "increasing capacity or increasing thread count");
            }
            putByteCounter += eventByteSize;
        }

        /**
         *    从MemoryChannel的queue队列中取元素，然后放入takeList里面，作为本次事务需要提交的Event
         * @return
         * @throws InterruptedException
         */
        @Override// 从阻塞队列中获取event
        protected Event doTake() throws InterruptedException {
            channelCounter.incrementEventTakeAttemptCount(); //将正在从channel中取出的event计数器原子的加一
            if (takeList.remainingCapacity() == 0) { //takeList队列剩余容量为0，抛异常
                throw new ChannelException("Take list for MemoryTransaction, capacity " +
                        takeList.size() + " full, consider committing more frequently, " +
                        "increasing capacity, or increasing thread count");
            }
            if (!queueStored.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
                return null;
            }
            Event event;
            synchronized (queueLock) {//从Channel queue中take event，同一时间只能有一个线程访问，加锁同步
                // 获取并移除MemoryChannel双端队列表示的队列的头部(也就是队列的第一个元素)，队列为空返回null
                event = queue.poll(); // 获取event,
            }
            Preconditions.checkNotNull(event, "Queue.poll returned NULL despite semaphore " +
                    "signalling existence of entry");
            takeList.put(event);   //将取出的event放入takeList
            /* 计算event的byte大小 */
            int eventByteSize = (int) Math.ceil(estimateEventSize(event) / byteCapacitySlotSize);
            takeByteCounter += eventByteSize;

            return event;
        }

        /**
         *   事务提交
         * doCommit 的作用： 将putList队列中头部元素取出，然后commit传送到channel queue队列
         * @throws InterruptedException
         */
        @Override
        protected void doCommit() throws InterruptedException {
            //takeList.size()可以看成 sink，putList.size()看成  source
            int remainingChange = takeList.size() - putList.size();  //  2 < 5
            if (remainingChange < 0) {   //sink的消费速度慢于source的产生速度
                //判断是否有足够空间接收putList中的events所占的空间
                if (!bytesRemaining.tryAcquire(putByteCounter, keepAlive, TimeUnit.SECONDS)) {
                    throw new ChannelException("Cannot commit transaction. Byte capacity " +
                            "allocated to store event body " + byteCapacity * byteCapacitySlotSize +
                            "reached. Please increase heap space/byte capacity allocated to " +
                            "the channel as the sinks may not be keeping up with the sources");
                }
                //因为source速度快于sink速度，需判断queue是否还有空间接收event
                if (!queueRemaining.tryAcquire(-remainingChange, keepAlive, TimeUnit.SECONDS)) {
                    bytesRemaining.release(putByteCounter);
                    throw new ChannelFullException("Space for commit to queue couldn't be acquired." +
                            " Sinks are likely not keeping up with sources, or the buffer size is too tight");
                }
            }
            int puts = putList.size();//事务期间生产的event 个数
            int takes = takeList.size();   //事务期间等待消费的event 个数
            synchronized (queueLock) { //每次保证只有一个线程访问
                if (puts > 0) {
                    while (!putList.isEmpty()) {
                        // 从 putList (removeFirst)获取并移除此双端队列第一个元素。然后将event插入到queue的队尾
                        if (!queue.offer(putList.removeFirst())) {  //将新添加的events保存到queue中
                            throw new RuntimeException("Queue add failed, this shouldn't be able to happen");
                        }
                    }
                }
                putList.clear();//以上步骤执行成功，清空事务的putList和takeList
                takeList.clear();
            }
            bytesRemaining.release(takeByteCounter);
            takeByteCounter = 0;
            putByteCounter = 0;

            queueStored.release(puts);//从queueStored释放puts个信号量
            if (remainingChange > 0) {
                queueRemaining.release(remainingChange);
            }
            if (puts > 0) {  //更新成功放入Channel中的events监控指标数据
                channelCounter.addToEventPutSuccessCount(puts);
            }
            if (takes > 0) {  //更新成功从Channel中取出的events的数量
                channelCounter.addToEventTakeSuccessCount(takes);
            }

            channelCounter.setChannelSize(queue.size());
        }
        //事务回滚
        @Override
        protected void doRollback() {
            int takes = takeList.size();
            synchronized (queueLock) {
                Preconditions.checkState(queue.remainingCapacity() >= takeList.size(),
                        "Not enough space in memory channel " +
                                "queue to rollback takes. This should never happen, please report");
                while (!takeList.isEmpty()) {//如果takeList不为空，将其events全部放回queue
                    //removeLast()获取并移除此双端队列的最后一个元素
                    queue.addFirst(takeList.removeLast());
                }
                putList.clear();
            }
            putByteCounter = 0;
            takeByteCounter = 0;

            queueStored.release(takes);
            channelCounter.setChannelSize(queue.size());
        }

    }

    // lock to guard queue, mainly needed to keep it locked down during resizes
    // it should never be held through a blocking operation
    private Object queueLock = new Object();

    @GuardedBy(value = "queueLock")
    private LinkedBlockingDeque<Event> queue;  //Channel端用来的存储数据， 存储event的阻塞队列

    // invariant that tracks the amount of space remaining in the queue(with all uncommitted takeLists deducted)
    // we maintain the remaining permits = queue.remaining - takeList.size()
    // this allows local threads waiting for space in the queue to commit without denying access to the
    // shared lock to threads that would make more space on the queue
    private Semaphore queueRemaining;

    // used to make "reservations" to grab data from the queue.
    // by using this we can block for a while to get data without locking all other threads out
    // like we would if we tried to use a blocking call on queue
    private Semaphore queueStored;

    // maximum items in a transaction queue
    private volatile Integer transCapacity;
    private volatile int keepAlive;
    private volatile int byteCapacity;
    private volatile int lastByteCapacity;
    private volatile int byteCapacityBufferPercentage;
    private Semaphore bytesRemaining;
    private ChannelCounter channelCounter;

    public MemoryChannel() {
        super();
    }

    /**
     * Read parameters from context
     * <li>capacity = type long that defines the total number of events allowed at one time in the queue.
     * <li>transactionCapacity = type long that defines the total number of events allowed in one transaction.
     * <li>byteCapacity = type long that defines the max number of bytes used for events in the queue.
     * <li>byteCapacityBufferPercentage = type int that defines the percent of buffer between byteCapacity and the estimated event size.
     * <li>keep-alive = type int that defines the number of second to wait for a queue permit
     *
     * MemoryChannel 第三部分就是通过configure方法获取配置文件系统，
     * 初始化MemoryChannel，其中对于配置信息的读取有两种方法，只在启动时读取一次或者动态的加载配置文件，
     * 动态读取配置文件时若修改了Channel 的容量大小，则会调用 resizeQueue 方法进行调整，如下：
     * ————————————————
     * 原文链接：https://blog.csdn.net/ty_laurel/article/details/53907926
     *
     */
    @Override
    public void configure(Context context) {
        Integer capacity = null;
        try {
            capacity = context.getInteger("capacity", defaultCapacity);
        } catch (NumberFormatException e) {
            capacity = defaultCapacity;
            LOGGER.warn("Invalid capacity specified, initializing channel to "
                    + "default capacity of {}", defaultCapacity);
        }

        if (capacity <= 0) {
            capacity = defaultCapacity;
            LOGGER.warn("Invalid capacity specified, initializing channel to "
                    + "default capacity of {}", defaultCapacity);
        }
        try {
            transCapacity = context.getInteger("transactionCapacity", defaultTransCapacity);
        } catch (NumberFormatException e) {
            transCapacity = defaultTransCapacity;
            LOGGER.warn("Invalid transation capacity specified, initializing channel"
                    + " to default capacity of {}", defaultTransCapacity);
        }

        if (transCapacity <= 0) {
            transCapacity = defaultTransCapacity;
            LOGGER.warn("Invalid transation capacity specified, initializing channel"
                    + " to default capacity of {}", defaultTransCapacity);
        }
        Preconditions.checkState(transCapacity <= capacity,
                "Transaction Capacity of Memory Channel cannot be higher than " +
                        "the capacity.");

        try {
            byteCapacityBufferPercentage = context.getInteger("byteCapacityBufferPercentage",
                    defaultByteCapacityBufferPercentage);
        } catch (NumberFormatException e) {
            byteCapacityBufferPercentage = defaultByteCapacityBufferPercentage;
        }

        try {
            byteCapacity = (int) ((context.getLong("byteCapacity", defaultByteCapacity).longValue() *
                    (1 - byteCapacityBufferPercentage * .01)) / byteCapacitySlotSize);
            if (byteCapacity < 1) {
                byteCapacity = Integer.MAX_VALUE;
            }
        } catch (NumberFormatException e) {
            byteCapacity = (int) ((defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01)) /
                    byteCapacitySlotSize);
        }

        try {
            keepAlive = context.getInteger("keep-alive", defaultKeepAlive);
        } catch (NumberFormatException e) {
            keepAlive = defaultKeepAlive;
        }
         //queue不为null，则为动态修改配置文件时，重新指定了capacity
        if (queue != null) {
            try {
                resizeQueue(capacity);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {//初始化queue，根据指定的capacity申请双向阻塞队列，并初始化信号量
            synchronized (queueLock) {
                queue = new LinkedBlockingDeque<Event>(capacity);
                queueRemaining = new Semaphore(capacity);
                queueStored = new Semaphore(0);
            }
        }

        if (bytesRemaining == null) {
            bytesRemaining = new Semaphore(byteCapacity);
            lastByteCapacity = byteCapacity;
        } else {
            if (byteCapacity > lastByteCapacity) {
                bytesRemaining.release(byteCapacity - lastByteCapacity);
                lastByteCapacity = byteCapacity;
            } else {
                try {
                    if (!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity, keepAlive,
                            TimeUnit.SECONDS)) {
                        LOGGER.warn("Couldn't acquire permits to downsize the byte capacity, resizing has been aborted");
                    } else {
                        lastByteCapacity = byteCapacity;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (channelCounter == null) {
            channelCounter = new ChannelCounter(getName());
        }
    }

    /**
     * 动态调整 Channel 容量主要分为三种情况：
     * 新老容量相同，则直接返回；
     * 老容量大于新容量，缩容，需先给未被占用的空间加锁，防止在缩容时有线程再往其写数据，然后创建新容量的队列，将原本队列加入中所有的 event 添加至新队列中；
     * 老容量小于新容量，扩容，然后创建新容量的队列，将原本队列加入中所有的 event 添加至新队列中。
     * ————————————————
     * 原文链接：https://blog.csdn.net/ty_laurel/article/details/53907926
     * @param capacity
     * @throws InterruptedException
     */
    // 对阻塞队列进行扩容
    private void resizeQueue(int capacity) throws InterruptedException {
        int oldCapacity;  //计算原本的Channel Queue的容量
        synchronized (queueLock) {
            oldCapacity = queue.size() + queue.remainingCapacity();
        }

        if (oldCapacity == capacity) {    //新容量和老容量相等，不需要调整返回
            return;
        } else if (oldCapacity > capacity) {  //缩容
            //首先要预占用未被占用的容量，防止其他线程进行操作
            if (!queueRemaining.tryAcquire(oldCapacity - capacity, keepAlive, TimeUnit.SECONDS)) {
                LOGGER.warn("Couldn't acquire permits to downsize the queue, resizing has been aborted");
            } else {         //锁定queueLock进行缩容，先创建新capacity的双端阻塞队列，然后复制老Queue数据。线程安全
                synchronized (queueLock) {
                    LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
                    newQueue.addAll(queue);
                    queue = newQueue;
                }
            }
        } else {  //扩容，加锁，创建新newQueue，复制老queue数据
            synchronized (queueLock) {
                LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
                newQueue.addAll(queue);
                queue = newQueue;
            }
            queueRemaining.release(capacity - oldCapacity);   //释放capacity - oldCapacity个许可，即就是增加这么多可用许可
        }
    }

    @Override
    public synchronized void start() {
        channelCounter.start();
        channelCounter.setChannelSize(queue.size());
        channelCounter.setChannelCapacity(Long.valueOf(
                queue.size() + queue.remainingCapacity()));
        super.start();
    }

    @Override
    public synchronized void stop() {
        channelCounter.setChannelSize(queue.size());
        channelCounter.stop();
        super.stop();
    }

    @Override
    protected BasicTransactionSemantics createTransaction() {
        return new MemoryTransaction(transCapacity, channelCounter);
    }

    private long estimateEventSize(Event event) {
        byte[] body = event.getBody();
        if (body != null && body.length != 0) {
            return body.length;
        }
        //Each event occupies at least 1 slot, so return 1.
        return 1;
    }

    @VisibleForTesting
    int getBytesRemainingValue() {
        return bytesRemaining.availablePermits();
    }
}
