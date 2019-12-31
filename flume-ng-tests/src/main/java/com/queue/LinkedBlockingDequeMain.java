package com.queue;


import java.util.Date;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * LinkedBlockingDeque是双向链表实现的双向并发阻塞队列。该阻塞队列同时支持FIFO和FILO两种操作方式，
 * 即可以从队列的头和尾同时操作(插入/删除)；并且，该阻塞队列是支持线程安全。
 * 此外，LinkedBlockingDeque还是可选容量的(防止过度膨胀)，即可以指定队列的容量。如果不指定，默认容量大小等于Integer.MAX_VALUE。
 *
 * takeFirst()和takeLast()：分别返回类表中第一个和最后一个元素，返回的元素会从类表中移除。如果列表为空，调用的方法的线程将会被阻塞直达列表中有可用元素。
 * getFirst()和getLast():分别返回类表中第一个和最后一个元素，返回的元素不会从列表中移除。如果列表为空，则抛出NoSuckElementException异常。
 * peek()、peekFirst()和peekLast():分别返回列表中第一个元素和最后一个元素，返回元素不会被移除。如果列表为空返回null.
 * poll()、pollFirst()和pollLast():分别返回类表中第一个和最后一个元素，返回的元素会从列表中移除。如果列表为空，返回Null。
 *
 */
public class LinkedBlockingDequeMain {

    public static void main(String[] args) throws Exception {
        LinkedBlockingDeque<String> list = new LinkedBlockingDeque<String>(3);
        ClientThread client = new ClientThread(list);
        Thread thread = new Thread(client);
        thread.start();
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 3; j++) {
                String request = list.take();// 获取并移除此双端队列的第一个元素，必要时将一直等待可用元素
                System.out.println("Main:Request:" + request + " at " + new Date() + " Size " + list.size());
            }
            TimeUnit.MILLISECONDS.sleep(300);
        }
        System.out.println("Main:End");
    }

}

/**
 // 创建一个容量为 Integer.MAX_VALUE 的 LinkedBlockingDeque。
 LinkedBlockingDeque()
 // 创建一个容量为 Integer.MAX_VALUE 的 LinkedBlockingDeque，最初包含给定 collection 的元素，以该 collection 迭代器的遍历顺序添加。
 LinkedBlockingDeque(Collection<? extends E> c)
 // 创建一个具有给定（固定）容量的 LinkedBlockingDeque。
 LinkedBlockingDeque(int capacity)

 // 在不违反容量限制的情况下，将指定的元素插入此双端队列的末尾。
 boolean add(E e)
 // 如果立即可行且不违反容量限制，则将指定的元素插入此双端队列的开头；如果当前没有空间可用，则抛出 IllegalStateException。
 void addFirst(E e)
 // 如果立即可行且不违反容量限制，则将指定的元素插入此双端队列的末尾；如果当前没有空间可用，则抛出 IllegalStateException。
 void addLast(E e)
 // 以原子方式 (atomically) 从此双端队列移除所有元素。
 void clear()
 // 如果此双端队列包含指定的元素，则返回 true。
 boolean contains(Object o)
 // 返回在此双端队列的元素上以逆向连续顺序进行迭代的迭代器。
 Iterator<E> descendingIterator()
 // 移除此队列中所有可用的元素，并将它们添加到给定 collection 中。
 int drainTo(Collection<? super E> c)
 // 最多从此队列中移除给定数量的可用元素，并将这些元素添加到给定 collection 中。
 int drainTo(Collection<? super E> c, int maxElements)
 // 获取但不移除此双端队列表示的队列的头部。
 E element()
 // 获取，但不移除此双端队列的第一个元素。
 E getFirst()
 // 获取，但不移除此双端队列的最后一个元素。
 E getLast()
 // 返回在此双端队列元素上以恰当顺序进行迭代的迭代器。
 Iterator<E> iterator()
 // 如果立即可行且不违反容量限制，则将指定的元素插入此双端队列表示的队列中（即此双端队列的尾部），并在成功时返回 true；如果当前没有空间可用，则返回 false。
 boolean offer(E e)
 // 将指定的元素插入此双端队列表示的队列中（即此双端队列的尾部），必要时将在指定的等待时间内一直等待可用空间。
 boolean offer(E e, long timeout, TimeUnit unit)
 // 如果立即可行且不违反容量限制，则将指定的元素插入此双端队列的开头，并在成功时返回 true；如果当前没有空间可用，则返回 false。
 boolean offerFirst(E e)
 // 将指定的元素插入此双端队列的开头，必要时将在指定的等待时间内等待可用空间。
 boolean offerFirst(E e, long timeout, TimeUnit unit)
 // 如果立即可行且不违反容量限制，则将指定的元素插入此双端队列的末尾，并在成功时返回 true；如果当前没有空间可用，则返回 false。
 boolean offerLast(E e)
 // 将指定的元素插入此双端队列的末尾，必要时将在指定的等待时间内等待可用空间。
 boolean offerLast(E e, long timeout, TimeUnit unit)
 // 获取但不移除此双端队列表示的队列的头部（即此双端队列的第一个元素）；如果此双端队列为空，则返回 null。
 E peek()
 // 获取，但不移除此双端队列的第一个元素；如果此双端队列为空，则返回 null。
 E peekFirst()
 // 获取，但不移除此双端队列的最后一个元素；如果此双端队列为空，则返回 null。
 E peekLast()
 // 获取并移除此双端队列表示的队列的头部（即此双端队列的第一个元素）；如果此双端队列为空，则返回 null。
 E poll()
 // 获取并移除此双端队列表示的队列的头部（即此双端队列的第一个元素），如有必要将在指定的等待时间内等待可用元素。
 E poll(long timeout, TimeUnit unit)
 // 获取并移除此双端队列的第一个元素；如果此双端队列为空，则返回 null。
 E pollFirst()
 // 获取并移除此双端队列的第一个元素，必要时将在指定的等待时间等待可用元素。
 E pollFirst(long timeout, TimeUnit unit)
 // 获取并移除此双端队列的最后一个元素；如果此双端队列为空，则返回 null。
 E pollLast()
 // 获取并移除此双端队列的最后一个元素，必要时将在指定的等待时间内等待可用元素。
 E pollLast(long timeout, TimeUnit unit)
 // 从此双端队列所表示的堆栈中弹出一个元素。
 E pop()
 // 将元素推入此双端队列表示的栈。
 void push(E e)
 // 将指定的元素插入此双端队列表示的队列中（即此双端队列的尾部），必要时将一直等待可用空间。
 void put(E e)
 // 将指定的元素插入此双端队列的开头，必要时将一直等待可用空间。
 void putFirst(E e)
 // 将指定的元素插入此双端队列的末尾，必要时将一直等待可用空间。
 void putLast(E e)
 // 返回理想情况下（没有内存和资源约束）此双端队列可不受阻塞地接受的额外元素数。
 int remainingCapacity()
 // 获取并移除此双端队列表示的队列的头部。
 E remove()
 // 从此双端队列移除第一次出现的指定元素。
 boolean remove(Object o)
 // 获取并移除此双端队列第一个元素。
 E removeFirst()
 // 从此双端队列移除第一次出现的指定元素。
 boolean removeFirstOccurrence(Object o)
 // 获取并移除此双端队列的最后一个元素。
 E removeLast()
 // 从此双端队列移除最后一次出现的指定元素。
 boolean removeLastOccurrence(Object o)
 // 返回此双端队列中的元素数。
 int size()
 // 获取并移除此双端队列表示的队列的头部（即此双端队列的第一个元素），必要时将一直等待可用元素。
 E take()
 // 获取并移除此双端队列的第一个元素，必要时将一直等待可用元素。
 E takeFirst()
 // 获取并移除此双端队列的最后一个元素，必要时将一直等待可用元素。
 E takeLast()
 // 返回以恰当顺序（从第一个元素到最后一个元素）包含此双端队列所有元素的数组。
 Object[] toArray()
 // 返回以恰当顺序包含此双端队列所有元素的数组；返回数组的运行时类型是指定数组的运行时类型。
 <T> T[] toArray(T[] a)
 // 返回此 collection 的字符串表示形式。
 String toString()
 ————————————————
 版权声明：本文为CSDN博主「戎码一生」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
 原文链接：https://blog.csdn.net/u010923921/article/details/77488043
 **/