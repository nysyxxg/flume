/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.kafka;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

public class KafkaSourceEmbeddedZookeeper {
    private int zkPort;
    private ZooKeeperServer zookeeper;
    // 主要启动zk服务
    private NIOServerCnxnFactory factory;
    File dir;

    public KafkaSourceEmbeddedZookeeper(int zkPort) {
        int tickTime = 2000;
        this.zkPort = zkPort;
        String dataDirectory = System.getProperty("java.io.tmpdir");
        dir = new File(dataDirectory, "zookeeper" + UUID.randomUUID()).getAbsoluteFile();

        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            this.zookeeper = new ZooKeeperServer(dir, dir, tickTime); // 实例化zk
            // NIOServerCnxnFactory基于NIO实现了一个多线程的ServerCnxnFactory，线程间的通信都是通过queue来完成的。
            this.factory = new NIOServerCnxnFactory();
            factory.configure(new InetSocketAddress(KafkaSourceEmbeddedKafka.HOST, zkPort), 0);
            factory.startup(zookeeper); // 启动zk
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stopZookeeper() throws IOException { // 停止zk
        zookeeper.shutdown();
        factory.shutdown();
        FileUtils.deleteDirectory(dir);
    }

    public String getConnectString() { // 得到zk连接字符串
        return KafkaSourceEmbeddedKafka.HOST + ":" + zkPort;
    }
}
