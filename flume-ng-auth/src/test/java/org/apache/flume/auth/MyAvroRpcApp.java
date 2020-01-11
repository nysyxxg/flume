package org.apache.flume.auth;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;

/**
 * 1: Client SDK
 * 虽然Flume包含许多内置机制（即Source）来摄取数据，但通常需要能够直接从自定义应用程序与Flume进行通信。
 * Flume Client SDK是一个库，使应用程序能够连接到Flume并通过RPC将数据发送到Flume的数据流。
 * <p>
 * 2: RPC客户端接口
 * Flume的RpcClient接口的实现封装了Flume支持的RPC机制。
 * 用户的应用程序可以简单地调用Flume Client SDK的append（Event）或appendBatch（List <Event>）来发送数据，
 * 而不用担心底层的消息交换细节。用户可以通过直接实现Event接口，使用简单实现（如SimpleEvent类）
 * 或使用 EventBuilder重载的withBody（）静态帮助器方法来提供所需的Event arg 。
 * <p>
 * 3: RPC客户端- Avro和Thrift
 * 从Flume 1.4.0开始，Avro是默认的RPC协议。该NettyAvroRpcClient和ThriftRpcClient实现RpcClient接口。
 * 客户端需要使用目标Flume代理的主机和端口创建此对象，然后可以使用RpcClient将数据发送到代理。
 * 以下示例显示如何在用户的数据生成应用程序中使用Flume
 */
public class MyAvroRpcApp {
    public static void main(String[] args) {

        MyRpcClientFacade client = new MyRpcClientFacade();
        // Initialize client with the remote Flume agent's host and port
        client.init("host.example.org", 41414);

        // Send 10 events to the remote Flume agent. That agent should be
        // configured to listen with an AvroSource.
        String sampleData = "Hello Flume!";
        for (int i = 0; i < 10; i++) {
            client.sendDataToFlume(sampleData);
        }
        client.cleanUp();
    }
}

class MyRpcClientFacade {
    private RpcClient client;
    private String hostname;
    private int port;

    public void init(String hostname, int port) {
        // Setup the RPC connection
        this.hostname = hostname;
        this.port = port;
        this.client = RpcClientFactory.getDefaultInstance(hostname, port);
        // Use the following method to create a thrift client (instead of the above line):
        // this.client = RpcClientFactory.getThriftInstance(hostname, port);
    }

    public void sendDataToFlume(String data) {
        // Create a Flume Event object that encapsulates the sample data
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
        // Send the event
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            // clean up and recreate the client
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(hostname, port);
            // Use the following method to create a thrift client (instead of the above line):
            // this.client = RpcClientFactory.getThriftInstance(hostname, port);
        }
    }

    public void cleanUp() {
        // Close the RPC connection
        client.close();
    }
}