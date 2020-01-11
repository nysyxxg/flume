package org.apache.flume.auth;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.api.SecureRpcClientFactory;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClient;

import java.nio.charset.Charset;
import java.util.Properties;

/**
 * 安全RPC客户端-Thrift： 通过RPC-Thrift客户端发送数据
 * 从Flume 1.6.0开始，ThriftSource和接收器支持基于kerberos的身份验证。
 * 客户端需要使用的getThriftInstance方法SecureRpcClientFactory 获得的保持SecureThriftRpcClient。
 * SecureThriftRpcClient扩展了 ThriftRpcClient，它实现了RpcClient接口。
 * 当使用SecureRpcClientFactory时，kerberos身份验证模块驻留在类路径中需要的flume -ng-auth模块中。
 * 客户端主体和客户端密钥表都应作为参数通过属性传入，并反映客户端的凭据以对kerberos KDC进行身份验证。
 * 此外，还应提供此客户端连接到的目标ThriftSource的服务器主体。
 * 以下示例显示如何 在用户的数据生成应用程序中使用SecureRpcClientFactory：
 * ————————————————
 * 版权声明：本文为CSDN博主「张伯毅」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/zhanglong_4444/article/details/88565862
 */
public class MyThriftRpcApp {

    public static void main(String[] args) {
        MySecureRpcClientFacade client = new MySecureRpcClientFacade();
        Properties props = new Properties();
        props.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE, "thrift");
        props.setProperty("hosts", "h1");
        props.setProperty("hosts.h1", "client.example.org" + ":" + String.valueOf(41414));

        // Initialize client with the kerberos authentication related properties
        props.setProperty("kerberos", "true");
        props.setProperty("client-principal", "flumeclient/client.example.org@EXAMPLE.ORG");
        props.setProperty("client-keytab", "/tmp/flumeclient.keytab");
        props.setProperty("server-principal", "flume/server.example.org@EXAMPLE.ORG");
        client.init(props);

        // Send 10 events to the remote Flume agent. That agent should be
        // configured to listen with an AvroSource.
        String sampleData = "Hello Flume!";
        for (int i = 0; i < 10; i++) {
            client.sendDataToFlume(sampleData);
        }
        client.cleanUp();
    }
}

class MySecureRpcClientFacade {

    private RpcClient client;
    private Properties properties;

    public void init(Properties properties) {
        // Setup the RPC connection
        this.properties = properties;
        // Create the ThriftSecureRpcClient instance by using SecureRpcClientFactory
        this.client = SecureRpcClientFactory.getThriftInstance(properties);
    }

    public void sendDataToFlume(String data) {
        // Create a Flume Event object that encapsulates the sample data
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
        try {
            client.append(event);  // Send the event
        } catch (EventDeliveryException e) {
            // clean up and recreate the client
            client.close();
            client = null;
            client = SecureRpcClientFactory.getThriftInstance(properties);
        }
    }

    public void cleanUp() {
        // Close the RPC connection
        client.close();
    }
}