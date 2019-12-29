package org.apache.flume.source;

import org.junit.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

public class TestSyslogUdpSourceV2 {

    /**
     * 测试 udp source ，发送udp报文
     */
    @Test
    public void testSysLogUdp() throws Exception {

        try (DatagramSocket syslogSocket = new DatagramSocket()) { // 放在这里，可以自动关闭
            byte[] largePayload = getPayload(1000).getBytes();

            DatagramPacket datagramPacket = new DatagramPacket(largePayload, largePayload.length);

            InetSocketAddress addr = new InetSocketAddress("localhost", 8888);
            // InetAddress  address = InetAddress.getByName("localhost");
            datagramPacket.setAddress(addr.getAddress());
            datagramPacket.setPort(8888);
            syslogSocket.send(datagramPacket);
            syslogSocket.close();
        } catch (SocketException e) {
            e.printStackTrace();
        }

    }

    private String getPayload(int length) {
        StringBuilder payload = new StringBuilder(length);
        for (int n = 0; n < length; ++n) {
            payload.append("x");
        }
        return payload.toString();
    }

}
