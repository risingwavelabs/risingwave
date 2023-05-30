package com.risingwave.functions.example;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class ExtractTcpInfo {
    public static class TcpPacketInfo {
        public final String srcAddr;
        public final String dstAddr;
        public final short srcPort;
        public final short dstPort;

        public TcpPacketInfo(String srcAddr, String dstAddr, short srcPort, short dstPort) {
            this.srcAddr = srcAddr;
            this.dstAddr = dstAddr;
            this.srcPort = srcPort;
            this.dstPort = dstPort;
        }
    }

    public static TcpPacketInfo eval(byte[] tcpPacket) throws UnknownHostException {
        ByteBuffer buffer = ByteBuffer.wrap(tcpPacket);
        int srcAddrInt = buffer.getInt(12);
        int dstAddrInt = buffer.getInt(16);
        short srcPort = buffer.getShort(20);
        short dstPort = buffer.getShort(22);

        String srcAddr = InetAddress.getByAddress(ByteBuffer.allocate(4).putInt(srcAddrInt).array()).getHostAddress();
        String dstAddr = InetAddress.getByAddress(ByteBuffer.allocate(4).putInt(dstAddrInt).array()).getHostAddress();

        return new TcpPacketInfo(srcAddr, dstAddr, srcPort, dstPort);
    }
}