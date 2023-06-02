package com.risingwave.functions.example;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.risingwave.functions.ScalarFunction;
import com.risingwave.functions.TableFunction;
import com.risingwave.functions.UdfServer;

public class UdfExample {
    public static void main(String[] args) throws IOException {
        try (var server = new UdfServer("0.0.0.0", 8815)) {
            server.addFunction("int42", new Int42());
            server.addFunction("gcd", new Gcd());
            server.addFunction("gcd3", new Gcd());
            server.addFunction("extract_tcp_info", new ExtractTcpInfo());
            server.addFunction("series", new Series());

            server.start();
            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Int42 extends ScalarFunction {
        public static int eval() {
            return 42;
        }
    }

    public static class Gcd extends ScalarFunction {
        public static int eval(int a, int b) {
            while (b != 0) {
                int temp = b;
                b = a % b;
                a = temp;
            }
            return a;
        }

        // TODO: support multiple eval functions
        // public static int eval(int a, int b, int c) {
        // return eval(eval(a, b), c);
        // }
    }

    public static class ExtractTcpInfo extends ScalarFunction {
        public static class TcpPacketInfo {
            public String srcAddr;
            public String dstAddr;
            public short srcPort;
            public short dstPort;
        }

        public static TcpPacketInfo eval(byte[] tcpPacket) {
            var info = new TcpPacketInfo();
            var buffer = ByteBuffer.wrap(tcpPacket);
            info.srcAddr = intToIpAddr(buffer.getInt(12));
            info.dstAddr = intToIpAddr(buffer.getInt(16));
            info.srcPort = buffer.getShort(20);
            info.dstPort = buffer.getShort(22);
            return info;
        }

        static String intToIpAddr(int addr) {
            return String.format("%d.%d.%d.%d", (addr >> 24) & 0xff, (addr >> 16) & 0xff, (addr >> 8) & 0xff,
                    addr & 0xff);
        }
    }

    public static class Series extends TableFunction<Integer> {
        public void eval(int n) {
            for (int i = 0; i < n; i++) {
                collect(i);
            }
        }
    }
}
