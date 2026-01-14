package plus.jmqx.broker.util;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Objects;

/**
 * 端口工具，主要处理端口分配的支撑
 *
 * @author maxid
 * @since 2026/1/14 09:18
 */
public class PortUtil {

    private final static String ANY_ADDRESS             = "0.0.0.0";

    private PortUtil() {
    }

    /**
     * 获取 TCP 可用端口
     *
     * @param port 默认端口
     * @return 可用端口
     */
    public static int getAvailablePort(Integer port) {
        if (Objects.nonNull(port) && port > 0) {
            boolean using = PortUtil.inuse(port);
            return using ? getAvailablePort(port + 1) : port;
        }
        return port;
    }

    /**
     * 获取 UDP 可用端口
     *
     * @param port 默认端口
     * @return 可用端口
     */
    public static int getUdpAvailablePort(Integer port) {
        if (Objects.nonNull(port) && port > 0) {
            boolean using = PortUtil.udpInuse(port);
            return using ? getUdpAvailablePort(port + 1) : port;
        }
        return port;
    }

    /**
     * 检查 IP TCP 端口是否可用
     *
     * @param port 端口
     * @return 端口是否可用
     */
    public static boolean inuse(int port) {
        return inuse(ANY_ADDRESS, port);
    }

    /**
     * 检查 IP TCP 端口是否可用
     *
     * @param host 监听 IP
     * @param port 端口
     * @return 端口是否可用
     */
    public static boolean inuse(String host, int port) {
        try {
            return checkTcp(host, port);
        } catch (Exception ignore) {
            return true;
        }
    }

    /**
     * 检查 IP UDP 端口是否可用
     *
     * @param port 端口
     * @return 端口是否可用
     */
    public static boolean udpInuse(int port) {
        return udpInuse(ANY_ADDRESS, port);
    }

    /**
     * 检查 IP UDP 端口是否可用
     *
     * @param host 监听 IP
     * @param port 端口
     * @return 端口是否可用
     */
    public static boolean udpInuse(String host, int port) {
        try {
            return checkUdp(host, port);
        } catch (Exception ignore) {
            return true;
        }
    }

    /**
     * 检查 IP TCP 端口是否可用
     *
     * @param host IP
     * @param port 端口
     * @return 端口是否可用
     */
    private static boolean checkTcp(String host, int port) {
        try {
            new Socket(InetAddress.getByName(host), port).close();
            return true;
        } catch (IOException ignore) {
            return false;
        }
    }

    /**
     * 检查 IP UDP 端口是否可用
     *
     * @param host IP
     * @param port 端口
     * @return 端口是否可用
     */
    private static boolean checkUdp(String host, int port) {
        try {
            new DatagramSocket(port, InetAddress.getByName(host)).close();
            return false;
        } catch (IOException ignore) {
            return true;
        }
    }

}
