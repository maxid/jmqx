package plus.jmqx.client.mqtt.internal.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * MQTT packet ID 的单一来源（1..65535）。由 PUBLISH(QoS1/2)、SUBSCRIBE、UNSUBSCRIBE 共享。
 *
 * <p>线程安全，wraparound 循环递增，永不返回 0。
 *
 * @author maxid
 */
public final class PacketIdManager {

    /**
     * 最小 packetId（MQTT 规范不允许为 0）
     */
    private static final int MIN = 1;
    /**
     * 最大 packetId
     */
    private static final int MAX = 65535;

    /**
     * 下一个分配的 packetId
     */
    private final AtomicInteger next = new AtomicInteger(MIN);

    /**
     * 获取下一个可用 packetId（1..65535 循环递增，永不返回 0）。
     *
     * @return 可用的 packetId
     */
    public int nextPacketId() {
        while (true) {
            int id = next.getAndUpdate(v -> (v >= MAX ? MIN : v + 1));
            if (id != 0) {
                return id;
            }
        }
    }

}
