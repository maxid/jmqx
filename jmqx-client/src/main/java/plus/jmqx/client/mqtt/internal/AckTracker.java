package plus.jmqx.client.mqtt.internal;

import plus.jmqx.client.mqtt.message.MqttPublishResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按 packetId 跟踪出站 QoS1/2 消息。
 *
 * <p>"什么在等待 ACK" 的单一真相来源 —— 喂给 {@link MqttOutbox} 的 inflight 计数。
 *
 * @author maxid
 */
public final class AckTracker {

    private final Map<Integer, PendingOutbound> pending = new ConcurrentHashMap<>();

    public void register(int packetId, PendingOutbound po) {
        pending.put(packetId, po);
    }

    /** QoS1 PUBACK 或 QoS2 PUBCOMP —— 最终完成。 */
    public void complete(int packetId, MqttPublishResult result) {
        PendingOutbound po = pending.remove(packetId);
        if (po != null) {
            po.getResultSink().tryEmitValue(result);
        }
    }

    /** QoS2 PUBREC 已接收 —— 保持 pending（等待 PUBCOMP）；调用方发送 PUBREL。 */
    public void markReceived(int packetId) {
        // map 条目无状态变化；它保留至 PUBCOMP
    }

    /** 传输断开或错误 —— 以给定原因失败该 pending。 */
    public void fail(int packetId, Throwable error) {
        PendingOutbound po = pending.remove(packetId);
        if (po != null) {
            po.getResultSink().tryEmitValue(new MqttPublishResultImpl(po.getPublish(), error));
        }
    }

    public PendingOutbound remove(int packetId) {
        return pending.remove(packetId);
    }

    public boolean isComplete(int packetId) {
        return !pending.containsKey(packetId);
    }

    public int size() {
        return pending.size();
    }

    /** 失败所有 pending 条目（用于禁用重连的硬断开）。 */
    public void failAll(Throwable error) {
        for (var entry : pending.entrySet()) {
            entry.getValue().getResultSink().tryEmitValue(
                    new MqttPublishResultImpl(entry.getValue().getPublish(), error));
        }
        pending.clear();
    }
}
