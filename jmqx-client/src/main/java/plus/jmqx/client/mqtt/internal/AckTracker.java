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

    /**
     * 待 ACK 的出站消息映射（packetId -> PendingOutbound）
     */
    private final Map<Integer, PendingOutbound> pending = new ConcurrentHashMap<>();

    /**
     * 注册一个待 ACK 的出站消息。
     *
     * @param packetId 消息的 packetId
     * @param po       待确认的出站消息对象
     */
    public void register(int packetId, PendingOutbound po) {
        pending.put(packetId, po);
    }

    /**
     * QoS1 PUBACK 或 QoS2 PUBCOMP —— 最终完成。
     *
     * @param packetId 已确认的 packetId
     * @param result   发布结果
     */
    public void complete(int packetId, MqttPublishResult result) {
        PendingOutbound po = pending.remove(packetId);
        if (po != null) {
            po.getResultSink().tryEmitValue(result);
        }
    }

    /**
     * QoS2 PUBREC 已接收 —— 保持 pending（等待 PUBCOMP）；调用方发送 PUBREL。
     *
     * @param packetId 收到 PUBREC 的 packetId
     */
    public void markReceived(int packetId) {
        // map 条目无状态变化；它保留至 PUBCOMP
    }

    /**
     * 传输断开或错误 —— 以给定原因失败该 pending。
     *
     * @param packetId 要失败的 packetId
     * @param error    失败原因
     */
    public void fail(int packetId, Throwable error) {
        PendingOutbound po = pending.remove(packetId);
        if (po != null) {
            po.getResultSink().tryEmitValue(new MqttPublishResultImpl(po.getPublish(), error));
        }
    }

    /**
     * 移除并返回指定 packetId 的 pending 条目。
     *
     * @param packetId 要移除的 packetId
     * @return 被移除的 PendingOutbound，若不存在则返回 null
     */
    public PendingOutbound remove(int packetId) {
        return pending.remove(packetId);
    }

    /**
     * 检查指定 packetId 是否已完成（已 ACK）。
     *
     * @param packetId 要检查的 packetId
     * @return 若已完成返回 true，否则返回 false
     */
    public boolean isComplete(int packetId) {
        return !pending.containsKey(packetId);
    }

    /**
     * 当前待 ACK 的条目数量。
     *
     * @return pending 条目数量
     */
    public int size() {
        return pending.size();
    }

    /**
     * 失败所有 pending 条目（用于禁用重连的硬断开）。
     *
     * @param error 失败原因
     */
    public void failAll(Throwable error) {
        for (var entry : pending.entrySet()) {
            entry.getValue().getResultSink().tryEmitValue(
                    new MqttPublishResultImpl(entry.getValue().getPublish(), error));
        }
        pending.clear();
    }

}
