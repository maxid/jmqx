package plus.jmqx.broker.cluster;

import lombok.Data;
import plus.jmqx.broker.mqtt.message.CloseMqttMessage;
import plus.jmqx.broker.mqtt.message.HeapMqttMessage;

/**
 * 集群消息
 *
 * @author maxid
 * @since 2025/4/17 09:52
 */
@Data
public class ClusterMessage {

    private ClusterEvent clusterEvent;

    private Object message;

    /**
     * 集群事件
     */
    public enum ClusterEvent {
        /**
         * 发布
         */
        PUBLISH,
        /**
         * 关闭
         */
        CLOSE,
        /**
         * 向指定客户端发布
         */
        PUBLISH_TARGET,
        /**
         * 订阅变更通知（用于主题路由同步）
         */
        SUBSCRIBE
    }

    /**
     * 构造空集群消息
     */
    public ClusterMessage(){}

    /**
     * 构造集群发布消息
     *
     * @param heapMqttMessage 发布消息
     */
    public ClusterMessage(HeapMqttMessage heapMqttMessage) {
        this(heapMqttMessage, ClusterEvent.PUBLISH);
    }

    /**
     * 构造集群关闭消息
     *
     * @param closeMqttMessage 关闭消息
     */
    public ClusterMessage(CloseMqttMessage closeMqttMessage) {
        this(closeMqttMessage, ClusterEvent.CLOSE);
    }

    /**
     * 构造集群消息
     *
     * @param message 发布消息
     * @param clusterEvent 集群事件
     */
    public ClusterMessage(Object message, ClusterEvent clusterEvent) {
        this.clusterEvent = clusterEvent;
        this.message = message;
    }

}
