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
        CLOSE
    }

    public ClusterMessage(){}

    public ClusterMessage(HeapMqttMessage heapMqttMessage) {
        this.clusterEvent = ClusterEvent.PUBLISH;
        this.message = heapMqttMessage;
    }

    public ClusterMessage(CloseMqttMessage closeMqttMessage) {
        this.clusterEvent = ClusterEvent.CLOSE;
        this.message = closeMqttMessage;
    }
}
