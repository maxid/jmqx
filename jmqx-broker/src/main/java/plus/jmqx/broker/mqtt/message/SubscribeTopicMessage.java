package plus.jmqx.broker.mqtt.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 订阅主题同步消息
 * <p>
 * 用于集群节点间同步订阅信息，实现主题感知路由。
 * 当节点上的设备订阅/取消订阅某个主题时，向集群广播此消息，
 * 其他节点据此更新路由表，后续 PUBLISH 消息只发给有匹配订阅的节点。
 *
 * @author maxid
 * @since 2025/4/22 09:56
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SubscribeTopicMessage {

    /**
     * 节点标识
     */
    private String nodeId;

    /**
     * 主题过滤器（支持 + 和 # 通配符）
     */
    private String topicFilter;

    /**
     * 是否订阅（true=订阅, false=取消订阅）
     */
    private boolean subscribe;

}
