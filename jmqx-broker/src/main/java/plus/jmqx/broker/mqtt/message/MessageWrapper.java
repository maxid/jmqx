package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import lombok.*;
import plus.jmqx.broker.mqtt.channel.MqttSession;

/**
 * MQTT 消息包装
 *
 * @author maxid
 * @since 2025/4/9 17:44
 */
@Getter
@Setter
@ToString
@RequiredArgsConstructor()
public class MessageWrapper<T extends MqttMessage> {
    /**
     * MQTT 消息
     */
    private final T           message;
    /**
     * 消息时间
     */
    private final long        timestamp;
    /**
     * 是否集群消息
     */
    private final Boolean     clustered;
    /**
     * MQTT 会话
     */
    private       MqttSession session;
}
