package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * MQTT 消息包装
 *
 * @author maxid
 * @since 2025/4/9 17:44
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
public class MessageWrapper<T extends MqttMessage> {
    /**
     * MQTT 消息
     */
    private T       message;
    /**
     * 消息时间
     */
    private long    timestamp;
    /**
     * 是否集群消息
     */
    private Boolean clustered;
}
