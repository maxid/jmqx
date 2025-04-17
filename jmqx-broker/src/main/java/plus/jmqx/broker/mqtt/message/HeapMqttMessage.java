package plus.jmqx.broker.mqtt.message;

import io.netty.handler.codec.mqtt.MqttProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * 集群间 MQTT 消息
 *
 * @author maxid
 * @since 2025/4/16 17:35
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HeapMqttMessage {
    private long timestamp;

    private String clientId;

    private String topic;

    private int qos;

    private boolean retain;

    private Object message;

    private MqttProperties properties;

    public Map<String, Object> getKeyMap() {
        Map<String, Object> keys = new HashMap<>(5);
        keys.put("clientId", this.clientId);
        keys.put("topic", this.topic);
        keys.put("qos", this.qos);
        keys.put("retain", this.retain);
        keys.put("msg", message);
        return keys;
    }
}
