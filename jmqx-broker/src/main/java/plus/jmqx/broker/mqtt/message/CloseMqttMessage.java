package plus.jmqx.broker.mqtt.message;

import lombok.Data;

/**
 * 关闭消息
 *
 * @author maxid
 * @since 2025/4/17 09:53
 */
@Data
public class CloseMqttMessage {
    private String clientId;
}
