package plus.jmqx.broker.mqtt.message.dispatch;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * 发布消息
 *
 * @author maxid
 * @since 2025/4/21 15:59
 */
@Data
@Builder
@ToString
public class PublishMessage {
    private final String clientId;
    private final String username;
    private final String topic;
    private final byte[] payload;
}
