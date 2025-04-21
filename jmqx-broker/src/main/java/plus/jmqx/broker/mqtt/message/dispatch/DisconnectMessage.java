package plus.jmqx.broker.mqtt.message.dispatch;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * 断开连接消息
 *
 * @author maxid
 * @since 2025/4/21 15:58
 */
@Data
@Builder
@ToString
public class DisconnectMessage {
    private final String clientId;
    private final String username;
}
