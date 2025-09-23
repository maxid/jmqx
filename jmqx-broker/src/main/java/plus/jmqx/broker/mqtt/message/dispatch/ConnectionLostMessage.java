package plus.jmqx.broker.mqtt.message.dispatch;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import plus.jmqx.broker.mqtt.channel.SessionStatus;

/**
 * 失去连接消息
 *
 * @author maxid
 * @since 2025/4/21 16:00
 */
@Data
@Builder
@ToString
public class ConnectionLostMessage {
    private final String        clientId;
    private final String        username;
    private final SessionStatus status;
}
