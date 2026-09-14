package plus.jmqx.broker.mqtt.message.dispatch;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * PINGREQ 心跳消息
 *
 * @author maxid
 * @since 2026/9/14
 */
@Data
@Builder
@ToString
public class PingMessage {

    private final String clientId;
    private final String username;

}
