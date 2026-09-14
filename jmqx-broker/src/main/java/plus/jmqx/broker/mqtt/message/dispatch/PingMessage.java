package plus.jmqx.broker.mqtt.message.dispatch;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * PINGREQ 心跳回调消息
 *
 * @author maxid
 * @since 2026/9/14
 */
@Data
@Builder
@ToString
public class PingMessage {

    /**
     * 客户端标识
     */
    private final String clientId;
    /**
     * 连接用户名，未携带时为 {@code null}
     */
    private final String username;

}
