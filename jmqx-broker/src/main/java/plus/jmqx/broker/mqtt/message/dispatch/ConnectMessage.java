package plus.jmqx.broker.mqtt.message.dispatch;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * 设备连接消息
 *
 * @author maxid
 * @since 2025/4/21 15:58
 */
@Data
@Builder
@ToString
public class ConnectMessage {
    private final String clientId;
    private final String username;
    private final String protocolName;
    private final int    version;
}
