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

    /**
     * 客户端标识
     */
    private final String clientId;
    /**
     * 连接用户名，未携带时为 {@code null}
     */
    private final String username;
    /**
     * 协议名，如 MQTT
     */
    private final String protocolName;
    /**
     * 协议级别，MQTT 3.1.1 为 4
     */
    private final int    version;
    /**
     * MQTT CONNECT Keep Alive，单位秒；客户端声明为 0 时保持 0
     */
    private final int    keepAlive;

}
