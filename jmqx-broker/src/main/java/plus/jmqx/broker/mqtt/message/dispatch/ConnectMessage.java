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
    /**
     * MQTT CONNECT Keep Alive，单位秒；客户端声明为 0 时保持 0。
     */
    private final int    keepAlive;

}
