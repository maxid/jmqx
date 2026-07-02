package plus.jmqx.client.mqtt.internal.transport;

import lombok.Builder;
import lombok.Data;

/**
 * WebSocket 配置（mqtt 子协议）。
 *
 * @author maxid
 */
@Data
@Builder
public class MqttWebSocketConfig {

    private String path = "/mqtt";
    private String subprotocol = "mqtt";
    private String query;
}
