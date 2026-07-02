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

    /**
     * WebSocket 路径，默认 "/mqtt"
     */
    private String path        = "/mqtt";
    /**
     * WebSocket 子协议，默认 "mqtt"
     */
    private String subprotocol = "mqtt";
    /**
     * WebSocket 查询参数
     */
    private String query;

}
