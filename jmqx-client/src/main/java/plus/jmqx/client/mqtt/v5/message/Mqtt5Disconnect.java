package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;

/**
 * MQTT 5.0 DISCONNECT。
 *
 * @author maxid
 */
@Value
@Builder
public class Mqtt5Disconnect {

    /**
     * 断开连接原因码
     */
    byte reasonCode;

}
