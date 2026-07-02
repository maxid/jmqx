package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttPublish;

/**
 * MQTT 5.0 CONNECT。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt5Connect {

    /**
     * 客户端标识符
     */
    String      clientId;
    /**
     * cleanStart 标志
     */
    boolean     cleanStart;
    /**
     * Keep Alive 间隔（秒）
     */
    int         keepAliveSeconds;
    /**
     * 会话过期时间（秒）
     */
    long        sessionExpiryInterval;
    /**
     * Receive Maximum
     */
    int         receiveMaximum;
    /**
     * 认证用户名
     */
    String      username;
    /**
     * 认证密码
     */
    byte[]      password;
    /**
     * 遗嘱消息
     */
    MqttPublish willPublish;

}
