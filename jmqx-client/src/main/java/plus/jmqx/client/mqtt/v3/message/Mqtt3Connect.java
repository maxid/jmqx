package plus.jmqx.client.mqtt.v3.message;

import lombok.Builder;
import lombok.Value;
import plus.jmqx.client.mqtt.message.MqttPublish;

/**
 * MQTT 3.1.1 CONNECT 报文参数。
 *
 * <p>包含连接所需的所有参数：客户端标识符、清理会话标志、保活间隔、认证信息及遗嘱消息。
 *
 * @author maxid
 */
@Value
@Builder(toBuilder = true)
public class Mqtt3Connect {

    /**
     * 客户端标识符
     */
    String      clientId;
    /**
     * 是否清理会话
     */
    boolean     cleanSession;
    /**
     * Keep Alive 间隔（秒）
     */
    int         keepAliveSeconds;
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
