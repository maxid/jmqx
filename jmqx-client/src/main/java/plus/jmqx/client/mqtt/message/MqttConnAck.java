package plus.jmqx.client.mqtt.message;

/**
 * 版本无关的 CONNACK 标记接口。具体类型在 v3/v5 message 子包中。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface MqttConnAck {

    /**
     * 获取 broker 是否保留了上一次会话
     *
     * @return broker 是否保留了上一次会话
     */
    boolean isSessionPresent();

}
