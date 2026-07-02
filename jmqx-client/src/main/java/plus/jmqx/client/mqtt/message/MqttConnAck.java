package plus.jmqx.client.mqtt.message;

/**
 * 版本无关的 CONNACK 标记接口。具体类型在 v3/v5.message 中。
 *
 * @author maxid
 */
public interface MqttConnAck {

    boolean isSessionPresent();
}
