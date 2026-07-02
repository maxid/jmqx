package plus.jmqx.client.mqtt.message;

/**
 * 版本无关的 CONNECT 标记接口。
 *
 * @author maxid
 */
public interface MqttConnect {

    String getClientId();

    boolean isCleanSession();

    int getKeepAliveSeconds();

    MqttPublish getWillPublish();

    String getUsername();

    byte[] getPassword();
}
