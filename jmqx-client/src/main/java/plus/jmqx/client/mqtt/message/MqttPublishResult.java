package plus.jmqx.client.mqtt.message;

/**
 * 版本无关的 PUBLISH 结果。
 *
 * @author maxid
 */
public interface MqttPublishResult {

    MqttPublish getPublish();

    Throwable getError();
}
