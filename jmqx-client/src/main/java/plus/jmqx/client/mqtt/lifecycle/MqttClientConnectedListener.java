package plus.jmqx.client.mqtt.lifecycle;

/**
 * 连接成功建立时触发的监听器。
 *
 * @author maxid
 */
@FunctionalInterface
public interface MqttClientConnectedListener {

    void onConnected(MqttClientConnectedContext context);
}
