package plus.jmqx.client.mqtt.lifecycle;

/**
 * 连接成功建立时触发的监听器。
 *
 * @author maxid
 * @since 1.4.14
 */
@FunctionalInterface
public interface MqttClientConnectedListener {

    /**
     * 连接建立后回调。
     *
     * @param context 连接上下文
     */
    void onConnected(MqttClientConnectedContext context);

}
