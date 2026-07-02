package plus.jmqx.client.mqtt.lifecycle;

/**
 * 连接断开时触发的监听器。
 *
 * <p>监听器可通过对传入的 {@link MqttClientDisconnectedContext#getReconnector()} 进行修改，
 * 以影响后续重连行为（是否重连、延迟、是否重新订阅）。
 *
 * @author maxid
 * @since 1.4.14
 */
@FunctionalInterface
public interface MqttClientDisconnectedListener {

    /**
     * 连接断开后回调。
     *
     * @param context 断开上下文（含 {@link MqttClientReconnector}）
     */
    void onDisconnected(MqttClientDisconnectedContext context);

}
