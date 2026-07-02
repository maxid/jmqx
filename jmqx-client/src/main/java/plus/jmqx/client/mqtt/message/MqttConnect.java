package plus.jmqx.client.mqtt.message;

/**
 * 版本无关的 CONNECT 标记接口。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface MqttConnect {

    /**
     * 获取客户端标识符
     *
     * @return 客户端标识符
     */
    String getClientId();

    /**
     * 获取 cleanSession / cleanStart 标志
     *
     * @return cleanSession / cleanStart 标志
     */
    boolean isCleanSession();

    /**
     * 获取 Keep Alive 间隔（秒）
     *
     * @return Keep Alive 间隔（秒）
     */
    int getKeepAliveSeconds();

    /**
     * 获取遗嘱消息
     *
     * @return 遗嘱消息；可能为 {@code null}
     */
    MqttPublish getWillPublish();

    /**
     * 认证用户名
     *
     * @return 认证用户名；可能为 {@code null}
     */
    String getUsername();

    /**
     * 获取认证密码
     *
     * @return 认证密码；可能为 {@code null}
     */
    byte[] getPassword();

}
