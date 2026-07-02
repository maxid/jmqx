package plus.jmqx.client.mqtt.message;

/**
 * PUBLISH 操作结果。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface MqttPublishResult {

    /**
     * 获取原始发布消息
     *
     * @return 原始发布消息
     */
    MqttPublish getPublish();

    /**
     * 获取非致命错误
     *
     * @return 非致命错误；成功时为空
     */
    Throwable getError();

}
