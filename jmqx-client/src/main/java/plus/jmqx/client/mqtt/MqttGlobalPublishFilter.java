package plus.jmqx.client.mqtt;

/**
 * 全局 publishes() 流的过滤策略。
 *
 * @author maxid
 */
public enum MqttGlobalPublishFilter {
    /** 所有入站 PUBLISH。 */
    ALL,
    /** 仅匹配当前订阅的 PUBLISH。 */
    SUBSCRIBED,
    /** 仅未被任何订阅匹配的 PUBLISH（未被请求的）。 */
    UNSOLICITED
}
