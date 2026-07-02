package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;

import java.util.Collections;
import java.util.Map;

/**
 * MQTT 5.0 PUBLISH 属性。
 *
 * @author maxid
 */
@Value
@Builder
public class Mqtt5PublishProperties {

    /** 消息过期时间（秒） */
    Integer messageExpiryInterval;
    /** 响应主题 */
    String  responseTopic;
    /** 关联数据 */
    byte[]  correlationData;
    /** 用户属性 */
    @Builder.Default
    Map<String, String> userProperties = Collections.emptyMap();
    /** 主题别名 */
    Integer topicAlias;
    /** 内容类型 */
    String  contentType;

}
