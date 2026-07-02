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

    Integer messageExpiryInterval;
    String responseTopic;
    byte[] correlationData;
    @Builder.Default
    Map<String, String> userProperties = Collections.emptyMap();
    Integer topicAlias;
    String contentType;
}
