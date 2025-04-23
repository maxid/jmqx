package plus.jmqx.broker.mqtt.message;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.Builder;
import lombok.Data;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.util.JacksonUtil;
import plus.jmqx.broker.mqtt.util.MessageUtils;

import java.util.HashMap;
import java.util.Optional;

/**
 * MQTT 保留消息
 *
 * @author maxid
 * @since 2025/4/16 14:22
 */
@Data
@Builder
public class RetainMessage {
    private int qos;

    private String topic;

    private byte[] body;

    private boolean retain;

    private String userProperties;

    public static RetainMessage of(MqttPublishMessage message) {
        MqttPublishVariableHeader header = message.variableHeader();
        return RetainMessage.builder()
                .topic(header.topicName())
                .qos(message.fixedHeader().qosLevel().value())
                .retain(message.fixedHeader().isRetain())
                .body(MessageUtils.copyByteBuf(message.payload()))
                .userProperties(JacksonUtil.map2Json(Optional.ofNullable(header
                                .properties()
                                .getProperties(MqttProperties.MqttPropertyType.USER_PROPERTY.value()))
                        .map(list -> {
                            HashMap<String, String> propertiesMap = new HashMap<>(list.size());
                            list.forEach(property -> {
                                MqttProperties.StringPair pair = (MqttProperties.StringPair) property.value();
                                propertiesMap.put(pair.key, pair.value);
                            });
                            return propertiesMap;
                        }).orElseGet(HashMap::new)))
                .build();
    }

    public MqttPublishMessage toPublishMessage(MqttSession session) {
        return MqttMessageBuilder.publishMessage(
                false,
                MqttQoS.AT_MOST_ONCE,
                retain,
                qos > 0 ? session.generateMessageId() : 0,
                topic,
                PooledByteBufAllocator.DEFAULT.directBuffer().writeBytes(body),
                JacksonUtil.json2Map(userProperties, String.class, String.class));
    }
}
