package plus.jmqx.broker.mqtt.message;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class RetainMessage {

    private int qos;

    private String topic;

    private byte[] body;

    private boolean retain;

    private String userProperties;

    /**
     * 根据 MQTT 发布消息构建保留消息
     *
     * @param message MQTT 发布消息
     * @return 保留消息
     */
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

    public MqttPublishMessage toPublishMessage(MqttSession session, int topicQos) {
        int packetId = qos > 0 ? session.generateMessageId() : 0;
        if (packetId < 0) {
            // 65535 个 ID 全占满（极端场景），降级为 QoS0 投递，避免构造非法 packetId
            log.warn("no available packet ID for [{}], degrade retain msg to QoS0", session.getClientId());
            packetId = 0;
        }
        return MqttMessageBuilder.publishMessage(
                false,
                MqttQoS.valueOf(Math.min(topicQos, this.qos)),
                retain,
                packetId,
                topic,
                PooledByteBufAllocator.DEFAULT.directBuffer().writeBytes(body),
                JacksonUtil.json2Map(userProperties, String.class, String.class));
    }

}
