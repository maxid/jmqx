package plus.jmqx.example.broker.dispatch;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.dispatch.PublishMessage;

import java.nio.charset.StandardCharsets;

/**
 * 设备上报消息处理
 *
 * @author maxid
 * @since 2025/4/28 10:01
 */
@Slf4j
@Component
public class PublishMessageProcessor {

    public void process(PublishMessage message) {
        log.info("【设备消息上报】{}", message);
        // 消息下发示例
        MqttPublishMessage reply = MqttMessageBuilder.publishMessage(
                false, MqttQoS.EXACTLY_ONCE, 0, message.getTopic(),
                Unpooled.wrappedBuffer("message reply".getBytes(StandardCharsets.UTF_8)));
        ContextHolder.getContext().getMessageDispatcher().publish(reply);
    }
}
