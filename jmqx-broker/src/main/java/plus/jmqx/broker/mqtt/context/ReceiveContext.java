package plus.jmqx.broker.mqtt.context;

import io.netty.handler.codec.mqtt.MqttMessage;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttChannel;
import plus.jmqx.broker.mqtt.message.MessageWrapper;

import java.util.function.BiConsumer;

/**
 * 上下文
 *
 * @author maxid
 * @since 2025/4/9 11:55
 */
public interface ReceiveContext<C extends Configuration> extends BiConsumer<MqttChannel, MessageWrapper<MqttMessage>> {
}
