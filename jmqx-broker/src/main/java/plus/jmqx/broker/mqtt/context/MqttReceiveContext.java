package plus.jmqx.broker.mqtt.context;

import io.netty.handler.codec.mqtt.MqttMessage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.channel.MqttChannel;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.transport.Transport;

/**
 * MQTT服务上下文
 *
 * @author maxid
 * @since 2025/4/9 18:14
 */
@Slf4j
@Getter
@Setter
public class MqttReceiveContext extends AbstractReceiveContext<MqttConfiguration> {

    public MqttReceiveContext(MqttConfiguration configuration, Transport<MqttConfiguration> transport) {
        super(configuration, transport);
    }

    public void apply(MqttChannel session) {

    }

    @Override
    public void accept(MqttChannel channel, MessageWrapper<MqttMessage> message) {

    }
}
