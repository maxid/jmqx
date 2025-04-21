package plus.jmqx.broker.mqtt.context;

import io.netty.handler.codec.mqtt.MqttMessage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.cluster.ClusterReceiver;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
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

    private final ClusterReceiver clusterReceiver;

    public MqttReceiveContext(MqttConfiguration configuration, Transport<MqttConfiguration> transport) {
        super(configuration, transport);
        this.clusterReceiver = new ClusterReceiver(this);
        this.clusterReceiver.registry();
    }

    public void apply(MqttSession session) {
        session.registryDelayTcpClose()
                .getConnection()
                .inbound()
                .receiveObject()
                .cast(MqttMessage.class)
                .onErrorContinue( (err, msg) -> log.error("on message error {}", msg, err))
                .filter(mqttMessage -> mqttMessage.decoderResult().isSuccess())
                .subscribe(mqttMessage -> this.accept(session, new MessageWrapper<>(mqttMessage, System.currentTimeMillis(), Boolean.FALSE)));
    }

    @Override
    public void accept(MqttSession session, MessageWrapper<MqttMessage> message) {
        this.getMessageDispatcher().dispatch(session, message, this);
    }
}
