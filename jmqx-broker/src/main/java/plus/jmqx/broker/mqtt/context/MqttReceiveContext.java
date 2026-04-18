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

    /**
     * 创建 MQTT 上下文并注册集群接收器
     *
     * @param configuration MQTT 配置
     * @param transport     传输实现
     */
    public MqttReceiveContext(MqttConfiguration configuration, Transport<MqttConfiguration> transport) {
        super(configuration, transport);
        this.clusterReceiver = new ClusterReceiver(this);
        this.clusterReceiver.registry();
    }

    /**
     * 绑定会话并订阅其入站消息流
     *
     * @param session 会话
     */
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

    /**
     * 接收并分发消息到消息处理器
     *
     * @param session 会话
     * @param message 消息包装
     */
    @Override
    public void accept(MqttSession session, MessageWrapper<MqttMessage> message) {
        this.getMessageDispatcher().dispatch(session, message, this);
    }

}
