package plus.jmqx.broker.mqtt.message.interceptor;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import plus.jmqx.broker.cluster.ClusterMessage;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.HeapMqttMessage;
import plus.jmqx.broker.mqtt.message.MessageAdapter;
import plus.jmqx.broker.mqtt.message.MessageWrapper;
import plus.jmqx.broker.mqtt.util.JacksonUtil;
import plus.jmqx.broker.mqtt.util.MessageUtils;
import plus.jmqx.broker.spi.DynamicLoader;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ReactorNetty;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 消息代理
 *
 * @author maxid
 * @since 2025/4/16 17:27
 */
public class MessageProxy {

    private final List<Interceptor> interceptors = DynamicLoader.findAll(Interceptor.class)
            .sorted(Comparator.comparing(Interceptor::sort))
            .collect(Collectors.toList());

    public MessageAdapter proxy(MessageAdapter adapter) {
        adapter = new TailIntercept().proxy(adapter);
        for (Interceptor interceptor : interceptors) {
            adapter = interceptor.proxy(adapter);
        }
        return new HeadIntercept().proxy(adapter);
    }

    static class TailIntercept implements Interceptor {

        @Override
        @SuppressWarnings("unchecked")
        public Object intercept(Invocation invocation) {
            MqttSession session = (MqttSession) invocation.getArgs()[0];
            MessageWrapper<MqttMessage> wrapper = (MessageWrapper<MqttMessage>) invocation.getArgs()[1];
            ReceiveContext<Configuration> context = (ReceiveContext<Configuration>) invocation.getArgs()[2];
            MqttMessage message = wrapper.getMessage();
            if (!wrapper.getClustered() && message instanceof MqttPublishMessage) {
                MqttPublishMessage publishMessage = (MqttPublishMessage) message;
                HeapMqttMessage heapMqttMessage = this.clusterMessage(publishMessage, session, wrapper.getTimestamp());
                if(context.getConfiguration().getClusterConfig().isEnable()) {
                    context.getClusterRegistry().spreadPublishMessage(new ClusterMessage(heapMqttMessage))
                            .subscribeOn(Schedulers.boundedElastic())
                            .subscribe();
                }
            }
            return invocation.proceed();
        }

        @Override
        public int sort() {
            return 0;
        }

        private HeapMqttMessage clusterMessage(MqttPublishMessage message, MqttSession session, long timestamp) {
            MqttPublishVariableHeader header = message.variableHeader();
            MqttFixedHeader fixedHeader = message.fixedHeader();
            return HeapMqttMessage.builder()
                    .timestamp(timestamp)
                    .clientId(session.getClientId())
                    .message(JacksonUtil.dynamic(new String(MessageUtils.copyReleaseByteBuf(message.payload()), StandardCharsets.UTF_8)))
                    .topic(header.topicName())
                    .retain(fixedHeader.isRetain())
                    .qos(fixedHeader.qosLevel().value())
                    .properties(header.properties())
                    .build();
        }
    }

    static class HeadIntercept implements Interceptor {

        @Override
        @SuppressWarnings("unchecked")
        public Object intercept(Invocation invocation) {
            MessageWrapper<MqttMessage> wrapper = (MessageWrapper<MqttMessage>) invocation.getArgs()[1];
            MqttMessage message = wrapper.getMessage();
            try {
                if (message instanceof MqttPublishMessage) {
                    MqttPublishMessage publishMessage = (MqttPublishMessage) message;
                    publishMessage.retain();
                }
                return invocation.proceed();
            } finally {
                if (wrapper.getClustered()) {
                    ReactorNetty.safeRelease(message.payload());
                }
            }
        }

        @Override
        public int sort() {
            return 0;
        }
    }
}
