package plus.jmqx.broker.mqtt.topic;

import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.Getter;
import lombok.Setter;
import plus.jmqx.broker.mqtt.channel.MqttSession;

import java.util.Objects;

/**
 * 会话订阅主题
 *
 * @author maxid
 * @since 2025/4/14 17:24
 */
@Getter
@Setter
public class SubscribeTopic {

    private final String topicFilter;

    private final MqttQoS qoS;

    private final MqttSession mqttChannel;

    public SubscribeTopic(String topicFilter, MqttQoS qoS, MqttSession mqttChannel) {
        this.topicFilter = topicFilter;
        this.qoS = qoS;
        this.mqttChannel = mqttChannel;
    }

    public SubscribeTopic compareQos(MqttQoS mqttQoS) {
        MqttQoS minQos = MqttQoS.valueOf(Math.min(mqttQoS.value(), qoS.value()));
        return new SubscribeTopic(topicFilter, minQos, mqttChannel);
    }

    public void linkSubscribe() {
        mqttChannel.getTopics().add(this);
    }

    public void unLinkSubscribe() {
        mqttChannel.getTopics().remove(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscribeTopic that = (SubscribeTopic) o;
        return Objects.equals(topicFilter, that.topicFilter) &&
                Objects.equals(mqttChannel, that.mqttChannel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicFilter, mqttChannel);
    }

    @Override
    public String toString() {
        return "SubscribeTopic{" +
                "topicFilter='" + topicFilter + '\'' +
                ", qoS=" + qoS +
                ", mqttChannel=" + mqttChannel +
                '}';
    }
}
