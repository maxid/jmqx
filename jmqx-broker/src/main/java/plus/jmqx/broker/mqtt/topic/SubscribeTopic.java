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

    private final MqttSession session;

    public SubscribeTopic(String topicFilter, MqttQoS qoS, MqttSession session) {
        this.topicFilter = topicFilter;
        this.qoS = qoS;
        this.session = session;
    }

    public SubscribeTopic compareQos(MqttQoS mqttQoS) {
        MqttQoS minQos = MqttQoS.valueOf(Math.min(mqttQoS.value(), qoS.value()));
        return new SubscribeTopic(topicFilter, minQos, session);
    }

    public void linkSubscribe() {
        session.getTopics().add(this);
    }

    public void unLinkSubscribe() {
        session.getTopics().remove(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscribeTopic that = (SubscribeTopic) o;
        return Objects.equals(topicFilter, that.topicFilter) &&
                Objects.equals(session, that.session);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicFilter, session);
    }

    @Override
    public String toString() {
        return "SubscribeTopic{" +
                "topicFilter='" + topicFilter + '\'' +
                ", qoS=" + qoS +
                ", mqttChannel=" + session +
                '}';
    }
}
