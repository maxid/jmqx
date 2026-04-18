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

    /**
     * 创建订阅对象
     *
     * @param topicFilter 主题过滤器
     * @param qoS         QoS
     * @param session     会话
     */
    public SubscribeTopic(String topicFilter, MqttQoS qoS, MqttSession session) {
        this.topicFilter = topicFilter;
        this.qoS = qoS;
        this.session = session;
    }

    /**
     * 计算与指定 QoS 的最小值订阅
     *
     * @param mqttQoS 目标 QoS
     * @return 新订阅对象
     */
    public SubscribeTopic compareQos(MqttQoS mqttQoS) {
        MqttQoS minQos = MqttQoS.valueOf(Math.min(mqttQoS.value(), qoS.value()));
        return new SubscribeTopic(topicFilter, minQos, session);
    }

    /**
     * 绑定订阅到会话
     */
    public void linkSubscribe() {
        session.getTopics().add(this);
    }

    /**
     * 从会话解绑订阅
     */
    public void unLinkSubscribe() {
        session.getTopics().remove(this);
    }

    /**
     * 判断订阅是否相等
     *
     * @param o 对象
     * @return 是否相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscribeTopic that = (SubscribeTopic) o;
        return Objects.equals(topicFilter, that.topicFilter) &&
                Objects.equals(session, that.session);
    }

    /**
     * 计算哈希值
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(topicFilter, session);
    }

    /**
     * 输出订阅信息
     *
     * @return 字符串
     */
    @Override
    public String toString() {
        return "SubscribeTopic{" +
                "topicFilter='" + topicFilter + '\'' +
                ", qoS=" + qoS +
                ", mqttChannel=" + session +
                '}';
    }

}
