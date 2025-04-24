package plus.jmqx.broker.cluster;

import io.netty.handler.codec.mqtt.MqttMessage;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import reactor.core.publisher.Mono;

/**
 * 集群会话
 *
 * @author maxid
 * @since 2025/4/17 10:27
 */
public class ClusterSession extends MqttSession {
    private ClusterSession() {
    }

    public final static ClusterSession DEFAULT_CLUSTER_SESSION = new ClusterSession();

    public static ClusterSession wrapClientId(String clientId) {
        ClusterSession session = new ClusterSession();
        session.setClientId(clientId);
        return session;
    }

    @Override
    public void write(MqttMessage mqttMessage, boolean retry) {
    }


    @Override
    public Boolean getIsCluster() {
        return true;
    }


    @Override
    public String toString() {
        return "ClusterSession{" +
                ", clientId='" + getClientId() + '\'' + '}';
    }
}
