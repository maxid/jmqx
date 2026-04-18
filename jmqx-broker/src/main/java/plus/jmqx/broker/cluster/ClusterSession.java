package plus.jmqx.broker.cluster;

import io.netty.handler.codec.mqtt.MqttMessage;
import plus.jmqx.broker.mqtt.channel.MqttSession;

/**
 * 集群会话
 *
 * @author maxid
 * @since 2025/4/17 10:27
 */
public class ClusterSession extends MqttSession {

    /**
     * 构造集群会话
     */
    private ClusterSession() {
    }

    public final static ClusterSession DEFAULT_CLUSTER_SESSION = new ClusterSession();

    /**
     * 根据客户端 ID 创建集群会话
     *
     * @param clientId 客户端 ID
     * @return 集群会话
     */
    public static ClusterSession wrapClientId(String clientId) {
        ClusterSession session = new ClusterSession();
        session.setClientId(clientId);
        return session;
    }

    /**
     * 集群会话不进行本地发送
     *
     * @param mqttMessage 消息体
     * @param retry       是否重试
     */
    @Override
    public void write(MqttMessage mqttMessage, boolean retry) {
    }


    /**
     * 标识为集群会话
     *
     * @return 是否为集群会话
     */
    @Override
    public Boolean getIsCluster() {
        return true;
    }


    /**
     * 输出集群会话信息
     *
     * @return 会话字符串
     */
    @Override
    public String toString() {
        return "ClusterSession{" +
                "clientId='" + getClientId() + '\'' + '}';
    }

}
