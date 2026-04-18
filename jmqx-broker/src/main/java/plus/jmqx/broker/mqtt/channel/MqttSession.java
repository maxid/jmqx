package plus.jmqx.broker.mqtt.channel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.netty.handler.codec.mqtt.*;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.mqtt.retry.Ack;
import plus.jmqx.broker.mqtt.retry.RetryAck;
import plus.jmqx.broker.mqtt.retry.TimeAckManager;
import plus.jmqx.broker.mqtt.topic.SubscribeTopic;
import plus.jmqx.broker.mqtt.util.MessageUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * MQTT 设备连接会话
 *
 * @author maxid
 * @since 2025/4/9 17:50
 */
@Slf4j
@Getter
@Setter
public class MqttSession {

    private Connection connection;

    private String clientId;

    private SessionStatus status;

    private long authTime;

    private long connectTime;

    private boolean sessionPersistent;

    private Will will;

    private long keepalive;

    private String username;

    private String address;

    private byte protocolVersion;

    @JsonIgnore
    private Set<SubscribeTopic> topics;

    @JsonIgnore
    private Boolean isCluster = false;

    @JsonIgnore
    private transient AtomicInteger atomicInteger;

    @JsonIgnore
    private transient MqttMessageSink mqttMessageSink;

    @JsonIgnore
    private transient Map<Integer, MqttPublishMessage> qos2MsgCache;

    @JsonIgnore
    private Map<MqttMessageType, Map<Integer, Disposable>> replyMqttMessageMap;

    @JsonIgnore
    private Disposable closeDisposable;

    @JsonIgnore
    private TimeAckManager timeAckManager;

    /**
     * 取消或处置底层任务或资源。
     */
    public void disposableClose() {
        if (closeDisposable != null && !closeDisposable.isDisposed()) {
            closeDisposable.dispose();
        }
    }

    /**
     * 连接是否活跃
     *
     * @return 是否活跃
     */
    public boolean isActive() {
        return connection == null && !connection.isDisposed();
    }

    /**
     * 初始化 MQTT 连接会话
     *
     * @param connection     连接
     * @param timeAckManager 确认管理器
     * @return MQTT 连接会话
     */
    public static MqttSession init(Connection connection, TimeAckManager timeAckManager) {
        MqttSession session = new MqttSession();
        session.setTopics(new CopyOnWriteArraySet<>());
        session.setAtomicInteger(new AtomicInteger(0));
        session.setReplyMqttMessageMap(new ConcurrentHashMap<>());
        session.setMqttMessageSink(new MqttMessageSink());
        session.setQos2MsgCache(new ConcurrentHashMap<>());
        session.setConnection(connection);
        session.setStatus(SessionStatus.INIT);
        session.setAddress(connection.address().toString().replaceAll("/", ""));
        session.setTimeAckManager(timeAckManager);
        return session;
    }

    /**
     * 缓存 QoS 2 消息
     *
     * @param messageId      消息 ID
     * @param publishMessage 发布消息
     */
    public void cacheQos2Msg(int messageId, MqttPublishMessage publishMessage) {
        qos2MsgCache.put(messageId, publishMessage);
    }

    /**
     * 缓存中是否存在 QoS 2 消息
     *
     * @param messageId 消息 ID
     * @return 是否存在 QoS 2 消息
     */
    public Boolean existQos2Msg(int messageId) {
        return qos2MsgCache.containsKey(messageId);
    }

    /**
     * 从缓存中移除 QoS 2 消息
     *
     * @param messageId 消息 ID
     * @return 发布消息
     */
    public MqttPublishMessage removeQos2Msg(int messageId) {
        return qos2MsgCache.remove(messageId);
    }

    /**
     * 关闭设备连接
     */
    public void close() {
        this.clearReplyMessage();
        this.qos2MsgCache.values().forEach(MessageUtils::safeRelease);
        this.qos2MsgCache.clear();
        if (!this.sessionPersistent) {
            this.topics.clear();
        }
        if (!this.connection.isDisposed()) {
            this.connection.dispose();
        }
    }

    /**
     * 注册关闭设备连接延时事件
     *
     * @return 当前会话
     */
    public MqttSession registryDelayTcpClose() {
        // registry tcp close event
        Connection connection = this.getConnection();
        this.setCloseDisposable(Mono.fromRunnable(() -> {
            if (!connection.isDisposed()) {
                connection.dispose();
            }
        }).delaySubscription(Duration.ofSeconds(10)).subscribe());
        return this;
    }

    /**
     * 注册关闭连接时要执行的任务
     *
     * @param consumer 关闭回调
     */
    public void registryClose(Consumer<MqttSession> consumer) {
        this.connection.onDispose(() -> consumer.accept(this));
    }

    /**
     * 会话是否活跃
     *
     * @return 是否活跃
     */
    public boolean active() {
        return status == SessionStatus.ONLINE;
    }

    /**
     * 生成序列消息 ID
     *
     * @return 消息 ID
     */
    public int generateMessageId() {
        int value;
        while (qos2MsgCache.containsKey(value = atomicInteger.incrementAndGet())) {
            if (value >= 65535) {
                synchronized (this) {
                    value = atomicInteger.incrementAndGet();
                    if (value >= 65535) {
                        atomicInteger.set(0);
                    } else {
                        break;
                    }
                }
            }
        }
        return value;
    }

    /**
     * 遗愿消息
     */
    @Data
    @Builder
    public static class Will {

        private boolean isRetain;

        private String willTopic;

        private MqttQoS mqttQoS;

        private byte[] willMessage;

    }

    /**
     * 生成消息 ID
     *
     * @param type      消息类型
     * @param messageId 消息ID
     * @return 消息 ID
     */
    public long generateId(MqttMessageType type, Integer messageId) {
        return (long) connection.channel().hashCode() << 32 | (long) type.value() << 28 | messageId << 4 >>> 4;
    }

    /**
     * 写入消息
     *
     * @param mqttMessage 消息体
     * @param retry       是否重试
     */
    public void write(MqttMessage mqttMessage, boolean retry) {
        if (!this.getIsCluster() || this.active()) {
            MqttMessageSink.MQTT_SINK.sendMessage(mqttMessage, this, retry);
        }
    }

    /**
     * 取消重发
     *
     * @param type      消息类型
     * @param messageId 消息 ID
     */
    public void cancelRetry(MqttMessageType type, Integer messageId) {
        this.removeReply(type, messageId);
    }

    /**
     * 删除重发
     *
     * @param type      消息类型
     * @param messageId 消息 ID
     */
    private void removeReply(MqttMessageType type, Integer messageId) {
        Optional.ofNullable(replyMqttMessageMap.get(type)).map(messageIds -> messageIds.remove(messageId)).ifPresent(Disposable::dispose);
    }

    /**
     * 写入消息
     *
     * @param messageMono 消息体
     */
    private void write(Mono<MqttMessage> messageMono) {
        if (this.connection.channel().isActive() && this.connection.channel().isWritable()) {
            connection.outbound().sendObject(messageMono).then().subscribe();
        }
    }

    /**
     * 清理所有回复相关资源。
     */
    private void clearReplyMessage() {
        replyMqttMessageMap.values().forEach(maps -> maps.values().forEach(Disposable::dispose));
        replyMqttMessageMap.clear();
    }

    /**
     * 发送消息并处理重试消息
     */
    private static class MqttMessageSink {

        /**
         * 构造消息发送器。
         */
        private MqttMessageSink() {
        }

        public static MqttMessageSink MQTT_SINK = new MqttMessageSink();


        /**
         * 发送消息并按需注册重试。
         *
         * @param mqttMessage 消息体
         * @param session     会话
         * @param retry       是否重试
         */
        public void sendMessage(MqttMessage mqttMessage, MqttSession session, boolean retry) {
            if (log.isDebugEnabled()) {
                log.debug("write channel {} message {}", session.getConnection(), mqttMessage);
            }
            if (retry) {
                /*
                Increase the reference count of bytebuf, and the reference count of retrybytebuf is 2
                mqttChannel.write() method releases a reference count.
                 */
                MqttMessage reply = getReplyMqttMessage(mqttMessage);

                Runnable runnable = () -> session.write(Mono.just(reply));
                Runnable cleaner = () -> MessageUtils.safeRelease(reply);

                Ack ack = new RetryAck(session.generateId(reply.fixedHeader().messageType(), getMessageId(reply)),
                        5, 5, runnable, session.getTimeAckManager(), cleaner);
                ack.start();
                session.write(Mono.just(mqttMessage));
            } else {
                session.write(Mono.just(mqttMessage));
            }
        }

        /**
         * 获取消息 ID。
         *
         * @param mqttMessage 消息体
         * @return 消息 ID
         */
        private int getMessageId(MqttMessage mqttMessage) {
            Object object = mqttMessage.variableHeader();
            if (object instanceof MqttPublishVariableHeader) {
                return ((MqttPublishVariableHeader) object).packetId();
            } else if (object instanceof MqttMessageIdVariableHeader) {
                return ((MqttMessageIdVariableHeader) object).messageId();
            } else {
                return -1; // client send connect key
            }
        }


        /**
         * 获取重试时的回复消息。
         *
         * @param mqttMessage 原消息
         * @return 回复消息
         */
        private MqttMessage getReplyMqttMessage(MqttMessage mqttMessage) {
            if (mqttMessage instanceof MqttPublishMessage) {
                return ((MqttPublishMessage) mqttMessage).copy().retain(Integer.MAX_VALUE >> 2);
            } else {
                return mqttMessage;
            }
        }


        /**
         * 设置重发标记并构建副本。
         *
         * @param mqttMessage 原消息
         * @return 带重发标记的消息
         */
        @SuppressWarnings("unused")
        private MqttMessage getDupMessage(MqttMessage mqttMessage) {
            MqttFixedHeader oldFixedHeader = mqttMessage.fixedHeader();
            MqttFixedHeader fixedHeader = new MqttFixedHeader(oldFixedHeader.messageType(), true, oldFixedHeader.qosLevel(), oldFixedHeader.isRetain(), oldFixedHeader.remainingLength());
            Object payload = mqttMessage.payload();
            try {
                Constructor<?> constructor = mqttMessage.getClass().getDeclaredConstructors()[0];
                constructor.setAccessible(true);
                if (constructor.getParameterCount() == 2) {
                    return (MqttMessage) constructor.newInstance(fixedHeader, mqttMessage.variableHeader());
                } else {
                    return (MqttMessage) constructor.newInstance(fixedHeader, mqttMessage.variableHeader(), payload);
                }
            } catch (Exception e) {
                return mqttMessage;
            }

        }

    }

    /**
     * 输出会话信息。
     *
     * @return 会话字符串
     */
    @Override
    public String toString() {
        return "MqttSession{" + "address='" + this.connection.address() + '\'' + ", clientId='" + clientId + '\''
                + ", status=" + status + ", keepalive=" + keepalive + ", username='" + username + "'}";
    }

}
