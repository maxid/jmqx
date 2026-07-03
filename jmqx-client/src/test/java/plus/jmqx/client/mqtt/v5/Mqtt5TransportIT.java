package plus.jmqx.client.mqtt.v5;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.it.BrokerITSupport;

/**
 * MQTT 5.0 传输层（MQTTS / WS / WSS）↔ jmqx-broker 集成测试。
 *
 * <p>默认内嵌 broker 启用全部监听；对接外部 broker 时端口默认与 jmqx-broker 一致：
 * TCP {@code 1883}、MQTTS {@code 8883}、WS {@code 1884}、WSS {@code 8884}，可通过系统属性覆盖。
 *
 * <p>显式运行：{@code mvn -pl jmqx-client test -Dtest=Mqtt5TransportIT}
 */
class Mqtt5TransportIT extends BrokerITSupport {

    private Mqtt5RxClient client;

    @AfterEach
    void cleanup() {
        disconnectQuietly(client);
        client = null;
    }

    @Test
    void mqttsConnectAndPublishSubscribe() throws Exception {
        client = v5RxTls(uniqueId("it-v5-mqtts"));
        v5PublishSubscribeSmoke(client);
    }

    @Test
    void wsConnectAndPublishSubscribe() throws Exception {
        client = v5RxWs(uniqueId("it-v5-ws"));
        v5PublishSubscribeSmoke(client);
    }

    @Test
    void wssConnectAndPublishSubscribe() throws Exception {
        client = v5RxWss(uniqueId("it-v5-wss"));
        v5PublishSubscribeSmoke(client);
    }

}
