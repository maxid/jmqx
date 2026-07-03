package plus.jmqx.client.mqtt.v3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.it.BrokerITSupport;

/**
 * MQTT 3.1.1 传输层（MQTTS / WS / WSS）↔ jmqx-broker 集成测试。
 *
 * <p>默认内嵌 broker 启用全部监听；对接外部 broker 时端口默认与 jmqx-broker 一致：
 * TCP {@code 1883}、MQTTS {@code 8883}、WS {@code 1884}、WSS {@code 8884}，可通过系统属性覆盖。
 *
 * <p>显式运行：{@code mvn -pl jmqx-client test -Dtest=Mqtt3TransportIT}
 */
class Mqtt3TransportIT extends BrokerITSupport {

    private Mqtt3RxClient client;

    @AfterEach
    void cleanup() {
        disconnectQuietly(client);
        client = null;
    }

    @Test
    void mqttsConnectAndPublishSubscribe() throws Exception {
        client = v3RxTls(uniqueId("it-v3-mqtts"));
        v3PublishSubscribeSmoke(client);
    }

    @Test
    void wsConnectAndPublishSubscribe() throws Exception {
        client = v3RxWs(uniqueId("it-v3-ws"));
        v3PublishSubscribeSmoke(client);
    }

    @Test
    void wssConnectAndPublishSubscribe() throws Exception {
        client = v3RxWss(uniqueId("it-v3-wss"));
        v3PublishSubscribeSmoke(client);
    }

}
