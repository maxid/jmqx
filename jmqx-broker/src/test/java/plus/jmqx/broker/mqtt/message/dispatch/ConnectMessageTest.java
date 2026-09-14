package plus.jmqx.broker.mqtt.message.dispatch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * CONNECT 生命周期回调须携带 MQTT Keep Alive（秒）。
 */
class ConnectMessageTest {

    /**
     * Builder 写入的 keepAlive 可按秒读出。
     */
    @Test
    void keepAliveSecondsComeFromConnectHeader() {
        ConnectMessage message = ConnectMessage.builder()
                .clientId("dev-1")
                .username("user-1")
                .protocolName("MQTT")
                .version(4)
                .keepAlive(60)
                .build();
        assertEquals(60, message.getKeepAlive());
        assertEquals("dev-1", message.getClientId());
        assertEquals("user-1", message.getUsername());
    }
}
