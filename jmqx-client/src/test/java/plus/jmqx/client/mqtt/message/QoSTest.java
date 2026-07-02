package plus.jmqx.client.mqtt.message;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QoSTest {

    @Test
    void fromValue_roundTrips() {
        for (QoS q : QoS.values()) {
            assertSame(q, QoS.fromValue(q.value()));
        }
    }

    @Test
    void fromValue_invalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> QoS.fromValue(3));
        assertThrows(IllegalArgumentException.class, () -> QoS.fromValue(-1));
    }

    @Test
    void values_are012() {
        assertEquals(0, QoS.AT_MOST_ONCE.value());
        assertEquals(1, QoS.AT_LEAST_ONCE.value());
        assertEquals(2, QoS.EXACTLY_ONCE.value());
    }
}
