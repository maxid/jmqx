package plus.jmqx.client.mqtt.internal.buffer;

/**
 * 离线消息缓冲已满时抛出。
 *
 * @author maxid
 */
public class MessageBufferFullException extends RuntimeException {

    public MessageBufferFullException() {
        super("Offline message buffer is full");
    }

}
