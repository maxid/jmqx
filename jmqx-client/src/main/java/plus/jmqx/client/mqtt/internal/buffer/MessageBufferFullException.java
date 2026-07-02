package plus.jmqx.client.mqtt.internal.buffer;

/**
 * 离线消息缓冲已满时抛出。
 *
 * @author maxid
 */
public class MessageBufferFullException extends RuntimeException {

    /** 构造异常，预设消息为"离线消息缓冲已满"。 */
    public MessageBufferFullException() {
        super("Offline message buffer is full");
    }

}
