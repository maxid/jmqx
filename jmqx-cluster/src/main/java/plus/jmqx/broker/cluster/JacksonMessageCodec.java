package plus.jmqx.broker.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Jackson 消息序列化、反序列化配置
 *
 * @author maxid
 * @since 2025/4/22 09:56
 */
public class JacksonMessageCodec implements MessageCodec {

    private final ObjectMapper delegate;

    /**
     * 构造默认 Jackson 编解码器
     */
    public JacksonMessageCodec() {
        this(DefaultObjectMapper.OBJECT_MAPPER);
    }

    /**
     * 构造 Jackson 编解码器
     *
     * @param delegate ObjectMapper
     */
    public JacksonMessageCodec(ObjectMapper delegate) {
        this.delegate = delegate;
    }

    /**
     * 反序列化消息
     *
     * @param stream 输入流
     * @return 消息
     * @throws Exception 反序列化异常
     */
    @Override
    public Message deserialize(InputStream stream) throws Exception {
        return this.delegate.readValue(stream, Message.class);
    }

    /**
     * 序列化消息
     *
     * @param message 消息
     * @param stream  输出流
     * @throws Exception 序列化异常
     */
    @Override
    public void serialize(Message message, OutputStream stream) throws Exception {
        this.delegate.writeValue(stream, message);
    }

}
