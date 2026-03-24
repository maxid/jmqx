package plus.jmqx.broker.mqtt.util;

/**
 * Topic 工具
 *
 * @author maxid
 * @since 2026/3/24 14:40
 */
public final class TopicUtils {

    /**
     * 工具类禁止实例化。
     */
    private TopicUtils() {
    }

    /**
     * 按 '/' 拆分主题字符串。
     *
     * @param topic 主题字符串
     * @return 拆分后的主题层级数组
     */
    public static String[] splitTopic(String topic) {
        if (topic == null || topic.isEmpty()) {
            return new String[0];
        }
        int count = 1;
        for (int i = 0; i < topic.length(); i++) {
            if (topic.charAt(i) == '/') {
                count++;
            }
        }
        String[] parts = new String[count];
        int index = 0;
        int start = 0;
        for (int i = 0; i <= topic.length(); i++) {
            if (i == topic.length() || topic.charAt(i) == '/') {
                parts[index++] = topic.substring(start, i);
                start = i + 1;
            }
        }
        return parts;
    }

}
