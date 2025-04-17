package plus.jmqx.broker.mqtt.util;

/**
 * 尽量简洁一句描述
 *
 * @author maxid
 * @since 2025/4/16 15:00
 */
public class TopicRegexUtils {
    public static final TopicRegexUtils instance = new TopicRegexUtils();

    public static String regexTopic(String topic) {
        if (topic.startsWith("$")) {
            topic = "\\" + topic;
        }
        return topic
                .replaceAll("/", "\\\\/")
                .replaceAll("\\+", "[^/]+")
                .replaceAll("#", "(.+)") + "$";
    }

    public boolean match(String sourcesTopic, String targetTopic) {
        if (sourcesTopic == null || "".equals(sourcesTopic) || targetTopic == null || "".equals(targetTopic)) {
            return false;
        }
        return sourcesTopic.matches(TopicRegexUtils.regexTopic(targetTopic));
    }
}
