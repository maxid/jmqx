package plus.jmqx.client.mqtt.internal.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TopicMatcherTest {

    @Test
    void exactMatch() {
        assertTrue(TopicMatcher.matches("a/b/c", "a/b/c"));
        assertFalse(TopicMatcher.matches("a/b/c", "a/b/d"));
    }

    @Test
    void singleLevelWildcard() {
        assertTrue(TopicMatcher.matches("a/+/c", "a/b/c"));
        assertTrue(TopicMatcher.matches("a/+/c", "a/x/c"));
        assertFalse(TopicMatcher.matches("a/+/c", "a/b/d"));
        assertFalse(TopicMatcher.matches("a/+/c", "a/b/x/c"));
    }

    @Test
    void multiLevelWildcard() {
        assertTrue(TopicMatcher.matches("a/#", "a/b/c"));
        assertTrue(TopicMatcher.matches("a/#", "a/b"));
        assertTrue(TopicMatcher.matches("#", "a/b/c/d"));
        assertFalse(TopicMatcher.matches("a/#", "b/c"));
    }

    @Test
    void wildcardMustBeWholeLevel() {
        assertFalse(TopicMatcher.matches("a/b+", "a/bx"));
        assertFalse(TopicMatcher.matches("a/#c", "a/bc"));
    }

    @Test
    void emptyLevelsHandled() {
        assertTrue(TopicMatcher.matches("a//c", "a//c"));
        assertTrue(TopicMatcher.matches("a/+/c", "a//c"));
    }

    @Test
    void multiLevelMustBeLast() {
        // '#' only valid as the final level
        assertTrue(TopicMatcher.matches("a/#", "a"));
        assertFalse(TopicMatcher.matches("a/#/c", "a/b/c"));
    }

    @Test
    void plusDoesNotMatchMultiLevels() {
        assertFalse(TopicMatcher.matches("+", "a/b"));
        assertTrue(TopicMatcher.matches("+", "a"));
    }
}
