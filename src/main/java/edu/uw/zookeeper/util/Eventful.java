package edu.uw.zookeeper.util;

/**
 * EventBus interface for event generators.
 * 
 * @see <a href="http://docs.guava-libraries.googlecode.com/git-history/release/javadoc/com/google/common/eventbus/package-summary.html">com.google.common.eventbus</a>
 */
public interface Eventful {
    void register(Object handler);

    void unregister(Object handler);
}
