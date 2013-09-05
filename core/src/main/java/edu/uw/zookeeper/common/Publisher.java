package edu.uw.zookeeper.common;

/**
 * EventBus interface for event publishers.
 * 
 * @see <a href="http://docs.guava-libraries.googlecode.com/git-history/release/javadoc/com/google/common/eventbus/package-summary.html">com.google.common.eventbus</a>
 */
public interface Publisher extends Eventful {
    void post(Object event);
}
