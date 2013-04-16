package edu.uw.zookeeper.util;

/**
 * Interface for the com.google.common.eventbus API.
 * 
 * @see <a href="http://docs.guava-libraries.googlecode.com/git-history/release/javadoc/com/google/common/eventbus/package-summary.html">com.google.common.eventbus</a>
 */
public interface Eventful {
    void post(Object event);

    void register(Object object);

    void unregister(Object object);
}
