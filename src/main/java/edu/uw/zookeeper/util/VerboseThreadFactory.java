package edu.uw.zookeeper.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates threads with sensible names, groups them in a ThreadGroup, and logs uncaught exceptions. 
 */
public class VerboseThreadFactory implements ThreadFactory {

    public static class UncaughtExceptionHandler implements
            Thread.UncaughtExceptionHandler {

        private final Logger logger;
        
        public UncaughtExceptionHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (e instanceof ThreadDeath) {
                t.getThreadGroup().uncaughtException(t, e);
            } else {
                logger.error("Uncaught exception in Thread {}", t, e);
            }
        }
    }

    private static final AtomicInteger counter = new AtomicInteger(0);

    private final Logger logger = LoggerFactory
            .getLogger(VerboseThreadFactory.class);
    private final ThreadGroup threadGroup;
    private final UncaughtExceptionHandler exceptionHandler;

    public VerboseThreadFactory() {
        this(null);
    }

    public VerboseThreadFactory(String name) {
        this(name, null);
    }

    public VerboseThreadFactory(String name, ThreadGroup parent) {
        if (name == null) {
            name = getClass().getSimpleName();
        }
        if (parent == null) {
            parent = Thread.currentThread().getThreadGroup();
        }
        threadGroup = new ThreadGroup(parent, name);
        exceptionHandler = new UncaughtExceptionHandler(logger);
    }

    @Override
    public Thread newThread(Runnable runnable) {
        int count = counter.incrementAndGet();
        String name = String.format("%s-%d", runnable.getClass()
                .getSimpleName(), count);
        Thread thread = new Thread(threadGroup, runnable, name);
        thread.setUncaughtExceptionHandler(exceptionHandler);
        return thread;
    }
}
