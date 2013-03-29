package org.apache.zookeeper.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VerboseThreadFactory implements ThreadFactory {

    public static class VerboseThreadUncaughtExceptionHandler implements
            Thread.UncaughtExceptionHandler {

        public final Logger logger = LoggerFactory
                .getLogger(VerboseThreadUncaughtExceptionHandler.class);
        
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (e instanceof ThreadDeath) {
                t.getThreadGroup().uncaughtException(t, e);
            } else {
                logger.error("Uncaught exception in Thread {}", t,
                        e);
            }
        }
    }

    protected static final AtomicInteger counter = new AtomicInteger(0);
    
    protected final Logger logger = LoggerFactory
            .getLogger(VerboseThreadFactory.class);
    protected final ThreadGroup threadGroup;

    public VerboseThreadFactory() {
        this(null, null);
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

        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
            Thread.setDefaultUncaughtExceptionHandler(new VerboseThreadUncaughtExceptionHandler());
        }
    }

    @Override
    public Thread newThread(Runnable runnable) {
        int count = counter.incrementAndGet();
        String name = String.format("%s-%d", runnable.getClass()
                .getSimpleName(), count);
        Thread thread = new Thread(threadGroup, runnable, name);
        return thread;
    }

}