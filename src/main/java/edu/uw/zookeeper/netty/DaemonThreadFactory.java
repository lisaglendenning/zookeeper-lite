package edu.uw.zookeeper.netty;

import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.uw.zookeeper.util.ParameterizedFactory;

public enum DaemonThreadFactory implements ParameterizedFactory<ThreadFactory, ThreadFactory> {
    INSTANCE;

    public static DaemonThreadFactory getInstance() {
        return INSTANCE;
    }

    private final String threadNameFormat = "netty-%d";
    
    @Override
    public ThreadFactory get(ThreadFactory backingFactory) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setThreadFactory(backingFactory)
            .setDaemon(true)
            .setNameFormat(threadNameFormat)
            .build();
        return threadFactory;
    }
}