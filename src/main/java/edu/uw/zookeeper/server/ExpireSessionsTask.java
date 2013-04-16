package edu.uw.zookeeper.server;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.typesafe.config.ConfigException;

import edu.uw.zookeeper.util.Configurable;
import edu.uw.zookeeper.util.ConfigurableTime;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.TimeValue;

public class ExpireSessionsTask extends AbstractIdleService implements
        Runnable, Configurable {

    public static final String CONFIG_PATH = "Sessions.Expire";
    public static final long DEFAULT_EXPIRE_TICK = 1000;
    public static final String DEFAULT_EXPIRE_TICK_UNIT = "MILLISECONDS";

    protected final ExpiringSessionManager manager;
    protected final ScheduledExecutorService executor;
    protected final ConfigurableTime tickTime;
    protected ScheduledFuture<?> future = null;

    @Inject
    protected ExpireSessionsTask(Configuration configuration,
            ExpiringSessionManager manager, ScheduledExecutorService executor) {
        this(manager, executor);
        configure(configuration);
    }

    protected ExpireSessionsTask(
            ExpiringSessionManager manager, ScheduledExecutorService executor) {
        super();
        this.manager = manager;
        this.executor = executor;
        this.tickTime = ConfigurableTime.create(
                DEFAULT_EXPIRE_TICK,
                DEFAULT_EXPIRE_TICK_UNIT);
    }
    
    @Override
    public void configure(Configuration configuration) {
        try {
            tickTime.get(configuration.get().getConfig(CONFIG_PATH));
        } catch (ConfigException.Missing e) {}
    }

    @Override
    public void run() {
        manager.triggerExpired();
    }

    @Override
    protected void startUp() throws Exception {
        TimeValue value = tickTime.get();
        long tick = value.value();
        TimeUnit tickUnit = value.unit();
        future = executor.scheduleAtFixedRate(this, tick, tick, tickUnit);
    }

    @Override
    protected void shutDown() throws Exception {
        if (future != null) {
            future.cancel(true);
        }
    }
}