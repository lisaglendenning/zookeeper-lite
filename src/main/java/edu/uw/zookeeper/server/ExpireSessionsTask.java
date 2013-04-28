package edu.uw.zookeeper.server;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import edu.uw.zookeeper.util.ConfigurableTime;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.TimeValue;

public class ExpireSessionsTask extends AbstractIdleService implements
        Runnable {

    public static ExpireSessionsTask create(
            ExpiringSessionManager manager, ScheduledExecutorService executor,
            Optional<Configuration> configuration) {
        TimeValue tickTime;
        ConfigurableTime timeFactory = ConfigurableTime.create(
                DEFAULT_EXPIRE_TICK,
                DEFAULT_EXPIRE_TICK_UNIT);
        if (configuration.isPresent()) {
            Config config = configuration.get().asConfig();
            if (config.hasPath(CONFIG_PATH)) {
                config = config.getConfig(CONFIG_PATH);
            }
            tickTime = timeFactory.get(config);
        } else {
            tickTime = timeFactory.get();
        }
        return create(manager, executor, tickTime); 
    }
    
    public static ExpireSessionsTask create(
            ExpiringSessionManager manager, ScheduledExecutorService executor,
            TimeValue tickTime) {
        return new ExpireSessionsTask(manager, executor, tickTime);
    }

    public static final String CONFIG_PATH = "Sessions.Expire";
    public static final long DEFAULT_EXPIRE_TICK = 1000;
    public static final String DEFAULT_EXPIRE_TICK_UNIT = "MILLISECONDS";

    private final ExpiringSessionManager manager;
    private final ScheduledExecutorService executor;
    private final TimeValue tickTime;
    private ScheduledFuture<?> future = null;

    private ExpireSessionsTask(
            ExpiringSessionManager manager, ScheduledExecutorService executor,
            TimeValue tickTime) {
        super();
        this.manager = manager;
        this.executor = executor;
        this.tickTime = tickTime;
    }
    
    @Override
    public void run() {
        manager.triggerExpired();
    }

    @Override
    protected void startUp() throws Exception {
        long tick = tickTime.value();
        TimeUnit tickUnit = tickTime.unit();
        future = executor.scheduleAtFixedRate(this, tick, tick, tickUnit);
    }

    @Override
    protected void shutDown() throws Exception {
        if (future != null) {
            future.cancel(true);
        }
    }
}