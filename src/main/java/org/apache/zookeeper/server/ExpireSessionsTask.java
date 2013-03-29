package org.apache.zookeeper.server;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.util.Configurable;
import org.apache.zookeeper.util.ConfigurableTime;
import org.apache.zookeeper.util.Configuration;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

public class ExpireSessionsTask extends AbstractIdleService implements Runnable, Configurable {

    public static final String PARAM_KEY_EXPIRE_TICK = "Sessions.ExpireTick";
    public static final long PARAM_DEFAULT_EXPIRE_TICK = 1000;
    public static final String PARAM_KEY_EXPIRE_TICK_UNIT = "Sessions.ExpireTickUnit";
    public static final String PARAM_DEFAULT_EXPIRE_TICK_UNIT = "MILLISECONDS";

    protected final ExpiringSessionManager manager;
    protected final ScheduledExecutorService executor;
    protected final ConfigurableTime tickTime;
    protected ScheduledFuture<?> future = null;
    
    @Inject
    protected ExpireSessionsTask(
            Configuration configuration,
            ExpiringSessionManager manager,
            ScheduledExecutorService executor) {
        super();
        this.manager = manager;
        this.executor = executor;
        this.tickTime = ConfigurableTime.create(
                PARAM_KEY_EXPIRE_TICK, 
                PARAM_DEFAULT_EXPIRE_TICK, 
                PARAM_KEY_EXPIRE_TICK_UNIT, 
                PARAM_DEFAULT_EXPIRE_TICK_UNIT);
        configure(configuration);
    }

    @Override
    public void configure(Configuration configuration) {
        tickTime.configure(configuration);
    }

    @Override
    public void run() {
        manager.triggerExpired();
    }

    @Override
    protected void startUp() throws Exception {
        long tick = tickTime.time();
        TimeUnit tickUnit = tickTime.timeUnit();
        future = executor.scheduleAtFixedRate(this, tick, tick, tickUnit);
    }

    @Override
    protected void shutDown() throws Exception {
        if (future != null) {
            future.cancel(true);
        }
    }
}