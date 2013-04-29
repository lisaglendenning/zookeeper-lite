package edu.uw.zookeeper.server;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import edu.uw.zookeeper.util.ConfigurableTime;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.TimeValue;

public class ExpireSessionsTask extends AbstractIdleService implements
        Runnable {

    public static enum ConfigurableTickTime implements DefaultsFactory<Configuration, TimeValue> {
        DEFAULT;

        public static ConfigurableTickTime getInstance() {
            return DEFAULT;
        }

        public static final String CONFIG_PATH = "Sessions.Expire";
        public static final long DEFAULT_EXPIRE_TICK = 2000;
        public static final String DEFAULT_EXPIRE_TICK_UNIT = "MILLISECONDS";
        
        private final ConfigurableTime timeFactory = ConfigurableTime.create(
                DEFAULT_EXPIRE_TICK,
                DEFAULT_EXPIRE_TICK_UNIT);
        
        @Override
        public TimeValue get() {
            return timeFactory.get();
        }

        @Override
        public TimeValue get(Configuration value) {
            Config config = value.asConfig();
            if (config.hasPath(CONFIG_PATH)) {
                config = config.getConfig(CONFIG_PATH);
                return timeFactory.get(config);
            } else {
                return get();
            }
        }
        
    }

    public static ExpireSessionsTask newInstance(
            ExpiringSessionManager manager, ScheduledExecutorService executor,
            Configuration configuration) {
        return newInstance(manager, executor, ConfigurableTickTime.getInstance().get(configuration));
    }
    
    public static ExpireSessionsTask newInstance(
            ExpiringSessionManager manager, ScheduledExecutorService executor,
            TimeValue tickTime) {
        return new ExpireSessionsTask(manager, executor, tickTime);
    }

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