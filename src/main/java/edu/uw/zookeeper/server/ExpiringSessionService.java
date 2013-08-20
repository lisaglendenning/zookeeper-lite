package edu.uw.zookeeper.server;

import java.util.concurrent.ScheduledExecutorService;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.typesafe.config.Config;

import edu.uw.zookeeper.common.ConfigurableTime;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.TimeValue;

public class ExpiringSessionService extends AbstractScheduledService implements
        Reference<ExpiringSessionTable> {

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

    public static ExpiringSessionService newInstance(
            ExpiringSessionTable sessions,
            ScheduledExecutorService executor,
            Configuration configuration) {
        return newInstance(sessions, executor,
                ConfigurableTickTime.getInstance().get(configuration));
    }
    
    public static ExpiringSessionService newInstance(
            ExpiringSessionTable sessions,
            ScheduledExecutorService executor,
            TimeValue tickTime) {
        return new ExpiringSessionService(sessions, executor, tickTime);
    }

    private final ExpiringSessionTable sessions;
    private final ScheduledExecutorService executor;
    private final TimeValue tickTime;

    private ExpiringSessionService(
            ExpiringSessionTable sessions,
            ScheduledExecutorService executor,
            TimeValue tickTime) {
        super();
        this.sessions = sessions;
        this.executor = executor;
        this.tickTime = tickTime;
    }
    
    @Override
    public ExpiringSessionTable get() {
        return sessions;
    }

    @Override
    protected ScheduledExecutorService executor() {
        return executor;
    }

    @Override
    protected void runOneIteration() throws Exception {
        sessions.triggerExpired();
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(tickTime.value(), tickTime.value(), tickTime.unit());
    }
}