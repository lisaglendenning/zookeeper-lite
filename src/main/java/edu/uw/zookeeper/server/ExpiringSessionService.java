package edu.uw.zookeeper.server;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.AbstractScheduledService;
import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.TimeValue;

public class ExpiringSessionService extends AbstractScheduledService implements
        Reference<ExpiringSessionTable> {

    @Configurable(path="Sessions", key="ExpireTick", value="2 seconds", help="Time")
    public static class ConfigurableTickTime extends DefaultMain.ConfigurableTimeout {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTickTime().apply(configuration);
        }
        
    }

    public static ExpiringSessionService newInstance(
            ExpiringSessionTable sessions,
            ScheduledExecutorService executor,
            Configuration configuration) {
        return newInstance(sessions, executor,
                ConfigurableTickTime.get(configuration));
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