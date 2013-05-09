package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoopGroup;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.util.Singleton;
import edu.uw.zookeeper.util.TimeValue;

public class EventLoopGroupService<T extends EventLoopGroup> extends AbstractIdleService implements Singleton<T> {
    public static <T extends EventLoopGroup> EventLoopGroupService<T> newInstance(T group) {
        return new EventLoopGroupService<T>(group, Optional.<Executor>absent());
    }

    public static <T extends EventLoopGroup> EventLoopGroupService<T> newInstance(T group, Executor thisExecutor) {
        return new EventLoopGroupService<T>(group, Optional.<Executor>of(thisExecutor));
    }

    private static final TimeValue DEFAULT_WAIT_INTERVAL = TimeValue.create(30L, TimeUnit.SECONDS);

    private final Logger logger = LoggerFactory
            .getLogger(EventLoopGroupService.class);
    private final Optional<Executor> thisExecutor;
    private final T group;

    protected EventLoopGroupService(T group,
            Optional<Executor> thisExecutor) {
        this.thisExecutor = thisExecutor;
        this.group = checkNotNull(group);
    }

    @Override
    protected Executor executor() {
        if (thisExecutor.isPresent()) {
            return thisExecutor.get();
        } else {
            return super.executor();
        }
    }

    public T get() {
        return group;
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        if (! get().isShuttingDown()) {
            get().shutdownGracefully(); // take the defaults
        }
        if (! get().isTerminated()) {
            if (! get().awaitTermination(DEFAULT_WAIT_INTERVAL.value(), DEFAULT_WAIT_INTERVAL.unit())) {
                logger.warn("Failed to gracefully terminate EventLoopGroup {}", get());
            }
        }
    }
}
