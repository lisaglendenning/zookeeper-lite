package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.Executor;
import io.netty.channel.EventLoopGroup;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import edu.uw.zookeeper.util.Singleton;

public class EventLoopGroupService<T extends EventLoopGroup> extends AbstractIdleService implements Singleton<T> {
    public static <T extends EventLoopGroup> EventLoopGroupService<T> newInstance(T group) {
        return new EventLoopGroupService<T>(group, Optional.<Executor>absent());
    }

    public static <T extends EventLoopGroup> EventLoopGroupService<T> newInstance(T group, Executor thisExecutor) {
        return new EventLoopGroupService<T>(group, Optional.<Executor>of(thisExecutor));
    }
    
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
        get().shutdown();
    }
}
