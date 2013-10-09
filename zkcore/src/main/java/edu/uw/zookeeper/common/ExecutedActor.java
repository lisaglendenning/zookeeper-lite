package edu.uw.zookeeper.common;

import java.util.concurrent.Executor;

public abstract class ExecutedActor<I> extends QueuedActor<I> {

    protected abstract Executor executor();

    @Override
    protected void doSchedule() {
        executor().execute(this);
    }
}
