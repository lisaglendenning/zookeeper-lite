package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

public class ActorExecutor extends CallbackActor<Runnable> implements Executor {

    public static ActorExecutor newInstance(Executor executor) {
        return new ActorExecutor(
                new ConcurrentLinkedQueue<Runnable>(), 
                executor);
    }
    
    protected final Queue<Runnable> mailbox;
    protected final Executor executor;
    
    protected ActorExecutor(
            Queue<Runnable> mailbox,
            Executor executor) {
        this.mailbox = mailbox;
        this.executor = executor;
    }
    
    @Override
    public void execute(Runnable command) {
        if (! send(command)) {
            flush(command);
        }
    }

    protected boolean apply(Runnable input) {
        input.run();
        return true;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected Queue<Runnable> mailbox() {
        return mailbox;
    }
}
