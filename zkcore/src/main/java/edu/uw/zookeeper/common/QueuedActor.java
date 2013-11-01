package edu.uw.zookeeper.common;

import java.util.Queue;


public abstract class QueuedActor<T> extends AbstractActor<T> {
    
    protected QueuedActor() {
        super();
    }

    protected QueuedActor(State state) {
        super(state);
    }
    
    @Override
    protected boolean doSend(T message) {
        if (! mailbox().offer(message)) {
            return false;
        }
        if (! schedule() && (state() == State.TERMINATED)) {
            mailbox().remove(message);
            return false;
        }
        return true;
    }

    @Override
    protected boolean schedule() {
        if (! mailbox().isEmpty()) {
            return super.schedule();
        } else {
            return false;
        }
    }

    @Override
    protected void doRun() throws Exception {
        T next;
        while ((next = mailbox().poll()) != null) {
            logger().debug("Applying {} ({})", next, this);
            if (! apply(next)) {
                break;
            }
        }
    }

    @Override
    protected void runExit() {
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            schedule();
        }
    }

    @Override
    protected void doStop() {
        mailbox().clear();
    }

    protected abstract boolean apply(T input) throws Exception;

    protected abstract Queue<T> mailbox();
}
