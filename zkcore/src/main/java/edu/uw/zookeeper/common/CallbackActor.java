package edu.uw.zookeeper.common;


public abstract class CallbackActor<T> extends ExecutedActor<T> {

    protected CallbackActor() {}
    
    protected synchronized void flush(T input) {
        doRun();
        apply(input);
    }

    @Override
    protected synchronized void doRun() {
        T next;
        while ((next = mailbox().poll()) != null) {
            apply(next);
        }
    }

    protected abstract boolean apply(T input);

    @Override
    protected void doStop() {
         doRun();
    }
}
