package edu.uw.zookeeper.net.intravm;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ActorPublisher;

public class IntraVmPublisher extends ActorPublisher {

    public static IntraVmPublisher newInstance(
            Publisher publisher,
            Executor executor) {
        return new IntraVmPublisher(
                publisher,
                executor,
                new ConcurrentLinkedQueue<Object>());
    }

    protected final AtomicBoolean registered;
    
    protected IntraVmPublisher(
            Publisher publisher,
            Executor executor, 
            Queue<Object> mailbox) {
        super(publisher, executor, mailbox);
        this.registered = new AtomicBoolean(false);
    }

    @Override
    public void register(Object object) {
        super.register(object);
        if (registered.compareAndSet(false, true)) {
            run();
        }
    }
    
    @Override
    protected boolean schedule() {
        if (! registered.get()) {
            return false;
        } else {
            return super.schedule();
        }
    }
    
}
