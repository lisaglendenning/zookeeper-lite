package edu.uw.zookeeper.net.intravm;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ActorPublisher;

public class IntraVmPublisher extends ActorPublisher {

    public static IntraVmPublisher newInstance(
            Publisher publisher,
            Executor executor) {
        return new IntraVmPublisher(
                publisher,
                executor,
                new ConcurrentLinkedQueue<Object>(),
                LogManager.getLogger(IntraVmPublisher.class));
    }

    protected final AtomicBoolean registered;
    
    protected IntraVmPublisher(
            Publisher publisher,
            Executor executor, 
            Queue<Object> mailbox,
            Logger logger) {
        super(publisher, executor, mailbox, logger);
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
