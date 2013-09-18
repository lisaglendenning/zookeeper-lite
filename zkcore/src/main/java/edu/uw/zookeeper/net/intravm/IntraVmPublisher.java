package edu.uw.zookeeper.net.intravm;

import java.util.Queue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Queues;

import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ActorPublisher;

public class IntraVmPublisher extends ActorPublisher {

    public static IntraVmPublisher newInstance(
            Publisher publisher,
            Executor executor) {
        return newInstance(
                publisher,
                executor,
                LogManager.getLogger(IntraVmPublisher.class));
    }

    public static IntraVmPublisher newInstance(
            Publisher publisher,
            Executor executor,
            Logger logger) {
        return new IntraVmPublisher(
                publisher,
                executor,
                Queues.<Object>newConcurrentLinkedQueue(),
                logger);
    }

    // don't post events until someone is listening
    protected volatile boolean registered;
    
    protected IntraVmPublisher(
            Publisher publisher,
            Executor executor, 
            Queue<Object> mailbox,
            Logger logger) {
        super(publisher, executor, mailbox, logger);
        this.registered = false;
    }

    @Override
    public void register(Object object) {
        super.register(object);
        if (! registered) {
            registered = true;
            run();
        }
    }
    
    @Override
    protected boolean schedule() {
        if (! registered) {
            return false;
        } else {
            return super.schedule();
        }
    }
    
}
