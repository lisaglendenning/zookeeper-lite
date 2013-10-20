package edu.uw.zookeeper.net.intravm;

import java.util.Queue;
import java.util.concurrent.Executor;

import net.engio.mbassy.PubSubSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Queues;

import edu.uw.zookeeper.common.ActorPublisher;

public class IntraVmPublisher extends ActorPublisher<Object> {

    public static IntraVmPublisher newInstance(
            PubSubSupport<Object> publisher,
            Executor executor) {
        return newInstance(
                publisher,
                executor,
                LogManager.getLogger(IntraVmPublisher.class));
    }

    public static IntraVmPublisher newInstance(
            PubSubSupport<Object> publisher,
            Executor executor,
            Logger logger) {
        return new IntraVmPublisher(
                publisher,
                executor,
                Queues.<Object>newConcurrentLinkedQueue(),
                logger);
    }

    // don't post events until someone is listening
    protected volatile boolean subscribed;
    
    protected IntraVmPublisher(
            PubSubSupport<Object> publisher,
            Executor executor, 
            Queue<Object> mailbox,
            Logger logger) {
        super(publisher, executor, mailbox, logger);
        this.subscribed = false;
    }

    @Override
    public void subscribe(Object object) {
        super.subscribe(object);
        if (! subscribed) {
            subscribed = true;
            run();
        }
    }
    
    @Override
    protected boolean schedule() {
        if (! subscribed) {
            return false;
        } else {
            return super.schedule();
        }
    }
}
