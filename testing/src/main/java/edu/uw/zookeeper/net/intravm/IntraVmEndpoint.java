package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Queues;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.net.Connection;

public final class IntraVmEndpoint<I,O> extends AbstractIntraVmEndpoint<I,O,I,O> implements Stateful<Connection.State>, Eventful<Connection.Listener<? super O>>, Executor {

    public static <I,O> IntraVmEndpoint<I,O> newInstance(
            SocketAddress address,
            Executor executor) {
        Logger logger = LogManager.getLogger(IntraVmEndpoint.class);
        return new IntraVmEndpoint<I,O>(
                address,
                executor, 
                logger,
                IntraVmPublisher.<O>defaults(executor, logger));
    }
    
    private final EndpointReader reader;
    private final EndpointWriter writer;
    
    protected IntraVmEndpoint(
            SocketAddress address,
            Executor executor,
            Logger logger,
            IntraVmPublisher<O> publisher) {
        super(address, executor, logger, publisher);
        this.reader = new EndpointReader();
        this.writer = new EndpointWriter();
    }
    
    @Override
    public EndpointReader reader() {
        return reader;
    }

    @Override
    public EndpointWriter writer() {
        return writer;
    }
    
    public final class EndpointWriter extends AbstractEndpointWriter {

        protected EndpointWriter() {
            super(Queues.<EndpointWrite<? extends I,I>>newLinkedBlockingDeque());
        }

        @Override
        protected boolean apply(EndpointWrite<? extends I, I> input)
                throws Exception {
            if (input.remote().reader().send(input.message())) {
                input.run();
                return true;
            } else {
                Exception e = new ClosedChannelException();
                input.setException(e);
                throw e;
            }
        }
    }

    public final class EndpointReader extends AbstractEndpointReader {

        protected EndpointReader() {
            super(Queues.<O>newLinkedBlockingDeque());
        }

        @Override
        protected boolean apply(O input) throws Exception {
            return publisher.send(input);
        }
    }
}
