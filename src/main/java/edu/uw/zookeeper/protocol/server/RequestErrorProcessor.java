package edu.uw.zookeeper.protocol.server;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processors;
import edu.uw.zookeeper.util.Processors.ForwardingProcessor;

public class RequestErrorProcessor<I extends Operation.Request> extends ForwardingProcessor<I, Records.Response> implements Processors.UncheckedProcessor<I, Records.Response> {

    public static <I extends Operation.Request> RequestErrorProcessor<I> create(
            Processors.CheckedProcessor<? super I, ? extends Records.Response, KeeperException> processor) {
        return new RequestErrorProcessor<I>(processor);
    }

    protected final Processors.CheckedProcessor<? super I, ? extends Records.Response, KeeperException> delegate;

    public RequestErrorProcessor(
            Processors.CheckedProcessor<? super I, ? extends Records.Response, KeeperException> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Records.Response apply(I input) {
        Records.Response result;
        try {
            result = delegate.apply(input);
        } catch (KeeperException e) {
            result = new IErrorResponse(e.code());
        }
        return result;
    }

    @Override
    protected Processors.CheckedProcessor<? super I, ? extends Records.Response, KeeperException> delegate() {
        return delegate;
    }
}
