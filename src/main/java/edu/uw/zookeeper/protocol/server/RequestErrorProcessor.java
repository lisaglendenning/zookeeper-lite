package edu.uw.zookeeper.protocol.server;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Throwables;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Processors;

public class RequestErrorProcessor<I extends Operation.Request> extends Processors.ForwardingProcessor<I, Records.Response> {

    public static <I extends Operation.Request> RequestErrorProcessor<I> create(
            Processor<? super I, ? extends Records.Response> processor) {
        return new RequestErrorProcessor<I>(processor);
    }

    protected final Processor<? super I, ? extends Records.Response> delegate;

    public RequestErrorProcessor(
            Processor<? super I, ? extends Records.Response> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Records.Response apply(I input) {
        Records.Response result;
        try {
            result = super.apply(input);
        } catch (Throwable t) {
            KeeperException.Code code;
            if (t instanceof KeeperException) {
                code = ((KeeperException) t).code();
            } else if (t instanceof IllegalArgumentException ||
                    t instanceof IllegalStateException) {
                code = KeeperException.Code.BADARGUMENTS;
            } else {
                throw Throwables.propagate(t);
            }
            result = new IErrorResponse(code);
        }
        return result;
    }

    @Override
    protected Processor<? super I, ? extends Records.Response> delegate() {
        return delegate;
    }
}
