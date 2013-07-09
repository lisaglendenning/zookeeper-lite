package edu.uw.zookeeper.server;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;

public class RequestErrorProcessor implements
        Processor<Records.Request, Records.Response> {

    public static RequestErrorProcessor newInstance(
            Processor<Records.Request, Records.Response> processor) {
        return new RequestErrorProcessor(processor);
    }

    protected final Processor<Records.Request, Records.Response> processor;

    protected RequestErrorProcessor(
            Processor<Records.Request, Records.Response> processor) {
        this.processor = processor;
    }

    @Override
    public Records.Response apply(Records.Request request) throws Exception {
        Records.Response reply;
        try {
            reply = processor.apply(request);
        } catch (Exception e) {
            KeeperException.Code code = null;
            if (e instanceof KeeperException) {
                code = ((KeeperException)e).code();
            } else if (e instanceof IllegalArgumentException ||
                    e instanceof IllegalStateException) {
                code = KeeperException.Code.BADARGUMENTS;
            } else {
                throw e;
            }
            reply = new IErrorResponse(code);
        }
        return reply;
    }
}
