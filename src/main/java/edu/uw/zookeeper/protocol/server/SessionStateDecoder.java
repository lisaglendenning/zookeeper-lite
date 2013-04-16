package edu.uw.zookeeper.protocol.server;

import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import edu.uw.zookeeper.SessionConnectionState;
import edu.uw.zookeeper.Zxid;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.ProcessorChain;

public class SessionStateDecoder implements
        Processor<Operation.Response, Operation.Response>,
        Decoder<Operation.Request> {

    public static SessionStateDecoder create(Zxid zxid, Eventful eventful) {
        return new SessionStateDecoder(zxid, eventful);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(SessionStateDecoder.class);
    protected final SessionConnectionState state;
    protected final ProcessorChain<Operation.Response> processor;
    protected final SessionStateRequestDecoder decoder;

    @Inject
    protected SessionStateDecoder(Zxid zxid, Eventful eventful) {
        this(zxid, SessionConnectionState.create(eventful));
    }

    protected SessionStateDecoder(Zxid zxid, SessionConnectionState state) {
        super();
        this.state = state;
        this.decoder = SessionStateRequestDecoder.create(state);
        this.processor = ProcessorChain.create();
        processor.add(GetZxidProcessor.create(zxid));
        processor.add(SessionStateResponseProcessor.create(state));
    }

    public SessionConnectionState state() {
        return state;
    }

    @Override
    public Operation.Response apply(Operation.Response response)
            throws Exception {
        return processor.apply(response);
    }

    @Override
    public Operation.Request decode(InputStream stream) throws IOException {
        return decoder.decode(stream);
    }
}
