package org.apache.zookeeper.protocol.server;

import java.io.IOException;
import java.io.InputStream;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.util.ChainedProcessor;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.Processor;
import org.apache.zookeeper.protocol.Decoder;
import org.apache.zookeeper.protocol.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class SessionStateDecoder implements Processor<Operation.Response, Operation.Response>, Decoder<Operation.Request> {

    public static SessionStateDecoder create(
            Zxid zxid, Eventful eventful) {
        return new SessionStateDecoder(zxid, eventful);
    }

    protected final Logger logger = LoggerFactory.getLogger(SessionStateDecoder.class);
    protected final SessionConnectionState state;
    protected final ChainedProcessor<Operation.Response> processor;
    protected final SessionStateRequestDecoder decoder;

    @Inject
    protected SessionStateDecoder(Zxid zxid, Eventful eventful) {
        this(zxid, SessionConnectionState.create(eventful));
    }
    
    protected SessionStateDecoder(Zxid zxid, SessionConnectionState state) {
        super();
        this.state = state;
        this.decoder = SessionStateRequestDecoder.create(state);
        this.processor = ChainedProcessor.create();
        processor.add(GetZxidProcessor.create(zxid));
        processor.add(SessionStateResponseProcessor.create(state));
    }
    
    public SessionConnectionState state() {
        return state;
    }

    @Override
    public Operation.Response apply(Operation.Response response) throws Exception {
        return processor.apply(response);
    }

    @Override
    public Operation.Request decode(InputStream stream) throws IOException {
        return decoder.decode(stream);
    }
}
