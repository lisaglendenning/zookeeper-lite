package org.apache.zookeeper.protocol.server;

import java.io.IOException;
import java.io.InputStream;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.protocol.Decoder;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class SessionStateDecoder implements Pipeline.Processor<Operation.Response>, Decoder<Operation.Request> {

    public static SessionStateDecoder create(
            Zxid zxid, Eventful eventful) {
        return new SessionStateDecoder(zxid, eventful);
    }

    protected final Logger logger = LoggerFactory.getLogger(SessionStateDecoder.class);
    protected final SessionConnectionState state;
    protected final Pipeline<Operation.Response> processors;
    protected final SessionStateRequestDecoder decoder;

    @Inject
    protected SessionStateDecoder(Zxid zxid, Eventful eventful) {
        this(zxid, SessionConnectionState.create(eventful));
    }
    
    protected SessionStateDecoder(Zxid zxid, SessionConnectionState state) {
        super();
        this.state = state;
        this.processors = new Pipeline<Operation.Response>();
        this.decoder = SessionStateRequestDecoder.create(state);

        processors.add(GetZxidProcessor.create(zxid));
        processors.add(SessionStateResponseProcessor.create(state));
    }
    
    public SessionConnectionState state() {
        return state;
    }

    @Override
    public Operation.Response apply(Operation.Response response) {
        return processors.apply(response);
    }

    @Override
    public Operation.Request decode(InputStream stream) throws IOException {
        return decoder.decode(stream);
    }
}
