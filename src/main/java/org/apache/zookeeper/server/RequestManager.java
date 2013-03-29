package org.apache.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionEvent;
import org.apache.zookeeper.SessionEventValue;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.protocol.FourLetterCommand;
import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.protocol.OpError;
import org.apache.zookeeper.protocol.OpPingAction;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operations;
import org.apache.zookeeper.protocol.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

public class RequestManager {

    protected final Logger logger = LoggerFactory.getLogger(RequestManager.class);

    protected final SessionManager sessions;
    protected final BiMap<Long, Processor> processors;
    
    @Inject
    public RequestManager(SessionManager sessions) {
        this(sessions,
                Maps.synchronizedBiMap(HashBiMap.<Long, Processor>create()));
    }
    
    protected RequestManager(SessionManager sessions,
            BiMap<Long, Processor> processors) {
        this.processors = processors;
        this.sessions = sessions;
        sessions.register(this);
    }
    
    protected BiMap<Long, Processor> processors() {
        return processors;
    }
    
    public SessionManager sessions() {
        return sessions;
    }
    
    public synchronized Processor processor(long sessionId) {
        return processors().get(sessionId);
    }

    public synchronized Session session(Processor processor) {
        Session session = null;
        Map<Processor, Long> inverse = processors().inverse();
        if (inverse.containsKey(processor)) {
            long id = inverse.get(checkNotNull(processor));
            session = sessions().get(id);
        }
        return session;
    }
    
    public Object submit(Object event) {
        if (event instanceof SessionEvent) {
            Session session = ((SessionEvent) event).session();
            if (event instanceof SessionEventValue && ((SessionEventValue)event).event() instanceof Operation.Request) {
                Operation.Request request = (Operation.Request)((SessionEventValue)event).event();
                switch (request.operation()) {
                case PING:
                    return apply((OpPingAction.Request)request);
                case CLOSE_SESSION:
                {
                    Operation.Response response = Operations.Responses.create(request.operation());
                    if (sessions().remove(session.id()) == null) {
                        // don't know anything about this session
                        response = OpError.create(response,
                                KeeperException.Code.SESSIONEXPIRED);
                    }
                    return response;
                }
                default:
                    throw new IllegalArgumentException();
                }
            }
                
        } else if (event instanceof Operation.Request) {
            switch (((Operation.Request)event).operation()) {
            case CREATE_SESSION:
                return apply((OpCreateSessionAction.Request)event);
            default:
                throw new IllegalArgumentException();
            }
        } else if (event instanceof FourLetterCommand) {
            return ((FourLetterCommand)event).word();
        } else {
            throw new IllegalArgumentException();
        }
        return null;
    }

    public OpPingAction.Response apply(OpPingAction.Request request) {
        return Operations.Responses.create(request.operation());
    }
    
    public OpCreateSessionAction.Response apply(OpCreateSessionAction.Request request) {
        ConnectResponse response = apply(request.request());
        OpCreateSessionAction.Response opResponse = Operations.Responses.create(request.operation());
        opResponse.setResponse(response)
            .setReadOnly(request.readOnly())
            .setWraps(request.wraps());
        return opResponse;
    }

    // FIXME: check lastZxid, readOnly
    public ConnectResponse apply(ConnectRequest request) {
        long sessionId = request.getSessionId();
        byte[] passwd = request.getPasswd();
        int timeOut = request.getTimeOut();
        SessionParameters parameters= new SessionParameters(timeOut, passwd);
        try {
            Session session = sessions().add(sessionId, parameters);
            sessionId = session.id();
            timeOut = Long.valueOf(TimeUnit.MILLISECONDS.convert(
                    session.parameters().timeOut(),
                    session.parameters().timeOutUnit())).intValue();
            passwd = session.parameters().password();
        } catch(IllegalArgumentException e) {
            sessionId = Session.UNINITIALIZED_ID;
            timeOut = 0;
            passwd = new byte[SessionParameters.PASSWORD_LENGTH];
        }
        ConnectResponse response = Records.Responses.create(Operation.CREATE_SESSION);
        response.setProtocolVersion(request.getProtocolVersion());
        response.setSessionId(sessionId);
        response.setTimeOut(timeOut);
        response.setPasswd(passwd);
        return response;
    }
}
