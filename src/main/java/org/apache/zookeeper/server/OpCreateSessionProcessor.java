package org.apache.zookeeper.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.data.OpCreateSessionAction;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.protocol.Records;
import org.apache.zookeeper.util.FilteredProcessor;
import org.apache.zookeeper.util.FilteringProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;


public class OpCreateSessionProcessor extends OpRequestProcessor {

    public static FilteringProcessor<Operation.Request, Operation.Response> create(
            SessionManager sessions) {
        return FilteredProcessor.create(EqualsFilter.create(Operation.CREATE_SESSION),
                new OpCreateSessionProcessor(sessions));
    }
    
    protected final Logger logger = LoggerFactory.getLogger(OpCreateSessionProcessor.class);
    protected final SessionManager sessions;
    
    @Inject
    protected OpCreateSessionProcessor(
            SessionManager sessions) {
        this.sessions = sessions;
    }

    public SessionManager sessions() {
        return sessions;
    }
    
    @Override
    public OpCreateSessionAction.Response apply(Operation.Request request) throws Exception {
        checkArgument(request.operation() == Operation.CREATE_SESSION);
        OpCreateSessionAction.Response opResponse = (OpCreateSessionAction.Response) super.apply(request);
        OpCreateSessionAction.Request opRequest = (OpCreateSessionAction.Request)request;
        ConnectResponse response = apply(opRequest.record());
        opResponse.setResponse(response)
            .setReadOnly(opRequest.readOnly())
            .setWraps(opRequest.wraps());
        return opResponse;
    }

    // TODO: check lastZxid, readOnly?
    public ConnectResponse apply(ConnectRequest request) {
        long sessionId = request.getSessionId();
        byte[] passwd = request.getPasswd();
        int timeOut = request.getTimeOut();
        SessionParameters parameters = SessionParameters.create(timeOut, passwd);
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
