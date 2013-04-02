package org.apache.zookeeper.server;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;


public class OpCreateSessionTask extends OpRequestTask {

    public static OpCreateSessionTask create(
            Operation.Request request,
            SessionManager sessions) {
        return new OpCreateSessionTask((OpCreateSessionAction.Request)request, sessions);
    }
    
    public static OpCreateSessionTask create(
            OpCreateSessionAction.Request request,
            SessionManager sessions) {
        return new OpCreateSessionTask(request, sessions);
    }
    
    protected final Logger logger = LoggerFactory.getLogger(OpCreateSessionTask.class);
    protected final SessionManager sessions;
    
    @Inject
    protected OpCreateSessionTask(
            OpCreateSessionAction.Request request,
            SessionManager sessions) {
        super(request);
        this.sessions = sessions;
    }

    public SessionManager sessions() {
        return sessions;
    }
    
    @Override
    public OpCreateSessionAction.Request request() {
        return (OpCreateSessionAction.Request) super.request();
    }

    @Override
    public OpCreateSessionAction.Response call() throws Exception {
        OpCreateSessionAction.Response opResponse = (OpCreateSessionAction.Response) super.call();
        ConnectResponse response = apply(request().record());
        opResponse.setResponse(response)
            .setReadOnly(request().readOnly())
            .setWraps(request().wraps());
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
