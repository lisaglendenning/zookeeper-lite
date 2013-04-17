package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.data.OpCreateSessionAction;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.FilteredProcessor;
import edu.uw.zookeeper.util.FilteringProcessor;

public class OpCreateSessionProcessor extends OpRequestProcessor {

    public static FilteringProcessor<Operation.Request, Operation.Response> create(
            SessionManager sessions) {
        return FilteredProcessor.create(
                EqualsFilter.create(Operation.CREATE_SESSION),
                new OpCreateSessionProcessor(sessions));
    }

    protected final Logger logger = LoggerFactory
            .getLogger(OpCreateSessionProcessor.class);
    protected final SessionManager sessions;

    @Inject
    protected OpCreateSessionProcessor(SessionManager sessions) {
        this.sessions = sessions;
    }

    public SessionManager sessions() {
        return sessions;
    }

    @Override
    public OpCreateSessionAction.Response apply(Operation.Request request)
            throws Exception {
        checkArgument(request.operation() == Operation.CREATE_SESSION);
        OpCreateSessionAction.Response opResponse = (OpCreateSessionAction.Response) super
                .apply(request);
        OpCreateSessionAction.Request opRequest = (OpCreateSessionAction.Request) request;
        try {
            apply(opRequest.record(), opResponse.record());
        } catch (IllegalArgumentException e) {
            opResponse = OpCreateSessionAction.InvalidResponse.create();
        }
        opResponse.setReadOnly(opRequest.readOnly())
                .setWraps(opRequest.wraps());
        return opResponse;
    }

    // TODO: check lastZxid, readOnly?
    public void apply(ConnectRequest request, ConnectResponse response) {
        long sessionId = request.getSessionId();
        byte[] passwd = request.getPasswd();
        int timeOut = request.getTimeOut();
        Session.Parameters parameters = Session.Parameters
                .create(timeOut, passwd);
        Session session = sessions().add(sessionId, parameters);
        sessionId = session.id();
        timeOut = Long.valueOf(
                TimeUnit.MILLISECONDS.convert(session.parameters().timeOut().value(),
                        session.parameters().timeOut().unit())).intValue();
        passwd = session.parameters().password();
        response.setProtocolVersion(request.getProtocolVersion());
        response.setSessionId(sessionId);
        response.setTimeOut(timeOut);
        response.setPasswd(passwd);
    }
}
