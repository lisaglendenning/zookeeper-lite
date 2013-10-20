package edu.uw.zookeeper.protocol.server;

import java.util.Map;
import java.util.concurrent.Executor;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.listener.Handler;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.ExpiringSessionTable;

public class ExpiringSessionRequestExecutor extends SessionRequestExecutor {

    public static ExpiringSessionRequestExecutor newInstance(
            ExpiringSessionTable sessions,
            Executor executor,
            Map<Long, PubSubSupport<Object>> listeners,
            Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor) {
        return new ExpiringSessionRequestExecutor(sessions, executor, listeners, processor);
    }
    
    protected final ExpiringSessionTable sessions;
    
    public ExpiringSessionRequestExecutor(
            ExpiringSessionTable sessions,
            Executor executor,
            Map<Long, PubSubSupport<Object>> listeners,
            Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor) {
        super(executor, listeners, processor);
        this.sessions = sessions;
        
        sessions.subscribe(this);
    }

    @Override
    public boolean send(PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>> message) {
        boolean send = super.send(message);
        if (send) {
            sessions.touch(message.task().getSessionId());
        }
        return send;
    }

    @Handler
    public void handleSessionStateEvent(SessionStateEvent event) {
        switch (event.event()) {
        case SESSION_EXPIRED:
        {
            Message.ClientRequest<Records.Request> request = ProtocolRequestMessage.of(0, Records.Requests.getInstance().get(OpCode.CLOSE_SESSION));
            submit(SessionRequest.of(event.session().id(), request));
            break;
        }
        default:
            break;
        }
    }
}
