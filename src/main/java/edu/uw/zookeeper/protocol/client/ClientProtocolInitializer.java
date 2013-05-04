package edu.uw.zookeeper.protocol.client;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.TimeValue;

public abstract class ClientProtocolInitializer implements Callable<ListenableFuture<Session>> {

    public static ClientProtocolInitializer.NewSessionConnectionInitializer newSession(
            ClientCodecConnection codecConnection,
            Reference<Long> lastZxid,
            TimeValue timeOut) {
        return NewSessionConnectionInitializer.create(codecConnection, lastZxid, timeOut);
    }
    
    public static ClientProtocolInitializer.RenewSessionConnectionInitializer renewSession(
            ClientCodecConnection codecConnection,
            Reference<Long> lastZxid,
            Session session) {
        return RenewSessionConnectionInitializer.create(codecConnection, lastZxid, session);
    }
    
    protected final ClientCodecConnection codecConnection;
    protected final Reference<Long> lastZxid;

    protected ClientProtocolInitializer(
            ClientCodecConnection codecConnection,
            Reference<Long> lastZxid) {
        this.codecConnection = codecConnection;
        this.lastZxid = lastZxid;
    }
    
    @Override
    public ListenableFuture<Session> call() throws IOException {
        checkState(codecConnection.asCodec().state() == ProtocolState.ANONYMOUS);
        OpCreateSession.Request message = request();
        ClientConnectListener listener = new ClientConnectListener(codecConnection);
        try {
            codecConnection.write(message);
        } catch (Exception e) {
            listener.cancel();
            Throwables.propagateIfInstanceOf(e, IOException.class);
            throw Throwables.propagate(e);
        }
        ListenableFuture<Session> future = listener.promise();
        return future;
    }
    
    protected abstract OpCreateSession.Request request();

    public static class NewSessionConnectionInitializer extends ClientProtocolInitializer {
        
        public static ClientProtocolInitializer.NewSessionConnectionInitializer create(
                ClientCodecConnection codecConnection,
                Reference<Long> lastZxid,
                TimeValue timeOut) {
            return new NewSessionConnectionInitializer(codecConnection, lastZxid, timeOut);
        }
        
        private final TimeValue timeOut;
        
        private NewSessionConnectionInitializer(
                ClientCodecConnection codecConnection,
                Reference<Long> lastZxid,
                TimeValue timeOut) {
            super(codecConnection, lastZxid);
            this.timeOut = timeOut;
        }

        protected OpCreateSession.Request request() {
            OpCreateSession.Request message = OpCreateSession.Request.NewRequest.newInstance(timeOut, lastZxid.get());
            return message;
        }
    }
    
    public static class RenewSessionConnectionInitializer extends ClientProtocolInitializer {

        public static ClientProtocolInitializer.RenewSessionConnectionInitializer create(
                ClientCodecConnection codecConnection,
                Reference<Long> lastZxid,
                Session session) {
            return new RenewSessionConnectionInitializer(codecConnection, lastZxid, session);
        }
        
        private final Session session;

        private RenewSessionConnectionInitializer(
                ClientCodecConnection codecConnection,
                Reference<Long> lastZxid,
                Session session) {
            super(codecConnection, lastZxid);
            checkArgument(session.initialized());
            this.session = session;
        }
        
        protected OpCreateSession.Request request() {
            OpCreateSession.Request message = OpCreateSession.Request.RenewRequest.newInstance(session, lastZxid.get());
            return message;
        }
    } 
}