package edu.uw.zookeeper.server;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Processors;
import edu.uw.zookeeper.util.Reference;

public class OpCreateSessionProcessor  implements
Processor<OpCreateSession.Request, OpCreateSession.Response> {

    public static class Filtered implements Processors.FilteringProcessor<Message.ClientMessage, Message.ServerMessage> {
        public static enum Filter implements Predicate<Message.ClientMessage> {
            INSTANCE;
            
            public static Filter getInstance() {
                return INSTANCE;
            }
            
            @Override
            public boolean apply(@Nullable Message.ClientMessage input) {
                return (input instanceof OpCreateSession.Request);
            }
        }

        public static Filtered newInstance(Processor<OpCreateSession.Request, OpCreateSession.Response> processor) {
            return new Filtered(processor);
        }

        private final Processor<OpCreateSession.Request, OpCreateSession.Response> processor;
        
        protected Filtered(Processor<OpCreateSession.Request, OpCreateSession.Response> processor) {
            this.processor = processor;
        }
        
        @Override
        public Message.ServerMessage apply(Message.ClientMessage input) throws Exception {
            if (filter().apply(input)) {
                return processor.apply((OpCreateSession.Request)input);
            } else {
                return null;
            }
        }

        @Override
        public Predicate<? super Message.ClientMessage> filter() {
            return Filter.getInstance();
        }
    }
    
    public static Filtered filtered(
            SessionTable sessions,
            Reference<Long> lastZxid) {
        return Filtered.newInstance(newInstance(sessions, lastZxid));
    }
    
    public static OpCreateSessionProcessor newInstance(
            SessionTable sessions,
            Reference<Long> lastZxid) {
        return new OpCreateSessionProcessor(sessions, lastZxid);
    }
    
    protected final Logger logger = LoggerFactory
            .getLogger(OpCreateSessionProcessor.class);
    protected final SessionTable sessions;
    protected final Reference<Long> lastZxid;

    protected OpCreateSessionProcessor(
            SessionTable sessions,
            Reference<Long> lastZxid) {
        this.sessions = sessions;
        this.lastZxid = lastZxid;
    }

    public Reference<Long> lastZxid() {
        return lastZxid;
    }
    
    public SessionTable sessions() {
        return sessions;
    }

    @Override
    public OpCreateSession.Response apply(OpCreateSession.Request request) {
        // emulating the the behavior of ZooKeeperServer,
        // which is to just close the connection
        // without replying when the zxid is out of sync
        long myZxid = lastZxid().get();
        if (request.asRecord().getLastZxidSeen() > myZxid) {
            throw new IllegalStateException(String.format("Zxid 0x%x > 0x%x",
                    Long.toHexString(request.asRecord().getLastZxidSeen()),
                    Long.toHexString(myZxid)));
        }
        
        // TODO: readOnly?
        Session session = null;
        if (request instanceof OpCreateSession.Request.NewRequest) {
            session = sessions().validate(request.toParameters());
        } else if (request instanceof OpCreateSession.Request.RenewRequest) {
            try {
                session = sessions().validate(request.toSession());
            } catch (Exception e) {
                session = null;
            }
        } else {
            throw new IllegalArgumentException(request.toString());
        }
        OpCreateSession.Response response = (session == null)
            ? OpCreateSession.Response.Invalid.create(request.readOnly(), request.wraps())
            : OpCreateSession.Response.Valid.create(session, request.readOnly(), request.wraps());
        return response;
    }
}
