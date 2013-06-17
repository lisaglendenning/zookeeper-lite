package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.jute.OutputArchive;
import org.apache.jute.Record;

import com.google.common.base.Objects;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.Session.Parameters;
import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ByteBufOutputArchive;
import edu.uw.zookeeper.protocol.proto.IConnectRequest;
import edu.uw.zookeeper.protocol.proto.IConnectResponse;
import edu.uw.zookeeper.protocol.proto.IOperationalRecord;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Operational;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.TimeValue;

@Operational(opcode=OpCode.CREATE_SESSION)
public abstract class ConnectMessage<T extends Record & Records.ConnectHolder> extends IOperationalRecord<T>
        implements Operation.Action, Message, Records.ConnectHolder {

    public static abstract class Request extends
            ConnectMessage<IConnectRequest> implements
            Operation.Request, Message.ClientSessionMessage {

        public static abstract class RequestsFactory implements Factory<ConnectMessage.Request> {
            protected final Reference<Long> lastZxid;
        
            protected RequestsFactory(
                    Reference<Long> lastZxid) {
                this.lastZxid = lastZxid;
            }
        }

        public static Request decode(ByteBuf input) throws IOException {
            ByteBufInputArchive archive = new ByteBufInputArchive(input);
            IConnectRequest record = new IConnectRequest();
            record.deserialize(archive, Records.CONNECT_TAG);
            boolean readOnly = false;
            boolean wraps = false;
            try {
                readOnly = archive.readBool("readOnly");
            } catch (IOException e) {
                wraps = true;
            }
            Request out = Request.newInstance(record, readOnly, wraps);
            return out;
        }

        public static Request newInstance(IConnectRequest record, boolean readOnly,
                boolean wraps) {
            return NewRequest.newInstance(record, readOnly, wraps);
        }

        protected Request(IConnectRequest record) {
            super(record);
        }
        
        protected Request(IConnectRequest record, boolean readOnly, boolean wraps) {
            super(record, readOnly, wraps);
        }
        
        public long getLastZxidSeen() {
            return get().getLastZxidSeen();
        }
        
        public static class NewRequest extends Request {

            public static class NewSessionRequestsFactory extends RequestsFactory {
                public static NewSessionRequestsFactory newInstance(
                        Reference<Long> lastZxid,
                        TimeValue timeOut) {
                    return new NewSessionRequestsFactory(lastZxid, timeOut);
                }
                
                protected final TimeValue timeOut;
                
                protected NewSessionRequestsFactory(
                        Reference<Long> lastZxid,
                        TimeValue timeOut) {
                    super(lastZxid);
                    this.timeOut = timeOut;
                }
            
                @Override
                public ConnectMessage.Request get() {
                    return ConnectMessage.Request.NewRequest.newInstance(timeOut, lastZxid.get());
                }
            }
            
            public static NewSessionRequestsFactory factory(
                    Reference<Long> lastZxid,
                    TimeValue timeOut) {
                return NewSessionRequestsFactory.newInstance(lastZxid, timeOut);
            }

            public static IConnectRequest newRecord() {
                return toRecord(0, 0L);
            }

            public static IConnectRequest toRecord(TimeValue timeOut, long lastZxid) {
                return toRecord(timeOut.value(TIMEOUT_UNIT).intValue(), lastZxid);
            }

            public static IConnectRequest toRecord(int timeOutMillis, long lastZxid) {
                IConnectRequest record = new IConnectRequest(
                        Records.PROTOCOL_VERSION,
                        lastZxid,
                        timeOutMillis,
                        Session.UNINITIALIZED_ID,
                        Parameters.NO_PASSWORD);
                return record;
            }
            
            public static Request newInstance() {
                return newInstance(newRecord());
            }

            public static Request newInstance(TimeValue timeOut, long lastZxid) {
                return newInstance(toRecord(timeOut, lastZxid));
            }
            
            public static Request newInstance(IConnectRequest record) {
                return newInstance(record, false, false);
            }

            public static Request newInstance(IConnectRequest record, boolean readOnly, boolean wraps) {
                if (record.getSessionId() != Session.UNINITIALIZED_ID) {
                    return RenewRequest.newInstance(record, readOnly, wraps);
                } else {
                    return new NewRequest(record, readOnly, wraps);
                }
            }

            private NewRequest(IConnectRequest record, boolean readOnly, boolean wraps) {
                super(record, readOnly, wraps);
            }
        }
        
        public static class RenewRequest extends Request {

            public static class RenewSessionRequestsFactory extends RequestsFactory {
                public static RenewSessionRequestsFactory newInstance(
                        Reference<Long> lastZxid,
                        Session session) {
                    return new RenewSessionRequestsFactory(lastZxid, session);
                }
                
                protected final Session session;
            
                protected RenewSessionRequestsFactory(
                        Reference<Long> lastZxid,
                        Session session) {
                    super(lastZxid);
                    checkArgument(session.initialized());
                    this.session = session;
                }
                
                @Override
                public ConnectMessage.Request get() {
                    return ConnectMessage.Request.RenewRequest.newInstance(session, lastZxid.get());
                }
            }
            
            public static RenewSessionRequestsFactory factory(
                    Reference<Long> lastZxid,
                    Session session) {
                return RenewSessionRequestsFactory.newInstance(lastZxid, session);
            }

            public static IConnectRequest toRecord(Session session) {
                return toRecord(session, 0L);
            }

            public static IConnectRequest toRecord(Session session, long lastZxid) {
                IConnectRequest record = new IConnectRequest(
                        Records.PROTOCOL_VERSION, 
                        lastZxid,
                        session.parameters().timeOut().value(TIMEOUT_UNIT).intValue(),
                        session.id(),
                        session.parameters().password());
                return record;
            }
            
            public static RenewRequest newInstance(Session session) {
                return newInstance(toRecord(session));
            }

            public static RenewRequest newInstance(Session session, long lastZxid) {
                return newInstance(toRecord(session, lastZxid));
            }

            public static RenewRequest newInstance(IConnectRequest record) {
                return newInstance(record, false, false);
            }

            public static RenewRequest newInstance(IConnectRequest record, boolean readOnly, boolean wraps) {
                return new RenewRequest(record, readOnly, wraps);
            }

            private RenewRequest(IConnectRequest record, boolean readOnly, boolean wraps) {
                super(record, readOnly, wraps);
            }
        }
    }

    public static abstract class Response extends
            ConnectMessage<IConnectResponse> implements
            Operation.Response, Message.ServerSessionMessage {

        public static Response newInstance(IConnectResponse record, boolean readOnly,
                boolean wraps) {
            return Valid.newInstance(record, readOnly, wraps);
        }

        public static Response decode(ByteBuf input) throws IOException {
            ByteBufInputArchive archive = new ByteBufInputArchive(input);
            IConnectResponse record = new IConnectResponse();
            record.deserialize(archive, Records.CONNECT_TAG);
            boolean readOnly = false;
            boolean wraps = false;
            try {
                readOnly = archive.readBool("readOnly");
            } catch (IOException e) {
                wraps = true;
            }
            Response out = Response.newInstance(record, readOnly, wraps);
            return out;
        }

        protected Response(IConnectResponse record) {
            super(record);
        }

        protected Response(IConnectResponse record, boolean readOnly, boolean wraps) {
            super(record, readOnly, wraps);
        }

        public static class Valid extends Response implements Operation.Response {

            public static IConnectResponse toRecord(Session session) {
                IConnectResponse record = new IConnectResponse(
                        Records.PROTOCOL_VERSION, 
                        session.parameters().timeOut().value(TIMEOUT_UNIT).intValue(),
                        session.id(),
                        session.parameters().password());
                return record;
            }
            
            public static Response newInstance(Session session) {
                return newInstance(session, false, false);
            }

            public static Response newInstance(Session session, boolean readOnly, boolean wraps) {
                return newInstance(toRecord(session), readOnly, wraps);
            }

            public static Response newInstance(IConnectResponse record) {
                return newInstance(record, false, false);
            }

            public static Response newInstance(IConnectResponse record, boolean readOnly,
                    boolean wraps) {
                if (record.getSessionId() == Session.UNINITIALIZED_ID) {
                    return Invalid.newInstance(readOnly, wraps);
                } else {
                    return new Valid(record, readOnly, wraps);
                }
            }

            private Valid(IConnectResponse record, boolean readOnly, boolean wraps) {
                super(record, readOnly, wraps);
            }
        }

        public static class Invalid extends Response {

            protected final static IConnectResponse RECORD = 
                    new IConnectResponse(Records.PROTOCOL_VERSION, 0, Session.UNINITIALIZED_ID, Parameters.NO_PASSWORD);

            public static Invalid newInstance() {
                return newInstance(false, false);
            }

            public static Invalid newInstance(boolean readOnly, boolean wraps) {
                return new Invalid(readOnly, wraps);
            }

            private Invalid(boolean readOnly, boolean wraps) {
                super(RECORD, readOnly, wraps);
            }
            
            @Override
            public Session toSession() {
                return Session.uninitialized();
            }
            
            @Override
            public Session.Parameters toParameters() {
                return Session.Parameters.uninitialized();
            }
        }
    }

    protected static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
    
    protected final boolean readOnly;
    protected final boolean wraps;

    protected ConnectMessage(T record) {
        this(record, false, false);
    }

    protected ConnectMessage(T record, boolean readOnly, boolean wraps) {
        super(record);
        this.readOnly = readOnly;
        this.wraps = wraps;
    }

    public boolean getReadOnly() {
        return readOnly;
    }

    public boolean getWraps() {
        return wraps;
    }
    
    @Override
    public int getProtocolVersion() {
        return get().getProtocolVersion();
    }

    @Override
    public int getTimeOut() {
        return get().getTimeOut();
    }

    @Override
    public long getSessionId() {
        return get().getSessionId();
    }

    @Override
    public byte[] getPasswd() {
        return get().getPasswd();
    }
    
    public Session toSession() {
        return Session.create(getSessionId(), toParameters());
    }
    
    public Session.Parameters toParameters() {
        return Session.Parameters.create(getTimeOut(), getPasswd());
    }
    
    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        get().serialize(archive, tag);
        if (!getWraps()) {
            archive.writeBool(getReadOnly(), "readOnly");
        }
    }
    
    @Override
    public void encode(ByteBuf output) throws IOException {
        ByteBufOutputArchive archive = new ByteBufOutputArchive(output);
        serialize(archive, Records.CONNECT_TAG);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("record", get())
                .add("readOnly", getReadOnly()).add("wraps", getWraps()).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ConnectMessage<?> other = (ConnectMessage<?>) obj;
        return Objects.equal(get(), other.get())
                && Objects.equal(getReadOnly(), other.getReadOnly())
                && Objects.equal(getWraps(), other.getWraps());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(get(), getReadOnly(), getWraps());
    }
}
