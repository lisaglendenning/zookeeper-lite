package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.jute.OutputArchive;
import org.apache.jute.Record;

import com.google.common.base.Objects;

import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ByteBufOutputArchive;
import edu.uw.zookeeper.protocol.proto.IConnectRequest;
import edu.uw.zookeeper.protocol.proto.IConnectResponse;
import edu.uw.zookeeper.protocol.proto.IOperationalRecord;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Operational;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.ZxidReference;

@Operational(value=OpCode.CREATE_SESSION)
public abstract class ConnectMessage<T extends Record & Records.ConnectGetter> extends IOperationalRecord<T>
        implements Message.Session, Records.ConnectGetter {

    public static abstract class Request extends
            ConnectMessage<IConnectRequest> implements
            Records.Request, Message.ClientSession {

        public static RequestsFactory factory(
                TimeValue timeOut,
                ZxidReference lastZxid) {
            return new RequestsFactory(timeOut, lastZxid);
        }
        
        public static class RequestsFactory extends Pair<TimeValue, ZxidReference> implements DefaultsFactory<edu.uw.zookeeper.Session, ConnectMessage.Request> {
            
            public RequestsFactory(
                    TimeValue timeOut,
                    ZxidReference lastZxid) {
                super(timeOut, lastZxid);
            }

            @Override
            public ConnectMessage.Request get() {
                return NewRequest.newInstance(first(), second().get());
            }

            @Override
            public ConnectMessage.Request get(edu.uw.zookeeper.Session session) {
                return RenewRequest.newInstance(session, second().get());
            }
        }

        public static ConnectMessage.Request decode(ByteBuf input) throws IOException {
            ByteBufInputArchive archive = new ByteBufInputArchive(input);
            IConnectRequest record = new IConnectRequest();
            record.deserialize(archive, Records.CONNECT_TAG);
            boolean readOnly = false;
            boolean legacy = false;
            try {
                readOnly = archive.readBool("readOnly");
            } catch (IOException e) {
                legacy = true;
            }
            ConnectMessage.Request out = ConnectMessage.Request.newInstance(record, readOnly, legacy);
            return out;
        }

        public static ConnectMessage.Request newInstance(IConnectRequest record, boolean readOnly,
                boolean legacy) {
            return NewRequest.newInstance(record, readOnly, legacy);
        }

        protected Request(IConnectRequest record) {
            super(record);
        }
        
        protected Request(IConnectRequest record, boolean readOnly, boolean legacy) {
            super(record, readOnly, legacy);
        }
        
        public long getLastZxidSeen() {
            return record.getLastZxidSeen();
        }
        
        public static class NewRequest extends ConnectMessage.Request {

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
                        edu.uw.zookeeper.Session.UNINITIALIZED_ID,
                        edu.uw.zookeeper.Session.Parameters.NO_PASSWORD);
                return record;
            }
            
            public static ConnectMessage.Request newInstance() {
                return newInstance(newRecord());
            }

            public static ConnectMessage.Request newInstance(TimeValue timeOut, long lastZxid) {
                return newInstance(toRecord(timeOut, lastZxid));
            }
            
            public static ConnectMessage.Request newInstance(IConnectRequest record) {
                return newInstance(record, false, false);
            }

            public static ConnectMessage.Request newInstance(IConnectRequest record, boolean readOnly, boolean legacy) {
                if (record.getSessionId() != edu.uw.zookeeper.Session.UNINITIALIZED_ID) {
                    return RenewRequest.newInstance(record, readOnly, legacy);
                } else {
                    return new NewRequest(record, readOnly, legacy);
                }
            }

            private NewRequest(IConnectRequest record, boolean readOnly, boolean legacy) {
                super(record, readOnly, legacy);
            }
        }
        
        public static class RenewRequest extends ConnectMessage.Request {

            public static IConnectRequest toRecord(edu.uw.zookeeper.Session session) {
                return toRecord(session, 0L);
            }

            public static IConnectRequest toRecord(edu.uw.zookeeper.Session session, long lastZxid) {
                IConnectRequest record = new IConnectRequest(
                        Records.PROTOCOL_VERSION, 
                        lastZxid,
                        session.parameters().timeOut().value(TIMEOUT_UNIT).intValue(),
                        session.id(),
                        session.parameters().password());
                return record;
            }
            
            public static RenewRequest newInstance(edu.uw.zookeeper.Session session) {
                return newInstance(toRecord(session));
            }

            public static RenewRequest newInstance(edu.uw.zookeeper.Session session, long lastZxid) {
                return newInstance(toRecord(session, lastZxid));
            }

            public static RenewRequest newInstance(IConnectRequest record) {
                return newInstance(record, false, false);
            }

            public static RenewRequest newInstance(IConnectRequest record, boolean readOnly, boolean legacy) {
                return new RenewRequest(record, readOnly, legacy);
            }

            private RenewRequest(IConnectRequest record, boolean readOnly, boolean legacy) {
                super(record, readOnly, legacy);
            }
        }
    }

    public static abstract class Response extends
            ConnectMessage<IConnectResponse> implements
            Records.Response, Message.ServerSession {

        public static ConnectMessage.Response newInstance(IConnectResponse record, boolean readOnly,
                boolean legacy) {
            return Valid.newInstance(record, readOnly, legacy);
        }

        public static ConnectMessage.Response decode(ByteBuf input) throws IOException {
            ByteBufInputArchive archive = new ByteBufInputArchive(input);
            IConnectResponse record = new IConnectResponse();
            record.deserialize(archive, Records.CONNECT_TAG);
            boolean readOnly = false;
            boolean legacy = false;
            try {
                readOnly = archive.readBool("readOnly");
            } catch (IOException e) {
                legacy = true;
            }
            ConnectMessage.Response out = ConnectMessage.Response.newInstance(record, readOnly, legacy);
            return out;
        }

        protected Response(IConnectResponse record) {
            super(record);
        }

        protected Response(IConnectResponse record, boolean readOnly, boolean legacy) {
            super(record, readOnly, legacy);
        }

        public static class Valid extends ConnectMessage.Response implements Operation.Response {

            public static IConnectResponse toRecord(edu.uw.zookeeper.Session session) {
                IConnectResponse record = new IConnectResponse(
                        Records.PROTOCOL_VERSION, 
                        session.parameters().timeOut().value(TIMEOUT_UNIT).intValue(),
                        session.id(),
                        session.parameters().password());
                return record;
            }
            
            public static ConnectMessage.Response newInstance(edu.uw.zookeeper.Session session) {
                return newInstance(session, false, false);
            }

            public static ConnectMessage.Response newInstance(edu.uw.zookeeper.Session session, boolean readOnly, boolean legacy) {
                return newInstance(toRecord(session), readOnly, legacy);
            }

            public static ConnectMessage.Response newInstance(IConnectResponse record) {
                return newInstance(record, false, false);
            }

            public static ConnectMessage.Response newInstance(IConnectResponse record, boolean readOnly,
                    boolean legacy) {
                if (record.getSessionId() == edu.uw.zookeeper.Session.UNINITIALIZED_ID) {
                    return Invalid.newInstance(readOnly, legacy);
                } else {
                    return new Valid(record, readOnly, legacy);
                }
            }

            private Valid(IConnectResponse record, boolean readOnly, boolean legacy) {
                super(record, readOnly, legacy);
            }
        }

        public static class Invalid extends ConnectMessage.Response {

            protected final static IConnectResponse RECORD = 
                    new IConnectResponse(Records.PROTOCOL_VERSION, 0, edu.uw.zookeeper.Session.UNINITIALIZED_ID, edu.uw.zookeeper.Session.Parameters.NO_PASSWORD);

            public static Invalid newInstance() {
                return newInstance(false, false);
            }

            public static Invalid newInstance(boolean readOnly, boolean legacy) {
                return new Invalid(readOnly, legacy);
            }

            private Invalid(boolean readOnly, boolean legacy) {
                super(RECORD, readOnly, legacy);
            }
            
            @Override
            public edu.uw.zookeeper.Session toSession() {
                return edu.uw.zookeeper.Session.uninitialized();
            }
            
            @Override
            public edu.uw.zookeeper.Session.Parameters toParameters() {
                return edu.uw.zookeeper.Session.Parameters.uninitialized();
            }
        }
    }

    protected static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
    
    protected final boolean readOnly;
    protected final boolean legacy;

    protected ConnectMessage(T record) {
        this(record, false, false);
    }

    protected ConnectMessage(T record, boolean readOnly, boolean legacy) {
        super(record);
        this.readOnly = readOnly;
        this.legacy = legacy;
    }

    public boolean legacy() {
        return legacy;
    }

    public boolean getReadOnly() {
        return readOnly;
    }

    @Override
    public int getProtocolVersion() {
        return record.getProtocolVersion();
    }

    @Override
    public int getTimeOut() {
        return record.getTimeOut();
    }

    @Override
    public long getSessionId() {
        return record.getSessionId();
    }

    @Override
    public byte[] getPasswd() {
        return record.getPasswd();
    }
    
    public edu.uw.zookeeper.Session toSession() {
        return edu.uw.zookeeper.Session.create(getSessionId(), toParameters());
    }
    
    public edu.uw.zookeeper.Session.Parameters toParameters() {
        return edu.uw.zookeeper.Session.Parameters.create(getTimeOut(), getPasswd());
    }
    
    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        record.serialize(archive, tag);
        if (!legacy) {
            archive.writeBool(readOnly, "readOnly");
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
                .add("record", record)
                .add("readOnly", readOnly)
                .add("legacy", legacy).toString();
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
        return Objects.equal(record, other.record)
                && Objects.equal(readOnly, other.readOnly)
                && Objects.equal(legacy, other.legacy);
    }
}
