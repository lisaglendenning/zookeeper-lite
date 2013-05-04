package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;

import com.google.common.base.Objects;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.Session.Parameters;
import edu.uw.zookeeper.protocol.proto.IConnectRequest;
import edu.uw.zookeeper.protocol.proto.IConnectResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.TimeValue;

public abstract class OpCreateSession<T extends Records.ConnectRecord>
        extends OpRecord<T> {

    public static abstract class Request extends
            OpCreateSession<IConnectRequest> implements
            Operation.Request, Message.ClientSessionMessage {

        public static Request decode(InputStream input) throws IOException {
            IConnectRequest record = Records.decode(newRecord(), input);
            boolean readOnly = false;
            boolean wraps = false;
            BinaryInputArchive bia = BinaryInputArchive.getArchive(input);
            try {
                readOnly = bia.readBool("readOnly");
            } catch (IOException e) {
                wraps = true;
            }
            Request out = Request.newInstance(record, readOnly, wraps);
            return out;
        }
        
        public static IConnectRequest newRecord() {
            IConnectRequest request = (IConnectRequest) Records.Requests.getInstance().get(OPCODE);
            request.setProtocolVersion(Records.PROTOCOL_VERSION);
            return request;
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
        
        public static class NewRequest extends Request {

            public static IConnectRequest newRecord() {
                return newRecord(0, 0L);
            }

            public static IConnectRequest newRecord(TimeValue timeOut, long lastZxid) {
                return newRecord(timeOut.value(TIMEOUT_UNIT).intValue(), lastZxid);
            }

            public static IConnectRequest newRecord(int timeOutMillis, long lastZxid) {
                IConnectRequest record = Request.newRecord();
                record.setSessionId(Session.UNINITIALIZED_ID);
                record.setTimeOut(timeOutMillis);
                record.setPasswd(Parameters.NO_PASSWORD);
                record.setLastZxidSeen(lastZxid);
                return record;
            }
            
            public static Request newInstance() {
                return newInstance(newRecord());
            }

            public static Request newInstance(TimeValue timeOut, long lastZxid) {
                return newInstance(newRecord(timeOut, lastZxid));
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

            public static IConnectRequest newRecord(Session session) {
                return newRecord(session, 0L);
            }

            public static IConnectRequest newRecord(Session session, long lastZxid) {
                IConnectRequest record = Request.newRecord();
                record.setSessionId(session.id());
                record.setTimeOut(session.parameters().timeOut().value(TIMEOUT_UNIT).intValue());
                record.setPasswd(session.parameters().password());
                record.setLastZxidSeen(lastZxid);
                return record;
            }
            
            public static RenewRequest newInstance(Session session) {
                return newInstance(newRecord(session));
            }

            public static RenewRequest newInstance(Session session, long lastZxid) {
                return newInstance(newRecord(session, lastZxid));
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
            OpCreateSession<IConnectResponse> implements
            Operation.Response, Message.ServerSessionMessage {

        public static Response newInstance(IConnectResponse record, boolean readOnly,
                boolean wraps) {
            return Valid.newInstance(record, readOnly, wraps);
        }

        public static Response decode(InputStream input) throws IOException {
            IConnectResponse record = Records.decode(newRecord(), input);
            boolean readOnly = false;
            boolean wraps = false;
            BinaryInputArchive bia = BinaryInputArchive.getArchive(input);
            try {
                readOnly = bia.readBool("readOnly");
            } catch (IOException e) {
                wraps = true;
            }
            Response out = Response.newInstance(record, readOnly, wraps);
            return out;
        }

        public static IConnectResponse newRecord() {
            IConnectResponse response = (IConnectResponse) Records.Responses.getInstance().get(OPCODE);
            response.setProtocolVersion(Records.PROTOCOL_VERSION);
            return response;
        }

        protected Response(IConnectResponse record) {
            super(record);
        }

        protected Response(IConnectResponse record, boolean readOnly, boolean wraps) {
            super(record, readOnly, wraps);
        }

        public static class Valid extends Response implements Operation.Response {

            public static IConnectResponse newRecord(Session session) {
                IConnectResponse record = newRecord();
                record.setSessionId(session.id());
                record.setTimeOut(session.parameters().timeOut().value(TIMEOUT_UNIT).intValue());
                record.setPasswd(session.parameters().password());
                return record;
            }
            
            public static Response newInstance(Session session) {
                return newInstance(session, false, false);
            }

            public static Response newInstance(Session session, boolean readOnly, boolean wraps) {
                return newInstance(newRecord(session), readOnly, wraps);
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

            public static IConnectResponse newRecord() {
                IConnectResponse record = Response.newRecord();
                record.setSessionId(Session.UNINITIALIZED_ID);
                record.setTimeOut(0);
                record.setPasswd(Parameters.NO_PASSWORD);
                return record;
            }

            public static Invalid newInstance() {
                return newInstance(false, false);
            }

            public static Invalid newInstance(boolean readOnly, boolean wraps) {
                return new Invalid(readOnly, wraps);
            }

            private Invalid(boolean readOnly, boolean wraps) {
                super(newRecord(), readOnly, wraps);
            }
            
            @Override
            public Session toSession() {
                return Session.uninitialized();
            }
            
            @Override
            public Session.Parameters toParameters(){
                return Session.Parameters.uninitialized();
            }
        }
    }

    private static final OpCode OPCODE = OpCode.CREATE_SESSION;
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
    
    private final boolean readOnly;
    private final boolean wraps;

    protected OpCreateSession(T record) {
        this(record, false, false);
    }

    protected OpCreateSession(T record, boolean readOnly, boolean wraps) {
        super(record);
        this.readOnly = readOnly;
        this.wraps = wraps;
    }

    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    public boolean readOnly() {
        return readOnly;
    }

    public boolean wraps() {
        return wraps;
    }

    public Session toSession() {
        return Session.create(asRecord().getSessionId(),
                toParameters());
    }
    
    public Session.Parameters toParameters() {
        Records.ConnectRecord record = asRecord();
        return Session.Parameters.create(record.getTimeOut(),
                record.getPasswd());
    }
    
    @Override
    public ByteBuf encode(ByteBufAllocator output) throws IOException {
        ByteBuf out = checkNotNull(output).buffer();
        OutputStream outs = new ByteBufOutputStream(out);
        Records.encode(asRecord(), outs);
        if (!wraps()) {
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(outs);
            boa.writeBool(readOnly(), "readOnly");
        }
        return out;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("opcode", opcode())
                .add("record", Records.toString(asRecord()))
                .add("readOnly", readOnly()).add("wraps", wraps()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(asRecord(), readOnly(), wraps());
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
        @SuppressWarnings("unchecked")
        OpCreateSession<T> other = (OpCreateSession<T>) obj;
        return Objects.equal(asRecord(), other.asRecord())
                && Objects.equal(readOnly(), other.readOnly())
                && Objects.equal(wraps(), other.wraps());
    }
}
