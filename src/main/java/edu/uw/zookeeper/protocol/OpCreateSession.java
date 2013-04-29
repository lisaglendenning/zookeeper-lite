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
import org.apache.jute.Record;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;

import com.google.common.base.Objects;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.Session.Parameters;
import edu.uw.zookeeper.util.TimeValue;

public abstract class OpCreateSession<T extends Record>
        extends OpCodeRecord<T> {

    public static abstract class Request extends
            OpCreateSession<ConnectRequest> implements
            Operation.Request, Message.ClientSessionMessage {

        public static Request decode(InputStream input) throws IOException {
            ConnectRequest record = Records.decode(createRecord(), input, Records.CONNECT_TAG);
            boolean readOnly = false;
            boolean wraps = false;
            BinaryInputArchive bia = BinaryInputArchive.getArchive(input);
            try {
                readOnly = bia.readBool("readOnly");
            } catch (IOException e) {
                wraps = true;
            }
            Request out = Request.create(record, readOnly, wraps);
            return out;
        }
        
        public static ConnectRequest createRecord() {
            ConnectRequest request = OpCodeRecord.Request.createRecord(OPCODE);
            request.setProtocolVersion(Records.PROTOCOL_VERSION);
            return request;
        }

        public static Request create(ConnectRequest record, boolean readOnly,
                boolean wraps) {
            return NewRequest.create(record, readOnly, wraps);
        }

        protected Request(ConnectRequest record) {
            super(record);
        }
        
        protected Request(ConnectRequest record, boolean readOnly, boolean wraps) {
            super(record, readOnly, wraps);
        }
        
        public static class NewRequest extends Request {

            public static ConnectRequest createRecord() {
                return createRecord(0, 0L);
            }

            public static ConnectRequest createRecord(TimeValue timeOut, long lastZxid) {
                return createRecord(timeOut.value(TIMEOUT_UNIT).intValue(), lastZxid);
            }

            public static ConnectRequest createRecord(int timeOutMillis, long lastZxid) {
                ConnectRequest record = Request.createRecord();
                record.setSessionId(Session.UNINITIALIZED_ID);
                record.setTimeOut(timeOutMillis);
                record.setPasswd(Parameters.NO_PASSWORD);
                record.setLastZxidSeen(lastZxid);
                return record;
            }
            
            public static Request create() {
                return create(createRecord());
            }

            public static Request create(TimeValue timeOut, long lastZxid) {
                return create(createRecord(timeOut, lastZxid));
            }
            
            public static Request create(ConnectRequest record) {
                return create(record, false, false);
            }

            public static Request create(ConnectRequest record, boolean readOnly, boolean wraps) {
                if (record.getSessionId() != Session.UNINITIALIZED_ID) {
                    return RenewRequest.create(record, readOnly, wraps);
                } else {
                    return new NewRequest(record, readOnly, wraps);
                }
            }

            private NewRequest(ConnectRequest record, boolean readOnly, boolean wraps) {
                super(record, readOnly, wraps);
            }
            
            public Session toSession() {
                return Session.uninitialized();
            }
        }
        
        public static class RenewRequest extends Request {

            public static ConnectRequest createRecord(Session session) {
                return createRecord(session, 0L);
            }

            public static ConnectRequest createRecord(Session session, long lastZxid) {
                ConnectRequest record = Request.createRecord();
                record.setSessionId(session.id());
                record.setTimeOut(session.parameters().timeOut().value(TIMEOUT_UNIT).intValue());
                record.setPasswd(session.parameters().password());
                record.setLastZxidSeen(lastZxid);
                return record;
            }
            
            public static RenewRequest create(Session session) {
                return create(createRecord(session));
            }

            public static RenewRequest create(Session session, long lastZxid) {
                return create(createRecord(session, lastZxid));
            }

            public static RenewRequest create(ConnectRequest record) {
                return create(record, false, false);
            }

            public static RenewRequest create(ConnectRequest record, boolean readOnly, boolean wraps) {
                return new RenewRequest(record, readOnly, wraps);
            }

            private RenewRequest(ConnectRequest record, boolean readOnly, boolean wraps) {
                super(record, readOnly, wraps);
            }

            public Session.Parameters toParameters() {
                ConnectRequest record = asRecord();
                return Session.Parameters.create(record.getTimeOut(),
                        record.getPasswd());
            }
            
            public Session toSession() {
                return Session.create(asRecord().getSessionId(),
                        toParameters());
            }
        }
    }

    public static abstract class Response extends
            OpCreateSession<ConnectResponse> implements
            Operation.Response, Message.ServerSessionMessage {

        public static Response create(ConnectResponse record, boolean readOnly,
                boolean wraps) {
            return Valid.create(record, readOnly, wraps);
        }

        public static Response decode(InputStream input) throws IOException {
            ConnectResponse record = Records.decode(createRecord(), input, Records.CONNECT_TAG);
            boolean readOnly = false;
            boolean wraps = false;
            BinaryInputArchive bia = BinaryInputArchive.getArchive(input);
            try {
                readOnly = bia.readBool("readOnly");
            } catch (IOException e) {
                wraps = true;
            }
            Response out = Response.create(record, readOnly, wraps);
            return out;
        }

        public static ConnectResponse createRecord() {
            ConnectResponse response = OpCodeRecord.Response.createRecord(OPCODE);
            response.setProtocolVersion(Records.PROTOCOL_VERSION);
            return response;
        }

        protected Response(ConnectResponse record) {
            super(record);
        }

        protected Response(ConnectResponse record, boolean readOnly, boolean wraps) {
            super(record, readOnly, wraps);
        }

        public static class Valid extends Response implements Operation.Response {

            public static ConnectResponse createRecord(Session session) {
                ConnectResponse record = createRecord();
                record.setSessionId(session.id());
                record.setTimeOut(session.parameters().timeOut().value(TIMEOUT_UNIT).intValue());
                record.setPasswd(session.parameters().password());
                return record;
            }
            
            public static Response create(Session session) {
                return create(session, false, false);
            }

            public static Response create(Session session, boolean readOnly, boolean wraps) {
                return create(createRecord(session), readOnly, wraps);
            }

            public static Response create(ConnectResponse record) {
                return create(record, false, false);
            }

            public static Response create(ConnectResponse record, boolean readOnly,
                    boolean wraps) {
                if (record.getSessionId() == Session.UNINITIALIZED_ID) {
                    return Invalid.create(readOnly, wraps);
                } else {
                    return new Valid(record, readOnly, wraps);
                }
            }

            private Valid(ConnectResponse record, boolean readOnly, boolean wraps) {
                super(record, readOnly, wraps);
            }     

            public Session.Parameters toParameters() {
                ConnectResponse record = asRecord();
                return Session.Parameters.create(record.getTimeOut(),
                        record.getPasswd());
            }
            
            public Session toSession() {
                return Session.create(asRecord().getSessionId(),
                        toParameters());
            }
        }

        public static class Invalid extends Response {

            public static ConnectResponse createRecord() {
                ConnectResponse record = Response.createRecord();
                record.setSessionId(Session.UNINITIALIZED_ID);
                record.setTimeOut(0);
                record.setPasswd(Parameters.NO_PASSWORD);
                return record;
            }

            public static Invalid create() {
                return create(false, false);
            }

            public static Invalid create(boolean readOnly, boolean wraps) {
                return new Invalid(readOnly, wraps);
            }

            private Invalid(boolean readOnly, boolean wraps) {
                super(createRecord(), readOnly, wraps);
            }
            
            public Session toSession() {
                return Session.uninitialized();
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
    
    public abstract Session toSession();

    @Override
    public ByteBuf encode(ByteBufAllocator output) throws IOException {
        ByteBuf out = checkNotNull(output).buffer();
        OutputStream outs = new ByteBufOutputStream(out);
        Records.encode(asRecord(), outs, Records.CONNECT_TAG);
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
