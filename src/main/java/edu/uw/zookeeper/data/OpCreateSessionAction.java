package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import com.google.common.base.Objects;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionParameters;
import edu.uw.zookeeper.protocol.Records;

public abstract class OpCreateSessionAction<C extends OpCreateSessionAction<C, T>, T extends Record>
        extends OpRecordAction<T> {

    public static Operation OPERATION = Operation.CREATE_SESSION;

    public static class Request extends
            OpCreateSessionAction<Request, ConnectRequest> implements
            Operation.Request {

        public static ConnectRequest createRecord() {
            ConnectRequest request = Records.Requests
                    .<ConnectRequest> create(OPERATION);
            request.setProtocolVersion(Records.PROTOCOL_VERSION);
            return request;
        }

        public static Request create() {
            return new Request();
        }

        public static Request create(ConnectRequest record) {
            return new Request(record);
        }

        public static Request create(ConnectRequest record, boolean readOnly,
                boolean wraps) {
            return new Request(record, readOnly, wraps);
        }

        public Request() {
            this(createRecord());
        }

        public Request(ConnectRequest record) {
            super(record);
        }

        public Request(ConnectRequest record, boolean readOnly, boolean wraps) {
            super(record, readOnly, wraps);
        }

        public Request setRequest(ConnectRequest request) {
            setRecord(request);
            return this;
        }

        @Override
        public Request decode(InputStream stream) throws IOException {
            if (record() == null) {
                setRecord(createRecord());
            }
            return super.decode(stream);
        }
    }

    public static class Response extends
            OpCreateSessionAction<Response, ConnectResponse> implements
            Operation.Response {

        public static ConnectResponse createRecord() {
            ConnectResponse response = Records.Responses
                    .<ConnectResponse> create(OPERATION);
            response.setProtocolVersion(Records.PROTOCOL_VERSION);
            return response;
        }

        public static Response create() {
            return new Response();
        }

        public static Response create(ConnectResponse record) {
            return new Response(record);
        }

        public static Response create(ConnectResponse record, boolean readOnly,
                boolean wraps) {
            return new Response(record, readOnly, wraps);
        }

        public Response() {
            this(createRecord());
        }

        public Response(ConnectResponse record) {
            super(record);
        }

        public Response(ConnectResponse record, boolean readOnly, boolean wraps) {
            super(record, readOnly, wraps);
        }

        public Response setResponse(ConnectResponse response) {
            setRecord(response);
            return this;
        }

        @Override
        public Response decode(InputStream stream) throws IOException {
            if (record() == null) {
                setRecord(createRecord());
            }
            super.decode(stream);

            if (record().getSessionId() == Session.UNINITIALIZED_ID) {
                return InvalidResponse.create();
            }
            return this;
        }
    }

    public static class InvalidResponse extends Response implements
            Operation.Error {

        public static ConnectResponse createRecord() {
            ConnectResponse record = Response.createRecord();
            record.setSessionId(Session.UNINITIALIZED_ID);
            record.setTimeOut(0);
            record.setPasswd(SessionParameters.NO_PASSWORD);
            return record;
        }

        public static InvalidResponse create() {
            return new InvalidResponse();
        }

        public static InvalidResponse create(boolean readOnly, boolean wraps) {
            return new InvalidResponse(readOnly, wraps);
        }

        public InvalidResponse() {
            super(createRecord());
        }

        public InvalidResponse(boolean readOnly, boolean wraps) {
            super(createRecord(), readOnly, wraps);
        }

        public Response setResponse(ConnectResponse response) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Code error() {
            // TODO
            return KeeperException.Code.SESSIONEXPIRED;
        }
    }

    protected boolean readOnly;
    protected boolean wraps;

    protected OpCreateSessionAction(T record) {
        this(record, false, false);
    }

    protected OpCreateSessionAction(T record, boolean readOnly, boolean wraps) {
        super(record);
        this.readOnly = readOnly;
        this.wraps = wraps;
    }

    @Override
    public Operation operation() {
        return OPERATION;
    }

    public boolean readOnly() {
        return readOnly;
    }

    @SuppressWarnings("unchecked")
    public C setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return (C) this;
    }

    public boolean wraps() {
        return wraps;
    }

    @SuppressWarnings("unchecked")
    public C setWraps(boolean wraps) {
        this.wraps = wraps;
        return (C) this;
    }

    @Override
    public OutputStream encode(OutputStream stream) throws IOException {
        T record = record();
        checkState(record != null);
        stream = Records.serialize(record, stream, Records.CONNECT_TAG);
        if (!wraps()) {
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(stream);
            boa.writeBool(readOnly(), "readOnly");
        }
        return stream;
    }

    @Override
    @SuppressWarnings("unchecked")
    public C decode(InputStream stream) throws IOException {
        T record = record();
        checkState(record != null);
        setRecord(Records.deserialize(record, stream, Records.CONNECT_TAG));
        boolean readOnly = false;
        boolean wraps = false;
        BinaryInputArchive bia = BinaryInputArchive.getArchive(stream);
        try {
            readOnly = bia.readBool("readOnly");
        } catch (IOException e) {
            wraps = true;
        }
        setWraps(wraps);
        setReadOnly(readOnly);
        return (C) this;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("operation", operation())
                .add("record", Records.toString(record()))
                .add("readOnly", readOnly()).add("wraps", wraps()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(record(), readOnly(), wraps());
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
        OpCreateSessionAction<C, T> other = (OpCreateSessionAction<C, T>) obj;
        return Objects.equal(record(), other.record())
                && Objects.equal(readOnly(), other.readOnly())
                && Objects.equal(wraps(), other.wraps());
    }
}
