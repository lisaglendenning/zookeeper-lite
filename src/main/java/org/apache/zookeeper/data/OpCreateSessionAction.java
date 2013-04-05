package org.apache.zookeeper.data;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.protocol.Records;
import org.apache.zookeeper.protocol.Records.Requests;
import org.apache.zookeeper.protocol.Records.Responses;

import com.google.common.base.Objects;

public abstract class OpCreateSessionAction<C extends OpCreateSessionAction<C,T>, T extends Record> extends OpRecordAction<T> {

    public static Operation OPERATION = Operation.CREATE_SESSION;
    
    public static class Request extends OpCreateSessionAction<Request, ConnectRequest> implements Operation.Request {
    
        public static ConnectRequest createRecord() {
            return Records.Requests.<ConnectRequest>create(OPERATION);
        }
        
        public static Request create() {
            return new Request();
        }
    
        public static Request create(ConnectRequest record, boolean readOnly,
                boolean wraps) {
            return new Request(record, readOnly, wraps);
        }
    
        public Request() {
            super(createRecord());
        }
    
        public Request(ConnectRequest record, boolean readOnly,
                boolean wraps) {
            super(record, readOnly, wraps);
        }
        
        public Request setRequest(ConnectRequest request) {
            setRecord(request);
            return this;
        }

        @Override
        public Request decode(InputStream stream) throws IOException  {
            if (record() == null) {
                setRecord(createRecord());
            }
            return super.decode(stream);
        }
    }

    public static class Response extends OpCreateSessionAction<Response, ConnectResponse> implements Operation.Response {

        public static ConnectResponse createRecord() {
            return Records.Responses.<ConnectResponse>create(OPERATION);
        }
        
        public static Response create() {
            return new Response();
        }

        public static Response create(ConnectResponse record, boolean readOnly,
                boolean wraps) {
            return new Response(record, readOnly, wraps);
        }
    
        public Response() {
            super(createRecord());
        }

        public Response(ConnectResponse record, boolean readOnly,
                boolean wraps) {
            super(record, readOnly, wraps);
        }
        
        public boolean isValid() {
            return (record() != null && record().getSessionId() != Session.UNINITIALIZED_ID);
        }
        
        public Response setResponse(ConnectResponse response) {
            setRecord(response);
            return this;
        }

        @Override
        public Response decode(InputStream stream) throws IOException  {
            if (record() == null) {
                setRecord(createRecord());
            }
            return super.decode(stream);
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
        if (! wraps()) {
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(stream);
            boa.writeBool(readOnly(), "readOnly");
        }
        return stream;
    }

    @Override
    @SuppressWarnings("unchecked")
    public C decode(InputStream stream) throws IOException  {
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
        return Objects.toStringHelper(this)
              .add("operation", operation())
              .add("record", Records.toString(record()))
              .add("readOnly", readOnly())
              .add("wraps", wraps())
              .toString();
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
        OpCreateSessionAction<C,T> other = (OpCreateSessionAction<C,T>) obj;
        return Objects.equal(record(), other.record()) 
                && Objects.equal(readOnly(), other.readOnly())
                && Objects.equal(wraps(), other.wraps());
    }
}
