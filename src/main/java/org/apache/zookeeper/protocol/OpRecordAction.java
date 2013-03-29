package org.apache.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.jute.Record;

import com.google.common.base.Objects;

public abstract class OpRecordAction<T extends Record> implements Operation.Action, Encodable, Decodable {

    public static class Request<T extends Record> extends OpRecordAction<T> implements Operation.RequestValue<T> {
    
        public static <T extends Record> Request<T> create(T record) {
            return new Request<T>(record);
        }
    
        public static <T extends Record> Request<T> create(Operation operation) {
            return new Request<T>(Records.Requests.<T>create(operation));
        }
    
        public Request(T record) {
            super(record);
        }
    
        @Override
        public OutputStream encode(OutputStream stream) throws IOException {
            checkState(record() != null);
            return Records.Requests.serialize(record(), stream);
        }
    
        @Override
        public Request<T> decode(InputStream stream) throws IOException {
            checkState(record() != null);
            Records.Requests.<T>deserialize(record(), stream);
            return this;
        }
    
        @Override
        public T request() {
            return record();
        }
    }

    public static class Response<T extends Record> extends OpRecordAction<T> implements Operation.ResponseValue<T> {
    
        public static <T extends Record> Response<T> create(T record) {
            return new Response<T>(record);
        }
    
        public static <T extends Record> Response<T> create(Operation operation) {
            return new Response<T>(Records.Responses.<T>create(operation));
        }
    
        public Response(T record) {
            super(record);
        }
    
        @Override
        public OutputStream encode(OutputStream stream) throws IOException {
            checkState(record() != null);
            return Records.Responses.serialize(record(), stream);
        }
    
        @Override
        public Response<T> decode(InputStream stream) throws IOException {
            checkState(record() != null);
            Records.Responses.<T>deserialize(record(), stream);
            return this;
        }
    
        @Override
        public T response() {
            return record();
        }
    }

    protected T record;

    protected OpRecordAction() {
        this(null);
    }
    
    protected OpRecordAction(T record) {
        this.record = record;
    }
    
    @Override
    public Operation operation() {
        return Records.recordToOperation(record());
    }
    
    public T record() {
        return record;
    }
    
    public OpRecordAction<T> setRecord(T record) {
        this.record = record;
        return this;
    }

    @Override
    public OutputStream encode(OutputStream stream) throws IOException {
        checkState(record() != null);
        return Records.serialize(record(), stream);
    }

    @Override
    public OpRecordAction<T> decode(InputStream stream)
            throws IOException {
        checkState(record() != null);
        Records.deserialize(record(), stream);
        return this;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("record", record())
                .toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(record());
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
        OpRecordAction<T> other = (OpRecordAction<T>) obj;
        return Objects.equal(record(), other.record());
    }
}
