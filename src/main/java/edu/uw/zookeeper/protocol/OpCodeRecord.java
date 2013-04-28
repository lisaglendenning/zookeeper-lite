package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jute.Record;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.WatcherEvent;

import com.google.common.base.Objects;


public abstract class OpCodeRecord<T extends Record>
        implements Operation.Action, Operation.RecordHolder<T>, Encodable {

    public static class Request<T extends Record> extends OpCodeRecord<T> 
            implements Operation.Request {

        public static Operation.Request decode(OpCode opcode, InputStream input) throws IOException {
            return create(Records.Requests.decode(opcode, input));
        }
        
        @SuppressWarnings("unchecked")
        public static <T extends Record> T createRecord(OpCode opcode) {
            switch (opcode) {
            case AUTH:
                return (T) OpAuth.Request.createRecord();
            default:
                return Records.Requests.<T>create(opcode);
            }
        }
         
        public static Operation.Request create(OpCode opcode) {
            switch (opcode) {
            case AUTH:
                return OpAuth.Request.create();
            default:
                return create(createRecord(opcode));
            }
        }

        public static <T extends Record> Operation.Request create(T record) {
            if (record instanceof AuthPacket) {
                return OpAuth.Request.create((AuthPacket)record);
            } else {
                return new Request<T>(record);
            }
        }

        protected Request(T record) {
            super(record);
        }

        @Override
        public ByteBuf encode(ByteBufAllocator output) throws IOException {
            ByteBuf out = checkNotNull(output).buffer();
            Records.Requests.encode(asRecord(), new ByteBufOutputStream(out));
            return out;
        }
    }

    public static class Response<T extends Record> extends OpCodeRecord<T> 
            implements Operation.Response {

        public static Operation.Response decode(OpCode opcode, InputStream input) throws IOException {
            return create(Records.Responses.decode(opcode, input));
        }
        
        @SuppressWarnings("unchecked")
        public static <T extends Record> T createRecord(OpCode opcode) {
            switch (opcode) {
            case NOTIFICATION:
                return (T) OpNotification.Response.createRecord();
            default:
                return Records.Responses.<T>create(opcode);
            }
        }

        public static Operation.Response create(OpCode opcode) {
            switch (opcode) {
            case NOTIFICATION:
                return OpNotification.Response.create();
            default:
                return create(createRecord(opcode));
            }
        }

        public static <T extends Record> Operation.Response create(T record) {
            if (record instanceof WatcherEvent) {
                return OpNotification.Response.create((WatcherEvent)record);
            } else {
                return new Response<T>(record);
            }
        }

        protected Response(T record) {
            super(record);
        }

        @Override
        public ByteBuf encode(ByteBufAllocator output) throws IOException {
            ByteBuf out = checkNotNull(output).buffer();
            Records.Responses.encode(asRecord(), new ByteBufOutputStream(out));
            return out;
        }
    }

    private final T record;

    protected OpCodeRecord(T record) {
        this.record = checkNotNull(record);
    }

    @Override
    public OpCode opcode() {
        return Records.getOpCode(asRecord());
    }

    public T asRecord() {
        return record;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("opcode", opcode())
                .add("record", Records.toString(asRecord())).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(asRecord());
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
        OpCodeRecord<T> other = (OpCodeRecord<T>) obj;
        return Objects.equal(asRecord(), other.asRecord());
    }
}
