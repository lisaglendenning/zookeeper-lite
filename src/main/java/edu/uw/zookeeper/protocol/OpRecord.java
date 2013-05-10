package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Objects;

import edu.uw.zookeeper.data.Encodable;
import edu.uw.zookeeper.protocol.proto.IAuthRequest;
import edu.uw.zookeeper.protocol.proto.IPingRequest;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.Records;


public abstract class OpRecord<T extends Records.OperationRecord>
        implements Operation.Action, Operation.RecordHolder<T>, Encodable {

    public static class OpRequest<T extends Records.RequestRecord> extends OpRecord<T> 
            implements Operation.Request {

        public static OpRequest<?> decode(OpCode opcode, InputStream input) throws IOException {
            return newInstance(Records.Requests.decode(opcode, input));
        }
        
        @SuppressWarnings("unchecked")
        public static <T extends Records.RequestRecord> T newRecord(OpCode opcode) {
            switch (opcode) {
            case AUTH:
                return (T) OpAuth.OpAuthRequest.newRecord();
            case PING:
                return (T) OpPing.OpPingRequest.newRecord();
            default:
                return (T) Records.Requests.getInstance().get(opcode);
            }
        }
         
        @SuppressWarnings("unchecked")
        public static <T extends Records.RequestRecord> OpRequest<? extends T> newInstance(OpCode opcode) {
            switch (opcode) {
            case CREATE_SESSION:
                throw new IllegalArgumentException(opcode.toString());
            case AUTH:
                return (OpRequest<? extends T>) OpAuth.OpAuthRequest.newInstance();
            case PING:
                return (OpRequest<? extends T>) OpPing.OpPingRequest.newInstance();
            default:
                return (OpRequest<? extends T>) newInstance(newRecord(opcode));
            }
        }

        @SuppressWarnings("unchecked")
        public static <T extends Records.RequestRecord> OpRequest<? extends T> newInstance(T record) {
            switch (record.opcode()) {
            case CREATE_SESSION:
                throw new IllegalArgumentException(record.toString());
            case AUTH:
                return (OpRequest<T>) OpAuth.OpAuthRequest.newInstance((IAuthRequest)record);
            case PING:
                return (OpRequest<T>) OpPing.OpPingRequest.newInstance((IPingRequest)record);
            default:
                return new OpRequest<T>(record);
                
            }
        }

        protected OpRequest(T record) {
            super(record);
        }
    }

    public static class OpResponse<T extends Records.ResponseRecord> extends OpRecord<T> 
            implements Operation.Response {

        public static OpResponse<?> decode(OpCode opcode, InputStream input) throws IOException {
            return newInstance(Records.Responses.decode(opcode, input));
        }
        
        @SuppressWarnings("unchecked")
        public static <T extends Records.ResponseRecord> T newRecord(OpCode opcode) {
            switch (opcode) {
            case PING:
                return (T) OpPing.OpPingResponse.newRecord();
            case NOTIFICATION:
                return (T) OpNotification.OpNotificationResponse.newRecord();
            default:
                return (T) Records.Responses.getInstance().get(opcode);
            }
        }

        @SuppressWarnings("unchecked")
        public static <T extends Records.ResponseRecord> OpResponse<? extends T> newInstance(OpCode opcode) {
            switch (opcode) {
            case CREATE_SESSION:
                throw new IllegalArgumentException(opcode.toString());
            case PING:
                return (OpResponse<? extends T>) OpPing.OpPingResponse.newInstance();
            case NOTIFICATION:
                return (OpResponse<? extends T>) OpNotification.OpNotificationResponse.newInstance();
            default:
                return (OpResponse<? extends T>) newInstance(newRecord(opcode));
            }
        }

        @SuppressWarnings("unchecked")
        public static <T extends Records.ResponseRecord> OpResponse<? extends T> newInstance(T record) {
            switch (record.opcode()) {
            case CREATE_SESSION:
                throw new IllegalArgumentException(record.toString());
            case PING:
                return (OpResponse<? extends T>) OpPing.OpPingResponse.newInstance((IPingResponse)record);
            case NOTIFICATION:
                return (OpResponse<? extends T>) OpNotification.OpNotificationResponse.newInstance((IWatcherEvent)record);
            default:
                return new OpResponse<T>(record);
            }
        }

        protected OpResponse(T record) {
            super(record);
        }
    }

    private final T record;

    protected OpRecord(T record) {
        this.record = checkNotNull(record);
    }

    @Override
    public OpCode opcode() {
        return record.opcode();
    }

    @Override
    public T asRecord() {
        return record;
    }

    @Override
    public ByteBuf encode(ByteBufAllocator output) throws IOException {
        ByteBuf out = checkNotNull(output).buffer();
        Records.encode(asRecord(), new ByteBufOutputStream(out));
        return out;
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
        OpRecord<T> other = (OpRecord<T>) obj;
        return Objects.equal(asRecord(), other.asRecord());
    }
}
