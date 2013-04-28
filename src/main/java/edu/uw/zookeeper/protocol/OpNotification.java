package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.InputStream;

import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.WatchedEvent;

import com.google.common.base.Objects;


public abstract class OpNotification {

    public static class Response implements Operation.Response, Operation.RecordHolder<WatcherEvent>, Encodable, Operation.XidHeader {
        public static OpNotification.Response create() {
            return create(createRecord());
        }

        public static OpNotification.Response create(WatcherEvent record) {
            return create(new WatchedEvent(record));
        }

        public static OpNotification.Response create(WatchedEvent response) {
            return new OpNotification.Response(response);
        }

        public static WatcherEvent createRecord() {
            return Records.Responses.<WatcherEvent>create(opCodeXid().opcode());
        }

        public static OpNotification.Response decode(InputStream input) throws IOException {
            WatcherEvent record = Records.Responses.decode(opCodeXid().opcode(), input);
            return create(record);
        }

        private final WatchedEvent event;

        private Response(WatchedEvent event) {
            this.event = event;
        }

        public WatchedEvent event() {
            return event;
        }
        
        @Override
        public WatcherEvent asRecord() {
            return event().getWrapper();
        }

        @Override
        public OpCode opcode() {
            return opCodeXid().opcode();
        }

        @Override
        public int xid() {
            return opCodeXid().xid();
        }

        @Override
        public ByteBuf encode(ByteBufAllocator output) throws IOException {
            ByteBuf out = output.buffer();
            Records.Responses.encode(asRecord(), new ByteBufOutputStream(out));
            return out;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("event", event()).toString();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(event());
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
            OpNotification.Response other = (OpNotification.Response) obj;
            return Objects.equal(event(), other.event());
        }

    }

    public static Records.OpCodeXid opCodeXid() {
        return Records.OpCodeXid.NOTIFICATION;
    }

    private OpNotification() {}
}
