package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.proto.*;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;


/**
 * Utility functions for ZooKeeper <code>Record</code> objects and types.
 * 
 * @see org.apache.zookeeper.proto
 */
public class Records {

    private Records() {}
    
    // Used by ConnectRequest/Response
    public static final int PROTOCOL_VERSION = 0;

    // Used by ConnectRequest/Response
    public static final String CONNECT_TAG = "connect";

    // Used by Notify
    public static final String NOTIFICATION_TAG = "notification";

    // Doesn't seem to be used consistently, but used for encoding frame length
    public static final String LEN_TAG = "len";

    // These are hardcoded in various places in zookeeper code...
    public static enum OpCodeXid {
        // response only
        NOTIFICATION(-1, OpCode.NOTIFICATION), // zxid is -1?
        // request and response
        PING(-2, OpCode.PING), // zxid is lastZxid
        // request and response
        AUTH(-4, OpCode.AUTH), // zxid is 0
        // request and response
        SET_WATCHES(-8, OpCode.SET_WATCHES); // zxid is lastZxid

        private static ImmutableMap<Integer, OpCodeXid> byXid = Maps
                .uniqueIndex(Iterators.forArray(OpCodeXid.values()), 
                        new Function<OpCodeXid, Integer>() {
                            @Override public Integer apply(OpCodeXid input) {
                                return input.xid();
                            }});

        private final int xid;
        private final OpCode opcode;

        OpCodeXid(int xid, OpCode opcode) {
            this.xid = xid;
            this.opcode = opcode;
        }

        public int xid() {
            return xid;
        }

        public OpCode opcode() {
            return opcode;
        }

        public static boolean has(int xid) {
            return byXid.containsKey(xid);
        }

        public static OpCodeXid of(int xid) {
            checkArgument(byXid.containsKey(xid));
            return byXid.get(xid);
        }
    }
    
    private static final BiMap<OpCode, Class<? extends Record>> requestTypes = 
            Maps.unmodifiableBiMap(EnumHashBiMap.create(new ImmutableMap.Builder<OpCode, Class<? extends Record>>()
            .put(OpCode.CREATE, CreateRequest.class)
            .put(OpCode.DELETE, DeleteRequest.class)
            .put(OpCode.EXISTS, ExistsRequest.class)
            .put(OpCode.GET_DATA, GetDataRequest.class)
            .put(OpCode.SET_DATA, SetDataRequest.class)
            .put(OpCode.GET_ACL, GetACLRequest.class)
            .put(OpCode.SET_ACL, SetACLRequest.class)
            .put(OpCode.GET_CHILDREN, GetChildrenRequest.class)
            .put(OpCode.SYNC, SyncRequest.class)
            .put(OpCode.GET_CHILDREN2, GetChildren2Request.class)
            .put(OpCode.CHECK, CheckVersionRequest.class)
            .put(OpCode.MULTI, MultiTransactionRecord.class)
            .put(OpCode.AUTH, AuthPacket.class)
            .put(OpCode.SET_WATCHES, SetWatches.class)
            .put(OpCode.CREATE_SESSION, ConnectRequest.class)
            .build()));

    private static final BiMap<OpCode, Class<? extends Record>> responseTypes = 
            Maps.unmodifiableBiMap(EnumHashBiMap.create(new ImmutableMap.Builder<OpCode, Class<? extends Record>>()
            .put(OpCode.CREATE, CreateResponse.class)
            .put(OpCode.EXISTS, ExistsResponse.class)
            .put(OpCode.GET_DATA, GetDataResponse.class)
            .put(OpCode.SET_DATA, SetDataResponse.class)
            .put(OpCode.GET_ACL, GetACLResponse.class)
            .put(OpCode.SET_ACL, SetACLResponse.class)
            .put(OpCode.GET_CHILDREN, GetChildrenResponse.class)
            .put(OpCode.SYNC, SyncResponse.class)
            .put(OpCode.GET_CHILDREN2, GetChildren2Response.class)
            .put(OpCode.MULTI, MultiResponse.class)
            .put(OpCode.CREATE_SESSION, ConnectResponse.class)
            .put(OpCode.NOTIFICATION, WatcherEvent.class)
            .build()));

    public static class Headers {

        private Headers() {}
        
        // Used for RequestHeader/ReplyHeader
        public static final String TAG = "header";

        public static boolean contains(Class<? extends Record> recordType) {
            return Requests.Headers.contains(recordType)
                    || Responses.Headers.contains(recordType);
        }

        public static RequestHeader decode(RequestHeader header,
                InputStream stream) throws IOException {
            return Records.decode(header, stream, TAG);
        }

        public static ReplyHeader decode(ReplyHeader header,
                InputStream stream) throws IOException {
            return Records.decode(header, stream, TAG);
        }

        public static OutputStream encode(RequestHeader header,
                OutputStream stream) throws IOException {
            return Records.encode(header, stream, TAG);
        }

        public static OutputStream encode(ReplyHeader header,
                OutputStream stream) throws IOException {
            return Records.encode(header, stream, TAG);
        }
    }

    public static class Requests {
        private Requests() {}
        
        public static final String TAG = "request";

        public static class Headers {
            public static Class<RequestHeader> getType() {
                return RequestHeader.class;
            }

            public static boolean contains(Class<? extends Record> recordType) {
                return getType().isAssignableFrom(recordType);
            }

            public static RequestHeader create() {
                return new RequestHeader();
            }

            public static RequestHeader create(int xid, int type) {
                return new RequestHeader(xid, type);
            }

            public static RequestHeader create(int xid, OpCode op) {
                return create(xid, op.intValue());
            }

            public static RequestHeader decode(InputStream stream)
                    throws IOException {
                return Records.decode(create(), stream, TAG);
            }

            public static OutputStream encode(int xid, int type,
                    OutputStream stream) throws IOException {
                return Records.Headers.encode(create(xid, type), stream);
            }

            public static OutputStream encode(int xid, OpCode op,
                    OutputStream stream) throws IOException {
                return Records.Headers.encode(create(xid, op), stream);
            }
        }

        public static Class<? extends Record> getType(OpCode op) {
            return requestTypes.get(op);
        }

        public static OpCode getOpCode(Record record) {
            return getOpCode(record.getClass());
        }

        public static OpCode getOpCode(
                Class<? extends Record> recordType) {
            Map<Class<? extends Record>, OpCode> typeMap = requestTypes
                    .inverse();
            OpCode opcode = typeMap.get(recordType);
            return opcode;
        }

        public static boolean contains(Class<? extends Record> recordType) {
            Map<Class<? extends Record>, OpCode> typeMap = requestTypes
                    .inverse();
            return typeMap.containsKey(recordType);
        }

        @SuppressWarnings("unchecked")
        public static <T extends Record> T create(OpCode op) {
            Class<? extends Record> recordType = getType(op);
            if (recordType == null) {
                throw new IllegalArgumentException(
                        String.format("No Records.Requests type for %s", op));
            }
            T record;
            try {
                record = (T) recordType.getConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException();
            }
            return record;
        }

        public static <T extends Record> T decode(T request,
                InputStream stream) throws IOException {
            return Records.decode(request, stream, TAG);
        }

        @SuppressWarnings("unchecked")
        public static <T extends Record> T decode(OpCode op,
                InputStream stream) throws IOException {
            return decode((T) create(op), stream);
        }

        public static OutputStream encode(Record request, OutputStream stream)
                throws IOException {
            return Records.encode(request, stream, TAG);
        }
    }

    public static class Responses {
        private Responses() {}
        
        public static final String TAG = "response";

        public static class Headers {
            public static Class<ReplyHeader> getType() {
                return ReplyHeader.class;
            }

            public static boolean contains(Class<? extends Record> recordType) {
                return getType().isAssignableFrom(recordType);
            }

            public static ReplyHeader create() {
                return new ReplyHeader();
            }

            public static ReplyHeader create(int xid, long zxid, int err) {
                return new ReplyHeader(xid, zxid, err);
            }

            public static ReplyHeader create(int xid, long zxid,
                    KeeperException.Code code) {
                return create(xid, zxid, code.intValue());
            }

            public static ReplyHeader decode(InputStream stream)
                    throws IOException {
                return Records.Headers.decode(create(), stream);
            }

            public static OutputStream encode(int xid, long zxid, int err,
                    OutputStream stream) throws IOException {
                return Records.Headers
                        .encode(create(xid, zxid, err), stream);
            }

            public static OutputStream encode(int xid, long zxid,
                    KeeperException.Code code, OutputStream stream)
                    throws IOException {
                return Records.Headers.encode(create(xid, zxid, code),
                        stream);
            }
        }

        public static Class<? extends Record> getType(OpCode op) {
            return responseTypes.get(op);
        }

        public static OpCode getOpCode(Record record) {
            return getOpCode(record.getClass());
        }

        public static OpCode getOpCode(
                Class<? extends Record> recordType) {
            Map<Class<? extends Record>, OpCode> typeMap = responseTypes
                    .inverse();
            OpCode opcode = typeMap.get(recordType);
            return opcode;
        }

        public static boolean contains(Class<? extends Record> recordType) {
            Map<Class<? extends Record>, OpCode> typeMap = responseTypes
                    .inverse();
            return typeMap.containsKey(recordType);
        }

        @SuppressWarnings("unchecked")
        public static <T extends Record> T create(OpCode op) {
            Class<? extends Record> recordType = getType(op);
            if (recordType == null) {
                throw new IllegalArgumentException(
                        String.format("No Records.Responses type for %s", op));
            }
            T record;
            try {
                record = (T) recordType.getConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException();
            }
            return record;
        }

        public static <T extends Record> T decode(T response,
                InputStream stream) throws IOException {
            return Records.decode(response, stream, TAG);
        }

        @SuppressWarnings("unchecked")
        public static <T extends Record> T decode(OpCode op,
                InputStream stream) throws IOException {
            return decode((T) create(op), stream);
        }

        public static OutputStream encode(Record response,
                OutputStream stream) throws IOException {
            return Records.encode(response, stream, TAG);
        }
    }

    public static OpCode getOpCode(Record record) {
        return getOpCode(record.getClass());
    }

    public static OpCode getOpCode(Class<? extends Record> recordType) {
        OpCode opcode = Requests.getOpCode(recordType);
        if (opcode == null) {
            opcode = Responses.getOpCode(recordType);
        }
        return opcode;
    }

    public static String getTag(Record record) {
        return getTag(record.getClass());
    }

    public static String getTag(Class<? extends Record> recordType) {
        String tag = null;
        if (Headers.contains(recordType)) {
            tag = Headers.TAG;
        } else if ((ConnectRequest.class.isAssignableFrom(recordType))
                || (ConnectResponse.class.isAssignableFrom(recordType))) {
            tag = CONNECT_TAG;
        } else if (Requests.contains(recordType)) {
            tag = Requests.TAG;
        } else if (Responses.contains(recordType)) {
            tag = Responses.TAG;
        }
        return tag;
    }

    public static <T extends Record> T decode(T record, InputStream stream)
            throws IOException {
        return decode(record, stream, getTag(record));
    }

    public static <T extends Record> T decode(T record,
            InputStream stream, String tag) throws IOException {
        BinaryInputArchive bia = BinaryInputArchive.getArchive(stream);
        record.deserialize(bia, tag);
        return record;
    }

    public static <T extends Record> OutputStream encode(T record,
            OutputStream stream) throws IOException {
        return encode(record, stream, getTag(record));
    }

    public static <T extends Record> OutputStream encode(T record,
            OutputStream stream, String tag) throws IOException {
        BinaryOutputArchive bos = BinaryOutputArchive.getArchive(stream);
        record.serialize(bos, tag);
        return stream;
    }

    /**
     * More readable String.
     */
    public static String toString(Record record) {
        return Objects.toStringHelper(record)
                .addValue(record.toString().replaceAll("\\s", "")).toString();
    }
}
