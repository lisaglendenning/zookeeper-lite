package edu.uw.zookeeper.protocol.proto;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.ParameterizedFactory;


/**
 * Utility functions for ZooKeeper <code>Record</code> objects and types.
 * 
 * @see org.apache.zookeeper.proto
 */
public class Records {
    
    // Retrofits org.apache.zookeeper.proto Record types with some useful interfaces and functionality
    
    public static interface TaggedRecord extends Record {
        void serialize(OutputArchive archive) throws IOException;
        void deserialize(InputArchive archive) throws IOException;
    }
    
    public static interface OperationRecord extends Operation.Action, TaggedRecord {}
    public static interface RequestRecord extends Operation.Request, OperationRecord {}
    public static interface ResponseRecord extends Operation.Response, OperationRecord {}

    public static interface HeaderRecord extends TaggedRecord {}
    public static interface DataRecord extends OperationRecord {}
    public static interface MultiOpRequest extends RequestRecord {}
    public static interface MultiOpResponse extends ResponseRecord {}

    public static interface ConnectRecord extends OperationRecord {
        int getProtocolVersion();
        void setProtocolVersion(int version);
        int getTimeOut();
        void setTimeOut(int timeOut);
        long getSessionId();
        void setSessionId(long sessionId);
        byte[] getPasswd();
        void setPasswd(byte[] passwd);
    }
    
    public static interface PathHolder extends DataRecord {
        String getPath();
        void setPath(String path);
    }
    
    public static interface ChildrenHolder extends DataRecord {
        java.util.List<String> getChildren();
        void setChildren(java.util.List<String> m_);
    }
    
    // Used by ConnectRequest/Response
    public static final int PROTOCOL_VERSION = 0;

    // Used by ConnectRequest/Response
    public static final String CONNECT_TAG = "connect";

    // Used by Notify
    public static final String NOTIFICATION_TAG = "notification";

    // Doesn't seem to be used consistently, but used for encoding frame length
    public static final String LEN_TAG = "len";

    // These are hardcoded in various places in zookeeper code...
    public static enum OpCodeXid implements Operation.XidHeader, Operation.Action {
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

        public static boolean has(int xid) {
            return byXid.containsKey(xid);
        }

        public static final OpCodeXid of(int xid) {
            checkArgument(byXid.containsKey(xid));
            return byXid.get(xid);
        }
        
        private final int xid;
        private final OpCode opcode;

        private OpCodeXid(int xid, OpCode opcode) {
            this.xid = xid;
            this.opcode = opcode;
        }

        @Override
        public int xid() {
            return xid;
        }

        @Override
        public OpCode opcode() {
            return opcode;
        }
    }
    
    @SuppressWarnings("unchecked")
    private static final BiMap<OpCode, Class<? extends RequestRecord>> requestTypes = 
            Maps.unmodifiableBiMap(EnumHashBiMap.create(
                    Maps.<OpCode, Class<? extends RequestRecord>>uniqueIndex(
                            ImmutableList.<Class<? extends RequestRecord>>of(
                                    ICreateRequest.class,
                                    IDeleteRequest.class,
                                    IExistsRequest.class,
                                    IGetDataRequest.class,
                                    ISetDataRequest.class,
                                    IGetACLRequest.class,
                                    ISetACLRequest.class,
                                    IGetChildrenRequest.class,
                                    ISyncRequest.class,
                                    IPingRequest.class,
                                    IGetChildren2Request.class,
                                    ICheckVersionRequest.class,
                                    IAuthRequest.class,
                                    ISetWatchesRequest.class,
                                    IConnectRequest.class,
                                    IDisconnectRequest.class,
                                    IMultiRequest.class),
                            new Function<Class<? extends RequestRecord>, OpCode>() {
                               @Override public OpCode apply(Class<? extends RequestRecord> type) {
                                   try {
                                       return (OpCode)type.getField("OPCODE").get(null);
                                    } catch (Exception e) {
                                        throw new AssertionError(e);
                                    }
                               }
                            })));

    @SuppressWarnings("unchecked")
    private static final BiMap<OpCode, Class<? extends ResponseRecord>> responseTypes = 
            Maps.unmodifiableBiMap(EnumHashBiMap.create(
                    Maps.<OpCode, Class<? extends ResponseRecord>>uniqueIndex(
                            ImmutableList.<Class<? extends ResponseRecord>>of(
                                    IAuthResponse.class,
                                    ICreateResponse.class,
                                    IDeleteResponse.class,
                                    IDisconnectResponse.class,
                                    IExistsResponse.class,
                                    IGetDataResponse.class,
                                    ISetDataResponse.class,
                                    IGetACLResponse.class,
                                    ISetACLResponse.class,
                                    IGetChildrenResponse.class,
                                    ISyncResponse.class,
                                    IGetChildren2Response.class,
                                    IPingResponse.class,
                                    IConnectResponse.class,
                                    IWatcherEvent.class,
                                    ISetWatchesResponse.class,
                                    IErrorResponse.class,
                                    IMultiResponse.class),
                            new Function<Class<? extends ResponseRecord>, OpCode>() {
                                @Override public OpCode apply(Class<? extends ResponseRecord> type) {
                                    try {
                                        return (OpCode)type.getField("OPCODE").get(null);
                                     } catch (Exception e) {
                                         throw new AssertionError(e);
                                     }
                                }
                             })));

    public static class Headers {

        private Headers() {}
        
        // Used for RequestHeader/ReplyHeader
        public static final String TAG = "header";
    }

    public static enum Requests implements ParameterizedFactory<OpCode, RequestRecord> {
        INSTANCE;

        public static Requests getInstance() {
            return INSTANCE;
        }
        
        public static final String TAG = "request";

        public static class Headers {
            public static Class<IRequestHeader> typeOf() {
                return IRequestHeader.class;
            }

            public static IRequestHeader newInstance() {
                return new IRequestHeader();
            }

            public static IRequestHeader newInstance(int xid, int type) {
                return new IRequestHeader(xid, type);
            }

            public static IRequestHeader newInstance(int xid, OpCode op) {
                return newInstance(xid, op.intValue());
            }

            public static IRequestHeader decode(InputStream stream)
                    throws IOException {
                return Records.decode(newInstance(), stream);
            }

            public static OutputStream encode(int xid, int type,
                    OutputStream stream) throws IOException {
                return Records.encode(newInstance(xid, type), stream);
            }

            public static OutputStream encode(int xid, OpCode op,
                    OutputStream stream) throws IOException {
                return Records.encode(newInstance(xid, op), stream);
            }
        }

        public static Class<? extends RequestRecord> typeOf(OpCode opcode) {
            return requestTypes.get(opcode);
        }

        @SuppressWarnings("unchecked")
        public static <T extends RequestRecord> T decode(OpCode op,
                InputStream stream) throws IOException {
            return Records.decode((T) getInstance().get(op), stream);
        }

        @Override
        public RequestRecord get(OpCode opcode) {
            Class<? extends RequestRecord> type = typeOf(opcode);
            if (type == null) {
                throw new IllegalArgumentException(
                        String.format("No type for %s", opcode));
            }
            RequestRecord record = Records.get(type);
            return record;
        }
    }

    public static enum Responses implements ParameterizedFactory<OpCode, ResponseRecord> {
        INSTANCE;
        
        public static Responses getInstance() {
            return INSTANCE;
        }
        
        public static final String TAG = "response";

        public static class Headers {
            public static Class<IReplyHeader> typeOf() {
                return IReplyHeader.class;
            }
        
            public static IReplyHeader newInstance() {
                return new IReplyHeader();
            }
        
            public static IReplyHeader newInstance(int xid, long zxid,
                    KeeperException.Code code) {
                return newInstance(xid, zxid, code.intValue());
            }
        
            public static IReplyHeader newInstance(int xid, long zxid, int err) {
                return new IReplyHeader(xid, zxid, err);
            }
        
            public static IReplyHeader decode(InputStream stream)
                    throws IOException {
                return Records.decode(newInstance(), stream);
            }
        
            public static OutputStream encode(int xid, long zxid, int err,
                    OutputStream stream) throws IOException {
                return Records.encode(newInstance(xid, zxid, err), stream);
            }
        
            public static OutputStream encode(int xid, long zxid,
                    KeeperException.Code code, OutputStream stream)
                    throws IOException {
                return Records.encode(newInstance(xid, zxid, code),
                        stream);
            }
        }

        public static Class<? extends ResponseRecord> typeOf(OpCode opcode) {
            return responseTypes.get(opcode);
        }

        @SuppressWarnings("unchecked")
        public static <T extends ResponseRecord> T decode(OpCode op,
                InputStream stream) throws IOException {
            return Records.decode((T) getInstance().get(op), stream);
        }

        private Responses() {}
        
        @Override
        public ResponseRecord get(OpCode opcode) {
            Class<? extends ResponseRecord> type = typeOf(opcode);
            if (type == null) {
                throw new IllegalArgumentException(
                        String.format("No type for %s", opcode));
            }
            ResponseRecord record = Records.get(type);
            return record;
        }
    }

    public static <T extends TaggedRecord> T decode(T record, InputStream stream)
            throws IOException {
        BinaryInputArchive bis = BinaryInputArchive.getArchive(stream);
        record.deserialize(bis);
        return record;
    }

    public static <T extends TaggedRecord> OutputStream encode(T record,
            OutputStream stream) throws IOException {
        BinaryOutputArchive bos = BinaryOutputArchive.getArchive(stream);
        record.serialize(bos);
        return stream;
    }
    
    public static <T extends Record> T get(Class<? extends T> type) {
        T record;
        if (type.isEnum()) {
            record = type.getEnumConstants()[0];
        } else {
            try {
                record = type.getConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException();
            }
        }
        return record;
    }

    /**
     * More readable String.
     */
    public static String toString(Record record) {
        return Objects.toStringHelper(record)
                .addValue(record.toString().replaceAll("\\s", "")).toString();
    }

    private Records() {}
}
