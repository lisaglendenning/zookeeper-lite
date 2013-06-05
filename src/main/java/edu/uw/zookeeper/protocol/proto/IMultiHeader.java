package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.MultiHeader;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records.HeaderRecord;
import edu.uw.zookeeper.protocol.Records.Requests;
import edu.uw.zookeeper.protocol.Records.TaggedRecord;
import edu.uw.zookeeper.util.Singleton;

public class IMultiHeader extends MultiHeader implements TaggedRecord, HeaderRecord {
    
    public static int NO_TYPE = -1;
    public static int NO_ERR = -1;
    public static int ERR_OK = 0;

    public static ImmutableMultiHeader ofRequest(OpCode opcode) {
        switch (opcode) {
        case CREATE:
            return ImmutableMultiHeader.Holder.CREATE_REQUEST.get();
        case DELETE:
            return ImmutableMultiHeader.Holder.DELETE_REQUEST.get();
        case SET_DATA:
            return ImmutableMultiHeader.Holder.SET_DATA_REQUEST.get();
        case CHECK:
            return ImmutableMultiHeader.Holder.CHECK_REQUEST.get();
        default:
            throw new IllegalArgumentException(opcode.toString());
        }
    }

    public static ImmutableMultiHeader ofResponse(OpCode opcode) {
        switch (opcode) {
        case CREATE:
            return ImmutableMultiHeader.Holder.CREATE_RESPONSE.get();
        case DELETE:
            return ImmutableMultiHeader.Holder.DELETE_RESPONSE.get();
        case SET_DATA:
            return ImmutableMultiHeader.Holder.SET_DATA_RESPONSE.get();
        case CHECK:
            return ImmutableMultiHeader.Holder.CHECK_RESPONSE.get();
        default:
            throw new IllegalArgumentException(opcode.toString());
        }
    }
    
    public static ImmutableMultiHeader ofError(KeeperException.Code error) {
        return ofError(error.intValue());
    }

    public static ImmutableMultiHeader ofError(int error) {
        return new ImmutableMultiHeader(OpCode.ERROR.intValue(), false, error);
    }

    public static ImmutableMultiHeader ofEnd() {
        return ImmutableMultiHeader.Holder.END.get();
    }
    
    public IMultiHeader() {
        super();
    }

    public IMultiHeader(int type) {
        this(type, false, NO_ERR);
    }

    public IMultiHeader(int type, boolean done, int err) {
        super(type, done, err);
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Requests.TAG); 
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Requests.TAG);
    }
    
    public static class ImmutableMultiHeader extends IMultiHeader {

        public static enum Holder implements Singleton<ImmutableMultiHeader> {
            CREATE_REQUEST(new ImmutableMultiHeader(OpCode.CREATE.intValue(), false, NO_ERR)),
            DELETE_REQUEST(new ImmutableMultiHeader(OpCode.DELETE.intValue(), false, NO_ERR)),
            SET_DATA_REQUEST(new ImmutableMultiHeader(OpCode.SET_DATA.intValue(), false, NO_ERR)),
            CHECK_REQUEST(new ImmutableMultiHeader(OpCode.CHECK.intValue(), false, NO_ERR)),
            CREATE_RESPONSE(new ImmutableMultiHeader(OpCode.CREATE.intValue(), false,ERR_OK)),
            DELETE_RESPONSE(new ImmutableMultiHeader(OpCode.DELETE.intValue(), false, ERR_OK)),
            SET_DATA_RESPONSE(new ImmutableMultiHeader(OpCode.SET_DATA.intValue(), false, ERR_OK)),
            CHECK_RESPONSE(new ImmutableMultiHeader(OpCode.CHECK.intValue(), false, ERR_OK)),
            END(new ImmutableMultiHeader(NO_TYPE, true, NO_ERR));
            
            private final ImmutableMultiHeader instance;
            
            private Holder(ImmutableMultiHeader instance) {
                this.instance = instance;
            }

            public ImmutableMultiHeader get() {
                return instance;
            }
        }
        
        public ImmutableMultiHeader(int type, boolean done, int err) {
            super(type, done, err);
        }

        public void setType(int m_) {
            throw new UnsupportedOperationException();
        }

        public void setDone(boolean m_) {
            throw new UnsupportedOperationException();
        }

        public void setErr(int m_) {
            throw new UnsupportedOperationException();
        }
    }
}