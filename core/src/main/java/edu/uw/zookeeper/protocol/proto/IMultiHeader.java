package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.MultiHeader;

import edu.uw.zookeeper.common.Singleton;

public class IMultiHeader extends IRecord<MultiHeader> implements Records.Header {
    
    public static int NO_TYPE = -1;
    public static int NO_ERR = -1;
    public static int ERR_OK = 0;

    public static IMultiHeader ofRequest(OpCode opcode) {
        switch (opcode) {
        case CREATE:
            return IMultiHeader.Holder.CREATE_REQUEST.get();
        case DELETE:
            return IMultiHeader.Holder.DELETE_REQUEST.get();
        case SET_DATA:
            return IMultiHeader.Holder.SET_DATA_REQUEST.get();
        case CHECK:
            return IMultiHeader.Holder.CHECK_REQUEST.get();
        default:
            throw new IllegalArgumentException(opcode.toString());
        }
    }

    public static IMultiHeader ofResponse(OpCode opcode) {
        switch (opcode) {
        case CREATE:
            return IMultiHeader.Holder.CREATE_RESPONSE.get();
        case DELETE:
            return IMultiHeader.Holder.DELETE_RESPONSE.get();
        case SET_DATA:
            return IMultiHeader.Holder.SET_DATA_RESPONSE.get();
        case CHECK:
            return IMultiHeader.Holder.CHECK_RESPONSE.get();
        default:
            throw new IllegalArgumentException(opcode.toString());
        }
    }
    
    public static IMultiHeader ofError(KeeperException.Code error) {
        return ofError(error.intValue());
    }

    public static IMultiHeader ofError(int error) {
        return new IMultiHeader(OpCode.ERROR.intValue(), false, error);
    }

    public static IMultiHeader ofEnd() {
        return IMultiHeader.Holder.END.get();
    }

    public IMultiHeader() {
        this(new MultiHeader());
    }

    public IMultiHeader(int type) {
        this(new MultiHeader(type, false, NO_ERR));
    }

    public IMultiHeader(int type, boolean done, int err) {
        this(new MultiHeader(type, done, err));
    }

    public IMultiHeader(MultiHeader record) {
        super(record);
    }

    public int getType() {
        return record.getType();
    }

    public boolean getDone() {
        return record.getDone();
    }

    public int getErr() {
        return record.getErr();
    }

    public static enum Holder implements Singleton<IMultiHeader> {
        CREATE_REQUEST(new IMultiHeader(OpCode.CREATE.intValue(), false, NO_ERR)),
        DELETE_REQUEST(new IMultiHeader(OpCode.DELETE.intValue(), false, NO_ERR)),
        SET_DATA_REQUEST(new IMultiHeader(OpCode.SET_DATA.intValue(), false, NO_ERR)),
        CHECK_REQUEST(new IMultiHeader(OpCode.CHECK.intValue(), false, NO_ERR)),
        CREATE_RESPONSE(new IMultiHeader(OpCode.CREATE.intValue(), false,ERR_OK)),
        DELETE_RESPONSE(new IMultiHeader(OpCode.DELETE.intValue(), false, ERR_OK)),
        SET_DATA_RESPONSE(new IMultiHeader(OpCode.SET_DATA.intValue(), false, ERR_OK)),
        CHECK_RESPONSE(new IMultiHeader(OpCode.CHECK.intValue(), false, ERR_OK)),
        END(new IMultiHeader(NO_TYPE, true, NO_ERR));
        
        private final IMultiHeader instance;
        
        private Holder(IMultiHeader instance) {
            this.instance = instance;
        }

        @Override
        public IMultiHeader get() {
            return instance;
        }
    }
}