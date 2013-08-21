package edu.uw.zookeeper.protocol.server;


import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

public class AssignZxidProcessor implements
        Processors.UncheckedProcessor<OpCode, Long>,
        ZxidGenerator {

    public static AssignZxidProcessor newInstance() {
        return newInstance(ZxidEpochIncrementer.fromZero());
    }

    public static AssignZxidProcessor newInstance(
            ZxidGenerator zxid) {
        return new AssignZxidProcessor(zxid);
    }

    private final ZxidGenerator zxids;

    private AssignZxidProcessor(ZxidGenerator zxids) {
        this.zxids = zxids;
    }

    @Override
    public Long apply(OpCode input) {
        Long zxid;
        switch (input) {
        case NOTIFICATION:
            zxid = OpCodeXid.NOTIFICATION_ZXID;
            break;
        case CREATE:
        case CREATE2:
        case DELETE:
        case RECONFIG:
        case SET_DATA:
        case SET_ACL:
        case CHECK:
        case MULTI:
        case CREATE_SESSION:
        case CLOSE_SESSION:
        case ERROR:
            zxid = next();
            break;
        default:
            zxid = get();
            break;
        }
        return zxid;
    }

    @Override
    public long get() {
        return zxids.get();
    }

    @Override
    public long next() {
        return zxids.next();
    }
}
