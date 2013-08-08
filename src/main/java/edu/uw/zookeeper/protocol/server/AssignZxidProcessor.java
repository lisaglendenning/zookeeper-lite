package edu.uw.zookeeper.protocol.server;


import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.protocol.proto.OpCode;

public class AssignZxidProcessor implements
        Processors.UncheckedProcessor<OpCode, Long>,
        Generator<Long> {

    public static AssignZxidProcessor newInstance() {
        return newInstance(ZxidEpochIncrementer.fromZero());
    }

    public static AssignZxidProcessor newInstance(
            Generator<Long> zxid) {
        return new AssignZxidProcessor(zxid);
    }

    private final Generator<Long> zxids;

    private AssignZxidProcessor(Generator<Long> zxids) {
        this.zxids = zxids;
    }

    @Override
    public Long apply(OpCode input) {
        Long zxid;
        switch (input) {
        case NOTIFICATION:
            zxid = -1L;
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
    public Long get() {
        return zxids.get();
    }

    @Override
    public Long next() {
        return zxids.next();
    }
}
