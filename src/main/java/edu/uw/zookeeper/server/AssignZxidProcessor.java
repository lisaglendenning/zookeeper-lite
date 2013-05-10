package edu.uw.zookeeper.server;


import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Generator;
import edu.uw.zookeeper.util.Processor;

public class AssignZxidProcessor implements
        Processor<Operation.Reply, Long>,
        Generator<Long> {

    public static AssignZxidProcessor newInstance() {
        return newInstance(ZxidIncrementer.fromZero());
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
    public Long apply(Operation.Reply input) throws Exception {
        // TODO: for now we assume that Error does not get assigned a zxid
        // TODO: double check what real server does
        long zxid = -1;
        if (input instanceof Operation.Response) {
            switch (((Operation.Response)input).opcode()) {
            case PING:
            case AUTH:
            case NOTIFICATION:
            case SET_WATCHES:
                zxid = get();
                break;
            default:
                zxid = next();
                break;
            }
        } else {
            zxid = get();
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
