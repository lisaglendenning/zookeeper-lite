package edu.uw.zookeeper.protocol.server;


import com.google.common.base.Optional;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionReplyWrapper;
import edu.uw.zookeeper.util.Generator;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;

public class AssignZxidProcessor implements
        Processor<Pair<Optional<Operation.Request>, Operation.Reply>, Operation.SessionReply>,
        Generator<Long> {

    public static AssignZxidProcessor newInstance(
            Generator<Long> zxid) {
        return new AssignZxidProcessor(zxid);
    }

    private final Generator<Long> zxids;

    private AssignZxidProcessor(Generator<Long> zxids) {
        this.zxids = zxids;
    }

    @Override
    public Operation.SessionReply apply(Pair<Optional<Operation.Request>, Operation.Reply> input) throws Exception {
        Operation.Reply reply = input.second();
        if (reply instanceof OpCreateSession.Response) {
            return null;
        }
        
        Optional<Operation.Request> request = input.first();
        int xid;
        if (reply instanceof Operation.XidHeader) {
            xid = ((Operation.XidHeader)reply).xid();
        } else {
            if (request.isPresent() && request.get() instanceof Operation.XidHeader) {
                xid = ((Operation.XidHeader)request.get()).xid();
            } else {
                throw new IllegalArgumentException();
            }
        }
        
        // TODO: for now we assume that Error does not get assigned a zxid
        long zxid = get();
        if (reply instanceof Operation.Response) {
            switch (((Operation.Response)reply).opcode()) {
            case CREATE_SESSION:
                throw new AssertionError();
            case PING:
            case AUTH:
            case NOTIFICATION:
            case SET_WATCHES:
                break;
            default:
                zxid = next();
                break;
            }
        }
        
        return SessionReplyWrapper.create(xid, zxid, reply);
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
