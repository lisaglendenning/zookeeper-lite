package edu.uw.zookeeper.protocol;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Operation.SessionReply;
import edu.uw.zookeeper.protocol.Operation.SessionRequest;
import edu.uw.zookeeper.util.AbstractPair;

public class OpSessionResult extends AbstractPair<Operation.SessionRequest, Operation.SessionReply> implements Operation.SessionResult {

    public static OpSessionResult of(Operation.SessionRequest request, Operation.SessionReply reply) {
        return new OpSessionResult(request, reply);
    }
    
    protected OpSessionResult(Operation.SessionRequest request, Operation.SessionReply reply) {
        super(request, reply);
    }

    @Override
    public SessionRequest request() {
        return first;
    }

    @Override
    public SessionReply reply() {
        return second;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("request", request()).add("reply", reply()).toString();
    }
}
