package edu.uw.zookeeper.data;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;

import edu.uw.zookeeper.util.Pair;

public class OpError extends Pair<Operation.Response, KeeperException.Code>
        implements Operation.Error, Operation.ResponseValue {

    public static OpError create(Operation.Response response,
            KeeperException.Code error) {
        return new OpError(response, error);
    }

    public OpError(Operation.Response response, KeeperException.Code error) {
        super(response, error);
    }

    @Override
    public Operation operation() {
        return response().operation();
    }

    @Override
    public Operation.Response response() {
        return first();
    }

    @Override
    public KeeperException.Code error() {
        return second();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("response", response())
                .add("error", error()).toString();
    }
}
