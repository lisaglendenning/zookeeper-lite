package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.util.SettableTask;

public class RequestTask extends SettableTask<Operation.SessionRequest, Operation.SessionReply> {
    public static RequestTask create(Operation.SessionRequest request) {
        return new RequestTask(request);
    }

    protected RequestTask(Operation.SessionRequest request) {
        super(request);
    }
}
