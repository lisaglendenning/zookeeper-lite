package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.ReconfigRequest;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.RECONFIG)
public class IReconfigRequest extends IOperationalRecord<ReconfigRequest> implements Operation.Request {

    public IReconfigRequest() {
        this(new ReconfigRequest());
    }

    public IReconfigRequest(String joiningServers, String leavingServers,
            String newMembers, long curConfigId) {
        this(new ReconfigRequest(joiningServers, leavingServers, newMembers, curConfigId));
    }

    public IReconfigRequest(ReconfigRequest record) {
        super(record);
    }
}
