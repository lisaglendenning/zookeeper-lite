package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetACLResponse;

import edu.uw.zookeeper.protocol.Operation;
@Operational(opcode=OpCode.SET_ACL)
public class ISetACLResponse extends IOperationalRecord<SetACLResponse> implements Operation.Response, Records.StatHolder {

    public ISetACLResponse() {
        this(new SetACLResponse());
    }

    public ISetACLResponse(Stat stat) {
        this(new SetACLResponse(stat));
    }

    public ISetACLResponse(SetACLResponse record) {
        super(record);
    }

    @Override
    public Stat getStat() {
        return get().getStat();
    }
}