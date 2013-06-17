package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.ExistsResponse;

import edu.uw.zookeeper.protocol.Operation;
@Operational(opcode=OpCode.EXISTS)
public class IExistsResponse extends IOperationalRecord<ExistsResponse> implements Operation.Response, Records.StatHolder {

    public IExistsResponse() {
        this(new ExistsResponse());
    }
    
    public IExistsResponse(Stat stat) {
        this(new ExistsResponse(stat));
    }

    public IExistsResponse(ExistsResponse record) {
        super(record);
    }

    @Override
    public Stat getStat() {
        return get().getStat();
    }
}