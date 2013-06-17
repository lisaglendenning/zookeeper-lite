package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetDataResponse;

import edu.uw.zookeeper.protocol.Operation;
@Operational(opcode=OpCode.SET_DATA)
public class ISetDataResponse extends IOperationalRecord<SetDataResponse> implements Operation.Response, Records.StatHolder, Records.MultiOpResponse {

    public ISetDataResponse() {
        this(new SetDataResponse());
    }
    
    public ISetDataResponse(Stat stat) {
        this(new SetDataResponse(stat));
    }

    public ISetDataResponse(SetDataResponse record) {
        super(record);
    }

    @Override
    public Stat getStat() {
        return get().getStat();
    }
}