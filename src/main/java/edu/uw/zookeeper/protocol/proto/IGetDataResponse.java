package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetDataResponse;

import edu.uw.zookeeper.protocol.Operation;
@Operational(opcode=OpCode.GET_DATA)
public class IGetDataResponse extends IOperationalRecord<GetDataResponse> implements Operation.Response, Records.DataHolder, Records.StatHolder {
    
    public IGetDataResponse() {
        this(new GetDataResponse());
    }
    
    public IGetDataResponse(byte[] data, Stat stat) {
        this(new GetDataResponse(data, stat));
    }

    public IGetDataResponse(GetDataResponse record) {
        super(record);
    }

    @Override
    public Stat getStat() {
        return get().getStat();
    }

    @Override
    public byte[] getData() {
        return get().getData();
    }
}