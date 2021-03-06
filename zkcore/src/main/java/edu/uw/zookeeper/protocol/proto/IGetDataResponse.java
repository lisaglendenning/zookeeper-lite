package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetDataResponse;

@Operational(value=OpCode.GET_DATA)
public class IGetDataResponse extends IOperationalRecord<GetDataResponse> implements Records.Response, Records.DataGetter, Records.StatGetter {
    
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
        return record.getStat();
    }

    @Override
    public byte[] getData() {
        return record.getData();
    }
}